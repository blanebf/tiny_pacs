"""Shared constants and helpers of the DICOMweb application modules.

Holds the application-wide state (:class:`AppState`), the generic error
texts and the small response/query helpers used by the three PS3.18
service modules and the WSGI middleware, so the service modules never
import :mod:`~tiny_pacs_dicomweb.web` (which assembles them).
"""
import logging
from dataclasses import dataclass
from typing import Any, NoReturn

import bottle  # type: ignore[import-untyped]
import trolleybus
from tiny_pacs import events

#: Media type of DICOM JSON (PS3.18 F) responses
JSON_CT = 'application/dicom+json'

#: Plain alias accepted in ``Accept``/``Content-Type`` for DICOM JSON
JSON_CT_ALIAS = 'application/json'

#: Size of the retrieval stream chunks (WADO-RS/WADO-URI)
CHUNK_SIZE = 512 * 1024

#: Default page size of QIDO-RS queries without a ``limit`` parameter
DEFAULT_QUERY_LIMIT = 1000

#: Upper bound of the QIDO-RS ``limit`` parameter
MAX_QUERY_LIMIT = 10_000

#: Defensive cap on the QIDO-RS ``offset`` parameter: an unbounded offset
#: would reach the SQL OFFSET clause and force full row-skip scans
MAX_QUERY_OFFSET = 100_000

#: Upper bound of archive queries resolving a WADO retrieval target:
#: a single retrieval must stay bounded even for absurdly wide studies
MAX_RETRIEVE_INSTANCES = 100_000

#: Generic error message by HTTP status code. 401 has no entry on
#: purpose: refused authentication is answered by the AuthMiddleware
#: (an empty body plus the WWW-Authenticate challenge) before bottle is
#: reached, so no route ever aborts with 401.
ERROR_MESSAGES: dict[int, str] = {
    400: 'The request could not be understood.',
    404: 'No matching object was found.',
    405: 'This HTTP method is not allowed here.',
    406: 'The requested response format cannot be produced.',
    411: 'Content-Length is required.',
    413: 'The request is too large.',
    415: 'The request media type is not supported.',
    500: 'The DICOMweb service failed to handle the request.',
    503: 'The requested DICOMweb feature is not available.',
}


@dataclass
class AppState:
    """Shared state of the DICOMweb application.

    Built by the :class:`~tiny_pacs_dicomweb.component.DICOMWeb`
    component from validated configuration values; the service modules
    never see the config model, so there is no import cycle between the
    component and the application.

    :ivar bus: the live event bus every archive interaction travels on
    :ivar logger: component logger used for request logging and warnings
    :ivar auth: authentication mode: ``none``, ``basic`` or ``token``
    :ivar tokens: accepted bearer tokens for ``auth: token``
    :ivar max_part_size: STOW-RS request body size cap
    """

    bus: trolleybus.EventBus
    logger: logging.Logger
    auth: str = 'none'
    tokens: tuple[str, ...] = ()
    max_part_size: int = 64 * 1024 * 1024

    def has_feature(self, event: type[trolleybus.Event[Any, Any]]) -> bool:
        """Whether the listeners of one core event are present on the bus.

        Evaluated per request so an installed-but-disabled backend is
        reflected without a restart.

        :param event: the core event backing a feature
        :type event: type[trolleybus.Event]
        :return: True when at least one listener answers the event
        :rtype: bool
        """
        return self.bus.has_listeners(event)


def plain(status: int, text: str) -> bottle.HTTPResponse:
    """Builds a plain-text response with an explicit status code.

    :param status: HTTP status code
    :type status: int
    :param text: response body text
    :type text: str
    :return: the response to raise or return
    :rtype: bottle.HTTPResponse
    """
    return bottle.HTTPResponse(
        body=text + '\n', status=status,
        headers={'Content-Type': 'text/plain; charset=utf-8'}
    )


def abort(status: int, message: str | None = None) -> NoReturn:
    """Aborts the request with a generic plain-text error.

    :param status: HTTP status code
    :type status: int
    :param message: optional body text; defaults to the generic message
                    of the status code
    :type message: str or None
    :raises bottle.HTTPError: always
    """
    raise bottle.HTTPError(
        status=status, body=message or ERROR_MESSAGES.get(status)
    )


def accepts_json(accept: str) -> bool:
    """Whether an ``Accept`` header permits a DICOM JSON response.

    Accepts the DICOM JSON media type, its plain ``application/json``
    alias and wildcard requests; anything else (e.g. the unsupported
    DICOM XML encoding) is refused so the service answers 406 instead of
    silently ignoring the client's preference.

    :param accept: raw ``Accept`` header value
    :type accept: str
    :return: True when a DICOM JSON response satisfies the request
    :rtype: bool
    """
    if not accept or not accept.strip():
        return True
    acceptable = {
        part.split(';')[0].strip().lower()
        for part in accept.split(',') if part.strip()
    }
    return bool(acceptable & {'*/*', 'application/*', JSON_CT,
                              JSON_CT_ALIAS})


def base_url(environ: dict[str, Any]) -> str:
    """Builds the absolute base URL of the DICOMweb service.

    Honours ``X-Forwarded-Proto``/``X-Forwarded-Host`` of a reverse
    proxy when present, and the ``SCRIPT_NAME`` set by the core prefix
    dispatcher, so STOW-RS retrieve URLs point at the externally
    visible service root.

    :param environ: WSGI environment of the current request
    :type environ: dict
    :return: absolute service base URL without a trailing slash
    :rtype: str
    """
    scheme = str(environ.get('HTTP_X_FORWARDED_PROTO')
                 or environ.get('wsgi.url_scheme') or 'http')
    host = str(environ.get('HTTP_X_FORWARDED_HOST')
               or environ.get('HTTP_HOST')
               or '{}:{}'.format(environ.get('SERVER_NAME', 'localhost'),
                                 environ.get('SERVER_PORT', '80')))
    script = str(environ.get('SCRIPT_NAME') or '')
    return f'{scheme}://{host}{script.rstrip("/")}'


def require(state: AppState, event: type[trolleybus.Event[Any, Any]],
            feature: str) -> None:
    """Refuses the request when a feature has no listeners on the bus.

    :param state: shared application state
    :type state: AppState
    :param event: the core event backing the feature
    :type event: type[trolleybus.Event]
    :param feature: human-readable feature name for the error text
    :type feature: str
    :raises bottle.HTTPError: status 503 when no listener answers the
                              event
    """
    if not state.has_feature(event):
        abort(
            503,
            f'The "{feature}" feature is not available: no component '
            f'answers {event.__name__}. Enable the serving component '
            f'(see the tutorial) and restart the server.'
        )


def audit(state: AppState, event: str, username: str | None,
          details: dict[str, Any] | None = None,
          status: str = 'success') -> None:
    """Emits an :class:`~tiny_pacs.events.AuditRecord` fire-and-forget.

    Association-level audit does not cover HTTP, so the service-layer
    actions (STOW pushes, QIDO queries) announce themselves through the
    audit event; an installed audit component records them, and the
    absence of one is not an error.

    :param state: shared application state
    :type state: AppState
    :param event: audit event name, e.g. ``stow`` or ``qido``
    :type event: str
    :param username: authenticated user when known
    :type username: str or None
    :param details: additional JSON-serializable information (no PHI,
                    no secrets)
    :type details: dict or None
    :param status: ``success`` or ``failure``
    :type status: str
    """
    payload = events.AuditRecordPayload(
        category='service', event=event, username=username,
        status=status, details=details or {}
    )
    state.bus.broadcast_nothrow(events.AuditRecord, payload)


def int_param(raw: str | None, default: int, low: int, high: int,
              name: str) -> int:
    """Parses and bounds one integer query parameter.

    :param raw: raw parameter value, None when absent
    :type raw: str or None
    :param default: value used when the parameter is absent or empty
    :type default: int
    :param low: smallest accepted value
    :type low: int
    :param high: largest accepted value
    :type high: int
    :param name: parameter name used in the error message
    :type name: str
    :return: the parsed value bounded to ``[low, high]``
    :rtype: int
    :raises bottle.HTTPError: status 400 when the value is not an
                              integer
    """
    text = (raw or '').strip()
    if not text:
        return default
    try:
        value = int(text)
    except ValueError:
        abort(400, f'The "{name}" parameter must be an integer.')
    return max(low, min(high, value))
