"""The DICOMweb WSGI application.

A bottle application assembled from the three PS3.18 service modules —
QIDO-RS (:mod:`~tiny_pacs_dicomweb.qido`), WADO-RS/WADO-URI
(:mod:`~tiny_pacs_dicomweb.wado`) and STOW-RS
(:mod:`~tiny_pacs_dicomweb.stow`) — contributed to the core shared
``HttpServer`` as a plain WSGI callable. Shared constants, the
application state and the small response/query helpers live in
:mod:`~tiny_pacs_dicomweb.common`.

Security properties enforced here (see the proposal checklist):

* authentication runs in a WSGI middleware
  (:class:`~tiny_pacs_dicomweb.auth.AuthMiddleware`); failures answer
  401 with an empty body — no details about why a credential was
  refused;
* the request log records method, path/status and duration only — never
  query strings, never request bodies; headers are logged at DEBUG with
  ``Authorization`` stripped;
* responses carry ``X-Content-Type-Options: nosniff``;
* error responses are generic texts; bottle's debug mode is forced off,
  so tracebacks never reach the client;
* retrieval responses stream: the middleware never buffers a body, so a
  large WADO-RS multipart response does not materialize in memory.
"""
import logging
import time
from collections.abc import Callable, Iterable
from typing import Any

import bottle  # type: ignore[import-untyped]

from . import auth, common, qido, stow, wado
from .common import ERROR_MESSAGES, plain

#: Response headers added to every response by the middleware
SECURITY_HEADERS: list[tuple[str, str]] = [
    ('X-Content-Type-Options', 'nosniff'),
]


class _ResponseMiddleware:
    """WSGI middleware adding security headers, request logging and a
    last-resort generic 500 handler.

    Streams the wrapped body through untouched (no buffering), so large
    retrieval responses do not materialize in memory. The log line
    carries method, path and status only — never query strings, headers
    or bodies; headers are logged separately at DEBUG with
    ``Authorization`` stripped.
    """

    def __init__(self, app: Any, state: common.AppState) -> None:
        """Initializes the middleware.

        :param app: wrapped WSGI callable (the authentication middleware)
        :param state: shared application state
        """
        self.app = app
        self.state = state

    def __call__(
            self,
            environ: dict[str, Any],
            start_response: Callable[..., Any]
    ) -> Iterable[bytes]:
        """Serves one request with logging and guaranteed headers."""
        method = environ.get('REQUEST_METHOD', '-')
        path = environ.get('PATH_INFO', '-')
        if self.state.logger.isEnabledFor(logging.DEBUG):
            headers = {
                key[5:].replace('_', '-').lower(): value
                for key, value in environ.items()
                if key.startswith('HTTP_') and key != 'HTTP_AUTHORIZATION'
            }
            content_type = environ.get('CONTENT_TYPE')
            if content_type is not None:
                headers['content-type'] = content_type
            # The path is logged with %r: waitress percent-decodes the
            # request target, so a raw %s would let CR/LF forge log lines
            self.state.logger.debug(
                '%s %r headers: %r', method, path, headers
            )
        started = time.monotonic()
        status_box: list[str] = []

        def wrapped_start_response(
                status: str,
                headers: list[tuple[str, str]],
                exc_info: Any = None
        ) -> Any:
            status_box.append(status)
            return start_response(
                status, [*headers, *SECURITY_HEADERS], exc_info
            )

        try:
            result = self.app(environ, wrapped_start_response)
        except Exception:
            self.state.logger.exception(
                'Unhandled error serving %s %r', method, path
            )
            if status_box:
                raise
            status_box.append('500 Internal Server Error')
            start_response(
                status_box[0],
                [('Content-Type', 'text/plain; charset=utf-8'),
                 *SECURITY_HEADERS]
            )
            return [b'The DICOMweb service failed to handle the request.\n']

        def elapsed() -> float:
            return (time.monotonic() - started) * 1000.0

        return _LoggedIterable(
            result, self.state.logger, method, path, status_box, elapsed
        )


class _LoggedIterable:
    """Streams a response body through, logging once it is complete."""

    def __init__(
            self,
            iterable: Iterable[bytes],
            logger: logging.Logger,
            method: str,
            path: str,
            status_box: list[str],
            elapsed: Callable[[], float]
    ) -> None:
        """Wraps a WSGI response iterable.

        :param iterable: response iterable returned by the wrapped app
        :param logger: logger receiving the request log line
        :param method: request method
        :param path: request path (no query string)
        :param status_box: mutable response status holder
        :param elapsed: returns the milliseconds elapsed since the
                        request started
        """
        self._iterable = iterable
        self._iterator = iter(iterable)
        self._logger = logger
        self._method = method
        self._path = path
        self._status_box = status_box
        self._elapsed = elapsed
        self._logged = False

    def __iter__(self) -> '_LoggedIterable':
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._iterator)
        except StopIteration:
            self._log()
            raise

    def close(self) -> None:
        """Closes the wrapped iterable and logs the request."""
        close = getattr(self._iterable, 'close', None)
        if callable(close):
            close()
        self._log()

    def _log(self) -> None:
        if self._logged:
            return
        self._logged = True
        self._logger.info(
            '%s %r -> %s (%.1f ms)', self._method, self._path,
            self._status_box[0] if self._status_box else '-',
            self._elapsed()
        )


def build_app(state: common.AppState) -> Any:
    """Assembles the DICOMweb WSGI application.

    The three service modules register their routes on one bottle app;
    debug mode is forced off so tracebacks never reach a client, and
    every route answers errors with generic plain texts.

    :param state: shared application state
    :type state: common.AppState
    :return: WSGI callable contributed to the core shared HTTP server
    """
    bottle.DEBUG = False
    app = bottle.Bottle()
    app.catchall = True
    _install_error_pages(app)
    qido.register(state, app)
    wado.register(state, app)
    stow.register(state, app)
    return _ResponseMiddleware(auth.AuthMiddleware(app, state), state)


def _install_error_pages(app: Any) -> None:
    """Registers the generic plain-text error handler on one app."""
    def handler(error: Any) -> Any:
        status = int(getattr(error, 'status_code', 500) or 500)
        body = getattr(error, 'body', None)
        text = body if isinstance(body, str) and body \
            else ERROR_MESSAGES.get(status, 'Request failed.')
        return plain(status, text)

    for code in ERROR_MESSAGES:
        app.error(code)(handler)


__all__ = ['build_app']
