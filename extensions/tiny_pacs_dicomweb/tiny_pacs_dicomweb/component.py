"""DICOMweb component.

The :class:`DICOMWeb` component contributes the DICOMweb (PS3.18)
services to the core shared HTTP server
(:class:`tiny_pacs.http.HttpServer`): it answers
:class:`~tiny_pacs.events.HttpAppsRegistry` with its WSGI application
mounted at the configured ``prefix``. It runs no server of its own —
binding, the waitress worker pool and every bind-level policy (loopback
validation, port conflicts, the headless guard) belong to the core
component. For this extension, "disabled" means "no hook registered".

The registry handler builds the application from the validated config on
the spot — the same discipline ``ServicesRegistry`` providers follow — so
no component-startup ordering is assumed. Self-check failures mean "no
hook is registered", which never affects the shared server or the other
mounts:

* ``auth: basic`` without a :class:`~tiny_pacs.events.UserVerify`
  listener (no user registry installed) → basic authentication would be
  impossible, so the component warns and contributes no app (fail
  closed);
* ``TINY_PACS_HEADLESS`` set in the environment → headless runs never
  build an app.
"""
from typing import Any, Literal

import pydantic
import trolleybus
from tiny_pacs import component, events
from tiny_pacs.http import HEADLESS_ENV, is_headless, normalize_prefix


class DICOMWebConfig(component.ComponentConfig):
    """Configuration of the :class:`DICOMWeb` component.

    The HTTP transport (bind address, port, worker pool) is configured
    once on the core :class:`~tiny_pacs.http.HttpServer` component, which
    serves every installed HTTP front-end; only DICOMweb application-level
    settings live here.

    :ivar prefix: mount prefix on the shared HTTP server; must start with
                  a slash and must not be ``/`` (the root belongs to the
                  catch-all front-end, e.g. the administration console)
    :ivar auth: authentication mode: ``none`` (default — run behind an
                authenticating reverse proxy), ``basic`` (HTTP basic via
                the core ``UserVerify`` event) or ``token`` (static
                bearer tokens)
    :ivar tokens: accepted bearer tokens for ``auth: token``; required
                  to be non-empty in that mode. Use TLS or a local
                  reverse proxy — tokens travel in clear text otherwise
    :ivar max_part_size: upper bound in bytes for the total STOW-RS
                         request body (and thereby for every single
                         multipart part); pushes beyond it are refused
                         with 413
    """

    prefix: str = '/dicomweb'
    auth: Literal['none', 'basic', 'token'] = 'none'
    tokens: list[str] = pydantic.Field(default_factory=list)
    max_part_size: int = pydantic.Field(
        default=64 * 1024 * 1024, ge=1024
    )

    @pydantic.model_validator(mode='after')
    def _check_prefix(self) -> 'DICOMWebConfig':
        normalized = normalize_prefix(self.prefix)
        if normalized is None or normalized == '/':
            raise ValueError(
                f'DICOMWeb prefix {self.prefix!r} is unusable: it must '
                f'start with a slash and must not be the root prefix \'/\''
            )
        self.prefix = normalized
        return self

    @pydantic.model_validator(mode='after')
    def _check_tokens(self) -> 'DICOMWebConfig':
        if self.auth == 'token':
            if not self.tokens or any(
                    not token.strip() or not token.isascii()
                    for token in self.tokens):
                raise ValueError(
                    'auth: token requires at least one non-empty ASCII '
                    'entry in "tokens"'
                )
        return self


class DICOMWeb(component.Component[DICOMWebConfig]):
    """Component that contributes the DICOMweb services.

    Handles the following events:

        * :class:`~tiny_pacs.events.HttpAppsRegistry`

    The application itself consumes core events only: the archive query
    events (QIDO-RS, WADO object resolution),
    :class:`~tiny_pacs.events.GetFiles` (WADO-RS/WADO-URI retrieval),
    :class:`~tiny_pacs.events.GetFile` +
    :class:`~tiny_pacs.events.StoreDataset` (STOW-RS storage),
    :class:`~tiny_pacs.events.UserVerify` (``auth: basic``) and
    :class:`~tiny_pacs.events.AuditRecord` (fire-and-forget audit
    emission); whichever extension provides a listener is an installation
    concern.
    """

    config_model = DICOMWebConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: DICOMWebConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: DICOMWebConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.HttpAppsRegistry, self.http_apps)

    def http_apps(self, _: None = None) -> list[events.HttpAppHook]:
        """Handles `HttpAppsRegistry`: contributes the DICOMweb app.

        Performs the self-checks and builds the WSGI application from the
        validated configuration on the spot. Every refusal path logs and
        returns an empty list — the shared server and the other mounts
        are unaffected.

        :return: the DICOMweb hook (configured prefix) or no hook at all
        :rtype: list[events.HttpAppHook]
        """
        if is_headless():
            self.log_info(
                '%s is set; DICOMweb registers no app in this headless '
                'run', HEADLESS_ENV
            )
            return []
        if (self.config.auth == 'basic'
                and not self.bus.has_listeners(events.UserVerify)):
            self.log_warning(
                'No component answers UserVerify (no user registry '
                'installed): DICOMweb basic authentication would fail '
                'closed for every request, so no DICOMweb app is mounted '
                'on the shared HTTP server. Install and enable a user '
                'registry (e.g. the Users component of '
                'tiny-pacs-identity), or switch to "auth: none" behind '
                'an authenticating reverse proxy or "auth: token".'
            )
            return []
        # Imported lazily so headless CLI runs (which import this module
        # for the component class) never load the web stack
        from . import web
        from .common import AppState
        state = AppState(
            bus=self.bus,
            logger=self._logger,
            auth=self.config.auth,
            tokens=tuple(self.config.tokens),
            max_part_size=self.config.max_part_size
        )
        app = web.build_app(state)
        self.log_info(
            'Contributing the DICOMweb services to the shared HTTP '
            'server (prefix %s, auth %s)', self.config.prefix,
            self.config.auth
        )
        return [
            events.HttpAppHook(
                name=self.name(), prefix=self.config.prefix, app=app
            )
        ]
