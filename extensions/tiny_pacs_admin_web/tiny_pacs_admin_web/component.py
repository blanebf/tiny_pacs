"""Web administration component.

The :class:`AdminWeb` component contributes the administration console to
the core shared HTTP server (:class:`tiny_pacs.http.HttpServer`): it
answers :class:`~tiny_pacs.events.HttpAppsRegistry` with the console WSGI
application mounted on the root prefix (``/``, the catch-all). It runs no
server of its own — binding, the waitress worker pool and every bind-level
policy (loopback validation, port conflicts, the headless guard) belong to
the core component.

The registry handler builds the application from the validated config on
the spot, so there are no ordering assumptions about who ran
``on_started`` first. Self-check failures mean "no hook is registered",
which never affects the shared server or the other mounts:

* no :class:`~tiny_pacs.events.UserVerify` listener (no user registry
  installed) → login would be impossible, so the component warns and
  contributes no app;
* ``TINY_PACS_HEADLESS`` set in the environment → headless runs that do
  construct the component never build an app (the ``web-admin`` CLI only
  instantiates the ``Database`` component anyway).
"""
from typing import Any

import pydantic
import trolleybus
from tiny_pacs import component, events, schema
from tiny_pacs.http import HEADLESS_ENV, is_headless

from . import models


class AdminWebConfig(component.ComponentConfig):
    """Configuration of the :class:`AdminWeb` component.

    The HTTP transport (bind address, port, worker pool) is configured
    once on the core :class:`~tiny_pacs.http.HttpServer` component, which
    serves every installed HTTP front-end; only console application-level
    settings live here.

    :ivar secure_cookie: adds the ``Secure`` flag to the session cookie.
                        Enable behind the documented TLS-terminating
                        reverse proxy; leave off for plain loopback use
    :ivar session_ttl: session idle timeout in seconds
    :ivar max_sessions: maximum number of concurrent sessions; the least
                        recently used session is evicted beyond it
    :ivar max_login_failures: consecutive login failures tolerated per
                        (username, peer) before the exponential backoff
                        starts
    :ivar expose_users_to_viewer: lets the ``viewer`` role see the users
                        page read-only; usernames can be sensitive in
                        hospital settings, so the default hides them
    """

    secure_cookie: bool = False
    session_ttl: int = pydantic.Field(default=3600, ge=1)
    max_sessions: int = pydantic.Field(default=100, ge=1)
    max_login_failures: int = pydantic.Field(default=5, ge=1)
    expose_users_to_viewer: bool = False


class AdminWeb(component.Component[AdminWebConfig]):
    """Component that contributes the web administration console.

    Handles the following events:

        * :class:`~tiny_pacs.events.Migrations`
        * :class:`~tiny_pacs.events.HttpAppsRegistry`

    The console itself consumes core events only
    (:class:`~tiny_pacs.events.UserVerify` for login, the device/user
    CRUD events, the archive query events,
    :class:`~tiny_pacs.events.SchemaVersions` /
    :class:`~tiny_pacs.events.TableCounts` /
    :class:`~tiny_pacs.events.StorageStatsQuery` /
    :class:`~tiny_pacs.events.HttpMountsQuery` for the dashboard and
    :class:`~tiny_pacs.events.GetClient` for the device echo); whichever
    extension provides a listener is an installation concern. Missing
    listeners degrade pages to "not available" and never produce stack
    traces.
    """

    config_model = AdminWebConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: AdminWebConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: AdminWebConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.Migrations, self.migrations)
        self.subscribe(events.HttpAppsRegistry, self.http_apps)

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), models.TABLES, models.MIGRATIONS
        )

    def http_apps(self, _: None = None) -> list[events.HttpAppHook]:
        """Handles `HttpAppsRegistry`: contributes the console app.

        Performs the self-checks and builds the WSGI application from the
        validated configuration on the spot. Every refusal path logs and
        returns an empty list — the shared server and the other mounts
        are unaffected.

        :return: the console hook (prefix ``/``) or no hook at all
        :rtype: list[events.HttpAppHook]
        """
        if is_headless():
            self.log_info(
                '%s is set; the web administration console registers no '
                'app in this headless run', HEADLESS_ENV
            )
            return []
        if not self.bus.has_listeners(events.UserVerify):
            self.log_warning(
                'No component answers UserVerify (no user registry '
                'installed): login into the web administration console '
                'would be impossible, so no console app is mounted on '
                'the shared HTTP server. Install and enable a user '
                'registry (e.g. the Users component of '
                'tiny-pacs-identity).'
            )
            return []
        # Imported lazily so headless CLI runs (which import this module
        # for the component class) never load the web stack
        from . import web
        state = web.AppState(
            bus=self.bus,
            logger=self._logger,
            secure_cookie=self.config.secure_cookie,
            session_ttl=self.config.session_ttl,
            max_sessions=self.config.max_sessions,
            max_login_failures=self.config.max_login_failures,
            expose_users_to_viewer=self.config.expose_users_to_viewer
        )
        app = web.build_app(state)
        self.log_info(
            'Contributing the web administration console to the shared '
            'HTTP server (prefix /)'
        )
        return [events.HttpAppHook(name=self.name(), prefix='/', app=app)]
