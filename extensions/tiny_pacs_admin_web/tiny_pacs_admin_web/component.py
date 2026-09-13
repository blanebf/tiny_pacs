"""Web administration component.

The :class:`AdminWeb` component serves the administration console from a
waitress WSGI server running in a daemon thread inside the server
process. It binds nothing until the whole bus has started
(:class:`trolleybus.OnStarted`), stops cleanly on
:class:`trolleybus.OnExit` and never endangers the DICOM AE:

* no :class:`~tiny_pacs.events.UserVerify` listener (no user registry
  installed) → login would be impossible, so the component warns and
  stays disabled;
* the HTTP port cannot be bound (already in use) → CRITICAL log, the
  component stays disabled and the AE keeps running;
* ``TINY_PACS_HEADLESS`` set in the environment → no port is ever bound,
  so headless administration runs that pass an explicit component list
  can instantiate the component safely (the ``web-admin`` CLI does not
  even do that — it runs against the ``Database`` component only).
"""
import os
import threading
from typing import Any

import pydantic
import trolleybus
from tiny_pacs import component, events, schema
from waitress import create_server  # type: ignore[import-untyped]

from . import models

#: Environment variable that suppresses HTTP binding entirely. Set by the
#: ``web-admin`` CLI and honoured by the component so headless
#: administration runs never open the console port.
HEADLESS_ENV = 'TINY_PACS_HEADLESS'

#: Hosts treated as loopback binds (safe without ``allow_remote``)
LOOPBACK_HOSTS = frozenset({'127.0.0.1', '::1', 'localhost'})


def is_loopback(host: str) -> bool:
    """Whether a bind address is a loopback host.

    :param host: configured bind address
    :type host: str
    :return: True for loopback addresses
    :rtype: bool
    """
    return host.strip() in LOOPBACK_HOSTS


class AdminWebConfig(component.ComponentConfig):
    """Configuration of the :class:`AdminWeb` component.

    :ivar host: bind address; non-loopback binds are refused at config
                validation unless ``allow_remote`` is true
    :ivar port: bind port; ``0`` selects an ephemeral port (reported at
                startup through :attr:`AdminWeb.web_port`)
    :ivar allow_remote: permits binding a non-loopback address; such a
                        deployment logs a loud WARNING at startup
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
    :ivar threads: waitress worker threads serving console requests
    """

    host: str = '127.0.0.1'
    port: int = pydantic.Field(default=11113, ge=0, le=65535)
    allow_remote: bool = False
    secure_cookie: bool = False
    session_ttl: int = pydantic.Field(default=3600, ge=1)
    max_sessions: int = pydantic.Field(default=100, ge=1)
    max_login_failures: int = pydantic.Field(default=5, ge=1)
    expose_users_to_viewer: bool = False
    threads: int = pydantic.Field(default=8, ge=1)

    @pydantic.model_validator(mode='after')
    def _check_bind_address(self) -> 'AdminWebConfig':
        if not is_loopback(self.host) and not self.allow_remote:
            raise ValueError(
                f'AdminWeb refuses to bind the non-loopback address '
                f'{self.host!r} unless allow_remote is true; prefer '
                f'the loopback bind behind a TLS-terminating reverse '
                f'proxy (see the extension tutorial)'
            )
        return self


class AdminWeb(component.Component[AdminWebConfig]):
    """Component that serves the web administration console.

    Handles the following events:

        * :class:`~tiny_pacs.events.Migrations`

    The console itself consumes core events only
    (:class:`~tiny_pacs.events.UserVerify` for login, the device/user
    CRUD events, :class:`~tiny_pacs.events.SchemaVersions` /
    :class:`~tiny_pacs.events.TableCounts` /
    :class:`~tiny_pacs.events.StorageStatsQuery` for the dashboard and
    :class:`~tiny_pacs.events.GetClient` for the device echo); whichever
    extension provides a listener is an installation concern. Missing
    listeners degrade pages to "not available" and never produce stack
    traces.

    :ivar web_port: HTTP port actually bound while the console is up,
                    None when the component is disabled (headless run,
                    missing user registry or a port conflict)
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
        self._server: Any = None
        self._thread: threading.Thread | None = None
        self._stopping = False
        self.web_port: int | None = None

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), models.TABLES, models.MIGRATIONS
        )

    def on_started(self) -> None:
        """Handles `OnStarted`: starts the console HTTP server.

        Runs after every component has been constructed and the database
        is ready, so the listener self-check and the bound tables are
        reliable. Every refusal path logs and returns — the DICOM AE
        keeps running whatever happens here.
        """
        super().on_started()
        if os.environ.get(HEADLESS_ENV, '') != '':
            self.log_info(
                '%s is set; the web administration console does not bind '
                'its port in this headless run', HEADLESS_ENV
            )
            return
        if not self.bus.has_listeners(events.UserVerify):
            self.log_warning(
                'No component answers UserVerify (no user registry '
                'installed): login into the web administration console '
                'would be impossible, so the component stays disabled. '
                'Install and enable a user registry (e.g. the Users '
                'component of tiny-pacs-identity).'
            )
            return
        if self.config.allow_remote and not is_loopback(self.config.host):
            self.log_warning(
                '*** AdminWeb is binding the NON-LOOPBACK address %s:%d. '
                'waitress has no TLS support: console traffic (including '
                'login passwords) travels in clear text unless a '
                'TLS-terminating reverse proxy is in front of it. Set '
                'secure_cookie: true behind such a proxy and restrict '
                'network access. ***',
                self.config.host, self.config.port
            )
        # Imported lazily so headless CLI runs (which import this module
        # for HEADLESS_ENV) never load the web stack
        from . import web
        state = web.AppState(
            bus=self.bus,
            logger=self._logger,
            host=self.config.host,
            secure_cookie=self.config.secure_cookie,
            session_ttl=self.config.session_ttl,
            max_sessions=self.config.max_sessions,
            max_login_failures=self.config.max_login_failures,
            expose_users_to_viewer=self.config.expose_users_to_viewer
        )
        app = web.build_app(state)
        try:
            self._server = create_server(
                app,
                host=self.config.host,
                port=self.config.port,
                threads=self.config.threads,
                ident='tiny_pacs'
            )
        except OSError as error:
            self._server = None
            self.log_critical(
                'Cannot bind the web administration console on %s:%d '
                '(%s); the component stays disabled. The DICOM AE keeps '
                'running.',
                self.config.host, self.config.port, error
            )
            return
        self.web_port = self._bound_port()
        self._stopping = False
        self._thread = threading.Thread(
            target=self._serve, name='AdminWeb', daemon=True
        )
        self._thread.start()
        scheme = 'https' if self.config.secure_cookie else 'http'
        self.log_info(
            'Web administration console listening on %s://%s:%d',
            scheme, self.config.host, self.web_port
        )

    def on_exit(self) -> None:
        """Handles `OnExit`: stops the console HTTP server."""
        super().on_exit()
        server, self._server = self._server, None
        if server is None:
            return
        self._stopping = True
        server.close()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=10.0)
        self.web_port = None
        self.log_info('Web administration console stopped')

    def _serve(self) -> None:
        """Runs the waitress loop inside the daemon thread.

        ``close()`` during the poll loop raises ``OSError`` (bad file
        descriptor) out of ``run()``, so the expected shutdown race is
        swallowed; anything else is logged.
        """
        server = self._server
        if server is None:
            return
        try:
            server.run()
        except OSError:
            if not self._stopping:
                self.log_exception(
                    'Web administration console server failed'
                )
        except Exception:
            self.log_exception(
                'Web administration console server crashed'
            )

    def _bound_port(self) -> int:
        """Returns the HTTP port actually bound by waitress.

        ``create_server`` returns a ``TcpWSGIServer`` for a single listen
        address (``effective_port``) and a ``MultiSocketServer`` for
        several (``effective_listen``); both forms report the port after
        an ephemeral ``port: 0`` bind.
        """
        port = getattr(self._server, 'effective_port', None)
        if port:
            return int(port)
        listen = getattr(self._server, 'effective_listen', None) or []
        if listen:
            return int(listen[0][1])
        return self.config.port
