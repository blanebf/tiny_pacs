"""Shared HTTP server.

One HTTP server for the whole process: the :class:`HttpServer` component
broadcasts :class:`~tiny_pacs.events.HttpAppsRegistry` once the bus has
started, collects the WSGI applications contributed by the installed HTTP
front-ends (the administration console, DICOMweb, ...) and serves them
from a single waitress instance running in a daemon thread.

The dispatcher between waitress and the contributed applications is
dependency-free stdlib WSGI: it picks the hook with the longest matching
mount prefix, rewrites ``SCRIPT_NAME``/``PATH_INFO`` per the WSGI mount
contract and passes everything else to the root mount (prefix ``/``).
Providers keep their own request logging and security middleware; the
dispatcher logs nothing per request.

All bind-level policy lives here and only here: loopback default with
``allow_remote`` validation at config load, a resolution check at bind
time (a ``localhost`` that does not resolve to loopback is refused
fail-closed), the loud non-loopback WARNING, port-conflict degradation
(CRITICAL log, server disabled, DICOM AE keeps running), the
``TINY_PACS_HEADLESS`` guard and dormant behaviour when no application
is contributed. waitress is imported lazily and ships as the core extra
``tiny_pacs[http]``: without it the component logs a WARNING and stays
disabled.
"""
import ipaddress
import os
import socket
import threading
from collections.abc import Callable, Iterable
from typing import Any

import pydantic
import trolleybus

from . import component, events

#: Environment variable that suppresses HTTP binding entirely. Honoured
#: by admin CLIs and headless runs so no HTTP port is ever opened outside
#: a real server.
HEADLESS_ENV = 'TINY_PACS_HEADLESS'

#: Hosts treated as loopback binds (safe without ``allow_remote``)
LOOPBACK_HOSTS = frozenset({'127.0.0.1', '::1', 'localhost'})

#: Default TCP port of the shared HTTP server
DEFAULT_PORT = 11113

#: Default size of the single waitress thread pool
DEFAULT_THREADS = 16


def is_loopback(host: str) -> bool:
    """Whether a bind address is a loopback host.

    :param host: configured bind address
    :type host: str
    :return: True for loopback addresses
    :rtype: bool
    """
    return host.strip() in LOOPBACK_HOSTS


def is_headless() -> bool:
    """Whether HTTP serving is suppressed for this process.

    The mere presence of :data:`HEADLESS_ENV` counts as headless, so the
    guard fails closed for the set-but-empty values orchestration tools
    produce (``environment: ["TINY_PACS_HEADLESS"]`` pass-throughs,
    empty ``valueFrom`` results). Both the core :class:`HttpServer` and
    contributing front-ends call this single predicate.

    :return: True when HTTP front-ends must not bind or contribute apps
    :rtype: bool
    """
    return HEADLESS_ENV in os.environ


def resolves_to_loopback(host: str) -> bool:
    """Whether every address ``host`` resolves to is a loopback address.

    Guards the bind policy against name resolution: a ``localhost``
    mapping to a routable address (:file:`/etc/hosts`, container image)
    must not slip past the ``allow_remote`` refusal. Unresolvable hosts
    are not provably loopback either.

    :param host: configured bind address (name or IP literal)
    :type host: str
    :return: True when every resolved address is loopback
    :rtype: bool
    """
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except (OSError, UnicodeError):
        return False
    if not infos:
        return False
    for info in infos:
        try:
            if not ipaddress.ip_address(info[4][0]).is_loopback:
                return False
        except ValueError:
            return False
    return True


def normalize_prefix(prefix: str) -> str | None:
    """Normalizes a mount prefix.

    The root prefix stays ``/``; every other prefix keeps its leading
    slash and loses a trailing one, so ``/archive`` and ``/archive/``
    mount identically. Prefixes that do not start with a slash cannot be
    matched against ``PATH_INFO`` and are rejected.

    :param prefix: mount prefix contributed by a provider
    :type prefix: str
    :return: normalized prefix, None when the prefix is unusable
    :rtype: str or None
    """
    stripped = (prefix or '').strip()
    if not stripped.startswith('/'):
        return None
    if stripped == '/':
        return '/'
    return stripped.rstrip('/') or '/'


class Dispatcher:
    """Dependency-free stdlib WSGI prefix dispatcher.

    Routes every request to the hook with the longest matching prefix on
    path-segment boundaries (``/ar`` never matches ``/archive/...``),
    rewriting ``SCRIPT_NAME``/``PATH_INFO`` per the WSGI mount contract;
    the ``/`` hook is the catch-all. Duplicate prefixes are resolved
    first-wins with a WARNING — a provider conflict never crashes the
    server. Paths below no mount (possible only without a root app) get
    a plain 404.

    Instances are immutable after construction and safe to call from the
    waitress thread pool.
    """

    def __init__(
            self, hooks: Iterable[events.HttpAppHook], logger: Any
    ) -> None:
        """Builds the routing table.

        :param hooks: contributed WSGI applications
        :param logger: component logger receiving WARNING lines
        """
        self._logger = logger
        mounts: dict[str, events.HttpAppHook] = {}
        for hook in hooks:
            prefix = normalize_prefix(hook.prefix)
            if prefix is None:  # pragma: no cover - validated on collect
                continue
            if prefix in mounts:
                logger.warning(
                    'HTTP mount prefix %r is provided by both %r and %r; '
                    'mounting the first and skipping the rest',
                    prefix, mounts[prefix].name, hook.name
                )
                continue
            mounts[prefix] = hook
        # Longest prefix first so the first match wins; the root
        # catch-all sorts last
        self._mounts: list[tuple[str, events.HttpAppHook]] = sorted(
            mounts.items(), key=lambda item: (item[0] == '/', -len(item[0]))
        )

    @property
    def mounts(self) -> list[tuple[str, str]]:
        """The mounted ``(component name, prefix)`` pairs.

        :return: mounted applications in routing order
        :rtype: list[tuple[str, str]]
        """
        return [(hook.name, prefix) for prefix, hook in self._mounts]

    def __call__(
            self,
            environ: dict[str, Any],
            start_response: Callable[..., Any]
    ) -> Iterable[bytes]:
        """Serves one request through the longest matching mount."""
        path = environ.get('PATH_INFO', '') or '/'
        script = environ.get('SCRIPT_NAME', '') or ''
        for prefix, hook in self._mounts:
            if prefix != '/' and not (path == prefix
                                      or path.startswith(f'{prefix}/')):
                continue
            if prefix != '/':
                environ['SCRIPT_NAME'] = f'{script}{prefix}'
                environ['PATH_INFO'] = path[len(prefix):] or '/'
            return hook.app(environ, start_response)
        return self._not_found(environ, start_response, path)

    def _not_found(
            self,
            environ: dict[str, Any],
            start_response: Callable[..., Any],
            path: str
    ) -> Iterable[bytes]:
        """Answers paths below no mount (no root application).

        The path is logged through ``%r`` so percent-decoded control
        characters (CR/LF) in an attacker-supplied request target cannot
        forge log lines.
        """
        self._logger.warning('No HTTP app mounted for path %r', path)
        start_response(
            '404 Not Found',
            [('Content-Type', 'text/plain; charset=utf-8'),
             ('Content-Length', '10')]
        )
        return [b'Not found\n']


class _DbReleaseIterable:
    """WSGI response wrapper releasing the DB connection once served.

    Emits :class:`~tiny_pacs.events.CloseConnection` when the response
    iterable is exhausted or when the server calls ``close()`` (per the
    WSGI contract, also on aborted responses), whichever comes first —
    the broadcast is idempotent, so both paths firing is harmless.
    """

    def __init__(self, iterable: Iterable[bytes],
                 bus: trolleybus.EventBus) -> None:
        """Wraps a WSGI response iterable.

        :param iterable: response iterable returned by the mounted app
        :type iterable: Iterable[bytes]
        :param bus: event bus used to release the connection
        :type bus: trolleybus.EventBus
        """
        self._iterable = iterable
        self._iterator = iter(iterable)
        self._bus = bus

    def __iter__(self) -> '_DbReleaseIterable':
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._iterator)
        except StopIteration:
            self._release()
            raise

    def close(self) -> None:
        """Closes the wrapped iterable and releases the DB connection."""
        close = getattr(self._iterable, 'close', None)
        if callable(close):
            close()
        self._release()

    def _release(self) -> None:
        self._bus.broadcast_nothrow(events.CloseConnection, None)


class _DbReleaseApp:
    """WSGI middleware returning pooled DB connections after each request.

    The waitress worker threads are long-lived and peewee tracks
    connections per thread: a worker that served one DB-backed request
    held its pooled connection forever otherwise, keeping up to
    ``threads`` pool slots busy while idle and starving the per-association
    DICOM threads of connections. Releasing after each request follows
    peewee's web-application guidance and keeps the shared pool available
    to the DICOM side.
    """

    def __init__(self, app: Callable[..., Iterable[bytes]],
                 bus: trolleybus.EventBus) -> None:
        """Wraps a WSGI application.

        :param app: WSGI application (the prefix dispatcher)
        :type app: Callable[..., Iterable[bytes]]
        :param bus: event bus used to release the connection
        :type bus: trolleybus.EventBus
        """
        self._app = app
        self._bus = bus

    def __call__(
            self,
            environ: dict[str, Any],
            start_response: Callable[..., Any]
    ) -> Iterable[bytes]:
        """Serves one request, releasing the DB connection afterwards."""
        try:
            result = self._app(environ, start_response)
        except BaseException:
            self._bus.broadcast_nothrow(events.CloseConnection, None)
            raise
        return _DbReleaseIterable(result, self._bus)


class HttpServerConfig(component.ComponentConfig):
    """Configuration of the :class:`HttpServer` component.

    :ivar host: bind address; non-loopback binds are refused at config
                validation unless ``allow_remote`` is true
    :ivar port: bind port; ``0`` selects an ephemeral port (reported at
                startup through :attr:`HttpServer.web_port`)
    :ivar threads: the single waitress worker pool serving every
                   mounted front-end
    :ivar allow_remote: permits binding a non-loopback address; such a
                        deployment logs a loud WARNING at startup
    """

    host: str = '127.0.0.1'
    port: int = pydantic.Field(default=DEFAULT_PORT, ge=0, le=65535)
    threads: int = pydantic.Field(default=DEFAULT_THREADS, ge=1)
    allow_remote: bool = False

    @pydantic.model_validator(mode='after')
    def _check_bind_address(self) -> 'HttpServerConfig':
        if not is_loopback(self.host) and not self.allow_remote:
            raise ValueError(
                f'HttpServer refuses to bind the non-loopback address '
                f'{self.host!r} unless allow_remote is true; prefer the '
                f'loopback bind behind a TLS-terminating reverse proxy '
                f'(see the web administration tutorial)'
            )
        return self


class HttpServer(component.Component[HttpServerConfig]):
    """Component serving every contributed WSGI application.

    Handles the following events:

        * :class:`~tiny_pacs.events.HttpMountsQuery`

    Broadcasts :class:`~tiny_pacs.events.HttpAppsRegistry` on
    :class:`trolleybus.OnStarted`, builds the prefix dispatcher from the
    collected hooks and runs the single waitress server in a daemon
    thread until :class:`trolleybus.OnExit`. With zero contributed hooks
    nothing is bound and the component stays dormant, so enabling it is
    harmless on installs without any web front-end. Bind failures
    (port conflicts) and a missing waitress dependency disable the
    component with a log line and never endanger the DICOM AE.

    :ivar web_port: HTTP port actually bound while the server is up,
                    None when it is dormant or disabled
    """

    config_model = HttpServerConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: HttpServerConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: HttpServerConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.HttpMountsQuery, self._handle_mounts_query)
        self._server: Any = None
        self._thread: threading.Thread | None = None
        self._stopping = False
        self._mounts: list[tuple[str, str]] = []
        self.web_port: int | None = None

    def on_started(self) -> None:
        """Handles `OnStarted`: collects the apps and starts the server.

        Runs after every component has been constructed, so the registry
        broadcast reaches every provider regardless of component order.
        Every refusal path logs and returns — the DICOM AE keeps running
        whatever happens here.
        """
        super().on_started()
        if is_headless():
            self.log_info(
                '%s is set; the shared HTTP server does not bind its '
                'port in this headless run', HEADLESS_ENV
            )
            return
        if self.config.allow_remote and not is_loopback(self.config.host):
            self.log_warning(
                '*** HttpServer is binding the NON-LOOPBACK address '
                '%s:%d. waitress has no TLS support: traffic of every '
                'mounted HTTP front-end travels in clear text unless a '
                'TLS-terminating reverse proxy is in front of it. '
                'Restrict network access. ***',
                self.config.host, self.config.port
            )
        if (not self.config.allow_remote
                and not resolves_to_loopback(self.config.host)):
            self.log_critical(
                'Refusing to bind %r: it does not resolve to a loopback '
                'address and allow_remote is false. The shared HTTP '
                'server stays disabled; the DICOM AE keeps running.',
                self.config.host
            )
            return
        hooks = self._collect_hooks()
        if not hooks:
            self.log_info(
                'No HTTP application was contributed; the shared HTTP '
                'server stays dormant and binds nothing'
            )
            return
        dispatcher = Dispatcher(hooks, self._logger)
        try:
            from waitress import create_server  # type: ignore[import-untyped]
        except ImportError:
            self.log_warning(
                'waitress is not installed, so the shared HTTP server '
                'stays disabled: install the core HTTP extra with '
                '"pip install tiny_pacs[http]". The DICOM AE keeps '
                'running.'
            )
            return
        try:
            self._server = create_server(
                _DbReleaseApp(dispatcher, self.bus),
                host=self.config.host,
                port=self.config.port,
                threads=self.config.threads,
                ident='tiny_pacs'
            )
        except OSError as error:
            self._server = None
            self.log_critical(
                'Cannot bind the shared HTTP server on %s:%d (%s); it '
                'stays disabled. The DICOM AE keeps running.',
                self.config.host, self.config.port, error
            )
            return
        self._mounts = dispatcher.mounts
        self.web_port = self._bound_port()
        self._stopping = False
        self._thread = threading.Thread(
            target=self._serve, name='tiny_pacs-http', daemon=True
        )
        self._thread.start()
        mounts = ', '.join(f'{name} on {prefix}'
                           for name, prefix in self._mounts)
        self.log_info(
            'Shared HTTP server listening on http://%s:%d (%s)',
            self.config.host, self.web_port, mounts
        )

    def on_exit(self) -> None:
        """Handles `OnExit`: stops the shared HTTP server."""
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
        self._mounts = []
        self.log_info('Shared HTTP server stopped')

    def _handle_mounts_query(self, _: None = None) -> list[tuple[str, str]]:
        """Handles `HttpMountsQuery`: reports the mounted applications."""
        return list(self._mounts)

    def _collect_hooks(self) -> list[events.HttpAppHook]:
        """Broadcasts the registry and validates the contributed hooks.

        Provider failures are logged and skipped so one broken front-end
        never takes the shared server (or the other mounts) down; the
        same holds for malformed answers (a non-list result, an unusable
        prefix or a missing app).
        """
        hooks: list[events.HttpAppHook] = []
        results = self.bus.broadcast_nothrow(events.HttpAppsRegistry, None)
        for result in results:
            if not result.ok:
                self.log_warning(
                    'An HttpAppsRegistry provider failed (%r); its '
                    'applications are not mounted', result.error
                )
                continue
            value = result.value
            if value is not None and not isinstance(value, (list, tuple)):
                self.log_warning(
                    'Ignoring the malformed HttpAppsRegistry answer %r; '
                    'providers must answer with a list of HttpAppHook',
                    value
                )
                continue
            for hook in value or []:
                if (not isinstance(hook, events.HttpAppHook)
                        or normalize_prefix(hook.prefix) is None
                        or not callable(hook.app)):
                    self.log_warning(
                        'Ignoring the malformed HTTP app hook %r; mount '
                        'prefixes must start with a slash and the app '
                        'must be a WSGI callable', hook
                    )
                    continue
                hooks.append(hook)
        return hooks

    def _serve(self) -> None:
        """Runs the waitress loop inside the daemon thread.

        ``close()`` during the poll loop raises ``OSError`` (bad file
        descriptor) out of ``run()``, so the expected shutdown race is
        swallowed; anything else is logged.
        """
        server = self._server
        if server is None:  # pragma: no cover - defensive
            return
        try:
            server.run()
        except OSError:
            if not self._stopping:
                self.log_exception('Shared HTTP server failed')
        except Exception:
            self.log_exception('Shared HTTP server crashed')

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
