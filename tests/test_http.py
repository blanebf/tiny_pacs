"""Tests of the shared HTTP server: dispatcher, registry, lifecycle.

Covers the mount routing and the ``SCRIPT_NAME``/``PATH_INFO`` rewrite of
the stdlib dispatcher, the ``HttpAppsRegistry`` collection (including
failing/malformed providers), the ``HttpMountsQuery`` output, the
waitress thread lifecycle on an ephemeral port, and every degradation
path (port conflict, headless guard, missing waitress, dormant server)
plus the bind-policy configuration matrix.
"""
import json
import logging
import socket
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from io import BytesIO
from types import SimpleNamespace
from typing import Any

import pydantic
import pytest
import trolleybus

from tiny_pacs import events, http


def echo_app(environ: dict[str, Any],
             start_response: Callable[..., Any]) -> Any:
    """Test WSGI app returning the mount contract fields it was given."""
    body = json.dumps({
        'script_name': environ.get('SCRIPT_NAME', ''),
        'path_info': environ.get('PATH_INFO', ''),
        'query_string': environ.get('QUERY_STRING', ''),
    }).encode('utf-8')
    start_response('200 OK', [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(body))),
    ])
    return [body]


def _hook(name: str, prefix: str, app: Any = echo_app
          ) -> events.HttpAppHook:
    return events.HttpAppHook(name=name, prefix=prefix, app=app)


def _call(app: Any, path: str, script_name: str = '',
          method: str = 'GET') -> tuple[int, bytes, dict[str, Any]]:
    """Drives a WSGI callable with a hand-built environ."""
    environ: dict[str, Any] = {
        'REQUEST_METHOD': method,
        'SCRIPT_NAME': script_name,
        'PATH_INFO': path,
        'QUERY_STRING': '',
        'SERVER_NAME': 'localhost',
        'SERVER_PORT': '80',
        'SERVER_PROTOCOL': 'HTTP/1.1',
        'REMOTE_ADDR': '127.0.0.1',
        'wsgi.version': (1, 0),
        'wsgi.url_scheme': 'http',
        'wsgi.input': BytesIO(b''),
        'wsgi.errors': BytesIO(),
        'wsgi.multithread': True,
        'wsgi.multiprocess': False,
        'wsgi.run_once': False,
    }
    captured: dict[str, Any] = {}

    def start_response(status: str, headers: Any,
                       exc_info: Any = None) -> Callable[[bytes], Any]:
        captured['status'] = status
        captured['headers'] = list(headers)
        return lambda chunk: None

    result = app(environ, start_response)
    body = b''.join(result)
    close = getattr(result, 'close', None)
    if close is not None:
        close()
    return int(captured['status'].split(' ', 1)[0]), body, environ


def http_get(port: int, path: str) -> tuple[int, bytes]:
    """Performs a live GET against the bound server."""
    try:
        with urllib.request.urlopen(
                f'http://127.0.0.1:{port}{path}', timeout=10) as raw:
            return int(raw.status), raw.read()
    except urllib.error.HTTPError as error:
        return int(error.code), error.read()


@pytest.fixture
def live_bus(monkeypatch: pytest.MonkeyPatch
             ) -> Iterator[Callable[..., SimpleNamespace]]:
    """Builds buses with a real (port-binding) ``HttpServer``.

    Factory options: ``server_config`` overrides (default: ephemeral
    ``port: 0``), ``providers`` — callables subscribed to
    ``HttpAppsRegistry`` before the component is constructed.

    :yield: factory returning ``(bus, server)`` as a namespace
    """
    monkeypatch.delenv(http.HEADLESS_ENV, raising=False)
    started: list[trolleybus.EventBus] = []

    def build(
            server_config: dict[str, Any] | None = None,
            providers: list[Callable[[None], Any]] | None = None
    ) -> SimpleNamespace:
        bus = trolleybus.EventBus()
        for provider in providers or []:
            bus.subscribe(events.HttpAppsRegistry, provider)
        server = http.HttpServer(
            bus, {'on': True, 'port': 0, **(server_config or {})}
        )
        bus.start()
        started.append(bus)
        return SimpleNamespace(bus=bus, server=server)

    yield build

    for bus in reversed(started):
        bus.stop()


# -----------------------------------------------------------------------
# Dispatcher: routing and the WSGI mount contract
# -----------------------------------------------------------------------

def test_dispatcher_rewrites_mount_contract() -> None:
    dispatcher = http.Dispatcher([_hook('root', '/')], logging.getLogger('t'))
    status, body, environ = _call(dispatcher, '/anything')
    assert status == 200
    payload = json.loads(body)
    assert payload == {'script_name': '', 'path_info': '/anything',
                       'query_string': ''}
    assert environ['SCRIPT_NAME'] == '', 'root mount leaves SCRIPT_NAME'


def test_dispatcher_prefix_rewrite() -> None:
    dispatcher = http.Dispatcher(
        [_hook('archive', '/archive'), _hook('root', '/')],
        logging.getLogger('t')
    )
    _, body, environ = _call(dispatcher, '/archive/patients')
    assert json.loads(body) == {
        'script_name': '/archive', 'path_info': '/patients',
        'query_string': ''
    }
    assert environ['SCRIPT_NAME'] == '/archive'
    assert environ['PATH_INFO'] == '/patients'


def test_dispatcher_exact_prefix_maps_to_slash() -> None:
    dispatcher = http.Dispatcher(
        [_hook('archive', '/archive')], logging.getLogger('t')
    )
    _, body, _environ = _call(dispatcher, '/archive')
    payload = json.loads(body)
    assert payload['script_name'] == '/archive'
    assert payload['path_info'] == '/', 'the mount point itself is /'


def test_dispatcher_longest_prefix_wins() -> None:
    def maker(tag: str) -> Any:
        def app(environ: dict[str, Any],
                start_response: Callable[..., Any]) -> Any:
            start_response('200 OK', [])
            return [tag.encode()]
        return app

    dispatcher = http.Dispatcher(
        [_hook('root', '/'), _hook('sub', '/a/b', maker('sub')),
         _hook('short', '/a', maker('short'))],
        logging.getLogger('t')
    )
    assert _call(dispatcher, '/a/b/c')[1] == b'sub'
    assert _call(dispatcher, '/a/x')[1] == b'short'
    assert _call(dispatcher, '/a')[1] == b'short', 'prefix boundary'
    assert _call(dispatcher, '/other')[1] != b'short', 'root catch-all'


def test_dispatcher_matches_on_path_segments() -> None:
    dispatcher = http.Dispatcher(
        [_hook('a', '/a'), _hook('root', '/', echo_app)],
        logging.getLogger('t')
    )
    _, body, _environ = _call(dispatcher, '/ab/x')
    assert json.loads(body)['path_info'] == '/ab/x', \
        '/a must not match /ab/x'


def test_dispatcher_normalizes_trailing_slash() -> None:
    dispatcher = http.Dispatcher(
        [_hook('archive', '/archive/')], logging.getLogger('t')
    )
    _, body, _environ = _call(dispatcher, '/archive/x')
    assert json.loads(body)['script_name'] == '/archive'


def test_dispatcher_unmatched_path_404_without_root(
        caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = http.Dispatcher(
        [_hook('archive', '/archive')], logging.getLogger('HttpServer')
    )
    with caplog.at_level(logging.WARNING, logger='HttpServer'):
        status, body, _environ = _call(dispatcher, '/nope')
    assert status == 404
    assert body == b'Not found\n'
    assert any('No HTTP app mounted' in record.getMessage()
               for record in caplog.records)


def test_dispatcher_duplicate_prefix_first_wins(
        caplog: pytest.LogCaptureFixture) -> None:
    def maker(tag: str) -> Any:
        def app(environ: dict[str, Any],
                start_response: Callable[..., Any]) -> Any:
            start_response('200 OK', [])
            return [tag.encode()]
        return app

    with caplog.at_level(logging.WARNING, logger='HttpServer'):
        dispatcher = http.Dispatcher(
            [_hook('first', '/x', maker('first')),
             _hook('second', '/x', maker('second'))],
            logging.getLogger('HttpServer')
        )
    assert _call(dispatcher, '/x/y')[1] == b'first'
    assert dispatcher.mounts == [('first', '/x')]
    assert any(record.levelno >= logging.WARNING
               and "'second'" in record.getMessage()
               for record in caplog.records)


def test_dispatcher_silent_on_matched_requests(
        caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = http.Dispatcher(
        [_hook('root', '/')], logging.getLogger('DispatcherTest')
    )
    with caplog.at_level(logging.DEBUG, logger='DispatcherTest'):
        _call(dispatcher, '/anything')
    assert not [record for record in caplog.records
                if record.name == 'DispatcherTest']


def test_dispatcher_preserves_incoming_script_name() -> None:
    dispatcher = http.Dispatcher(
        [_hook('archive', '/archive')], logging.getLogger('t')
    )
    _, body, _environ = _call(dispatcher, '/archive/x',
                              script_name='/outer')
    assert json.loads(body)['script_name'] == '/outer/archive'


def test_normalize_prefix() -> None:
    assert http.normalize_prefix('/') == '/'
    assert http.normalize_prefix('/archive') == '/archive'
    assert http.normalize_prefix('/archive/') == '/archive'
    assert http.normalize_prefix('//') == '/'
    assert http.normalize_prefix('archive') is None
    assert http.normalize_prefix('') is None


# -----------------------------------------------------------------------
# Registry collection and mounts query
# -----------------------------------------------------------------------

def test_registry_collects_hooks_from_every_provider(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus(providers=[
        lambda _: [_hook('A', '/a')],
        lambda _: [_hook('B', '/b'), _hook('root', '/')],
    ])
    assert env.server.web_port not in (None, 0)
    mounts = sorted(env.bus.send_one(events.HttpMountsQuery, None))
    assert mounts == [('A', '/a'), ('B', '/b'), ('root', '/')]
    status, _body = http_get(env.server.web_port, '/a/x')
    assert status == 200


def test_failing_provider_does_not_break_others(
        live_bus: Callable[..., Any],
        caplog: pytest.LogCaptureFixture) -> None:
    def boom(_: None) -> Any:
        raise RuntimeError('provider exploded')

    with caplog.at_level(logging.WARNING):
        env = live_bus(providers=[boom, lambda _: [_hook('B', '/')]])
    assert env.server.web_port is not None
    assert http_get(env.server.web_port, '/')[0] == 200
    assert any('failed' in record.getMessage()
               and record.levelno >= logging.WARNING
               for record in caplog.records)


def test_malformed_hooks_skipped(live_bus: Callable[..., Any],
                                 caplog: pytest.LogCaptureFixture) -> None:
    bad_app = events.HttpAppHook(
        name='bad-app', prefix='/x', app=None  # type: ignore[arg-type]
    )
    with caplog.at_level(logging.WARNING):
        env = live_bus(providers=[lambda _: [
            _hook('bad-prefix', 'no-slash'),
            bad_app,
            _hook('good', '/'),
        ]])
    assert env.server.web_port is not None
    mounts = env.bus.send_one(events.HttpMountsQuery, None)
    assert mounts == [('good', '/')]
    assert sum('malformed' in record.getMessage()
               for record in caplog.records) == 2


def test_mounts_query_empty_while_dormant(live_bus: Callable[..., Any]
                                          ) -> None:
    env = live_bus()
    assert env.server.web_port is None
    assert env.bus.send_one(events.HttpMountsQuery, None) == []


def test_dormant_with_zero_hooks(live_bus: Callable[..., Any],
                                 caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO):
        env = live_bus(providers=[lambda _: []])
    assert env.server.web_port is None
    assert any('dormant' in record.getMessage()
               for record in caplog.records)


# -----------------------------------------------------------------------
# Lifecycle and degradation
# -----------------------------------------------------------------------

def test_lifecycle_bind_serve_stop(live_bus: Callable[..., Any]) -> None:
    env = live_bus(providers=[lambda _: [_hook('root', '/')]])
    port = env.server.web_port
    assert port not in (None, 0)
    status, body = http_get(port, '/hello')
    assert status == 200
    assert json.loads(body)['path_info'] == '/hello'
    env.bus.stop()
    with pytest.raises(OSError):
        http_get(port, '/hello')


def test_bound_port_reported_for_ephemeral_bind(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus(providers=[lambda _: [_hook('root', '/')]])
    assert isinstance(env.server.web_port, int)
    assert env.server.web_port > 0
    assert http_get(env.server.web_port, '/')[0] == 200


def test_port_conflict_disables_only_the_http_server(
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.delenv(http.HEADLESS_ENV, raising=False)
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(('127.0.0.1', 0))
    blocker.listen(1)
    port = int(blocker.getsockname()[1])
    bus = trolleybus.EventBus()
    bus.subscribe(events.HttpAppsRegistry, lambda _: [_hook('root', '/')])
    bus.subscribe(events.MainAET, lambda _: 'TEST_AET')
    try:
        with caplog.at_level(logging.CRITICAL):
            server = http.HttpServer(bus, {'on': True, 'port': port})
            bus.start()
        try:
            assert server.web_port is None
            assert any(record.levelno >= logging.CRITICAL
                       and 'Cannot bind' in record.getMessage()
                       for record in caplog.records)
            # The bus keeps serving: the DICOM AE side is untouched
            assert bus.send_any(events.MainAET, None) == 'TEST_AET'
        finally:
            bus.stop()
    finally:
        blocker.close()


def test_headless_env_guard(live_bus: Callable[..., Any],
                            monkeypatch: pytest.MonkeyPatch,
                            caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setenv(http.HEADLESS_ENV, '1')
    with caplog.at_level(logging.INFO):
        env = live_bus(providers=[lambda _: [_hook('root', '/')]])
    assert env.server.web_port is None
    assert any(http.HEADLESS_ENV in record.getMessage()
               for record in caplog.records)


@pytest.mark.parametrize('value', ['1', ''])
def test_headless_guard_presence_semantics(live_bus: Callable[..., Any],
                                           monkeypatch: pytest.MonkeyPatch,
                                           value: str) -> None:
    """Set-but-empty counts as headless: the guard fails closed for the
    empty values orchestration tooling produces."""
    monkeypatch.setenv(http.HEADLESS_ENV, value)
    assert http.is_headless() is True
    env = live_bus(providers=[lambda _: [_hook('root', '/')]])
    assert env.server.web_port is None


def test_is_headless_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(http.HEADLESS_ENV, raising=False)
    assert http.is_headless() is False


def test_non_list_provider_answer_is_skipped(live_bus: Callable[..., Any],
                                             caplog: pytest.LogCaptureFixture
                                             ) -> None:
    """A provider answering with a bare hook instead of a list is logged
    and skipped — it must never abort the bus start (and the AE)."""
    def bare(_: None) -> Any:
        return _hook('bare', '/bare')

    with caplog.at_level(logging.WARNING):
        env = live_bus(providers=[bare, lambda _: [_hook('B', '/')]])
    assert env.server.web_port is not None
    assert http_get(env.server.web_port, '/')[0] == 200
    assert any('malformed HttpAppsRegistry answer' in record.getMessage()
               for record in caplog.records)
    assert env.bus.send_one(events.HttpMountsQuery, None) == [('B', '/')]


def test_resolves_to_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    assert http.resolves_to_loopback('127.0.0.1') is True
    assert http.resolves_to_loopback('::1') is True
    assert http.resolves_to_loopback('') is False

    def remote_info(host: Any, *args: Any, **kwargs: Any) -> Any:
        return [(2, 1, 6, '', ('10.0.0.8', 0))]

    monkeypatch.setattr(socket, 'getaddrinfo', remote_info)
    assert http.resolves_to_loopback('localhost') is False

    def raising(*args: Any, **kwargs: Any) -> Any:
        raise OSError('no resolver')

    monkeypatch.setattr(socket, 'getaddrinfo', raising)
    assert http.resolves_to_loopback('localhost') is False


def test_localhost_resolving_remote_refuses_bind(
        live_bus: Callable[..., Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    """Config validation accepts the *name* ``localhost``; a resolution
    mapping it off-loopback must fail closed and bind nothing."""
    monkeypatch.setattr(http, 'resolves_to_loopback', lambda host: False)
    called: dict[str, bool] = {}

    def stub_create_server(app: Any, **kwargs: Any) -> Any:
        called['bound'] = True
        raise AssertionError('create_server must not be reached')

    monkeypatch.setattr('waitress.create_server', stub_create_server)
    with caplog.at_level(logging.CRITICAL):
        env = live_bus(
            server_config={'host': 'localhost'},
            providers=[lambda _: [_hook('root', '/')]]
        )
    assert env.server.web_port is None
    assert not called.get('bound')
    assert any('does not resolve to a loopback' in record.getMessage()
               for record in caplog.records)


def test_missing_waitress_disables_with_warning(
        live_bus: Callable[..., Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setitem(sys.modules, 'waitress', None)
    with caplog.at_level(logging.WARNING):
        env = live_bus(providers=[lambda _: [_hook('root', '/')]])
    assert env.server.web_port is None
    assert any('waitress' in record.getMessage()
               and record.levelno >= logging.WARNING
               for record in caplog.records)
    # The rest of the bus keeps working
    assert env.bus.send_one(events.HttpMountsQuery, None) == []


def test_non_loopback_with_allow_remote_warns(
        live_bus: Callable[..., Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    """The loud startup WARNING for an allowed remote bind.

    ``create_server`` is stubbed so the test never binds a real
    non-loopback interface.
    """
    calls: dict[str, Any] = {}

    class _StubServer:
        effective_port = 4242

        def run(self) -> None:
            calls['ran'] = True

        def close(self) -> None:
            calls['closed'] = True

    def stub_create_server(app: Any, **kwargs: Any) -> Any:
        calls.update(kwargs)
        return _StubServer()

    monkeypatch.setattr('waitress.create_server', stub_create_server)
    with caplog.at_level(logging.WARNING):
        env = live_bus(
            server_config={'host': '10.9.8.7', 'allow_remote': True},
            providers=[lambda _: [_hook('root', '/')]]
        )
    assert calls.get('host') == '10.9.8.7'
    assert calls.get('threads') == http.DEFAULT_THREADS
    assert env.server.web_port == 4242
    assert any('NON-LOOPBACK' in record.getMessage()
               for record in caplog.records)


def test_serve_failure_is_logged(live_bus: Callable[..., Any],
                                 monkeypatch: pytest.MonkeyPatch,
                                 caplog: pytest.LogCaptureFixture) -> None:
    """An unexpected serve crash is logged, never raised into the bus."""

    class _ExplodingServer:
        effective_port = 1

        def run(self) -> None:
            raise OSError('simulated socket death')

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        'waitress.create_server', lambda app, **kwargs: _ExplodingServer()
    )
    with caplog.at_level(logging.ERROR):
        env = live_bus(providers=[lambda _: [_hook('root', '/')]])
    env.server._thread.join(timeout=5)  # noqa: SLF001 - test introspection
    assert any('server failed' in record.getMessage()
               and record.levelno >= logging.ERROR
               for record in caplog.records)


# -----------------------------------------------------------------------
# Configuration validation matrix
# -----------------------------------------------------------------------

def test_config_defaults() -> None:
    config = http.HttpServerConfig()
    assert config.on is False
    assert config.host == '127.0.0.1'
    assert config.port == 11113
    assert config.threads == 16
    assert config.allow_remote is False


@pytest.mark.parametrize('host', ['127.0.0.1', '::1', 'localhost'])
def test_loopback_hosts_recognized(host: str) -> None:
    assert http.is_loopback(host)
    assert http.HttpServerConfig(host=host).host == host


@pytest.mark.parametrize('host', [
    '0.0.0.0', '192.168.1.5', 'pacs.example.org', ''
])
def test_remote_hosts_refused_without_allow_remote(host: str) -> None:
    assert not http.is_loopback(host)
    with pytest.raises(pydantic.ValidationError, match='allow_remote'):
        http.HttpServerConfig(host=host)


@pytest.mark.parametrize('host', ['0.0.0.0', '192.168.1.5'])
def test_remote_hosts_accepted_with_allow_remote(host: str) -> None:
    config = http.HttpServerConfig(host=host, allow_remote=True)
    assert config.host == host
    assert config.allow_remote is True


@pytest.mark.parametrize('data', [
    {'port': -1},
    {'port': 65536},
    {'threads': 0},
])
def test_config_bounds(data: dict[str, Any]) -> None:
    with pytest.raises(pydantic.ValidationError):
        http.HttpServerConfig(**data)


def test_config_boundary_values() -> None:
    assert http.HttpServerConfig(port=0).port == 0, 'ephemeral port'
    assert http.HttpServerConfig(port=65535).port == 65535
    assert http.HttpServerConfig(threads=1).threads == 1
