"""Tests of the AdminWeb component lifecycle on a headless bus.

Covers the HTTP start/stop paths with a live (ephemeral) port driven via
``urllib``, the listener self-checks, the ``TINY_PACS_HEADLESS`` guard,
the port-conflict degradation and the schema publication.
"""
import logging
import socket
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import trolleybus
from conftest import HttpClient, sqlite_config
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events

from tiny_pacs_admin_web.component import AdminWeb


@pytest.fixture
def live_bus(tmp_path: Path, monkeypatch: pytest.MonkeyPatch
             ) -> Iterator[Callable[..., SimpleNamespace]]:
    """Builds headless buses with a real (port-binding) AdminWeb.

    Factory options: ``web_config`` overrides, ``with_verify`` (a fake
    ``UserVerify`` listener accepting ``secret``) and ``extra`` (a
    callable receiving the bus before start).

    :yield: factory returning ``(bus, web, database, db_path)`` as a
            namespace
    """
    monkeypatch.delenv('TINY_PACS_HEADLESS', raising=False)
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []

    def build(
            web_config: dict[str, Any] | None = None,
            with_verify: bool = True,
            extra: Callable[[trolleybus.EventBus], None] | None = None
    ) -> SimpleNamespace:
        db_path = str(tmp_path / f'comp_{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, sqlite_config(db_path))
        if with_verify:
            bus.subscribe(
                core_events.UserVerify,
                lambda payload: (SimpleNamespace(
                    username=str(payload.get('username')))
                    if payload.get('password') == 'secret' else None)
            )
        if extra is not None:
            extra(bus)
        web = AdminWeb(bus, {'on': True, 'port': 0, **(web_config or {})})
        bus.start()
        started.append((bus, database))
        return SimpleNamespace(bus=bus, web=web, database=database,
                               db_path=db_path)

    yield build

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


def test_start_serves_and_stop_closes(live_bus: Callable[..., Any]) -> None:
    env = live_bus()
    assert env.web.web_port not in (None, 0)
    client = HttpClient(env.web.web_port)
    response = client.get('/login')
    assert response.status == 200
    assert 'Sign in' in response.text
    # on_exit closes the listening socket; the bus stop in the fixture
    # teardown is idempotent
    env.bus.stop()
    with pytest.raises(OSError):
        client.get('/login')


def test_missing_user_verify_disables_component(
        live_bus: Callable[..., Any],
        caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        env = live_bus(with_verify=False)
    assert env.web.web_port is None
    assert any('UserVerify' in record.getMessage()
               and record.levelno >= logging.WARNING
               for record in caplog.records), \
        'the refusal is logged at WARNING'
    # The rest of the bus keeps working
    assert env.bus.send_any(core_events.SchemaVersions, None) is not None


def test_headless_env_guard(live_bus: Callable[..., Any],
                            monkeypatch: pytest.MonkeyPatch,
                            caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setenv('TINY_PACS_HEADLESS', '1')
    with caplog.at_level(logging.INFO):
        env = live_bus()
    assert env.web.web_port is None
    assert any('TINY_PACS_HEADLESS' in record.getMessage()
               for record in caplog.records)


def test_port_conflict_disables_only_the_console(
        live_bus: Callable[..., Any],
        caplog: pytest.LogCaptureFixture) -> None:
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(('127.0.0.1', 0))
    blocker.listen(1)
    port = int(blocker.getsockname()[1])
    try:
        with caplog.at_level(logging.CRITICAL):
            env = live_bus(web_config={'port': port})
        assert env.web.web_port is None
        assert any(record.levelno >= logging.CRITICAL
                   and 'Cannot bind' in record.getMessage()
                   for record in caplog.records), 'CRITICAL is logged'
        # The bus keeps serving: the database and its events still work
        assert env.bus.send_any(core_events.SchemaVersions, None) is not None
    finally:
        blocker.close()


def test_non_loopback_requires_allow_remote_and_warns(
        live_bus: Callable[..., Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    """The refused-at-validation bind plus the loud startup WARNING.

    ``create_server`` is stubbed so the test never binds a real
    non-loopback interface.
    """
    from tiny_pacs_admin_web import component as component_module

    calls: dict[str, Any] = {}

    class _StubServer:
        effective_port = 4242

        def __init__(self, **kwargs: Any) -> None:
            calls.update(kwargs)

        def run(self) -> None:
            calls['ran'] = True

        def close(self) -> None:
            calls['closed'] = True

    def stub_create_server(app: Any, **kwargs: Any) -> Any:
        calls.update(kwargs)
        return _StubServer()

    monkeypatch.setattr(component_module, 'create_server',
                        stub_create_server)
    with caplog.at_level(logging.WARNING):
        env = live_bus(web_config={'host': '10.9.8.7',
                                   'allow_remote': True})
    assert calls.get('host') == '10.9.8.7'
    assert env.web.web_port == 4242
    assert any('NON-LOOPBACK' in record.getMessage()
               for record in caplog.records), 'loud WARNING is logged'


def test_serve_failure_is_logged(live_bus: Callable[..., Any],
                                 monkeypatch: pytest.MonkeyPatch,
                                 caplog: pytest.LogCaptureFixture) -> None:
    """An unexpected serve crash is logged, never raised into the bus."""
    from tiny_pacs_admin_web import component as component_module

    class _ExplodingServer:
        effective_port = 1

        def run(self) -> None:
            raise OSError('simulated socket death')

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        component_module, 'create_server',
        lambda app, **kwargs: _ExplodingServer()
    )
    with caplog.at_level(logging.ERROR):
        env = live_bus()
    env.web._thread.join(timeout=5)  # noqa: SLF001 - test introspection
    assert any('server failed' in record.getMessage()
               and record.levelno >= logging.ERROR
               for record in caplog.records)


def test_migrations_publish_grant_table(live_bus: Callable[..., Any]
                                        ) -> None:
    env = live_bus()
    database = env.database
    assert database.db is not None
    tables = database.db.get_tables()
    assert 'webgrantmodel' in tables
    versions = env.bus.send_any(core_events.SchemaVersions, None)
    assert versions is not None
    assert versions.get('AdminWeb') == 1


def test_config_invalid_non_loopback(live_bus: Callable[..., Any]) -> None:
    """Binding a remote address without the flag is refused early — the
    component under test never comes up (validation happens at config
    load), so the factory itself raises."""
    import pydantic
    with pytest.raises(pydantic.ValidationError):
        live_bus(web_config={'host': '10.9.8.7'})
