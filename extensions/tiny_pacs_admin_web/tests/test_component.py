"""Tests of the AdminWeb component as a shared-HTTP-server provider.

Covers the ``HttpAppsRegistry`` answer (the console hook contributed to
the core ``HttpServer``), the self-check degradations (missing
``UserVerify``, headless guard), live serving of the console through the
core ``HttpServer`` on an ephemeral port driven via ``urllib`` and the
grant-table schema publication. Bind-level lifecycle (port conflicts,
the waitress thread, bind policy) is tested with the core component and
is not repeated here.
"""
import logging
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
from tiny_pacs import http as core_http

from tiny_pacs_admin_web.component import AdminWeb


def _fake_verify(payload: dict[str, Any]) -> Any:
    """Fake ``UserVerify`` listener accepting ``secret`` for any user."""
    if payload.get('password') == 'secret':
        return SimpleNamespace(username=str(payload.get('username')))
    return None


@pytest.fixture
def live_bus(tmp_path: Path, monkeypatch: pytest.MonkeyPatch
             ) -> Iterator[Callable[..., SimpleNamespace]]:
    """Builds buses with ``AdminWeb`` on a core ``HttpServer``.

    Factory options: ``web_config`` overrides, ``server_config``
    ``HttpServer`` overrides (default: ephemeral port), ``with_verify``
    (the fake ``UserVerify`` listener), ``with_http`` (skip the
    ``HttpServer`` for registry-handler-only tests) and ``extra`` (a
    callable receiving the bus before start).

    :yield: factory returning ``(bus, web, http, database, db_path)`` as
            a namespace
    """
    monkeypatch.delenv('TINY_PACS_HEADLESS', raising=False)
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []

    def build(
            web_config: dict[str, Any] | None = None,
            server_config: dict[str, Any] | None = None,
            with_verify: bool = True,
            with_http: bool = True,
            extra: Callable[[trolleybus.EventBus], None] | None = None
    ) -> SimpleNamespace:
        db_path = str(tmp_path / f'comp_{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, sqlite_config(db_path))
        if with_verify:
            bus.subscribe(core_events.UserVerify, _fake_verify)
        if extra is not None:
            extra(bus)
        web = AdminWeb(bus, {'on': True, **(web_config or {})})
        http_server = None
        if with_http:
            http_server = core_http.HttpServer(
                bus, {'on': True, 'port': 0, **(server_config or {})}
            )
        bus.start()
        started.append((bus, database))
        return SimpleNamespace(bus=bus, web=web, http=http_server,
                               database=database, db_path=db_path)

    yield build

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


# -----------------------------------------------------------------------
# The registry answer
# -----------------------------------------------------------------------

def test_registry_hook_is_the_root_console_app(live_bus: Callable[..., Any]
                                               ) -> None:
    env = live_bus(with_http=False)
    hooks = env.web.http_apps()
    assert len(hooks) == 1
    assert hooks[0].name == 'AdminWeb'
    assert hooks[0].prefix == '/'
    assert callable(hooks[0].app)


def test_registry_handler_has_no_start_ordering_assumption(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """The handler answers on a constructed-but-unstarted bus, so it
    never depends on which component ran ``on_started`` first."""
    monkeypatch.delenv('TINY_PACS_HEADLESS', raising=False)
    bus = trolleybus.EventBus()
    bus.subscribe(core_events.UserVerify, _fake_verify)
    web = AdminWeb(bus, {'on': True})
    hooks = web.http_apps()
    assert len(hooks) == 1 and callable(hooks[0].app)


def test_console_served_by_the_shared_http_server(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus()
    assert env.http is not None
    port = env.http.web_port
    assert port not in (None, 0)
    client = HttpClient(port)
    response = client.get('/login')
    assert response.status == 200
    assert 'Sign in' in response.text
    mounts = env.bus.send_one(core_events.HttpMountsQuery, None)
    assert mounts == [('AdminWeb', '/')]
    # on_exit closes the listening socket; the bus stop in the fixture
    # teardown is idempotent
    env.bus.stop()
    with pytest.raises(OSError):
        client.get('/login')


# -----------------------------------------------------------------------
# Self-check degradations
# -----------------------------------------------------------------------

def test_missing_user_verify_registers_no_hook(
        live_bus: Callable[..., Any],
        caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        env = live_bus(with_verify=False)
    assert env.web.http_apps() == []
    # The shared server got no app at all: it stays dormant but the bus
    # keeps working
    assert env.http is not None
    assert env.http.web_port is None
    assert any('UserVerify' in record.getMessage()
               and record.levelno >= logging.WARNING
               for record in caplog.records), \
        'the refusal is logged at WARNING'
    assert env.bus.send_any(core_events.SchemaVersions, None) is not None


def test_refused_console_does_not_affect_other_mounts(
        live_bus: Callable[..., Any]) -> None:
    """AdminWeb contributes nothing (no user registry) while another
    provider keeps being served by the same ``HttpServer``."""
    def other_provider(_: None) -> list[core_events.HttpAppHook]:
        def app(environ: dict[str, Any],
                start_response: Callable[..., Any]) -> Any:
            start_response('200 OK',
                           [('Content-Type', 'text/plain; charset=utf-8')])
            return [b'other-front-end']

        return [core_events.HttpAppHook(name='Other', prefix='/other',
                                        app=app)]

    env = live_bus(
        with_verify=False,
        extra=lambda bus: bus.subscribe(core_events.HttpAppsRegistry,
                                        other_provider)
    )
    assert env.http is not None
    port = env.http.web_port
    assert port not in (None, 0)
    client = HttpClient(port)
    response = client.get('/other/anything')
    assert response.status == 200
    assert response.body == b'other-front-end'
    # The console root is not mounted: the dispatcher answers 404
    assert client.get('/login').status == 404
    assert env.bus.send_one(core_events.HttpMountsQuery, None) \
        == [('Other', '/other')]


@pytest.mark.parametrize('guard_value', ['1', ''])
def test_headless_env_guard(live_bus: Callable[..., Any],
                            monkeypatch: pytest.MonkeyPatch,
                            caplog: pytest.LogCaptureFixture,
                            guard_value: str) -> None:
    """Set-but-empty counts as headless too (the core ``is_headless``
    presence semantics)."""
    monkeypatch.setenv('TINY_PACS_HEADLESS', guard_value)
    with caplog.at_level(logging.INFO):
        env = live_bus()
        assert env.web.http_apps() == []
    assert env.http is not None
    assert env.http.web_port is None
    assert any('TINY_PACS_HEADLESS' in record.getMessage()
               and 'registers no app' in record.getMessage()
               for record in caplog.records)


# -----------------------------------------------------------------------
# Schema publication
# -----------------------------------------------------------------------

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
