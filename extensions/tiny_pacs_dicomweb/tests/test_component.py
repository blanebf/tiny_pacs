"""Tests of the DICOMWeb component as a shared-HTTP-server provider.

Covers the ``HttpAppsRegistry`` answer (the DICOMweb hook contributed to
the core ``HttpServer`` at the configured prefix), the self-check
degradations (missing ``UserVerify`` for basic auth, headless guard),
live serving through the core ``HttpServer`` on an ephemeral port driven
via ``urllib`` and the isolation of a refused mount from other
providers. Bind-level lifecycle (port conflicts, the waitress thread,
bind policy) is tested with the core component and is not repeated here.
"""
import logging
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import trolleybus
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events
from tiny_pacs import http as core_http
from tiny_pacs import pacs as core_pacs

from tiny_pacs_dicomweb.component import DICOMWeb

from .conftest import RawHTTPClient, sqlite_config


def _fake_verify(payload: dict[str, Any]) -> Any:
    """Fake ``UserVerify`` listener accepting ``secret`` for any user."""
    if payload.get('password') == 'secret':
        return SimpleNamespace(username=str(payload.get('username')))
    return None


@pytest.fixture
def live_bus(tmp_path: Path, monkeypatch: pytest.MonkeyPatch
             ) -> Iterator[Callable[..., SimpleNamespace]]:
    """Builds buses with ``DICOMWeb`` on a core ``HttpServer``.

    Factory options: ``config`` (``DICOMWeb`` overrides),
    ``server_config`` (``HttpServer`` overrides, default ephemeral
    port), ``with_verify`` (the fake ``UserVerify`` listener),
    ``with_http`` (skip the ``HttpServer`` for registry-handler-only
    tests) and ``extra`` (a callable receiving the bus before start).

    :yield: factory returning a namespace with ``bus``, ``component``,
            ``http`` and ``database``
    """
    monkeypatch.delenv('TINY_PACS_HEADLESS', raising=False)
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []

    def build(
            config: dict[str, Any] | None = None,
            server_config: dict[str, Any] | None = None,
            with_verify: bool = True,
            with_http: bool = True,
            extra: Callable[[trolleybus.EventBus], None] | None = None
    ) -> SimpleNamespace:
        db_path = str(tmp_path / f'comp_{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, sqlite_config(db_path))
        core_pacs.PACS(bus, {'on': True})
        if with_verify:
            bus.subscribe(core_events.UserVerify, _fake_verify)
        if extra is not None:
            extra(bus)
        comp = DICOMWeb(bus, {'on': True, **(config or {})})
        http_server = None
        if with_http:
            http_server = core_http.HttpServer(
                bus, {'on': True, 'port': 0, **(server_config or {})}
            )
        bus.start()
        started.append((bus, database))
        return SimpleNamespace(bus=bus, component=comp,
                               http=http_server, database=database,
                               db_path=db_path)

    yield build

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


# -----------------------------------------------------------------------
# The registry answer
# -----------------------------------------------------------------------

def test_registry_hook_uses_the_configured_prefix(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus(with_http=False)
    hooks = env.component.http_apps()
    assert len(hooks) == 1
    assert hooks[0].name == 'DICOMWeb'
    assert hooks[0].prefix == '/dicomweb'
    assert callable(hooks[0].app)


def test_registry_hook_honours_a_custom_prefix(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus(with_http=False, config={'prefix': '/wado-root/'})
    hooks = env.component.http_apps()
    assert hooks[0].prefix == '/wado-root'


def test_registry_handler_has_no_start_ordering_assumption(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """The handler answers on a constructed-but-unstarted bus, so it
    never depends on which component ran ``on_started`` first."""
    monkeypatch.delenv('TINY_PACS_HEADLESS', raising=False)
    bus = trolleybus.EventBus()
    bus.subscribe(core_events.UserVerify, _fake_verify)
    comp = DICOMWeb(bus, {'on': True})
    hooks = comp.http_apps()
    assert len(hooks) == 1 and callable(hooks[0].app)


def test_served_by_the_shared_http_server(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus()
    assert env.http is not None
    port = env.http.web_port
    assert port not in (None, 0)
    client = RawHTTPClient(port)
    response = client.get('/dicomweb/studies')
    assert response.status == 204, 'empty archive answers no content'
    mounts = env.bus.send_one(core_events.HttpMountsQuery, None)
    assert mounts == [('DICOMWeb', '/dicomweb')]
    # on_exit closes the listening socket; the bus stop in the fixture
    # teardown is idempotent
    env.bus.stop()
    with pytest.raises(OSError):
        client.get('/dicomweb/studies')


def test_custom_prefix_is_served(live_bus: Callable[..., Any]) -> None:
    env = live_bus(config={'prefix': '/wado-root'})
    assert env.http is not None
    port = env.http.web_port
    assert port not in (None, 0)
    client = RawHTTPClient(port)
    assert client.get('/wado-root/studies').status == 204
    assert client.get('/dicomweb/studies').status == 404


# -----------------------------------------------------------------------
# Self-check degradations
# -----------------------------------------------------------------------

def test_basic_auth_without_user_registry_registers_no_hook(
        live_bus: Callable[..., Any],
        caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        env = live_bus(with_verify=False, config={'auth': 'basic'})
    assert env.component.http_apps() == []
    # fail closed: nothing is mounted, and the reason is logged loudly
    assert env.http is not None
    assert env.http.web_port is None
    assert any('UserVerify' in record.getMessage()
               and record.levelno >= logging.WARNING
               for record in caplog.records)
    # the bus keeps working
    assert env.bus.send_any(core_events.SchemaVersions, None) is not None


def test_auth_none_does_not_need_a_user_registry(
        live_bus: Callable[..., Any]) -> None:
    env = live_bus(with_verify=False)
    hooks = env.component.http_apps()
    assert len(hooks) == 1


def test_refused_dicomweb_does_not_affect_other_mounts(
        live_bus: Callable[..., Any]) -> None:
    """DICOMWeb contributes nothing (basic without a user registry) while
    another provider keeps being served by the same ``HttpServer``."""
    def other_provider(_: None) -> list[core_events.HttpAppHook]:
        def app(environ: dict[str, Any],
                start_response: Callable[..., Any]) -> Any:
            start_response('200 OK',
                           [('Content-Type', 'text/plain; charset=utf-8')])
            return [b'other-front-end']

        return [core_events.HttpAppHook(name='Other', prefix='/other',
                                        app=app)]

    env = live_bus(
        with_verify=False, config={'auth': 'basic'},
        extra=lambda bus: bus.subscribe(core_events.HttpAppsRegistry,
                                        other_provider)
    )
    assert env.http is not None
    port = env.http.web_port
    assert port not in (None, 0)
    client = RawHTTPClient(port)
    response = client.get('/other/anything')
    assert response.status == 200
    assert response.body == b'other-front-end'
    # DICOMweb is not mounted: the dispatcher answers 404
    assert client.get('/dicomweb/studies').status == 404
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
        assert env.component.http_apps() == []
    assert env.http is not None
    assert env.http.web_port is None
    assert any('TINY_PACS_HEADLESS' in record.getMessage()
               and 'registers no app' in record.getMessage()
               for record in caplog.records)
