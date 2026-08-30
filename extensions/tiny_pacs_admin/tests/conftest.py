"""Shared fixtures for the admin extension tests."""
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import trolleybus
from tiny_pacs import config as core_config
from tiny_pacs import db as core_db
from tiny_pacs import devices as core_devices
from tiny_pacs import server as core_server

from tiny_pacs_admin.store import DeviceStore


def _sqlite_config(db_path: str) -> dict[str, Any]:
    """File-based SQLite configuration for offline administration."""
    return {'driver': 'sqlite', 'db_name': db_path, 'uri': False}


@pytest.fixture
def admin_bus(tmp_path: Path) -> Iterator[Callable[..., Any]]:
    """Starts a headless event bus with a file SQLite database.

    The bus carries the ``Database`` component plus, optionally, the
    ``Devices`` and ``DeviceStore`` components. Tests control what is
    instantiated; every started bus is stopped afterwards.

    :yield: factory ``(devices_conf=None, store_conf=None, db_path=None)``
            returning ``(bus, store, database, db_path)``
    """
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []

    def start(
            devices_conf: dict[str, Any] | None = None,
            store_conf: dict[str, Any] | None = None,
            db_path: str | None = None
    ) -> tuple[trolleybus.EventBus, DeviceStore, core_db.Database, str]:
        path = db_path or str(tmp_path / f'{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, _sqlite_config(path))
        if devices_conf is not None:
            core_devices.Devices(bus, devices_conf)
        store = DeviceStore(bus, store_conf or {})
        bus.start()
        started.append((bus, database))
        return bus, store, database, path

    yield start

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


@pytest.fixture
def config_file(tmp_path: Path) -> Iterator[Callable[..., str]]:
    """Writes admin configuration files for CLI tests.

    :yield: factory ``(extra='', db_name=None)`` returning the path of a
            YAML configuration with a file-based SQLite database
    """
    counter = {'n': 0}

    def write(extra: str = '', db_name: str | None = None) -> str:
        counter['n'] += 1
        name = db_name or str(tmp_path / f'{uuid.uuid4().hex}.db')
        path = tmp_path / f'conf_{counter["n"]}.yaml'
        path.write_text(
            'components:\n'
            '  Database:\n'
            '    on: true\n'
            f'    db_name: {name}\n'
            '    mode: rwc\n'
            f'{extra}'
        )
        return str(path)

    yield write


@pytest.fixture
def pacs_server() -> Iterator[core_server.Server]:
    """Runs a real tiny_pacs server on an OS-assigned port."""
    conf = core_config.Config()
    conf.update_config({
        'ae': {'port': 0},
        'components': {
            'Database': {'on': True, 'db_name': str(uuid.uuid4())}
        }
    })
    srv = core_server.Server(conf)
    srv.start()
    yield srv
    srv.exit()


@pytest.fixture
def pacs_port(pacs_server: core_server.Server) -> int:
    """Actual port the fixture server AE has bound (port 0 = OS-assigned)."""
    assert pacs_server.ae is not None
    return int(pacs_server.ae.server.server_address[1])
