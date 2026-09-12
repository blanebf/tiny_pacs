from pathlib import Path
from typing import Any

import pytest
import trolleybus

from tiny_pacs import db


def _database(_config: dict[str, Any]) -> db.Database:
    bus = trolleybus.EventBus()
    database = db.Database(bus, _config)
    database.on_start()
    return database


def test_string_sqlite_driver() -> None:
    database = _database({'driver': 'sqlite', 'db_name': ':memory:'})
    assert database.db is not None


def test_enum_driver_default() -> None:
    database = _database({'db_name': ':memory:'})
    assert database.db is not None


def test_invalid_driver() -> None:
    with pytest.raises(ValueError):
        _database({'driver': 'mysql', 'db_name': ':memory:'})


def _pragma(database: db.Database, name: str) -> Any:
    assert database.db is not None
    cursor = database.db.execute_sql(f'PRAGMA {name}')
    row = cursor.fetchone()
    return row[0] if row is not None else None


def test_sqlite_wal_default() -> None:
    config = db.DatabaseConfig()
    assert config.wal is True
    assert config.busy_timeout == 10000


def test_sqlite_wal_and_busy_timeout(tmp_path: Path) -> None:
    database = _database({
        'driver': 'sqlite',
        'db_name': str(tmp_path / 'test.db'),
        'mode': 'rwc'
    })
    assert _pragma(database, 'journal_mode') == 'wal'
    assert _pragma(database, 'busy_timeout') == 10000


def test_sqlite_wal_disabled(tmp_path: Path) -> None:
    database = _database({
        'driver': 'sqlite',
        'db_name': str(tmp_path / 'test.db'),
        'mode': 'rwc',
        'wal': False,
        'busy_timeout': 2500
    })
    assert _pragma(database, 'journal_mode') == 'delete'
    assert _pragma(database, 'busy_timeout') == 2500


def test_sqlite_memory_mode_ignores_wal() -> None:
    database = _database({'driver': 'sqlite', 'db_name': ':memory:'})
    assert database.db is not None
