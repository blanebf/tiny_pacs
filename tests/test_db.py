import threading
from pathlib import Path
from typing import Any

import pytest
import trolleybus

from tiny_pacs import db, events


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


def test_close_connection_releases_pool_slot() -> None:
    """CloseConnection returns the calling thread's pooled connection.

    The startup connection of the main thread occupies the single pool
    slot; every worker thread must get and release that slot in turn,
    otherwise the second thread already fails with
    ``playhouse.pool.MaxConnectionsExceeded``.
    """
    bus = trolleybus.EventBus()
    database = db.Database(bus, {
        'driver': 'sqlite', 'db_name': ':memory:', 'max_conn': 1
    })
    database.on_start()
    assert database.db is not None
    bus.broadcast(events.CloseConnection, None)
    errors: list[BaseException] = []

    def work() -> None:
        try:
            assert database.db is not None
            database.db.execute_sql('SELECT 1').fetchone()
            assert not database.db.is_closed()
            bus.broadcast(events.CloseConnection, None)
            assert database.db.is_closed()
        except BaseException as error:  # noqa: B036 - collected below
            errors.append(error)

    for _ in range(3):
        thread = threading.Thread(target=work)
        thread.start()
        thread.join()
    assert not errors


def test_close_connection_without_database() -> None:
    """CloseConnection before start is a silent no-op."""
    bus = trolleybus.EventBus()
    database = db.Database(bus, {
        'driver': 'sqlite', 'db_name': ':memory:'
    })
    bus.broadcast(events.CloseConnection, None)
    assert database.db is None
