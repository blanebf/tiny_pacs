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
