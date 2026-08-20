import pytest
import trolleybus

from tiny_pacs import db


def _database(_config):
    bus = trolleybus.EventBus()
    database = db.Database(bus, _config)
    database.on_start()
    return database


def test_string_sqlite_driver():
    database = _database({'driver': 'sqlite', 'db_name': ':memory:'})
    assert database.db is not None


def test_enum_driver_default():
    database = _database({'db_name': ':memory:'})
    assert database.db is not None


def test_invalid_driver():
    with pytest.raises(ValueError):
        _database({'driver': 'mysql', 'db_name': ':memory:'})
