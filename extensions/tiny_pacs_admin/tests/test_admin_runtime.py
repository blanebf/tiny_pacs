"""Tests of the headless admin runtime."""
from typing import Any

import pytest

from tiny_pacs_admin import events as admin_events
from tiny_pacs_admin import runtime


def _file_db_config(tmp_path_factory: Any) -> dict[str, Any]:
    db_name = str(tmp_path_factory.mktemp('db') / 'pacs.db')
    return {
        'components': {
            'Database': {'on': True, 'db_name': db_name, 'mode': 'rwc'},
            'DeviceStore': {'on': True}
        }
    }


def test_memory_mode_rejected(tmp_path_factory: Any) -> None:
    # The default SQLite configuration is in-memory
    conf = {'components': {
        'Database': {'on': True, 'mode': 'memory'}
    }}
    with pytest.raises(runtime.AdminError, match='persistent database'):
        with runtime.admin_context(conf):
            raise AssertionError('admin_context must not start')


def test_memory_mode_default_rejected() -> None:
    # No configuration at all means the default (memory) database
    with pytest.raises(runtime.AdminError, match='persistent database'):
        with runtime.admin_context(None):
            raise AssertionError('admin_context must not start')


def test_file_mode_allowed(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with runtime.admin_context(conf) as (bus, database):
        assert database.db is not None


def test_admin_context_starts_bus(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with runtime.admin_context(conf) as (bus, _):
        row = bus.send_one(admin_events.DeviceAdd, {
            'aet': 'RT_DEV', 'address': '10.0.0.20', 'port': 104
        })
        assert row.aet == 'RT_DEV'
        devices = bus.send_one(admin_events.DeviceList, None)
        assert [d.aet for d in devices] == ['RT_DEV']


def test_admin_context_forces_disabled_component(
        tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    # Disable the component in configuration; administration still manages
    conf['components']['DeviceStore']['on'] = False
    with runtime.admin_context(
            conf, components=['DeviceStore']) as (bus, _):
        devices = bus.send_one(admin_events.DeviceList, None)
        assert devices == []


def test_admin_context_unknown_component(
        tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with pytest.raises(runtime.AdminError, match='Unknown component'):
        with runtime.admin_context(conf, components=['NoSuchComponent']):
            raise AssertionError('admin_context must not start')


def test_admin_context_default_components(
        tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    # components=None defaults to every enabled component: DeviceStore is
    # on here, so its CRUD events must be answerable
    with runtime.admin_context(conf) as (bus, _):
        devices = bus.send_one(admin_events.DeviceList, None)
        assert devices == []


def test_admin_context_accepts_dict_source(
        tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    # Configuration sources are the same ones ``update_config`` accepts
    with runtime.admin_context(conf) as (bus, _):
        assert bus is not None


def test_device_store_not_required(tmp_path_factory: Any) -> None:
    # A configuration without the DeviceStore component still works when
    # only other components are managed
    db_name = str(tmp_path_factory.mktemp('db') / 'pacs.db')
    conf = {'components': {
        'Database': {'on': True, 'db_name': db_name, 'mode': 'rwc'}
    }}
    with runtime.admin_context(conf, components=[]) as (bus, database):
        assert bus is not None
        assert database.db is not None
