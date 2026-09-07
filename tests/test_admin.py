"""Tests of the headless administration runtime."""
from typing import Any

import pytest

from tiny_pacs import admin, events


def _file_db_config(tmp_path_factory: Any) -> dict[str, Any]:
    db_name = str(tmp_path_factory.mktemp('db') / 'pacs.db')
    storage_dir = str(tmp_path_factory.mktemp('storage'))
    return {
        'components': {
            'Database': {'on': True, 'db_name': db_name, 'mode': 'rwc'},
            'FileStorage': {'on': True, 'storage_dir': storage_dir},
            # The built-in default storage would also answer the
            # maintenance events
            'InMemoryStorage': {'on': False}
        }
    }


def test_memory_mode_rejected(tmp_path_factory: Any) -> None:
    # The default SQLite configuration is in-memory
    conf: dict[str, Any] = {'components': {
        'Database': {'on': True, 'mode': 'memory'}
    }}
    with pytest.raises(admin.AdminError, match='persistent database'):
        with admin.admin_context(conf):
            raise AssertionError('admin_context must not start')


def test_memory_mode_default_rejected() -> None:
    # No configuration at all means the default (memory) database
    with pytest.raises(admin.AdminError, match='persistent database'):
        with admin.admin_context(None):
            raise AssertionError('admin_context must not start')


def test_file_mode_allowed(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with admin.admin_context(conf) as (bus, database):
        assert database.db is not None


def test_admin_context_starts_bus(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with admin.admin_context(conf) as (bus, _):
        # The started bus answers the database inspection events
        counts = bus.send_one(events.TableCounts, None)
        assert 'storagefiles' in counts
        assert counts['storagefiles'] == 0


def test_admin_context_forces_disabled_component(
        tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    # Disable the component in configuration; administration still manages
    conf['components']['FileStorage']['on'] = False
    with admin.admin_context(conf, components=['FileStorage']) as (bus, _):
        report = bus.send_one(events.StorageStatsQuery, None)
        assert report.records_total == 0


def test_admin_context_unknown_component(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with pytest.raises(admin.AdminError, match='Unknown component'):
        with admin.admin_context(conf, components=['NoSuchComponent']):
            raise AssertionError('admin_context must not start')


def test_admin_context_default_components(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    # components=None defaults to every enabled component: FileStorage is
    # on here, so its maintenance events must be answerable
    with admin.admin_context(conf) as (bus, _):
        report = bus.send_one(events.StorageVerifyQuery, None)
        assert report.file_backend is True


def test_schema_versions_event(tmp_path_factory: Any) -> None:
    conf = _file_db_config(tmp_path_factory)
    with admin.admin_context(conf) as (bus, _):
        versions = bus.send_one(events.SchemaVersions, None)
        # The storage components record their shared schema version
        assert versions.get('Storage') == 1


def test_storage_component_not_required(tmp_path_factory: Any) -> None:
    # A configuration without any managed component still works
    db_name = str(tmp_path_factory.mktemp('db') / 'pacs.db')
    conf: dict[str, Any] = {'components': {
        'Database': {'on': True, 'db_name': db_name, 'mode': 'rwc'}
    }}
    with admin.admin_context(conf, components=[]) as (bus, database):
        assert bus is not None
        assert database.db is not None
