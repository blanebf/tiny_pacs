"""Tests of the schema version management."""
import pathlib
from collections.abc import Callable, Iterable
from typing import Any

import peewee
import pytest
import trolleybus

from tiny_pacs import component, db, events, schema, storage


class DummyModel(peewee.Model):
    """Table used by the tests' migrations."""

    name = peewee.CharField(max_length=64)
    extra = peewee.CharField(max_length=64, null=True)


class DummyComponent(component.Component[component.ComponentConfig]):
    """Component publishing a configurable migration list."""

    def __init__(
            self,
            bus: trolleybus.EventBus,
            migrations: list[schema.Migration]
    ) -> None:
        super().__init__(bus, {})
        self.subscribe(events.Migrations, self.on_migrations)
        self._migrations = migrations

    def on_migrations(self, _: None = None) -> schema.ComponentMigrations:
        return schema.ComponentMigrations(
            'Dummy', [DummyModel], self._migrations
        )


class LegacyComponent(component.Component[component.ComponentConfig]):
    """Component that declares tables but no migrations."""

    def __init__(
            self,
            bus: trolleybus.EventBus,
            tables: list[type[peewee.Model]]
    ) -> None:
        super().__init__(bus, {})
        self.subscribe(events.Tables, self.on_tables)
        self._tables = tables

    def on_tables(self, _: None = None) -> list[type[peewee.Model]]:
        return self._tables


class BothEventsComponent(component.Component[component.ComponentConfig]):
    """Component subscribed to both table and migration events."""

    def __init__(
            self,
            bus: trolleybus.EventBus,
            migrations: list[schema.Migration]
    ) -> None:
        super().__init__(bus, {})
        self.subscribe(events.Tables, self.on_tables)
        self.subscribe(events.Migrations, self.on_migrations)
        self._migrations = migrations

    def on_tables(self, _: None = None) -> list[type[peewee.Model]]:
        return [DummyModel]

    def on_migrations(self, _: None = None) -> schema.ComponentMigrations:
        return schema.ComponentMigrations(
            'Dummy', [DummyModel], self._migrations
        )


def _db_config(db_name: str) -> dict[str, Any]:
    return {'driver': 'sqlite', 'db_name': db_name, 'uri': False}


def _start(
        db_name: str,
        prepare: Callable[[trolleybus.EventBus], Any] | None = None
) -> db.Database:
    bus = trolleybus.EventBus()
    database = db.Database(bus, _db_config(db_name))
    if prepare is not None:
        prepare(bus)
    database.on_start()
    return database


def _close(database: db.Database) -> None:
    assert database.db is not None
    database.db.close()


def _version(name: str) -> int | None:
    row = db.SchemaVersion.get_or_none(db.SchemaVersion.component == name)
    return row.version if row is not None else None


def _columns(database: db.Database) -> set[str]:
    assert database.db is not None
    cursor = database.db.execute_sql('PRAGMA table_info(dummymodel)')
    return {row[1] for row in cursor.fetchall()}


def _create_migration(calls: list[int]) -> schema.Migration:
    def upgrade(_migrator: Any) -> list[Any]:
        calls.append(1)
        DummyModel.create_table(safe=True)
        return []
    return schema.Migration(1, 'Create tables', upgrade)


def _add_column_migration(
        version: int, calls: list[int], fail: bool = False
) -> schema.Migration:
    def upgrade(migrator: Any) -> Iterable[Any]:
        calls.append(version)
        if fail:
            DummyModel.insert(name='rollback-me').execute()
            raise RuntimeError('boom')
        column = peewee.CharField(max_length=10, null=True)
        return [
            migrator.add_column('dummymodel', f'col_v{version}', column)
        ]
    return schema.Migration(version, f'Version {version}', upgrade)


def test_fresh_db_creates_tables_and_stamps_latest(
        tmp_path: pathlib.Path
) -> None:
    calls: list[int] = []
    database = _start(str(tmp_path / 'fresh.db'), lambda bus: DummyComponent(
        bus,
        [_create_migration(calls), _add_column_migration(2, calls)]
    ))
    # Fresh database: tables are created from the current model
    # definitions and the component is stamped to the latest version
    # without running any migration
    assert calls == []
    assert _version('Dummy') == 2
    assert _columns(database) == {'id', 'name', 'extra'}
    _close(database)


def test_restart_applies_nothing(tmp_path: pathlib.Path) -> None:
    db_name = str(tmp_path / 'restart.db')
    first_calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus,
        [_create_migration(first_calls),
         _add_column_migration(2, first_calls)]
    ))
    _close(database)

    second_calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus,
        [_create_migration(second_calls),
         _add_column_migration(2, second_calls)]
    ))
    assert second_calls == []
    assert _version('Dummy') == 2
    _close(database)


def test_pending_migration_applied(tmp_path: pathlib.Path) -> None:
    db_name = str(tmp_path / 'pending.db')
    calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus, [_create_migration(calls)]
    ))
    assert _version('Dummy') == 1
    DummyModel.create(name='keep-me')
    _close(database)

    # A new migration added later is applied to the existing database
    upgrade_calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus,
        [_create_migration(upgrade_calls),
         _add_column_migration(2, upgrade_calls)]
    ))
    assert upgrade_calls == [2]
    assert _version('Dummy') == 2
    assert 'col_v2' in _columns(database)
    assert DummyModel.get(DummyModel.name == 'keep-me') is not None
    _close(database)


def test_legacy_db_runs_full_chain(tmp_path: pathlib.Path) -> None:
    db_name = str(tmp_path / 'legacy.db')

    # Simulate a database created before the migration mechanism
    # existed: the table is there, but there is no schema version row
    database = _start(db_name)
    assert database.db is not None
    database.db.execute_sql(
        'CREATE TABLE dummymodel (id INTEGER NOT NULL PRIMARY KEY, '
        'name VARCHAR(64) NOT NULL)'
    )
    database.db.execute_sql(
        "INSERT INTO dummymodel (name) VALUES ('legacy-row')"
    )
    _close(database)

    calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus,
        [_create_migration(calls), _add_column_migration(2, calls)]
    ))
    # Migration 1 is a no-op for the existing table, migration 2 is
    # applied, the data is preserved
    assert calls == [1, 2]
    assert _version('Dummy') == 2
    assert _columns(database) == {'id', 'name', 'col_v2'}
    # The pre-existing data is preserved; query the column the legacy
    # table actually has
    assert DummyModel.select(DummyModel.name).where(
        DummyModel.name == 'legacy-row'
    ).count() == 1
    _close(database)


def test_failed_migration_rolls_back(tmp_path: pathlib.Path) -> None:
    db_name = str(tmp_path / 'failure.db')
    calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus, [_create_migration(calls)]
    ))
    _close(database)

    def failing(bus: trolleybus.EventBus) -> None:
        upgrade_calls: list[int] = []
        DummyComponent(
            bus,
            [_create_migration(upgrade_calls),
             _add_column_migration(2, upgrade_calls, fail=True)]
        )
    with pytest.raises(RuntimeError):
        _start(db_name, failing)

    database = _start(
        db_name, lambda bus: LegacyComponent(bus, [DummyModel])
    )
    # The failed migration left no trace: neither the version bump nor
    # the data written by it were kept
    assert _version('Dummy') == 1
    assert 'col_v2' not in _columns(database)
    assert DummyModel.select().where(
        DummyModel.name == 'rollback-me'
    ).count() == 0
    _close(database)

    # The fixed migration applies cleanly on the next start
    upgrade_calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus,
        [_create_migration(upgrade_calls),
         _add_column_migration(2, upgrade_calls)]
    ))
    assert upgrade_calls == [2]
    assert _version('Dummy') == 2
    _close(database)


def test_duplicate_versions_rejected(tmp_path: pathlib.Path) -> None:
    calls: list[int] = []

    def duplicate(bus: trolleybus.EventBus) -> None:
        DummyComponent(
            bus,
            [_create_migration(calls), _create_migration(calls)]
        )
    with pytest.raises(RuntimeError):
        _start(str(tmp_path / 'duplicate.db'), duplicate)


def test_migrations_without_tables_rejected(
        tmp_path: pathlib.Path
) -> None:
    calls: list[int] = []

    class NoTablesComponent(component.Component[component.ComponentConfig]):
        def __init__(self, bus: trolleybus.EventBus) -> None:
            super().__init__(bus, {})
            self.subscribe(events.Migrations, self.on_migrations)

        def on_migrations(
                self, _: None = None
        ) -> schema.ComponentMigrations:
            return schema.ComponentMigrations(
                'NoTables', [], [_create_migration(calls)]
            )

    with pytest.raises(RuntimeError):
        _start(
            str(tmp_path / 'no_tables.db'),
            lambda bus: NoTablesComponent(bus)
        )


def test_component_subscribed_to_both_events(
        tmp_path: pathlib.Path
) -> None:
    calls: list[int] = []
    database = _start(str(tmp_path / 'both.db'), lambda bus:
                      BothEventsComponent(
                          bus,
                          [_create_migration(calls),
                           _add_column_migration(2, calls)]
                      ))
    # Tables published through both events are not pre-created by the
    # legacy path, so the fresh database is detected correctly: stamped
    # to the latest version without running any migration
    assert calls == []
    assert _version('Dummy') == 2
    assert _columns(database) == {'id', 'name', 'extra'}
    _close(database)


def test_fresh_db_stamp_failure_rolls_back(
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []

    def broken_set_version(
            self: db.Database, name: str, version: int
    ) -> None:
        raise RuntimeError('stamp failed')

    monkeypatch.setattr(db.Database, '_set_version', broken_set_version)
    with pytest.raises(RuntimeError):
        _start(str(tmp_path / 'stamp_fail.db'), lambda bus: DummyComponent(
            bus,
            [_create_migration(calls),
             _add_column_migration(2, calls)]
        ))

    # The table creation and the version stamp are rolled back together:
    # the database is still fresh for the next start
    monkeypatch.undo()
    database = _start(str(tmp_path / 'stamp_fail.db'))
    assert database.db is not None
    assert not database.db.table_exists('dummymodel')
    assert _version('Dummy') is None
    _close(database)


def test_downgrade_rejected(tmp_path: pathlib.Path) -> None:
    db_name = str(tmp_path / 'downgrade.db')
    calls: list[int] = []
    database = _start(db_name, lambda bus: DummyComponent(
        bus,
        [_create_migration(calls), _add_column_migration(2, calls)]
    ))
    _close(database)

    def downgraded(bus: trolleybus.EventBus) -> None:
        downgrade_calls: list[int] = []
        DummyComponent(bus, [_create_migration(downgrade_calls)])
    with pytest.raises(RuntimeError):
        _start(db_name, downgraded)


def test_tables_only_component(tmp_path: pathlib.Path) -> None:
    database = _start(
        str(tmp_path / 'legacy_component.db'),
        lambda bus: LegacyComponent(bus, [DummyModel])
    )
    assert database.db is not None
    # Tables of components without migrations are created as before and
    # get no schema version row
    assert database.db.table_exists('dummymodel')
    assert _version('Dummy') is None
    _close(database)


def test_storage_implementations_share_schema_key(
        tmp_path: pathlib.Path
) -> None:
    db_name = str(tmp_path / 'storage.db')
    database = _start(
        db_name,
        lambda bus: storage.FileStorage(
            bus, {'storage_dir': str(tmp_path / 'files')}
        )
    )
    assert _version('Storage') == 1
    _close(database)

    database = _start(
        db_name, lambda bus: storage.InMemoryStorage(bus, {})
    )
    # The other storage implementation continues the same version chain
    # instead of starting a new one
    rows = db.SchemaVersion.select().where(
        db.SchemaVersion.component == 'Storage'
    )
    assert rows.count() == 1
    assert _version('Storage') == 1
    _close(database)
