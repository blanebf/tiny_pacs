"""Database component.

Manages database connections and transactions, collects tables and schema
migrations from the other components and applies them on start.

Every component owns its tables and its own schema version. Components
publish their migrations through the :class:`~tiny_pacs.events.Migrations`
event (:class:`~tiny_pacs.schema.ComponentMigrations`); the
:class:`Database` component applies pending ones with peewee's built-in
manual migrations and records the applied versions in the
:class:`SchemaVersion` table.
"""
import datetime
import enum
from collections.abc import Callable, Iterator
from itertools import chain
from typing import Any, cast

import peewee
import trolleybus
from playhouse import migrate as pw_migrate  # type: ignore[import-untyped]
from playhouse import pool

from . import component, events, questions, schema

#: Advisory lock key serializing schema migrations between PostgreSQL
#: processes starting against the same database
MIGRATIONS_LOCK_KEY = 7480740


class DBDrivers(enum.Enum):
    """Supported DB drivers."""

    #: SQLite
    SQLITE = 'sqlite'

    #: PostgreSQL
    POSTGRES = 'postgres'


class DatabaseConfig(component.ComponentConfig):
    """Configuration of the :class:`Database` component.

    :ivar driver: DB driver to use
    :ivar db_name: database (file) name
    :ivar uri: connect via URI (SQLite only)
    :ivar mode: SQLite URI open mode (``memory``, ``rwc``, ...)
    :ivar max_conn: maximum number of pooled connections
    :ivar wal: enable the SQLite WAL journal mode (ignored by PostgreSQL)
    :ivar busy_timeout: SQLite ``busy_timeout`` in milliseconds; how long
                        a blocked write waits for the lock instead of
                        failing with ``database is locked``
    :ivar host: PostgreSQL host
    :ivar port: PostgreSQL port
    :ivar user: PostgreSQL user
    :ivar password: PostgreSQL password
    """

    driver: DBDrivers = DBDrivers.SQLITE
    db_name: str | None = None
    uri: bool = True
    mode: str = 'memory'
    max_conn: int = 20
    wal: bool = True
    busy_timeout: int = 10000
    host: str = 'localhost'
    port: int = 5432
    user: str = 'postgres'
    password: str = 'postgres'


class SchemaVersion(peewee.Model):
    """Applied schema version of a component.

    Maintained by the :class:`Database` component itself: one row per
    component schema (see :meth:`tiny_pacs.component.Component.schema`)
    holding the highest migration version applied to the tables of that
    component. A missing row is equivalent to version 0.
    """

    #: Component schema name
    component = peewee.CharField(max_length=64, unique=True)

    #: Highest applied migration version of the component
    version = peewee.IntegerField(default=0)

    #: When the version was last updated
    applied = peewee.DateTimeField()


class Database(component.Component[DatabaseConfig]):
    """DB component

    Handles database connections, transactions, schema version bookkeeping
    and migrations of all database models.
    """
    # TODO: Add thread locking for SQLite, to prevent timeout errors

    config_model = DatabaseConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: DatabaseConfig | dict[str, Any]
    ) -> None:
        """Initializes component

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component config
        :type config: DatabaseConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.Atomic, self.atomic)
        self.subscribe(events.StringAgg, self.string_agg_func)
        self.subscribe(events.SchemaVersions, self.schema_versions)
        self.subscribe(events.TableCounts, self.table_counts)
        self.subscribe(events.CloseConnection, self.close_connection)
        self.db: peewee.Database | None = None

    @classmethod
    def interactive(cls) -> 'DBQuestionnaire':
        """Returns interactive questionnaire for component configuration

        :return: DB configuration questionnaire
        :rtype: DBQuestionnaire
        """
        return DBQuestionnaire()

    def on_start(self) -> None:
        """Handles start event.

        Initializes database, collects tables and schema migrations from
        the other components and applies pending migrations. The DB driver
        is validated against :class:`DBDrivers` when the configuration is
        loaded, so by this point only supported drivers can reach the
        component.
        """
        super().on_start()
        db_driver = self.config.driver
        if db_driver == DBDrivers.SQLITE:
            self.db = self._init_sqlite()
        else:
            self.db = self._init_postgres()

        SchemaVersion.bind(self.db)
        SchemaVersion.create_table(safe=True)

        # Request tables of components that do not participate in
        # migrations; those tables are simply created (legacy behavior)
        component_tables = self.broadcast(events.Tables, None)
        legacy_tables = list(chain.from_iterable(component_tables))
        # Request tables and migrations of the other components
        component_migrations = self.broadcast(events.Migrations, None)
        managed_tables = list(chain.from_iterable(
            item.tables for item in component_migrations
        ))
        # Tables published through both events are handled by the
        # migration mechanism only; creating them here would poison the
        # fresh/legacy detection below
        managed = set(managed_tables)
        legacy_tables = [t for t in legacy_tables if t not in managed]

        # Binds all tables to Database instance
        self.db.bind(legacy_tables + managed_tables)
        self._create_tables(legacy_tables)

        migrator = self._make_migrator()
        # The migration phase runs in one transaction so that a crash
        # never leaves a half-migrated database: the version bookkeeping
        # is only committed together with the schema changes. PostgreSQL
        # startups are additionally serialized with an advisory lock, so
        # concurrent processes cannot race on detection and application;
        # on SQLite the write lock of the transaction provides the same
        # serialization.
        with self.atomic():
            if isinstance(self.db, peewee.PostgresqlDatabase):
                execute_sql = cast(
                    Callable[[str, tuple[int, ...]], Any],
                    self.db.execute_sql
                )
                execute_sql(
                    'SELECT pg_advisory_xact_lock(%s)',
                    (MIGRATIONS_LOCK_KEY,)
                )
            for item in component_migrations:
                self._apply_migrations(item, migrator)

    def atomic(self, _: None = None) -> Any:
        """Create an atomic transaction

        :return: atomic transaction
        """
        if self.db is None:
            raise RuntimeError('Database is not initialized')
        return self.db.atomic()

    def string_agg_func(self, _: None = None
                        ) -> Callable[..., peewee.Function]:
        """Handles `StringAgg` event.

        Returns the string aggregate SQL function of the active DB driver.

        :return: aggregate function
        :rtype: Callable[..., peewee.Function]
        :raises ValueError: raised for an unexpected DB object
        """
        if isinstance(self.db, peewee.SqliteDatabase):
            return peewee.fn.group_concat
        if isinstance(self.db, peewee.PostgresqlDatabase):
            return peewee.fn.string_agg
        raise ValueError(f'Unexpected DB object {self.db}')

    def schema_versions(self, _: None = None) -> dict[str, int]:
        """Handles `SchemaVersions` event.

        :return: applied schema version of every component
        :rtype: dict[str, int]
        :raises RuntimeError: raised when the database is not initialized
        """
        if self.db is None:
            raise RuntimeError('Database is not initialized')
        return {
            row.component: row.version
            for row in SchemaVersion.select()
        }

    def table_counts(self, _: None = None) -> dict[str, int]:
        """Handles `TableCounts` event.

        :return: row count of every table of the database
        :rtype: dict[str, int]
        :raises RuntimeError: raised when the database is not initialized
        """
        if self.db is None:
            raise RuntimeError('Database is not initialized')
        counts: dict[str, int] = {}
        for table in sorted(self.db.get_tables()):
            cursor = self.db.execute_sql(f'SELECT COUNT(*) FROM "{table}"')
            row = cursor.fetchone()
            counts[table] = int(row[0]) if row is not None else 0
        return counts

    def close_connection(self, _: None = None) -> None:
        """Handles `CloseConnection` event.

        Closes the database connection of the calling thread; peewee tracks
        connections per thread, so this runs in the thread that performed
        the queries. With the pooled drivers (see :class:`DatabaseConfig`)
        the connection is returned to the pool instead of being destroyed:
        worker threads (association threads, HTTP handlers) must send this
        event when their unit of work ends, because the pool only reclaims
        connections on close and thread exits alone leak them permanently.
        """
        if self.db is not None and not self.db.is_closed():
            self.db.close()

    def _init_sqlite(self) -> peewee.SqliteDatabase:
        """Initializes SQLite database."""
        config = self.config
        db_name = config.db_name or 'pacs.db'
        if config.uri:
            db_name = f'file:{db_name}?mode={config.mode}&cache=shared'
        pragmas: dict[str, Any] = {'busy_timeout': config.busy_timeout}
        if config.wal:
            pragmas['journal_mode'] = 'wal'
        self.log_info(
            'Initialized SQLite database %s (wal=%s, busy_timeout=%dms)',
            db_name, config.wal, config.busy_timeout
        )
        # ``check_same_thread=False`` is required for the pool: every
        # thread (association, HTTP worker) checks connections out and
        # back in, so a returned connection is legitimately reused by
        # another thread and sqlite3's same-thread guard would reject
        # it. The pool hands a connection to at most one thread at a
        # time and the sqlite3 module is serialized, so the reuse is
        # safe.
        return cast(
            peewee.SqliteDatabase,
            pool.PooledSqliteDatabase(
                db_name, uri=config.uri, max_connections=config.max_conn,
                pragmas=pragmas, check_same_thread=False
            )
        )

    def _init_postgres(self) -> peewee.PostgresqlDatabase:
        """Initializes PostgreSQL database."""
        config = self.config
        db_name = config.db_name or 'tiny_pacs_db'
        self.log_info(
            'Initializing PostgreSQL database with parameters: %s, %d %s',
            config.host, config.port, config.user
        )
        return cast(
            peewee.PostgresqlDatabase,
            pool.PooledPostgresqlDatabase(
                db_name, host=config.host, port=config.port, user=config.user,
                password=config.password, max_connections=config.max_conn
            )
        )

    def _create_tables(self, tables: list[type[peewee.Model]]) -> None:
        self.log_debug('Creating %d table(s)', len(tables))
        for table in tables:
            table.create_table(safe=True)

    def _make_migrator(self) -> Any:
        """Returns the schema migrator matching the active DB driver.

        :return: driver-specific ``playhouse.migrate`` migrator
        :raises ValueError: raised for an unexpected DB object
        """
        if isinstance(self.db, peewee.SqliteDatabase):
            return pw_migrate.SqliteMigrator(self.db)
        if isinstance(self.db, peewee.PostgresqlDatabase):
            return pw_migrate.PostgresqlMigrator(self.db)
        raise ValueError(f'Unexpected DB object {self.db}')

    def _apply_migrations(
            self,
            item: schema.ComponentMigrations,
            migrator: Any
    ) -> None:
        """Applies pending schema migrations of one component.

        Fresh databases (no version row and no tables) get their tables
        created from the current model definitions, which already include
        every migration, and are stamped to the latest version directly.
        Databases that have the tables but no version row were created
        before the migration mechanism existed; the whole migration chain
        runs over them — the first migration creates tables with
        ``safe=True`` and is a no-op for the existing tables.

        The method runs within the migration transaction of
        :meth:`on_start`, so nothing is committed if the phase fails.

        :param item: schema name, tables and migrations of one component
        :type item: schema.ComponentMigrations
        :param migrator: driver-specific migrator
        :raises RuntimeError: raised on duplicate migration versions, when
                              migrations are published without tables, or
                              when the database schema version is newer
                              than the latest known migration
        """
        name = item.component
        ordered = sorted(item.migrations, key=lambda m: m.version)
        versions = [m.version for m in ordered]
        if len(versions) != len(set(versions)):
            raise RuntimeError(
                f'Duplicate migration versions in component {name}: '
                f'{versions}'
            )
        if not ordered:
            # The component publishes tables but no migrations yet
            self._create_tables(item.tables)
            return
        if not item.tables:
            raise RuntimeError(
                f'Component {name} publishes migrations without tables'
            )
        latest = ordered[-1].version
        row = SchemaVersion.get_or_none(SchemaVersion.component == name)
        if row is not None:
            if row.version > latest:
                raise RuntimeError(
                    f'Component {name}: database schema version '
                    f'{row.version} is newer than the latest known '
                    f'version {latest}, refusing to start'
                )
            pending = [m for m in ordered if m.version > row.version]
        elif self._tables_exist(item.tables):
            self.log_info(
                'Found pre-existing tables of %s without a recorded '
                'schema version, running migrations from version %d',
                name, ordered[0].version
            )
            pending = ordered
        else:
            self._create_tables(item.tables)
            self._set_version(name, latest)
            return

        for migration in pending:
            # Another process starting against the same database may have
            # applied the migration while this one waited for the lock
            row = SchemaVersion.get_or_none(SchemaVersion.component == name)
            if row is not None and row.version >= migration.version:
                continue
            self.log_info(
                'Migrating %s to version %d: %s',
                name, migration.version, migration.description
            )
            with self.atomic():
                operations = migration.upgrade(migrator) or []
                pw_migrate.migrate(*operations)
                self._set_version(name, migration.version)

    def _tables_exist(self, tables: list[type[peewee.Model]]) -> bool:
        """Checks whether any of the given tables exists in the database.

        :param tables: tables to check
        :type tables: list[type[peewee.Model]]
        :return: True when at least one table exists
        :rtype: bool
        """
        assert self.db is not None
        return any(
            self.db.table_exists(table._meta.table_name) for table in tables
        )

    def _set_version(self, name: str, version: int) -> None:
        """Records the applied schema version of a component.

        Must be called within the transaction of the applied migration.
        The insert falls back to an update when a concurrent process
        created the row in the meantime.

        :param name: component schema name
        :type name: str
        :param version: applied migration version
        :type version: int
        """
        applied = datetime.datetime.now(datetime.timezone.utc)
        updated = SchemaVersion\
            .update(version=version, applied=applied)\
            .where(SchemaVersion.component == name)\
            .execute()
        if updated:
            return
        try:
            SchemaVersion\
                .insert(component=name, version=version, applied=applied)\
                .execute()
        except peewee.IntegrityError:
            SchemaVersion\
                .update(version=version, applied=applied)\
                .where(SchemaVersion.component == name)\
                .execute()


class DBQuestionnaire(questions.Questionnaire):
    """Interactive questionnaire for the :class:`Database` component."""

    def __init__(self) -> None:
        self.db_driver = questions.Question(
            'driver',
            'Enter DB driver type (sqlite, postgres)',
            lambda v: v, default='sqlite'
        )
        self.sqlite_db_name = questions.Question(
            'db_name', 'Enter SQLite database file name',
            lambda v: v, default=None, default_repr=':memory:'
        )
        self.postgres_db_name = questions.Question(
            'db_name', 'Enter PostgreSQL database name',
            lambda v: v, default='tiny_pacs_db'
        )
        self.postgres_db_host = questions.Question(
            'host', 'Enter PostgreSQL host name',
            lambda v: v, default='localhost'
        )
        self.postgres_port = questions.Question(
            'port', 'Enter PostgreSQL port',
            int, default='5432'
        )
        self.postgres_user = questions.Question(
            'user', 'Enter PostgreSQL username',
            lambda v: v, default='postgres'
        )
        self.postgres_password = questions.Question(
            'password', 'Enter PostgreSQL password',
            lambda v: v, default='postgres'
        )
        super().__init__([
            self.db_driver, self.sqlite_db_name, self.postgres_db_name,
            self.postgres_db_host, self.postgres_port, self.postgres_user,
            self.postgres_password
        ])

    def __iter__(self) -> Iterator[questions.Question]:
        """Yields questions for the selected DB driver"""
        yield self.db_driver
        if self.db_driver.value == 'sqlite':
            yield self.sqlite_db_name
        elif self.db_driver.value == 'postgres':
            yield self.postgres_db_name
            yield self.postgres_db_host
            yield self.postgres_port
            yield self.postgres_user
            yield self.postgres_password
        else:
            raise ValueError(f'Unsupported DB driver {self.db_driver.value}')

    def value(self) -> dict[str, Any]:
        """Returns the collected DB configuration values

        :return: DB configuration dictionary
        :rtype: dict[str, Any]
        """
        if self.db_driver.value == 'sqlite':
            return {
                'db_name': self.sqlite_db_name.value
            }
        else:
            return {
                'db_name': self.postgres_db_name.value,
                'host': self.postgres_db_host.value,
                'port': self.postgres_port.value,
                'user': self.postgres_user.value,
                'password': self.postgres_password.value
            }
