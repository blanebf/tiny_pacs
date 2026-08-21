"""Database component.

Manages database connections and transactions, collects tables from the
other components and creates them on start.
"""
import enum
from collections.abc import Callable, Iterator
from itertools import chain
from typing import Any, cast

import peewee
import trolleybus
from playhouse import pool  # type: ignore[import-untyped]

from . import component, events, questions


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
    host: str = 'localhost'
    port: int = 5432
    user: str = 'postgres'
    password: str = 'postgres'


class Database(component.Component[DatabaseConfig]):
    """DB component

    Handles database connections, transactions and all database models.
    """
    # TODO: Add thread locking for SQLite, to prevent timeout errors

    config_model = DatabaseConfig

    def __init__(self, bus: trolleybus.EventBus,
                 config: DatabaseConfig | dict[str, Any]):
        """Initializes component

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component config
        :type config: DatabaseConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.Atomic, self.atomic)
        self.subscribe(events.StringAgg, self.string_agg_func)
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

        Initializes database and creates tables. The DB driver is validated
        against :class:`DBDrivers` when the configuration is loaded, so by
        this point only supported drivers can reach the component.
        """
        super().on_start()
        db_driver = self.config.driver
        if db_driver == DBDrivers.SQLITE:
            self.db = self._init_sqlite()
        else:
            self.db = self._init_postgres()

        # Request all available tables
        component_tables = self.broadcast(events.Tables, None)
        tables = list(chain.from_iterable(component_tables))

        # Binds all tables to Database instance
        self.db.bind(tables)
        self._create_tables(tables)

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

    def _init_sqlite(self) -> peewee.SqliteDatabase:
        """Initializes SQLite database."""
        config = self.config
        db_name = config.db_name or 'pacs.db'
        if config.uri:
            db_name = f'file:{db_name}?mode={config.mode}&cache=shared'
        self.log_info('Initialized SQLite database %s', db_name)
        return cast(
            peewee.SqliteDatabase,
            pool.PooledSqliteDatabase(db_name, uri=config.uri,
                                      max_connections=config.max_conn)
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
        self.log_debug('Creating %d table', len(tables))
        for table in tables:
            table.create_table(safe=True)


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
