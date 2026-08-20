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


class Database(component.Component):
    """DB component

    Handles database connections, transaction and all database models.
    """
    # TODO: Add thread locking for SQLite, to prevent timeout errors

    def __init__(self, bus: trolleybus.EventBus, config: dict[str, Any]):
        """Initializes component

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component config
        :type config: dict
        """
        super().__init__(bus, config)
        self.subscribe(events.Atomic, self.atomic)
        self.subscribe(events.StringAgg, self.string_agg_func)
        self.db: peewee.Database | None = None

    @classmethod
    def interactive(cls) -> 'DBQuestionnaire':
        return DBQuestionnaire()

    def on_start(self) -> None:
        """Handles start event.

        Initializes database and creates tables

        :raises ValueError: raise `ValueError` if unsupported DB driver is
                            provided in component config
        """
        super().on_start()
        db_driver = self.config.get('driver', DBDrivers.SQLITE)
        if isinstance(db_driver, str):
            db_driver = DBDrivers(db_driver)
        if db_driver == DBDrivers.SQLITE:
            self.db = self._init_sqlite()
        elif db_driver == DBDrivers.POSTGRES:
            self.db = self._init_postgres()
        else:
            raise ValueError('Unsupported DB driver')

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

    def string_agg_func(self, _: None = None) -> Callable[..., peewee.Function]:
        if isinstance(self.db, peewee.SqliteDatabase):
            return peewee.fn.group_concat
        if isinstance(self.db, peewee.PostgresqlDatabase):
            return peewee.fn.string_agg
        raise ValueError(f'Unexpected DB object {self.db}')

    def _init_sqlite(self) -> peewee.SqliteDatabase:
        """Initializes SQLite database."""
        db_name = self.config.get('db_name', 'pacs.db')
        uri = self.config.get('uri', True)
        mode = self.config.get('mode', 'memory')
        max_conn = self.config.get('max_conn', 20)
        if uri:
            db_name = f'file:{db_name}?mode={mode}&cache=shared'
        self.log_info('Initialized SQLite database %s', db_name)
        return cast(
            peewee.SqliteDatabase,
            pool.PooledSqliteDatabase(db_name, uri=uri, max_connections=max_conn)
        )

    def _init_postgres(self) -> peewee.PostgresqlDatabase:
        """Initializes PostgreSQL database."""
        db_name = self.config.get('db_name', 'tiny_pacs_db')
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 5432)
        user = self.config.get('user', 'postgres')
        password = self.config.get('password', 'postgres')
        max_conn = self.config.get('max_conn', 20)
        self.log_info(
            'Initializing PostgreSQL database with parameters: %s, %d %s',
            host, port, user
        )
        return cast(
            peewee.PostgresqlDatabase,
            pool.PooledPostgresqlDatabase(
                db_name, host=host, port=port, user=user, password=password,
                max_connections=max_conn
            )
        )

    def _create_tables(self, tables: list[type[peewee.Model]]) -> None:
        self.log_debug('Creating %d table', len(tables))
        for table in tables:
            table.create_table(safe=True)


class DBQuestionnaire(questions.Questionnaire):
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
