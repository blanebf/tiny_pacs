# -*- coding: utf-8 -*-
import datetime
import enum
from itertools import chain

import peewee

from pydicom import Dataset
from pydicom import valuerep

from . import component
from . import event_bus
from . import questions


class DBDrivers(enum.Enum):
    """Supported DB drivers."""

    #: SQLite
    SQLITE = 'sqlite'

    #: PostgreSQL
    POSTGRES = 'postgres'


class DBChannels(enum.Enum):
    """Available DB channel messages."""

    #: Get an atomic transaction
    ATOMIC = 'db-atomic'

    #: Request a list of available tables from other components
    TABLES = 'db-get-tables'

    #: Requests string aggregate function
    STRING_AGG = 'db-string-agg'


class Database(component.Component):
    """DB component

    Handles database connections, transaction and all database models.
    """
    # TODO: Add thread locking for SQLite, to prevent timeout errors

    def __init__(self, bus: event_bus.EventBus, config: dict):
        """Initializes component

        :param bus: event bus
        :type bus: event_bus.EventBus
        :param config: component config
        :type config: dict
        """
        super().__init__(bus, config)
        self.subscribe(DBChannels.ATOMIC, self.atomic)
        self.subscribe(DBChannels.STRING_AGG, self.string_agg_func)
        self.db = None

    @classmethod
    def interactive(cls):
        return DBQuestionnaire()

    def on_start(self):
        """Handles start event.

        Initializes database and creates tables

        :raises ValueError: raise `ValueError` if unsupported DB driver is
                            provided in component config
        """
        super().on_start()
        db_driver = self.config.get('driver', DBDrivers.SQLITE)
        if db_driver == DBDrivers.SQLITE:
            self.db = self._init_sqlite()
        elif db_driver == DBDrivers.POSTGRES:
            self.db = self._init_postgres()
        else:
            raise ValueError('Unsupported DB driver')

        # Request all available tables
        tables = self.broadcast(DBChannels.TABLES)
        tables = list(chain.from_iterable(tables))

        # Binds all tables to Database instance
        self.db.bind(tables)
        self._create_tables(tables)

    def atomic(self):
        """Create an atomic transaction

        :return: atomic transaction
        :rtype: [type]
        """
        return self.db.atomic()

    def string_agg_func(self):
        if isinstance(self.db, peewee.SqliteDatabase):
            return getattr(peewee.fn, 'group_concat')
        elif isinstance(self.db, peewee.PostgresqlDatabase):
            return getattr(peewee.fn, 'string_agg')
        raise ValueError(f'Unexpected DB object {self.db}')

    def _init_sqlite(self):
        """Initializes SQLite database."""
        db_name = self.config.get('db_name', 'pacs.db')
        uri = self.config.get('uri', True)
        mode = self.config.get('mode', 'memory')
        if uri:
            db_name = f'file:{db_name}?mode={mode}&cache=shared'
        self.log_info('Initialized SQLite database %s', db_name)
        return peewee.SqliteDatabase(db_name, uri=uri)

    def _init_postgres(self):
        """Initializes PostgreSQL database."""
        db_name = self.config.get('db_name', 'tiny_pacs_db')
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 5432)
        user = self.config.get('user', 'postgres')
        password = self.config.get('password', 'postgres')
        self.log_info(
            'Initializing PostgreSQL database with parameters: %s, %d %s',
            host, port, user
        )
        return peewee.PostgresqlDatabase(
            db_name, host=host, port=port, user=user, password=password
        )

    def _create_tables(self, tables: list):
        self.log_debug('Creating %d table', len(tables))
        for table in tables:
            table.create_table(safe=True)


class DBQuestionnaire:
    def __init__(self):
        self.db_driver = questions.Question(
            'driver',
            'Enter DB driver type (sqlite, postgres)',
            lambda v: v, default='sqlite'
        )
        self.sqlite_db_name = questions.Question(
            'db_name', 'Enter SQLite database file name',
            lambda v: v, default=None
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

    def __iter__(self):
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

    def value(self):
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
                'password': self.postgres_password
            }
