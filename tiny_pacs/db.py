# -*- coding: utf-8 -*-
import datetime
import enum
from itertools import chain

import peewee

from pydicom import Dataset
from pydicom import valuerep

from . import component
from . import event_bus

DB = peewee.DatabaseProxy()


class DBDrivers(enum.Enum):
    SQLITE = 'sqlite'
    POSTGRES = 'postgres'


class DBChannels(enum.Enum):
    ATOMIC = 'db-atomic'
    TABLES = 'db-get-tables'


class Database(component.Component):
    # TODO: Add thread locking for SQLite, to prevent timeout errors

    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self.subscribe(DBChannels.ATOMIC, self.atomic)

    def on_start(self):
        super().on_start()
        db_driver = self.config.get('driver', DBDrivers.SQLITE)
        if db_driver == DBDrivers.SQLITE:
            self._init_sqlite()
        elif db_driver == DBDrivers.POSTGRES:
            self._init_postgres()
        else:
            raise ValueError('Unsupported DB driver')
        tables = self.broadcast(DBChannels.TABLES)
        tables = list(chain.from_iterable(tables))
        DB.bind(tables)
        self._create_tables(tables)

    def atomic(self):
        return DB.atomic()

    def _init_sqlite(self):
        db_name = self.config.get('db_name')
        self.log_info('Initializing SQLite databse %s', db_name)
        if db_name:
            DB.initialize(peewee.SqliteDatabase(db_name))
        else:
            DB.initialize(
                peewee.SqliteDatabase('file:pacs?mode=memory&cache=shared', uri=True)
            )

    def _init_postgres(self):
        db_name = self.config.get('db_name', 'tiny_pacs_db')
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 'port')
        user = self.config.get('user', 'postgres')
        password = self.config.get('password', 'postgres')
        self.log_info('Initializing PostgreSQL databse with parameters: %s, %d %s',
                      host, port, user)
        DB.initialize(peewee.PostgresqlDatabase(
            db_name, host=host, port=port, user=user, password=password
        ))

    def _create_tables(self, tables: list):
        self.log_debug('Creating %d table', len(tables))
        for table in tables:
            table.create_table(safe=True)


def string_agg_func():
    if isinstance(DB.obj, peewee.SqliteDatabase):
        return getattr(peewee.fn, 'group_concat')
    elif isinstance(DB.obj, peewee.PostgresqlDatabase):
        return getattr(peewee.fn, 'string_agg')
    raise ValueError(f'Unexpected DB object {DB.obj}')
