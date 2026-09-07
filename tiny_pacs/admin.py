"""Headless administration runtime.

Administration commands work offline: :func:`admin_context` loads the YAML
configuration, builds an event bus with the ``Database`` component plus
the components being managed, starts the bus (fires ``OnStart``: DB init,
table binding, migrations) and stops it afterwards. No AE/server thread
is started.

This helper is part of the documented extension-facing API: every
CLI-bearing extension runs its commands through it.

Administration against SQLite requires a file-based database
(``db_name`` with ``mode: rwc``); the in-memory default cannot persist
anything between command invocations and is rejected with a friendly
error.
"""
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any

import trolleybus

from . import config, db


class AdminError(Exception):
    """Administration runtime error.

    Raised for unusable configurations (e.g. an in-memory SQLite
    database) or unknown components; CLI commands report it and exit.
    """


@contextmanager
def admin_context(
        config_files: 'config.ConfigInput | None',
        components: Iterable[str] | None = None
) -> Iterator[tuple[trolleybus.EventBus, db.Database]]:
    """Runs a headless administration session.

    Loads the configuration, instantiates the ``Database`` component plus
    the requested components, starts the event bus, yields it and stops
    the bus on exit — no AE/server thread is started.

    :param config_files: configuration source(s) as accepted by
                         :meth:`tiny_pacs.config.Config.update_config`
    :param components: names of components to manage in addition to
                       ``Database``; they are enabled regardless of their
                       ``on`` flag, since administration must not depend
                       on the server configuration. Defaults to every
                       enabled component of the configuration.
    :yield: the running event bus and the ``Database`` component
    :raises AdminError: raised when the configured database cannot
                        persist data between command invocations
                        (SQLite memory mode) or when a requested
                        component is unknown
    """
    conf = config.Config()
    if config_files is not None:
        conf.update_config(config_files)
    _check_db_config(conf)

    bus = trolleybus.EventBus()
    database_config = conf.components['Database']
    assert isinstance(database_config, db.DatabaseConfig)
    database = db.Database(bus, database_config)

    names = _component_names(conf, components)
    for name in names:
        factory = config.COMPONENT_REGISTRY.get(name)
        if factory is None:
            raise AdminError(
                f'Unknown component {name!r}; is the extension providing '
                f'it installed?'
            )
        component_config = conf.components.get(name)
        if component_config is None:
            data: dict[str, Any] = {'on': True}
        else:
            data = {**component_config.model_dump(), 'on': True}
        factory(bus, data)

    bus.start()
    try:
        yield bus, database
    finally:
        bus.stop()


def _component_names(
        conf: config.Config, components: Iterable[str] | None
) -> list[str]:
    """Resolves the components to instantiate.

    Explicitly requested components keep the given order; the default is
    every enabled component (except ``Database``, which is always created
    first).
    """
    database_name = db.Database.name()
    if components is not None:
        return [name for name in components if name != database_name]
    return [
        name for name, component_config in conf.components.items()
        if component_config.on and name != database_name
    ]


def _check_db_config(conf: config.Config) -> None:
    """Rejects databases that cannot persist admin changes."""
    database_config = conf.components['Database']
    if not isinstance(database_config, db.DatabaseConfig):
        raise AdminError(
            'The Database component is replaced by '
            f'{type(database_config).__name__}; the admin commands only '
            'support the built-in Database component'
        )
    if (database_config.driver == db.DBDrivers.SQLITE
            and database_config.uri
            and database_config.mode == 'memory'):
        raise AdminError(
            'The administration commands need a persistent database. '
            'Configure a file-based SQLite database for the Database '
            'component (set "db_name" and "mode: rwc") or use PostgreSQL.'
        )
