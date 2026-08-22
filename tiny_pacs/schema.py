"""Schema version management primitives.

Every component owns its tables and its own schema version. Components
publish their schema changes as :class:`Migration` lists through the
:class:`~tiny_pacs.events.Migrations` event; the
:class:`~tiny_pacs.db.Database` component applies them with peewee's
built-in manual migrations (:mod:`playhouse.migrate`).
"""
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import peewee


class Migration:
    """One schema change of a component.

    Migrations of a component are numbered starting from 1. Version
    numbers increase monotonically and never change once released: new
    schema changes are always added as new migrations, existing
    migrations are never edited, renumbered or removed. The migration
    list must bring a fresh version-1 schema to exactly the current model
    definitions.

    :ivar version: migration number within its component
    :ivar description: short human-readable summary, logged when the
                       migration is applied
    :ivar upgrade: function performing the migration. Receives the
                   driver-specific ``playhouse.migrate`` migrator and
                   either executes statements directly (e.g.
                   ``Model.create_table(safe=True)``) and/or returns an
                   iterable of ``playhouse.migrate`` operations for the
                   runner to apply. The function is executed within the
                   migration transaction.
    """

    def __init__(
            self,
            version: int,
            description: str,
            upgrade: Callable[[Any], Iterable[Any] | None]
    ) -> None:
        """Initializes the migration.

        :param version: migration number within its component
        :type version: int
        :param description: short human-readable summary of the change
        :type description: str
        :param upgrade: migration body, see class description
        :type upgrade: callable
        """
        self.version = version
        self.description = description
        self.upgrade = upgrade


@dataclass(frozen=True)
class ComponentMigrations:
    """Schema migrations of one component.

    Result of the :class:`~tiny_pacs.events.Migrations` event.

    :ivar component: schema name of the component, see
                     :meth:`tiny_pacs.component.Component.schema`
    :ivar tables: tables owned by the component. The DB component uses
                  them to create the tables of fresh databases and to
                  detect databases created before the migration
                  mechanism was introduced
    :ivar migrations: migrations of the component in any order; the DB
                      component applies them sorted by version
    """

    component: str
    tables: list[type[peewee.Model]]
    migrations: list[Migration]


def create_tables_migration(
        tables: Sequence[type[peewee.Model]],
        description: str = 'Create tables',
        version: int = 1
) -> Migration:
    """Builds the baseline migration creating the given tables.

    The tables are created with ``safe=True`` in the list order, so the
    order may encode foreign-key dependencies and the migration is a
    no-op for tables that already exist in a pre-existing database.

    :param tables: tables to create; the same list the component
                   publishes in :class:`ComponentMigrations`
    :type tables: list[type[peewee.Model]]
    :param description: migration description
    :type description: str
    :param version: migration version, defaults to 1
    :type version: int
    :return: the baseline migration
    :rtype: Migration
    """
    ordered = list(tables)

    def upgrade(_migrator: Any) -> list[Any]:
        for table in ordered:
            table.create_table(safe=True)
        return []

    return Migration(version, description, upgrade)
