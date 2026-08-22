Schema migrations
=================

Every component owns its database tables and its own schema version. When
the schema of a component changes between releases — a new column, a type
change, a new index — the change is described as a migration, and the
:class:`~tiny_pacs.db.Database` component applies pending migrations on
start, before the server begins to serve. Migrations are built on peewee's
built-in manual migration machinery (:mod:`playhouse.migrate`) and are
forward-only.

How it works
------------

The :class:`~tiny_pacs.db.Database` component keeps one row per component
in the :class:`~tiny_pacs.db.SchemaVersion` table, recording the highest
migration version that was applied to that component's tables. On start it
broadcasts :class:`~tiny_pacs.events.Migrations`, collects every
component's :class:`~tiny_pacs.schema.ComponentMigrations` and compares
the declared versions with the recorded ones:

* **Fresh database** — the component has no version row and none of its
  tables exist. The tables are created straight from the current model
  definitions (which already include every migration) and the component
  is stamped to its latest version.
* **Pre-existing database** — the tables exist but there is no version
  row: the database was created before the migration mechanism existed.
  The whole migration chain runs over it; the first migration creates
  tables with ``safe=True`` and is a no-op for the tables that are
  already there.
* **Versioned database** — only the migrations newer than the recorded
  version are applied, in order.

Each migration runs inside the migration transaction together with its
version bookkeeping; the whole migration phase is committed as one
transaction at start, so a crash never leaves a half-migrated database
behind. A failed migration aborts the start and is retried on the next
start. Concurrent server processes starting against the same database are
serialized — a PostgreSQL advisory lock, the SQLite write lock — and
re-check recorded versions before applying, so at most one process applies
any given migration.

Writing migrations
------------------

A component publishes a :class:`~tiny_pacs.schema.ComponentMigrations`
instance through the :class:`~tiny_pacs.events.Migrations` event: the
component's schema name, its tables and its list of
:class:`~tiny_pacs.schema.Migration` objects:

.. code-block:: python

    import peewee

    from tiny_pacs import component, events, schema

    class MyRecord(peewee.Model):
        value = peewee.CharField()
        extra = peewee.CharField(null=True)

    TABLES = [MyRecord]

    MIGRATIONS = [
        schema.create_tables_migration(TABLES, 'Create tables'),
        schema.Migration(
            2, 'Add extra column',
            lambda m: [
                m.add_column('myrecord', 'extra',
                             peewee.CharField(null=True))
            ]
        ),
    ]

    class MyComponent(component.Component[component.ComponentConfig]):
        def __init__(self, bus, config):
            super().__init__(bus, config)
            self.subscribe(events.Migrations, self.migrations)

        def migrations(self, _: None) -> schema.ComponentMigrations:
            return schema.ComponentMigrations(
                self.schema(), TABLES, MIGRATIONS
            )

The first migration always creates the component's tables with
``safe=True`` — :func:`~tiny_pacs.schema.create_tables_migration` builds
it from the very table list the component publishes, so the two cannot
drift apart. Later migrations receive the driver-specific migrator
(:class:`~playhouse.migrate.SqliteMigrator` or
:class:`~playhouse.migrate.PostgresqlMigrator`, chosen from the configured
database driver) and return ``playhouse.migrate`` operations, or execute
statements directly.

Rules
-----

* **Append-only.** Once released, a migration is never edited, renumbered
  or removed; new changes get new versions.
* **Forward-only.** There is no automatic rollback. Stage destructive
  changes across versions (add column → backfill → drop old column) and
  back up databases before releases containing them.
* **Own tables only.** A migration may only touch the tables its component
  publishes; components never migrate each other's tables.
* **Models are the source of truth.** The migration chain must bring a
  version-1 database to exactly the current model definitions, so model
  changes and their migrations are committed together.
* **Stay portable.** New columns must be nullable or have a default; test
  migrations against both SQLite and PostgreSQL — note that peewee's
  SQLite migrator emulates operations SQLite does not support by
  rebuilding the table.

Schema names
------------

The version row is keyed by the component's schema name,
:meth:`~tiny_pacs.component.Component.schema`, which defaults to the
class name. Components whose implementations share the same tables must
set :attr:`~tiny_pacs.component.Component.schema_name` to a common name so
they share one schema version — the built-in storage components all use
``Storage``, for example.

Components without migrations
-----------------------------

Components that only broadcast :class:`~tiny_pacs.events.Tables` keep the
old behaviour: their tables are created with ``safe=True`` and are never
versioned. This keeps simple components simple; add a migration list only
when the schema starts to change between releases.
