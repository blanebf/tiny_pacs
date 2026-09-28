Extending tiny_pacs
===================

``tiny_pacs`` is built around components that communicate through an event
bus. Extending the server means writing a new component: a class that
subscribes to the events it cares about and, optionally, exposes its own
configuration. Components are discovered through Python entry points, so an
extension is an ordinary installable package — no core changes required.
This tutorial builds a small component from scratch.

How components work
-------------------

Every component subclasses :class:`tiny_pacs.component.Component` and is
registered in the component registry under the name used in configuration
files. At startup the :class:`~tiny_pacs.server.Server` instantiates every
enabled component, passing the shared event bus and the component's
validated configuration to it.

The registry is populated from two sources: the built-in components
(``Database``, ``Devices``, ``PACS`` and the storage backends) and the
``tiny_pacs.components`` :doc:`entry point group <extensions>` discovered
on the first configuration load — any installed package may contribute
components this way.

Components talk to each other through
`trolleybus <https://pypi.org/project/trolleybus/>`_ events defined in
:mod:`tiny_pacs.events`. An incoming C-STORE request, for example,
becomes a :class:`~tiny_pacs.events.Store` event — the observation point
carrying the association context, e.g. for auditing or access control —
and then a :class:`~tiny_pacs.events.StoreDataset` event with the decoded
dataset that the ``PACS`` component records in the archive; when the
dataset is safely stored, a :class:`~tiny_pacs.events.StoreDone` event is
broadcast. A component joins the conversation simply by subscribing to
events.

Every component provides its own configuration as a ``pydantic`` model, and
the config loader validates the raw ``components`` section against the model
each component declares.

A first component
-----------------

Let's build a ``StoreLogger`` component that writes a line to a log file
every time a dataset has been stored.

Step 1: declare the configuration model. Subclass
:class:`~tiny_pacs.component.ComponentConfig` — the ``on`` flag is provided
by the base class:

.. code-block:: python

    from tiny_pacs import component

    class StoreLoggerConfig(component.ComponentConfig):
        # ``on`` (enable/disable) is provided by ComponentConfig
        log_file: str = 'store.log'

Step 2: implement the component. The ``config_model`` attribute links the
component to its configuration model; subscriptions are set up in
``__init__``:

.. code-block:: python

    from typing import Any

    import pydicom
    import trolleybus

    from tiny_pacs import events

    class StoreLogger(component.Component[StoreLoggerConfig]):
        config_model = StoreLoggerConfig

        def __init__(self, bus: trolleybus.EventBus,
                     config: StoreLoggerConfig | dict[str, Any]):
            super().__init__(bus, config)
            self.subscribe(events.StoreDone, self.on_store_done)

        def on_store_done(self, ds: pydicom.Dataset) -> None:
            # ``self.config`` is a validated ``StoreLoggerConfig`` instance
            with open(self.config.log_file, 'a') as fp:
                fp.write(f'{ds.PatientID} {ds.SOPInstanceUID}\n')

The handler receives the event payload directly — for
:class:`~tiny_pacs.events.StoreDone` that is the stored dataset. The return
value of a handler is the listener result of the event; ``StoreDone``
listeners return ``None``, so it is simply ignored here.

Step 3: make the component discoverable. Components are registered in the
component registry; the registry is populated from the
``tiny_pacs.components`` entry point group. Declare the entry point in the
extension package's ``pyproject.toml`` (legacy Poetry metadata, the style
used throughout this repository):

.. code-block:: toml

    [tool.poetry.plugins."tiny_pacs.components"]
    StoreLogger = "store_logger:StoreLogger"

The entry point name — ``StoreLogger`` — is the name used in the
``components`` section of configuration files. A package built with PEP 621
metadata declares the identical entry point as
``[project.entry-points."tiny_pacs.components"]`` instead; the installed
metadata is the same either way (see :doc:`extensions`).

Programmatic registration is the equivalent for embedding ``tiny_pacs``
into another application:

.. code-block:: python

    from tiny_pacs import config

    config.register_component('StoreLogger', StoreLogger)

Step 4: enable the component in your configuration file:

.. code-block:: yaml

    components:
      StoreLogger:
        on: true
        log_file: /var/log/tiny_pacs/stored.log

Components are skipped unless their entry sets ``on: true`` explicitly, so
registering a component never changes the behaviour of existing
configurations. Loading the configuration validates ``log_file`` at load
time, rejecting unknown keys or wrong types.

.. note::

   Registration must happen before the configuration is loaded (the loader
   looks the component up in the registry). Entry points handle this
   automatically — they are loaded on the first ``Config`` construction; a
   programmatic ``register_component`` call must simply precede it.

Running the extended server
---------------------------

An extension shipped as a package with a ``tiny_pacs.components`` entry
point needs no runner script: installing it makes the component visible to
the stock CLI.

Put the pieces together — say, in ``store_logger.py`` in your project:

.. code-block:: python

    """store_logger.py — custom component for tiny_pacs."""
    from typing import Any

    import pydicom
    import trolleybus

    from tiny_pacs import component, events


    class StoreLoggerConfig(component.ComponentConfig):
        log_file: str = 'store.log'


    class StoreLogger(component.Component[StoreLoggerConfig]):
        config_model = StoreLoggerConfig

        def __init__(self, bus: trolleybus.EventBus,
                     config: StoreLoggerConfig | dict[str, Any]):
            super().__init__(bus, config)
            self.subscribe(events.StoreDone, self.on_store_done)

        def on_store_done(self, ds: pydicom.Dataset) -> None:
            with open(self.config.log_file, 'a') as fp:
                fp.write(f'{ds.PatientID} {ds.SOPInstanceUID}\n')

Declare the entry point and install the package (during development,
``pip install -e .``):

.. code-block:: toml

    [tool.poetry.plugins."tiny_pacs.components"]
    StoreLogger = "store_logger:StoreLogger"

Then enable the component in the configuration and start the server the
usual way — with ``-c``, or through the configuration folder's launcher
script (see :doc:`configuration`):

.. code-block:: bash

    tiny-pacs run -c config.yaml   # or ./cli.sh run from the config folder

The configuration merges the usual defaults with the ``StoreLogger`` entry;
every C-STORE the server accepts now also appends a line to the configured
log file.

Component lifecycle
-------------------

Components are created during :class:`~tiny_pacs.server.Server`
initialization and see three lifecycle events broadcasts by the event bus:

* :class:`trolleybus.OnStart` — handled by
  :meth:`~tiny_pacs.component.Component.on_start`. The ``Database``
  component collects models, applies schema migrations and creates tables
  at this point, so components that provide tables must already be
  subscribed.
* :class:`trolleybus.OnStarted` — handled by
  :meth:`~tiny_pacs.component.Component.on_started`. Good place for work
  that needs the whole system up (e.g. connecting to external services).
* :class:`trolleybus.OnExit` — handled by
  :meth:`~tiny_pacs.component.Component.on_exit`. Clean up resources here.

Component subscriptions are attached immediately on construction, so
handlers are available from the very first ``OnStart`` broadcast. Override
the ``on_start``/``on_started``/``on_exit`` methods (calling ``super()``) to
hook into the lifecycle.

Component conveniences
----------------------

:class:`~tiny_pacs.component.Component` provides a few helpers:

* ``self.subscribe(event, callback)`` — subscribe to an event; defaults to
  the component's ``priority`` (higher priority runs first).
* ``self.broadcast(event, payload)`` / ``self.send_one(...)`` /
  ``self.send_any(...)`` — emit events, provided by
  :class:`trolleybus.EmitterMixin`.
* ``self.log_debug`` / ``log_info`` / ``log_warning`` / ``log_error`` /
  ``log_critical`` / ``log_exception`` — logging under the component's own
  logger name.

Custom database tables
----------------------

The ``Database`` component asks every component for its models when it
starts: it broadcasts :class:`~tiny_pacs.events.Migrations` (or
:class:`~tiny_pacs.events.Tables` for components without migrations),
binds all returned models to the shared database connection and applies
the components' schema migrations. A component can thus bring its own
``peewee.Model`` classes:

.. code-block:: python

    import peewee

    from tiny_pacs import component, events, schema

    class MyRecord(peewee.Model):
        value = peewee.CharField()

    class MyComponent(component.Component[component.ComponentConfig]):
        def __init__(self, bus, config):
            super().__init__(bus, config)
            self.subscribe(events.Migrations, self.migrations)

        def migrations(self, _: None) -> schema.ComponentMigrations:
            return schema.ComponentMigrations(
                self.schema(), [MyRecord], MIGRATIONS
            )

``MIGRATIONS`` is the component's migration list; see
:doc:`migrations` for how schema changes between releases are described.
Components that never change their schema may simply subscribe to
:class:`~tiny_pacs.events.Tables` and return their models — those tables
are created at start without any versioning.

The tables are bound to the shared database connection, so the component
can query its own data within an atomic transaction requested through the
:class:`~tiny_pacs.events.Atomic` event:

.. code-block:: python

    atomic = self.send_one(events.Atomic, None)
    with atomic:
        MyRecord.create(value='hello')

Adding CLI subcommands
----------------------

Extensions may also add ``tiny-pacs`` subcommands through the
``tiny_pacs.cli`` entry point group. The entry point name becomes the
subcommand name; the entry point value is a ``register`` callable that
receives argparse's subparsers action:

.. code-block:: python

    # store_logger_cli.py
    import argparse

    def register(subparsers: argparse.Action) -> None:
        from tiny_pacs.__main__ import add_common_arguments

        parser = subparsers.add_parser(
            'storelog', help='inspect the store log'
        )
        add_common_arguments(parser)   # shared -c/--config handling
        parser.set_defaults(command_handler=storelog_command)

    def storelog_command(args: argparse.Namespace) -> None:
        ...

.. code-block:: toml

    [tool.poetry.plugins."tiny_pacs.cli"]
    storelog = "store_logger_cli:register"

``tiny-pacs`` dispatches to the callable stored by
``parser.set_defaults(command_handler=...)`` after parsing. Reserved
subcommand names (``run``, ``config``, ``help``) cannot be registered even
from inside ``register()``; registrations are otherwise isolated and
reverted on failure, so a plugin can never break the ``tiny-pacs`` binary.

Replacing built-in components
-----------------------------

Because components are looked up in the registry by name, a custom
component registered under a built-in name replaces the built-in one for
every configuration that references that name. The same works from an
entry point: a ``tiny_pacs.components`` entry point named ``Devices``
overrides the built-in ``Devices`` component (the replacement is logged).
Combined with the per-component configuration, this is how storage
backends, the device registry or even the PACS logic itself can be swapped
out without touching the rest of the server.

A programmatic ``register_component`` call always wins over installed
plugins — the embedder's escape hatch. See :doc:`extensions` for the full
extension contract.
