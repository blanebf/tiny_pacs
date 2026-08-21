Extending tiny_pacs
===================

``tiny_pacs`` is built around components that communicate through an event
bus. Extending the server means writing a new component: a class that
subscribes to the events it cares about and, optionally, exposes its own
configuration. This tutorial builds a small component from scratch.

How components work
-------------------

Every component subclasses :class:`tiny_pacs.component.Component` and is
registered in the component registry under the name used in configuration
files. At startup the :class:`~tiny_pacs.server.Server` instantiates every
enabled component, passing the shared event bus and the component's
validated configuration to it.

Components talk to each other through
`trolleybus <https://pypi.org/project/trolleybus/>`_ events defined in
:mod:`tiny_pacs.events`. An incoming C-STORE request, for example, becomes
a :class:`~tiny_pacs.events.Store` event that the ``PACS`` and storage
components handle; when the dataset is safely stored, a
:class:`~tiny_pacs.events.StoreDone` event is broadcast. A component joins
the conversation simply by subscribing to events.

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

Step 3: register the component so the loader knows which model to validate
its configuration against:

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
   looks the component up in the registry), which is why the example below
   imports the custom module before touching the configuration.

Running the extended server
---------------------------

The CLI does not know about third-party components, so an extended server
is started from a small Python script. Put the pieces together — say, in
``store_logger.py`` in your project:

.. code-block:: python

    """store_logger.py — custom component for tiny_pacs."""
    from typing import Any

    import pydicom
    import trolleybus

    from tiny_pacs import component, config, events


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


    config.register_component('StoreLogger', StoreLogger)

…and start the server from a runner that imports it:

.. code-block:: python

    import store_logger  # noqa: F401 — registers StoreLogger

    from tiny_pacs import config, server

    conf = config.Config()
    conf.update_config('config.yaml')

    srv = server.Server(conf)
    srv.start_with_block()

The configuration merges the usual defaults with the ``StoreLogger`` entry;
every C-STORE the server accepts now also appends a line to the configured
log file.

Component lifecycle
-------------------

Components are created during :class:`~tiny_pacs.server.Server`
initialization and see three lifecycle events broadcasts by the event bus:

* :class:`trolleybus.OnStart` — handled by
  :meth:`~tiny_pacs.component.Component.on_start`. The ``Database``
  component collects models and creates tables at this point, so components
  that provide tables must already be subscribed.
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
starts: it broadcasts :class:`~tiny_pacs.events.Tables` and creates all the
tables that are returned. A component can thus bring its own
``peewee.Model`` classes:

.. code-block:: python

    import peewee

    from tiny_pacs import component, events

    class MyRecord(peewee.Model):
        value = peewee.CharField()

    class MyComponent(component.Component[component.ComponentConfig]):
        def __init__(self, bus, config):
            super().__init__(bus, config)
            self.subscribe(events.Tables, self.tables)

        def tables(self, _: None) -> list[type[peewee.Model]]:
            return [MyRecord]

The tables are bound to the shared database connection, so the component
can query its own data within an atomic transaction requested through the
:class:`~tiny_pacs.events.Atomic` event:

.. code-block:: python

    atomic = self.send_one(events.Atomic, None)
    with atomic:
        MyRecord.create(value='hello')

Replacing built-in components
-----------------------------

Because components are looked up in the registry by name, a custom component
registered under a built-in name replaces the built-in one for every
configuration that references that name. Combined with the per-component
configuration, this is how storage backends, the device registry or even the
PACS logic itself can be swapped out without touching the rest of the
server.
