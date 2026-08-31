tiny_pacs
=========

.. image:: https://github.com/blanebf/tiny_pacs/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/blanebf/tiny_pacs/actions/workflows/ci.yml
   :alt: CI

``tiny_pacs`` is a small, pure-Python PACS (Picture Archiving and Communication
System). It implements a DICOM Service Class Provider (SCP) for the most common
services:

* Verification (C-ECHO)
* Storage (C-STORE)
* Query/Retrieve: C-FIND, C-MOVE and C-GET at the PATIENT, STUDY, SERIES and
  IMAGE levels
* Storage Commitment

Instead of a single monolith, the server is composed of *components* that talk
to each other through a `trolleybus <https://pypi.org/project/trolleybus/>`_
event bus. DICOM networking is provided by
`pynetdicom2 <https://github.com/blanebf/pynetdicom2>`_, dataset handling by
`pydicom <https://pydicom.github.io/>`_ and persistence by
`peewee <https://github.com/coleifer/peewee>`_.

Features
--------

* Modular design: database, device registry, PACS logic and storage are
  individual components that can be configured, replaced or extended
  independently
* SQLite (file or in-memory, the default) and PostgreSQL backends with
  connection pooling
* Storage backends: on-disk files, in-memory datasets or temporary files
* TLS for incoming DICOM connections
* DICOM user identity negotiation: credentials are presented on outgoing
  connections; incoming associations are authenticated per device by the
  optional ``tiny-pacs-identity`` extension
* Interactive configuration wizard
* YAML or JSON configuration files
* Configuration is described with `pydantic <https://docs.pydantic.dev>`_
  models and validated at load time; each component supplies its own config
  model
* Extensible through Python entry points: optional extension packages add
  components and ``tiny-pacs`` CLI subcommands without core changes

Requirements
------------

Python ``>= 3.10``. Runtime dependencies: ``pydicom`` 3.x, ``pynetdicom2``
0.9.x, ``peewee`` 4.x, ``PyYAML`` 6.x, ``trolleybus`` 0.2 and ``pydantic``
2.x.

For the PostgreSQL backend install a driver separately, e.g.
``pip install psycopg2-binary``.

Installation
------------

From the git repository:

.. code-block:: bash

    pip install git+https://github.com/blanebf/tiny_pacs.git

For development, with `Poetry <https://python-poetry.org/>`_:

.. code-block:: bash

    git clone git@github.com:blanebf/tiny_pacs.git
    cd tiny_pacs
    poetry install

Optional extensions
-------------------

Extensions are optional packages that plug into ``tiny_pacs`` through
entry points. Install them with the convenience extras:

.. code-block:: bash

    pip install tiny_pacs[admin]      # admin CLI + DB-backed device registry
    pip install tiny_pacs[identity]   # user management + association auth

``tiny-pacs-admin`` adds a database-backed device registry (per-device
identity policy, configurable auto-add defaults) and the ``devices``,
``components`` and ``db`` CLI subcommands for offline administration.
``tiny-pacs-identity`` adds user management and authenticates incoming
associations against the calling device's identity policy; it builds on
the admin extension and adds the ``users`` subcommand. Installing an
extension never changes server behaviour on its own. See the
`documentation <https://tiny-pacs.readthedocs.io/>`_ for details.

Quick start
-----------

The CLI provides two built-in commands: ``run`` starts the server and
``config`` generates or inspects a configuration file (``config show``
dumps the effective configuration). Running ``tiny-pacs`` without a command
is equivalent to ``tiny-pacs run``, so the traditional invocation keeps
working. Extensions can add further subcommands.

Start the server with the built-in defaults — AE title ``TINY_PACS``, port
``11112``, an in-memory SQLite database and in-memory storage:

.. code-block:: bash

    tiny-pacs run

Override the AE title and/or the port from the command line:

.. code-block:: bash

    tiny-pacs run -a MY_PACS -p 4242

Load configuration from a file (YAML by extension, JSON for ``*.json``):

.. code-block:: bash

    tiny-pacs run -c config.yaml

Generate a YAML configuration file filled with the default values — either
print it to stdout or write it to a file:

.. code-block:: bash

    tiny-pacs config
    tiny-pacs config -o config.yaml

Run either command in interactive mode: the wizard asks for every
configuration value; with ``config`` the result is written to ``--output``
(or printed to stdout), with ``run`` it is merged into the server
configuration and the wizard offers to save it to a file before starting
the server:

.. code-block:: bash

    tiny-pacs config -i -o config.yaml
    tiny-pacs run -i

Each command describes its options in its help:

.. code-block:: bash

    tiny-pacs run --help
    tiny-pacs config --help

Configuration
-------------

A configuration file has three optional top-level sections: ``ae``, ``log``
and ``components``. Anything that is not provided falls back to the built-in
defaults.

Configuration is described with `pydantic <https://docs.pydantic.dev>`_
models and validated when it is loaded, so unknown keys, wrong types or
out-of-range values are rejected up front with a ``pydantic.ValidationError``
instead of surfacing as obscure errors at runtime:

* ``ae`` → :class:`tiny_pacs.config.AEConfig` (with an optional nested
  :class:`tiny_pacs.config.TLSConfig`)
* each entry under ``components`` → the config model the corresponding
  component provides through its ``config_model`` attribute (see
  `Extending tiny_pacs`_)

Upgrading from the older dict-based configuration:

* validation is strict — keys that are unknown to the models (in ``ae``,
  ``tls`` or any component section) are rejected at load time instead of
  being silently ignored;
* every component is skipped unless its entry sets ``on: true`` explicitly;
* a component entry provided in a configuration source replaces any previous
  entry for that component wholesale — omitted fields fall back to the model
  defaults;
* :class:`~tiny_pacs.config.Config` is a ``pydantic`` model, not a ``dict``:
  use ``conf.ae``, ``conf.log``, ``conf.components`` and ``conf.model_dump()``;
* device entries are :class:`~tiny_pacs.devices.DeviceConfig` models — e.g.
  ``DICOMClient.remote_ae.username`` instead of ``remote_ae['username']``;
* custom components must declare their config model (see
  `Extending tiny_pacs`_) and be registered via
  :func:`tiny_pacs.config.register_component`.

``ae``
~~~~~~

Application entity settings:

* ``ae_title`` — a list of AE titles the server answers to
  (default: ``['TINY_PACS']``)
* ``port`` — the SCP TCP port (default: ``11112``)
* ``max_pdu_length`` — maximum PDU length in bytes (default: ``65536``)
* ``dump_ds`` — dump datasets and PDUs to the log (default: ``true``)
* ``supported_ts`` — a list of supported transfer syntax UIDs (default: the
  common uncompressed, JPEG, JPEG-LS, JPEG 2000, MPEG and RLE syntaxes)
* ``tls`` — when present, all incoming connections are wrapped in TLS. A
  mapping with the ``certificate`` and, if the key is stored separately, the
  ``key`` entry pointing to PEM files, and an optional ``ca`` entry. When
  ``ca`` is given, client certificates are verified against it:

.. code-block:: yaml

    ae:
      tls:
        certificate: /etc/tiny_pacs/server.pem
        key: /etc/tiny_pacs/server.key
        ca: /etc/tiny_pacs/ca.pem

``log``
~~~~~~~

A standard Python ``logging.config.dictConfig`` schema. The default
configuration installs a console handler with DEBUG level:

.. code-block:: yaml

    log:
      version: 1
      formatters:
        simple:
          format: '%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s'
      handlers:
        console:
          class: logging.StreamHandler
          level: DEBUG
          formatter: simple
          stream: ext://sys.stdout
      root:
        level: DEBUG
        handlers: [console]

``components``
~~~~~~~~~~~~~~

A mapping of component name (see the `component registry`_) to its
configuration. Every component accepts an ``on`` flag and is skipped unless it
is set to ``true``. When no components are configured at all, the default set
is used: ``Database``, ``Devices``, ``PACS`` and ``InMemoryStorage``.

Example
~~~~~~~

.. code-block:: yaml

    ae:
      ae_title: [TINY_PACS]
      port: 11112
      max_pdu_length: 65536
      dump_ds: false
    log:
      version: 1
      formatters:
        simple:
          format: '%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s'
      handlers:
        console:
          class: logging.StreamHandler
          level: INFO
          formatter: simple
          stream: ext://sys.stdout
      root:
        level: INFO
        handlers: [console]
    components:
      Database:
        on: true
        driver: sqlite            # or postgres
        db_name: pacs.db          # SQLite database name, default 'pacs.db'
        mode: rwc                 # 'rwc' persists to the db_name file; the
                                  # default 'memory' keeps data in RAM only
      Devices:
        on: true
        auto_add: true            # register calling AE titles automatically
        default_port: 11113       # port used for auto-added devices
        devices:
          WORKSTATION:
            aet: WORKSTATION
            address: 192.168.1.10
            port: 11113
            # Optional DICOM user identity for outgoing connections to this
            # device (used for C-MOVE sub-operations and Storage Commitment):
            # username: dicom_user
            # password: secret
      PACS:
        on: true
      FileStorage:
        on: true
        storage_dir: /var/lib/tiny_pacs/storage

The PostgreSQL driver takes connection parameters instead:

.. code-block:: yaml

    components:
      Database:
        on: true
        driver: postgres
        db_name: tiny_pacs_db
        host: localhost
        port: 5432
        user: postgres
        password: postgres
        max_conn: 20

Component registry
------------------

+---------------------+-----------------------------------------------------------+
| Component           | Description                                               |
+=====================+===========================================================+
| ``Database``        | Manages the database connection (SQLite by default, or    |
|                     | PostgreSQL), transactions and table creation. On startup  |
|                     | it collects the models of all components and creates      |
|                     | their tables.                                             |
+---------------------+-----------------------------------------------------------+
| ``Devices``         | Registry of remote DICOM devices by AE title, used as     |
|                     | C-MOVE destinations and for outgoing connections. When    |
|                     | ``auto_add`` is enabled, calling AE titles are registered |
|                     | automatically with ``default_port``.                      |
+---------------------+-----------------------------------------------------------+
| ``PACS``            | The PACS services themselves: handling of C-STORE,        |
|                     | C-FIND, C-MOVE, C-GET and Storage Commitment requests on  |
|                     | top of the ``Patient``/``Study``/``Series``/``Instance``  |
|                     | database models.                                          |
+---------------------+-----------------------------------------------------------+
| ``FileStorage``     | Stores incoming datasets on disk: one file per SOP        |
|                     | Instance in daily (``YYYYMMDD``) sub-folders of           |
|                     | ``storage_dir``. If no directory is configured, a         |
|                     | temporary one is created and removed on shutdown.         |
+---------------------+-----------------------------------------------------------+
| ``InMemoryStorage`` | Keeps incoming datasets in RAM. Intended for testing.     |
+---------------------+-----------------------------------------------------------+
| ``TempFileStorage`` | Stores incoming datasets in temporary files and removes   |
|                     | them on shutdown. Intended for testing.                   |
+---------------------+-----------------------------------------------------------+

Enable exactly one storage component — all storage components react to the
same events, so enabling more than one stores every dataset multiple times.

Extending tiny_pacs
-------------------

Every component provides its own configuration as a ``pydantic`` model, and
the config loader validates the raw ``components`` section against the model
each component declares. To add your own component, subclass
:class:`~tiny_pacs.component.Component` and declare a
:class:`~tiny_pacs.component.ComponentConfig` subclass through the
``config_model`` attribute:

.. code-block:: python

    from tiny_pacs import component, config

    class MyConfig(component.ComponentConfig):
        # ``on`` (enable/disable) is provided by ComponentConfig
        some_option: str = 'default'

    class MyComponent(component.Component[MyConfig]):
        config_model = MyConfig

        def on_start(self) -> None:
            super().on_start()
            # ``self.config`` is a validated ``MyConfig`` instance
            self.log_info('Got option %s', self.config.some_option)

Register the component so the loader knows which model to validate its
configuration against, then enable it in your config file:

.. code-block:: python

    config.register_component('MyComponent', MyComponent)

.. code-block:: yaml

    components:
      MyComponent:
        on: true
        some_option: hello

Loading a configuration for ``MyComponent`` now validates ``some_option`` at
load time, rejecting unknown keys or wrong types. Plain dicts handed directly
to a component constructor are validated the same way, so both the file-based
and the programmatic paths get the same guarantees.

Talking to tiny_pacs: command line examples
-------------------------------------------

The examples below use the CLI shipped with ``pynetdicom2``; DCMTK tools
(``echoscu``, ``storescu``, ``findscu``, ``movescu``) work just as well.

C-ECHO (verification):

.. code-block:: bash

    python -m pynetdicom2 verify --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112

C-STORE:

.. code-block:: bash

    python -m pynetdicom2 store --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --file_or_dir image.dcm

C-FIND (list patients whose name matches ``DOE*``):

.. code-block:: bash

    python -m pynetdicom2 find --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --level patient \
        --attr 'PatientName=DOE*' 'PatientID='

C-MOVE (retrieve one study):

.. code-block:: bash

    python -m pynetdicom2 move --local_aet CLIENT --aet TINY_PACS \
        --address localhost --port 11112 --level study \
        --attr 'StudyInstanceUID=1.2.3.4'

The server knows C-MOVE destinations only through its ``Devices`` registry —
either pre-configure the destination device, or rely on ``auto_add`` to
register it on its first association. Note that in ``pynetdicom2`` 0.9.8 the
CLI's own move receiver (``--local_port``) does not accept storage
associations, so use a real SCP (for example a second ``tiny_pacs``
instance) as the destination.

C-GET is supported on the server side as well and can be exercised with any
C-GET capable service class user.

Python API
----------

``tiny_pacs`` ships a small DICOM client for programmatic access:

.. code-block:: python

    import pydicom
    from tiny_pacs.client import DICOMClient

    remote = {'aet': 'TINY_PACS', 'address': 'localhost', 'port': 11112}
    client = DICOMClient('CLIENT', remote)

    # C-ECHO
    client.echo()

    # C-STORE
    client.store('image.dcm')

    # C-FIND
    query = pydicom.Dataset()
    query.QueryRetrieveLevel = 'STUDY'
    query.StudyDate = ''
    for match in client.find(query):
        print(match.StudyInstanceUID)

    # C-MOVE to a known destination
    move = pydicom.Dataset()
    move.QueryRetrieveLevel = 'STUDY'
    move.StudyInstanceUID = '1.2.3.4'
    client.move(move, dest_ae='WORKSTATION')

License
-------

``tiny_pacs`` is licensed under the MIT license, see the ``LICENSE`` file.
