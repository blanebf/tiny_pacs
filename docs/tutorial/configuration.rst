Configuration
=============

``tiny_pacs`` is configured through YAML or JSON files, command-line options
and an interactive wizard. This tutorial covers the configuration file
format and the tools to create one.

Configuration files
-------------------

A configuration file has three optional top-level sections: ``ae``, ``log``
and ``components``. Anything that is not provided falls back to the built-in
defaults.

Configuration is described with
`pydantic <https://docs.pydantic.dev>`_ models and validated when it is
loaded, so unknown keys, wrong types or out-of-range values are rejected up
front with a ``pydantic.ValidationError`` instead of surfacing as obscure
errors at runtime:

* ``ae`` → :class:`tiny_pacs.config.AEConfig` (with an optional nested
  :class:`tiny_pacs.config.TLSConfig`)
* each entry under ``components`` → the config model the corresponding
  component provides through its ``config_model`` attribute (see
  :doc:`extending`)

The ``ae`` section
~~~~~~~~~~~~~~~~~~

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

The ``log`` section
~~~~~~~~~~~~~~~~~~~

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

The ``components`` section
~~~~~~~~~~~~~~~~~~~~~~~~~~

A mapping of component name to its configuration. The built-in components
are described in :doc:`../introduction`; every component accepts an ``on``
flag and is skipped unless it is set to ``true``. When no components are
configured at all, the default set is used: ``Database``, ``Devices``,
``PACS`` and ``InMemoryStorage``.

.. note::

   A component entry provided in a configuration source replaces any
   previous entry for that component wholesale — omitted fields fall back
   to the model defaults.

A complete example
~~~~~~~~~~~~~~~~~~

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
        default_port: 11114       # port used for auto-added devices
        devices:
          WORKSTATION:
            aet: WORKSTATION
            address: 192.168.1.10
            port: 11114           # not 11113: keep clear of the shared
                                  # HTTP server default port
            # Optional DICOM user identity for outgoing connections to this
            # device (used for C-MOVE sub-operations and Storage Commitment):
            # username: dicom_user
            # password: secret
      PACS:
        on: true
        # Optional: Patient IDs (recognized case-insensitively) treated
        # as de-identification placeholders of incoming datasets. Such
        # datasets — like the ones with the PS3.15 E de-identification
        # attributes or with an empty Patient ID — are stored without
        # demographic conflict warnings (one shared record per distinct
        # Patient ID string), because the identity attributes of
        # anonymized data carry no identity semantics.
        # anonymous_patient_ids: [ANONYMOUS]
      FileStorage:
        on: true
        storage_dir: /var/lib/tiny_pacs/storage
        # Optional (shared by every storage component): when a C-STORE
        # arrives for an instance that is already stored, refuse it with a
        # failure status (default) or replace the stored instance. A
        # replacement keeps the stored copy until the new one is fully
        # stored, and a failed replacement rolls back to it.
        # overwrite: false

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

Generating configuration files
------------------------------

Instead of writing the file by hand, generate a YAML configuration filled
with the default values — either print it to stdout or write it to a file:

.. code-block:: bash

    tiny-pacs config
    tiny-pacs config -o config.yaml

The generated configuration contains every component available in the
environment: the built-in defaults plus all components provided by
installed extensions (see :doc:`extensions`), the latter disabled
(``on: false``) with their own default values — enabling an installed
extension is a matter of flipping its ``on`` flag. Extension components
whose configuration models require fields without defaults cannot be
pre-populated and are skipped with a warning; add their YAML entries by
hand.

Files written by ``tiny-pacs config`` are created with restrictive
permissions (``0600``), because configurations may contain credentials such
as the PostgreSQL password.

Launcher scripts
~~~~~~~~~~~~~~~~

A configuration folder usually lives outside the source tree, and the
relative paths inside it (database file, storage directory, log files) only
resolve when tiny-pacs runs from that folder. ``--launcher`` generates two
small helper scripts next to the configuration — ``cli.sh`` for POSIX
shells and ``cli.cmd`` for Windows — that take care of this. Each script
changes into its own folder and forwards every argument to tiny-pacs with
``-c <config>`` appended:

.. code-block:: bash

    tiny-pacs config -o config.yaml --launcher

    ./cli.sh run                # start the server
    ./cli.sh config show        # dump the effective configuration
    ./cli.sh users list         # extension subcommands work too

On Windows (from the command prompt or PowerShell):

.. code-block:: bat

    cli.cmd run
    cli.cmd config show

The scripts call the Python interpreter that generated them — always
one that has tiny_pacs installed — and fall back to ``tiny-pacs`` on the
``PATH`` when that interpreter is gone (e.g. the folder was copied to
another machine), so the folder keeps working without activating a
virtualenv. Both files are written on every platform, which keeps a
configuration folder portable between POSIX systems and Windows, and
every ``config --launcher`` run regenerates (overwrites) them together
with the configuration.

Interactive configuration
-------------------------

Both commands can run in interactive mode: the wizard asks for every
configuration value. With ``config`` the result is written to ``--output``
(or printed to stdout), with ``run`` it is merged into the server
configuration and the wizard offers to save it to a file before starting
the server:

.. code-block:: bash

    tiny-pacs config -i -o config.yaml
    tiny-pacs run -i

Next steps
----------

With a configuration file in hand, continue to :doc:`running` to start the
server and send your first DICOM association.
