tiny_pacs_admin
===============

Administration extension for `tiny_pacs
<https://github.com/blanebf/tiny_pacs>`_: an offline admin CLI and a
database-backed device registry.

What it provides
----------------

* The ``DeviceStore`` component — keeps remote DICOM devices in the
  database, working in tandem with the built-in ``Devices`` component.
  Devices carry a per-device identity policy (``none``, ``username``,
  ``password``); auto-added devices receive configurable defaults.
* New ``tiny-pacs`` subcommands:

  - ``tiny-pacs devices list|add|update|remove|echo`` — manage the
    devices stored in the database (``--format json|yaml`` supported);
  - ``tiny-pacs components list`` — registry contents with origin
    (built-in vs providing distribution) and enabled/disabled state;
  - ``tiny-pacs db info`` — schema versions and table row counts.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-admin

or with the convenience extra:

.. code-block:: bash

    pip install tiny_pacs[admin]

Installing the extension never changes server behaviour: the
``DeviceStore`` component stays disabled until enabled in the
configuration, and the CLI subcommands are additive.

Configuration
-------------

Enable the component and optionally tune the defaults applied to
auto-added devices:

.. code-block:: yaml

    components:
      DeviceStore:
        on: true
        default_identity: none    # none | username | password
        default_port: 11112

The administration commands work offline against the configured
database. Administration against SQLite requires a file-based database
— set ``db_name`` and ``mode: rwc`` on the ``Database`` component (the
in-memory default cannot persist data between command invocations):

.. code-block:: yaml

    components:
      Database:
        on: true
        db_name: pacs.db
        mode: rwc

The YAML-configured devices of the ``Devices`` component are imported
into the database at server start (upsert by AE title), so the
configuration remains the source of truth for configured devices. When
using ``DeviceStore``, disable ``auto_add`` on the ``Devices`` component
so newly seen devices are only bookkept in the database.

Documentation
-------------

See the `Administration tutorial
<https://tiny-pacs.readthedocs.io/en/latest/tutorial/extensions-admin.html>`_
in the tiny_pacs documentation.
