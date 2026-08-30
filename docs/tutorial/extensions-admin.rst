Administration: ``tiny-pacs-admin``
===================================

``tiny-pacs-admin`` is the first-party administration extension. It keeps
remote DICOM devices in the database and administers them — together with
the component registry and the database itself — from the command line,
fully offline.

Installing ``tiny-pacs-admin`` never changes server behaviour: the
``DeviceStore`` component stays disabled until enabled in the
configuration, and every CLI subcommand is additive.

Installation
------------

.. code-block:: bash

    pip install tiny_pacs[admin]

or the extension directly — the effect is identical:

.. code-block:: bash

    pip install tiny-pacs-admin

The device registry component
-----------------------------

The extension contributes the ``DeviceStore`` component, which keeps
remote devices in the database and works in tandem with the built-in
:class:`~tiny_pacs.devices.Devices` component:

* YAML-configured devices are imported into the database at server start
  (upsert by AE title), so the configuration remains the source of truth
  for configured devices;
* device lookups are answered from the database first; when a device is
  not in the database, the in-memory ``Devices`` component still answers
  from its configuration;
* devices first seen on an incoming association are automatically added
  to the database with configurable defaults.

Enable it in the configuration and tune the defaults applied to
auto-added devices:

.. code-block:: yaml

    components:
      DeviceStore:
        on: true
        default_identity: none     # none | username | password
        default_port: 11112

Per-device identity policy
^^^^^^^^^^^^^^^^^^^^^^^^^^

Every device carries an identity policy describing the user identity an
incoming association from that device must present:

.. list-table::
   :header-rows: 1

   * - Policy
     - Meaning
   * - ``none``
     - no user identity required
   * - ``username``
     - user identity required; the username must exist, the password is
       optional
   * - ``password``
     - username/password identity with a valid password required

The policy is configured per device — in YAML alongside the device:

.. code-block:: yaml

    components:
      Devices:
        on: true
        devices:
          SOME_MODALITY:
            aet: SOME_MODALITY
            address: 10.0.0.5
            port: 104
            identity: password

or in the database via ``devices add --identity`` /
``devices update --identity``. Authentication extensions (such as
``tiny-pacs-identity``) evaluate this policy per device when an
association arrives; without one installed the policy is simply carried
along and never enforced.

.. note::

   When ``DeviceStore`` is used, disable ``auto_add`` on the built-in
   ``Devices`` component so newly seen devices are bookkept in one place
   only:

   .. code-block:: yaml

       components:
         Devices:
           on: true
           auto_add: false

Offline administration
----------------------

The admin commands work offline: they load the configuration, start an
event bus with the database and the managed components (database
initialization and schema migrations included), run the command and stop
the bus again. No server thread is started.

Administration needs a persistent database. SQLite works only with a
file-based database, so configure the ``Database`` component with
``db_name`` and ``mode: rwc`` — the default in-memory database cannot
persist anything between command invocations and is rejected with an
error:

.. code-block:: yaml

    components:
      Database:
        on: true
        db_name: pacs.db
        mode: rwc

Devices
^^^^^^^

List the registered devices (the identity policy included):

.. code-block:: bash

    tiny-pacs devices list
    tiny-pacs devices list --format json
    tiny-pacs devices list --format yaml

Add a device; the port and the identity policy default to the
``DeviceStore`` configuration (``default_port``, ``default_identity``):

.. code-block:: bash

    tiny-pacs devices add MRI_01 --address 10.0.0.20 --port 104 \
        --identity password
    tiny-pacs devices add MRI_02 --address 10.0.0.21   # defaults apply

Outgoing identity credentials (used when tiny_pacs connects *to* the
device) can be stored as well:

.. code-block:: bash

    tiny-pacs devices add WORKSTATION --address 10.0.0.9 \
        --user operator --password secret

Update fields of an existing device, remove a device, or check
connectivity with a C-ECHO:

.. code-block:: bash

    tiny-pacs devices update MRI_01 --identity username
    tiny-pacs devices remove MRI_02
    tiny-pacs devices echo MRI_01

Every subcommand accepts the shared ``-c/--config`` flags:

.. code-block:: bash

    tiny-pacs devices list -c /etc/tiny_pacs/config.yaml

Components
^^^^^^^^^^

Inspect the component registry — which components are registered, where
each one comes from (built-in, registered programmatically, or the name
of the distribution providing it through an entry point) and whether it
is enabled in the effective configuration:

.. code-block:: bash

    tiny-pacs components list

.. code-block:: text

    NAME             ORIGIN           STATE
    ---------------  ---------------  --------
    Database         built-in         enabled
    DeviceStore      tiny_pacs_admin  enabled
    Devices          built-in         enabled
    ...

Database
^^^^^^^^

Show the recorded schema versions of every component and the row counts
of all tables:

.. code-block:: bash

    tiny-pacs db info

.. code-block:: text

    Schema versions:
    COMPONENT    VERSION
    -----------  -------
    DeviceStore  1
    PACS         1
    Storage      1

    Row counts:
    TABLE          ROWS
    -------------  ----
    devicemodel    3
    instance       0
    patient        0
    ...

Configuration
^^^^^^^^^^^^^

The built-in ``config`` command dumps the effective configuration
(defaults merged with the ``-c/--config`` sources):

.. code-block:: bash

    tiny-pacs config show
    tiny-pacs config show -c /etc/tiny_pacs/config.yaml

This is plain ``tiny_pacs`` functionality, not an extension command —
the ``config`` subcommand is reserved and cannot be provided by
extensions.

Reference implementation
------------------------

``tiny-pacs-admin`` doubles as the reference implementation of the
extension contract described in :doc:`extensions`: a component published
through ``tiny_pacs.components`` (with its own tables and
:class:`~tiny_pacs.events.Migrations`), CLI subcommands published through
``tiny_pacs.cli``, and the headless admin runtime used by all of them.
