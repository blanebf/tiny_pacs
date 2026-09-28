Administration: ``tiny-pacs-admin``
===================================

``tiny-pacs-admin`` is the first-party administration extension. It keeps
remote DICOM devices in the database and administers them — together with
the component registry, the database itself and the storage maintenance
operations — from the command line, fully offline.

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

Every command therefore needs that configuration passed to it. The
examples below omit the flag for brevity: either append
``-c /path/to/config.yaml`` explicitly, or run the commands through the
configuration folder's launcher scripts (``./cli.sh`` / ``cli.cmd``, see
:doc:`configuration`), which append it automatically.

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

Every subcommand accepts the shared ``-c/--config`` flags; the launcher
scripts of a configuration folder pass them automatically:

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

Storage maintenance
-------------------

The ``storage`` subcommands answer the core storage maintenance events
(:class:`~tiny_pacs.events.StorageStatsQuery`,
:class:`~tiny_pacs.events.StorageVerifyQuery` and
:class:`~tiny_pacs.events.StorageCleanupCommand`) with the storage
component from the configuration, instantiated headless through the
admin runtime. The headless bus starts the ``Database`` component and
the enabled storage components *only* — other components (device
registries, user stores) are never started, so these commands perform
no startup writes of their own beyond creating/migrating the storage
tables. They operate on record bookkeeping (the ``StorageFiles`` table)
and, for file-backed storage (``FileStorage``), on the files in the
storage directory. All of them accept the shared ``-c/--config``
flags, and a configuration without any enabled storage component fails
with a friendly error.

Statistics
^^^^^^^^^^

Show record totals split by stored/failed, the oldest and newest record
and — for a file backend — the file count, total bytes and the sizes of
the ``%Y%m%d`` day folders:

.. code-block:: bash

    tiny-pacs storage stats
    tiny-pacs storage stats --format json
    tiny-pacs storage stats --format yaml

The optional ``--quota GB`` flag is monitoring-script friendly: the
report is printed as usual, but the command exits with status **2**
when the disk usage exceeds the quota:

.. code-block:: bash

    tiny-pacs storage stats --quota 500 || alert "PACS storage over quota"

``--quota`` needs a file-backed storage component; with a backend that
has no directory (``InMemoryStorage``, ``TempFileStorage``) the command
exits non-zero with an explanatory error.

Verification
^^^^^^^^^^^^

The two-sided consistency check between database records and the files
on disk classifies:

* **missing files** — records whose file no longer exists;
* **orphan files** — files under the storage directory that no record
  references;
* **stuck records** — records with ``is_stored == False`` (a store that
  failed or a process that died mid-store).

.. code-block:: bash

    tiny-pacs storage verify
    tiny-pacs storage verify --format json

Without a file backend, ``verify`` degrades to the consistency report
of the database records (stuck records only).

Destructive follow-ups are double-gated opt-ins:
``--delete-missing-records`` and ``--delete-orphans`` select what may
go, and nothing is deleted until ``--apply`` is added — the default is
a dry run that only reports the counts:

.. code-block:: bash

    tiny-pacs storage verify --delete-orphans           # dry run
    tiny-pacs storage verify --delete-orphans --apply   # really delete

Both flags need a file-backed storage component; orphan deletion is
additionally gated inside the storage component so files newer than a
short grace window (an in-flight C-STORE creates the file before its
record) are never removed.

Cleanup
^^^^^^^

``cleanup`` removes stuck in-progress records older than
``--older-than DAYS`` together with their files when they exist on
disk:

.. code-block:: bash

    tiny-pacs storage cleanup --older-than 7            # dry run
    tiny-pacs storage cleanup --older-than 7 --apply

Safety rails:

* ``--older-than 0`` is refused — a running server may legitimately
  have in-progress stores; the recommended minimum is 1 day;
* an ``--apply`` run warns on stderr when it detects recent store
  activity (a best-effort hint that the server may be live);
* dry run is the default (``--dry-run`` and ``--apply`` are mutually
  exclusive);
* applied cleanups are broadcast as
  :class:`~tiny_pacs.events.AuditRecord` (category ``storage``) by the
  storage component, so an audit extension records what was deleted.

Crash-recovery runbook
^^^^^^^^^^^^^^^^^^^^^^

``FileStorage`` normally removes the leftovers of a failed store
itself; a crash *between* creating the file and reporting the store
result leaves a stuck record (and possibly the file) behind. To heal
such a storage:

1. Stop the server (recommended for the destructive steps; see below
   for what is safe on a live system).
2. Inspect the damage:

   .. code-block:: bash

       tiny-pacs storage verify -c /etc/tiny_pacs/config.yaml

   Stuck records are listed with their SOP Instance UIDs.

3. Preview the cleanup with a generous threshold (dry run is the
   default):

   .. code-block:: bash

       tiny-pacs storage cleanup --older-than 1 -c /etc/tiny_pacs/config.yaml

4. Apply it and re-verify:

   .. code-block:: bash

       tiny-pacs storage cleanup --older-than 1 --apply -c /etc/tiny_pacs/config.yaml
       tiny-pacs storage verify -c /etc/tiny_pacs/config.yaml

5. If the crash also left orphan files (files without any record — for
   example from a partially written rename), remove them explicitly:

   .. code-block:: bash

       tiny-pacs storage verify --delete-orphans -c /etc/tiny_pacs/config.yaml
       tiny-pacs storage verify --delete-orphans --apply -c /etc/tiny_pacs/config.yaml

Records with missing files (the file was deleted but the record
remains) are removed the same way with ``--delete-missing-records``.

.. warning::

   Missing/orphan classification is relative to the storage directory
   the *command* resolves from the configuration — with a relative
   ``storage_dir`` it depends on the working directory the command runs
   from, so run these commands from the server's working directory or
   (better) configure an absolute ``storage_dir``. A misresolved
   directory reports *every* record as missing; before applying
   ``--delete-missing-records``, always compare the dry-run count
   against the record total from ``storage stats`` — a missing count
   near the total indicates a misresolved directory (or a lost storage
   volume), not routine damage.

Running against a live server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The commands run headless against the same database the server may be
using:

* **PostgreSQL**: safe to run against a live server (transactional,
  pooled connections).
* **SQLite**: the ``stats``/``verify`` queries are read-only, but the
  headless bus still creates/migrates the storage tables on first run;
  destructive commands (``cleanup --apply``, ``verify --delete-*
  --apply``) against a busy live database can hit write locks. There is
  no retry in the deletion paths: lock contention is reported as errors
  with a non-zero exit, and by then part of the cleanup may already be
  committed — run destructive commands with the server stopped.
* Files being written *right now* may be misreported as orphans while
  an incoming store is in flight; the storage component refuses to
  delete files inside the grace window, but the double gate
  (``--delete-orphans`` **and** ``--apply``) exists precisely because
  of this race.

Reference implementation
------------------------

``tiny-pacs-admin`` doubles as the reference implementation of the
extension contract described in :doc:`extensions`: a component published
through ``tiny_pacs.components`` (with its own tables and
:class:`~tiny_pacs.events.Migrations`), CLI subcommands published through
``tiny_pacs.cli``, and the headless admin runtime
(:func:`tiny_pacs.admin.admin_context`) used by all of them.
