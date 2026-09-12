Audit log: ``tiny-pacs-audit``
==============================

``tiny-pacs-audit`` records **who did what** into a single append-only
table in the shared database: every incoming association (accepted *and*
rejected), every service request (C-STORE/-FIND/-MOVE/-GET, Storage
Commitment) and every administrative change other components emit. It is
the first-party answer to "what happened on this server, and on whose
behalf".

The extension contributes one component:

* ``AuditLog`` — an event-bus observer that subscribes to the core
  association-lifecycle, service and ``AuditRecord`` events and writes one
  row per audited action.

It depends on the core only. Attribution comes from the core association
context (:mod:`tiny_pacs.assoc_context`) and the ``session`` field of the
service payloads; administrative actions arrive through the core
:class:`~tiny_pacs.events.AuditRecord` event that whoever performs a
mutation emits — the audit extension never imports the admin or identity
packages.

Installing ``tiny-pacs-audit`` never changes server behaviour: the
``AuditLog`` component stays disabled until enabled in the configuration,
and every CLI subcommand is additive.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-audit

or with the convenience extra:

.. code-block:: bash

    pip install tiny_pacs[audit]

The effect is identical. From PyPI the extra pulls the published extension
distribution; when installing the core from the repository it resolves
against the bundled package — see :doc:`installation` for both routes.

Configuration
-------------

.. code-block:: yaml

    components:
      AuditLog:
        on: true
        retention_days: 0        # 0 = keep forever; >0 purges on startup
        record_details: true     # false stores only the fixed columns

* ``retention_days`` — when greater than zero, records older than this are
  deleted each time the server (or an admin command) starts. ``0`` keeps
  them forever.
* ``record_details`` — when ``false``, only the fixed columns are stored
  and the JSON ``details`` blob stays empty.

What is recorded
----------------

.. list-table::
   :header-rows: 1

   * - Event
     - Category
     - Record
   * - ``Assoc``
     - ``assoc``
     - the association attempt: device, peer, presented identity type and
       username
   * - ``AssocRejected``
     - ``assoc``
     - a rejected association with the reason
   * - ``AssocReleased``
     - ``assoc``
     - a completed association, carrying the authenticated principal
   * - ``Store`` / ``StoreDone`` / ``StoreFailure``
     - ``service``
     - the C-STORE request and its outcome (SOP Class / Instance UID)
   * - ``Find`` / ``Move`` / ``Get`` / ``Commitment``
     - ``service``
     - the service request, attributed to its session
   * - ``AuditRecord``
     - the emitter's category
     - administrative actions from other components (device/user changes,
       storage maintenance, ...)

The ``Assoc`` listener runs **above** the authentication (+20) and device
registry (+10) components, so an association is recorded even when a
lower-priority listener rejects it and aborts the remaining listeners. That
first row is a *pre-authentication* attempt (``event='attempt'``,
``status='pending'``) whose ``username`` column holds the **presented** —
not yet verified — identity, so an unauthenticated peer can never forge a
row claiming an authenticated association. The authoritative outcome comes
from the lifecycle events: a rejected association adds a ``rejected`` row
(with the reason), and a completed one a ``released`` row carrying the
**authenticated** principal (taken from the association context). All three
rows of one association share the ``correlation_id`` from the association
context in their ``details``, so an attempt can be tied to its outcome.

Attribution
-----------

Service rows are attributed through the session that travels on the service
payload (:class:`~tiny_pacs.assoc_context.AssocContext`), so the audit
component needs no access to authentication or device registries. When a
payload carries no session (for example the store *outcome* events and
Storage Commitment), the component falls back to the association context of
the handling thread — which the core threading contract guarantees is the
association's own thread, so concurrent associations never mix up their
principals.

Data model
----------

A single append-only table (:class:`~tiny_pacs_audit.models.AuditEventModel`):

.. code-block:: text

    timestamp   DateTimeField(index)   # always UTC
    category    CharField(index)       # assoc | service | storage | authn | admin
    event       CharField(index)       # attempt | rejected | released | store | ...
    device_aet  CharField(null, index) # calling AE title when known
    username    CharField(null, index) # presented (attempt) / authenticated user
    peer        CharField(null)        # peer address of the association
    status      CharField(index)       # pending | rejected | success | failure
    details     TextField (JSON)       # v1: UIDs, counts, correlation_id — small

The ``details`` blob is deliberately unstructured so any emitter can enrich
a row without a further migration. The component is **insert-only**: it
publishes no mutation events for its own records, and retention /
``audit cleanup`` are the only deletion paths.

Offline querying
----------------

The ``audit`` commands work offline through the headless admin runtime
(:func:`tiny_pacs.admin.admin_context`): they start an event bus with the
database and the ``AuditLog`` component, run the command and stop the bus
again — no server thread. Administration needs a persistent database, so
for SQLite configure a file-based database (``db_name`` and ``mode: rwc``);
the in-memory default is rejected with a friendly error:

.. code-block:: yaml

    components:
      Database:
        on: true
        db_name: pacs.db
        mode: rwc

Query
^^^^^

List the trail, filtered by category, event, device, user and time window.
Every command accepts the shared ``-c/--config`` flags and a
``--format table|json`` switch:

.. code-block:: bash

    tiny-pacs audit query --category assoc --limit 50
    tiny-pacs audit query --event rejected --aet MODALITY --format json
    tiny-pacs audit query --user alice --since 2024-01-01T00:00:00
    tiny-pacs audit query --category service --until 2024-06-30T23:59:59

``--category`` and ``--event`` are repeatable; ``--since``/``--until``
accept ISO-8601 timestamps (a trailing ``Z`` is fine, a naive timestamp is
read as UTC) and are inclusive. Rows come back oldest-first and
``--limit``/``--offset`` paginate from there.

Statistics
^^^^^^^^^^

Show the record total, the recorded time range and the counts per category,
event and status:

.. code-block:: bash

    tiny-pacs audit stats
    tiny-pacs audit stats --format json

Cleanup
^^^^^^^

Delete records older than a given age. The threshold is required and
refuses values below one day, so an empty flag set can never wipe the
trail:

.. code-block:: bash

    tiny-pacs audit cleanup --older-than 90

A configured ``retention_days`` performs the same purge automatically at
startup; the command is the on-demand equivalent for the CLI operator.

.. note::

   Running ``audit`` against a live server's SQLite database is safe for
   ``query``/``stats`` (read-only), but ``cleanup`` acquires a write
   transaction — run destructive maintenance with the server stopped or on
   PostgreSQL.

Privacy
-------

Detail blobs never contain passwords or keys: the DICOM user identity
sub-item is summarized as *type + username* and its secret-bearing
secondary field is never read; the opaque credentials of the not-yet-
supported identity types 3-5 are recorded as a type only. In v1 the details
carry UIDs and counts — no patient attributes. Column values are clipped to
their declared widths so a third-party emitter cannot break the insert.

Failure isolation
-----------------

Audit is an observer, and an audit failure must never affect DICOM service:

* every handler on the DICOM path swallows and logs exceptions and returns
  the neutral result of its event (a C-STORE still reports success, an
  association is still accepted/rejected on its own merits);
* records are written through :class:`~tiny_pacs.events.Atomic`; a locked
  SQLite database is retried once, then the record is dropped with an ERROR
  log.

The suite verifies this end to end: with the audit write path forced to
fail, a C-STORE to a live server still succeeds.

Reference implementation
------------------------

Like :doc:`extensions-admin` and :doc:`extensions-identity`,
``tiny-pacs-audit`` doubles as a reference implementation of the extension
contract described in :doc:`extensions`: a component published through
``tiny_pacs.components`` (with its own table and
:class:`~tiny_pacs.events.Migrations`) and a CLI subcommand published
through ``tiny_pacs.cli``.
