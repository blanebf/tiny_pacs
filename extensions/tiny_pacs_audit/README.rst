tiny_pacs_audit
===============

Append-only audit trail for `tiny_pacs
<https://github.com/blanebf/tiny_pacs>`_: records who did what — every
incoming association (accepted *and* rejected), every service request
(C-STORE/-FIND/-MOVE/-GET, Storage Commitment) and every administrative
change — into a single table in the shared database.

What it provides
----------------

* The ``AuditLog`` component — an observer that subscribes to the core
  association-lifecycle, service and ``AuditRecord`` events and writes one
  append-only row per audited action (time, device, user, peer, action,
  status and a JSON ``details`` blob). It is insert-only and publishes no
  mutation events for its own records.
* The ``tiny-pacs audit query|stats|cleanup`` subcommands for offline
  inspection and retention of the trail.

It depends on the core only: attribution comes from the core association
context and the ``session`` field of the service payloads, and
administrative actions arrive through the core
``AuditRecord`` event emitted by whatever component
performs the mutation — the audit extension never imports the admin or
identity packages.

Failure isolation
-----------------

The component is an observer: an audit failure must never affect DICOM
service. Every handler on the DICOM path swallows and logs exceptions and
returns the neutral result of its event, and records dropped after a locked
SQLite database is retried once are logged at ERROR.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-audit

or with the convenience extra:

.. code-block:: bash

    pip install tiny_pacs[audit]

Installing the extension never changes server behaviour: the component
stays disabled until enabled in the configuration.

Configuration
-------------

.. code-block:: yaml

    components:
      AuditLog:
        on: true
        retention_days: 0        # 0 = keep forever; >0 purges on startup
        record_details: true     # false stores only the fixed columns

The administration commands work offline against the configured database.
Administration against SQLite requires a file-based database — set
``db_name`` and ``mode: rwc`` on the ``Database`` component (the in-memory
default cannot persist data between command invocations):

.. code-block:: yaml

    components:
      Database:
        on: true
        db_name: pacs.db
        mode: rwc

Querying the trail
------------------

.. code-block:: bash

    tiny-pacs audit query --category assoc --limit 50
    tiny-pacs audit query --event rejected --aet MODALITY --format json
    tiny-pacs audit query --user alice --since 2024-01-01T00:00:00
    tiny-pacs audit stats
    tiny-pacs audit cleanup --older-than 90

Privacy
-------

Detail blobs never contain passwords or keys: the DICOM user identity
sub-item is summarized as *type + username* and its secondary field is
never read, and v1 details carry UIDs and counts only — no patient
attributes. Retention and ``audit cleanup`` are the deletion path.

Documentation
-------------

See the `Audit log tutorial
<https://tiny-pacs.readthedocs.io/en/latest/tutorial/extensions-audit.html>`_
in the tiny_pacs documentation.
