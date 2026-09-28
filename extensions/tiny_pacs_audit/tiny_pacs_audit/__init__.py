"""tiny-pacs-audit: append-only audit trail for tiny_pacs.

The extension plugs into tiny_pacs through entry points: the
:class:`~tiny_pacs_audit.audit.AuditLog` component records who did what —
every incoming association (accepted *and* rejected), every service
request (C-STORE/-FIND/-MOVE/-GET, Storage Commitment) and every
administrative change emitted by other components — into a single
append-only table in the shared database. The ``audit`` CLI subcommand
queries the trail offline.

It imports the core only: association and service attribution come from
core events and the core association context, and administrative actions
arrive through the core :class:`~tiny_pacs.events.AuditRecord` event.
"""
__version__ = '0.1.0'
