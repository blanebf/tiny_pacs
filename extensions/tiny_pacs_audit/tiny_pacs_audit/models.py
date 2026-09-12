"""Audit event model and schema migrations.

Provides the append-only audit record used by the
:class:`~tiny_pacs_audit.audit.AuditLog` component, together with its
baseline schema migration published through the
:class:`tiny_pacs.events.Migrations` mechanism.

The record is deliberately unstructured beyond its fixed columns: the
``details`` JSON blob lets any emitter enrich a trail entry without a
further migration, while the indexed columns serve the query and
retention paths.
"""
import datetime

import peewee
from tiny_pacs import schema


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class AuditEventModel(peewee.Model):
    """One append-only audit trail record.

    :ivar timestamp: when the audited action happened (UTC)
    :ivar category: record category, e.g. ``assoc`` | ``service`` |
                    ``storage`` | ``authn`` | ``admin``
    :ivar event: record event, e.g. ``attempt`` | ``rejected`` |
                 ``released`` | ``store`` | ``store-done`` |
                 ``store-failure`` | ``find`` | ``move`` | ``get`` |
                 ``commitment`` | ``device-add`` | ``user-add`` | ...
    :ivar device_aet: device the record relates to (the calling AE title
                      for association and service records), None when
                      unknown
    :ivar username: presented (association attempt) or acting/authenticated
                    user, None when unknown
    :ivar peer: peer address of the association, None when unknown
    :ivar status: ``pending`` | ``accepted`` | ``rejected`` | ``success`` |
                  ``failure``
    :ivar details: additional information, serialized as JSON
    """

    #: Row identifier
    id = peewee.AutoField()

    #: When the audited action happened
    timestamp = peewee.DateTimeField(default=_utcnow, index=True)

    #: Record category
    category = peewee.CharField(max_length=16, index=True)

    #: Record event
    event = peewee.CharField(max_length=32, index=True)

    #: Device the record relates to
    device_aet = peewee.CharField(max_length=16, null=True, index=True)

    #: Acting or authenticated user
    username = peewee.CharField(max_length=64, null=True, index=True)

    #: Peer address of the association
    peer = peewee.CharField(max_length=64, null=True)

    #: Record status
    status = peewee.CharField(max_length=16, index=True)

    #: Additional information, serialized as JSON
    details = peewee.TextField(default='{}')

    class Meta:
        # The dominant query pattern is a category filter combined with a
        # time window, ordered by time; this composite index serves both
        # ``audit query --category X --since/--until`` and the
        # ``by_category`` grouping in ``AuditStats`` without adding a
        # seventh single-column index to the write path.
        indexes = (
            (('category', 'timestamp'), False),
        )


#: Tables owned by the audit component
TABLES: list[type[peewee.Model]] = [AuditEventModel]

#: Schema migrations of the audit tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create audit tables')
]
