"""Append-only audit log component.

The :class:`AuditLog` component records who did what into a single
append-only table in the shared database: every incoming association
(accepted *and* rejected), every service request (C-STORE/-FIND/-MOVE/
-GET, Storage Commitment) and every administrative action other
components emit through the core :class:`~tiny_pacs.events.AuditRecord`
event.

Attribution comes from the core association context and the ``session``
field of the service payloads, so the component imports the core only —
never the admin or identity extensions.

Failure isolation
-----------------

The component is an observer: an audit failure must never affect DICOM
service. Every handler on the DICOM path swallows (and logs) exceptions
and returns the neutral result of its event, so

* a raising handler can never abort an ``Assoc`` rejection chain or a
  C-STORE/-FIND/-MOVE/-GET broadcast;
* DB writes go through :class:`~tiny_pacs.events.Atomic` and, on a locked
  SQLite database, are retried once before the record is dropped with an
  ERROR log.

Association records
-------------------

The component subscribes to :class:`~tiny_pacs.events.Assoc` *above* the
authentication (+20) and device registry (+10) listeners, so the
association is recorded before any lower-priority listener can reject it
and abort the broadcast. That record is a pre-authentication
``assoc``/``attempt`` row (status ``pending``) whose ``username`` holds the
*presented* — not yet verified — identity, so an unauthenticated peer can
never forge a row claiming an ``accepted``/authenticated association. The
authoritative outcome comes from the dedicated lifecycle events: a rejected
association produces an ``assoc``/``rejected`` row (with the reason) from
:class:`~tiny_pacs.events.AssocRejected`, and a completed one an
``assoc``/``released`` row from :class:`~tiny_pacs.events.AssocReleased`
carrying the *authenticated* principal. All three rows of one association
share the ``correlation_id`` (from the association context) in their
``details`` so the attempt can be tied to its outcome.
"""
import datetime
import json
from collections.abc import Callable, Iterable
from typing import Any, TypeVar, cast

import peewee
import pydantic
import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import pdu, statuses
from tiny_pacs import assoc_context, component, events, schema
from tiny_pacs import identity as core_identity
from tiny_pacs.assoc_context import AssocContext

from . import models
from .models import AuditEventModel

TR = TypeVar('TR')

#: Above the authentication (+20) and device registry (+10) listeners, so
#: an association attempt is recorded before a lower-priority listener can
#: reject it and abort the remaining ``Assoc`` listeners.
ASSOC_PRIORITY = trolleybus.DEFAULT_PRIORITY + 30

#: Below the PACS/storage components (default priority), so audit observes
#: service requests and outcomes without influencing their ordering.
SERVICE_PRIORITY = trolleybus.DEFAULT_PRIORITY - 30

#: DICOM user identity types that carry a username in the primary field
#: (PS3.7 D.3.3.7.1 types 1 and 2). The opaque credentials of types 3-5
#: are never recorded.
_USERNAME_IDENTITY_TYPES = frozenset((1, 2))

#: Upper bound for a single string value stored inside the ``details``
#: blob (peer-controlled request elements are clipped to this width).
MAX_DETAILS_VALUE = 64

#: Upper bound for the serialized ``details`` JSON blob; larger blobs are
#: replaced by a truncation marker so a single record cannot grow unbounded.
MAX_DETAILS_BLOB = 4096

#: Upper bound for a single query page. ``AuditFilter.limit`` values that
#: are non-positive or larger than this are clamped, so a query can never
#: hydrate the whole (only-growing) table in one call.
MAX_QUERY_ROWS = 10_000

#: Rows deleted per transaction during a purge, so a large retention or
#: cleanup pass never holds the (single-writer) SQLite lock for long or
#: builds an oversized rollback journal. Mirrors the storage component's
#: batching convention.
DELETE_CHUNK = 500

#: ``str.translate`` table removing every C0/C1 control character and DEL,
#: so peer-supplied text (the DICOM user identity username) can never
#: inject forged log rows or terminal escape sequences when printed.
_STRIP_CONTROL = dict.fromkeys([*range(0x00, 0x20), 0x7F, *range(0x80, 0xA0)])


class AuditCleanupOptions(pydantic.BaseModel):
    """Payload of the :class:`AuditCleanup` event.

    :ivar older_than_days: delete records older than this many days; the
                           minimum of 1 prevents accidentally emptying the
                           whole trail
    """

    model_config = pydantic.ConfigDict(extra='forbid')

    older_than_days: int = pydantic.Field(ge=1)


class AuditCleanup(trolleybus.Event[AuditCleanupOptions, int]):
    """One-shot audit trail cleanup request.

    Extension-local (a single producer — the ``audit cleanup`` CLI — and a
    single consumer, the :class:`AuditLog` component); the result is the
    number of deleted records. Deliberately not a core event: cross-
    extension cleanup would need a core addition, recorded as a follow-up.
    """


def _text(value: Any) -> str | None:
    """Normalizes peer-provided text to a safe, stripped string (or None).

    Embedded control characters (CR/LF, ANSI escapes, NUL, ...) are removed
    so a peer-controlled value can never forge additional rows in a printed
    audit report, manipulate an operator's terminal, or reach a downstream
    consumer (e.g. the web admin app) as an injection vector.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    text = str(value).translate(_STRIP_CONTROL).strip()
    return text or None


def _clip(value: str | None, width: int) -> str | None:
    """Truncates a column value to its declared width.

    Guards the insert on PostgreSQL (which enforces ``VARCHAR(n)``) against
    an arbitrary emitter — e.g. a third-party ``AuditRecord`` — supplying an
    over-long value; SQLite ignores the width, so the clipping keeps both
    drivers behaving identically.
    """
    if value is None:
        return None
    return str(value)[:width]


def _ds_value(ds: pydicom.Dataset, name: str) -> str | None:
    """Reads a dataset element as sanitized, width-bounded text.

    Peer-controlled request elements are clipped to
    :data:`MAX_DETAILS_VALUE` (pydicom/pynetdicom2 do not enforce the DICOM
    VR length limits on decode), so a malformed, oversized element cannot
    inflate the ``details`` blob.
    """
    value = getattr(ds, name, None)
    if value is None:
        return None
    return _clip(_text(value), MAX_DETAILS_VALUE)


class AuditLogConfig(component.ComponentConfig):
    """Configuration of the :class:`AuditLog` component.

    :ivar retention_days: records older than this many days are purged at
                          startup; ``0`` keeps them forever
    :ivar record_details: when False, only the fixed columns are stored and
                          the JSON ``details`` blob stays empty
    """

    retention_days: int = pydantic.Field(default=0, ge=0)
    record_details: bool = True


class AuditLog(component.Component[AuditLogConfig]):
    """Component that records an append-only audit trail.

    Handles the following events:

        * :class:`~tiny_pacs.events.Migrations`
        * :class:`~tiny_pacs.events.Assoc`
        * :class:`~tiny_pacs.events.AssocRejected`
        * :class:`~tiny_pacs.events.AssocReleased`
        * :class:`~tiny_pacs.events.Store`
        * :class:`~tiny_pacs.events.StoreDone`
        * :class:`~tiny_pacs.events.StoreFailure`
        * :class:`~tiny_pacs.events.Find`
        * :class:`~tiny_pacs.events.Move`
        * :class:`~tiny_pacs.events.Get`
        * :class:`~tiny_pacs.events.Commitment`
        * :class:`~tiny_pacs.events.AuditRecord`
        * :class:`~tiny_pacs.events.AuditQuery`
        * :class:`~tiny_pacs.events.AuditCount`
        * :class:`~tiny_pacs.events.AuditStats`

    The component is insert-only: it publishes no mutation events for
    :class:`~tiny_pacs_audit.models.AuditEventModel`. Retention and the
    ``audit cleanup`` CLI command are the only deletion paths.
    """

    config_model = AuditLogConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: AuditLogConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: AuditLogConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.Migrations, self.migrations)

        # Association lifecycle (the attempt is recorded above the
        # rejecting listeners; the outcome from the dedicated events)
        self.subscribe(events.Assoc, self.on_assoc, ASSOC_PRIORITY)
        self.subscribe(events.AssocRejected, self.on_assoc_rejected)
        self.subscribe(events.AssocReleased, self.on_assoc_released)

        # C-STORE request and outcome
        self.subscribe(events.Store, self.on_store, SERVICE_PRIORITY)
        self.subscribe(events.StoreDone, self.on_store_done, SERVICE_PRIORITY)
        self.subscribe(events.StoreFailure, self.on_store_failure,
                       SERVICE_PRIORITY)

        # Other DIMSE services
        self.subscribe(events.Find, self.on_find, SERVICE_PRIORITY)
        self.subscribe(events.Move, self.on_move, SERVICE_PRIORITY)
        self.subscribe(events.Get, self.on_get, SERVICE_PRIORITY)
        self.subscribe(events.Commitment, self.on_commitment, SERVICE_PRIORITY)

        # Administrative/opaque actions emitted by other components
        self.subscribe(events.AuditRecord, self.on_audit_record)

        # One-shot cleanup requested by the ``audit cleanup`` CLI
        self.subscribe(AuditCleanup, self.on_cleanup)

        # Query API consumed by the CLI and the web admin app
        self.subscribe(events.AuditQuery, self.on_query)
        self.subscribe(events.AuditCount, self.on_count)
        self.subscribe(events.AuditStats, self.on_stats)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Handles `OnStart`: applies the retention purge.

        Runs after the ``Database`` component has created and bound the
        audit tables (the same default priority, and the database
        component is always constructed first). A purge failure is logged
        and never aborts startup.

        Offline ``audit`` CLI runs construct the component with retention
        disabled (see the CLI), so this fires for a real server start, not
        as a side effect of a read-only ``query``/``stats`` command.
        """
        super().on_start()
        retention = self.config.retention_days
        if retention <= 0:
            return
        cutoff = models._utcnow() - datetime.timedelta(days=retention)
        try:
            removed = self._delete_before(cutoff)
        except Exception as error:
            self.log_error('Audit retention purge failed: %s', error)
            return
        if removed:
            self.log_info(
                'Audit retention: removed %d record(s) older than %d day(s)',
                removed, retention
            )

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), models.TABLES, models.MIGRATIONS
        )

    # ------------------------------------------------------------------
    # Association lifecycle
    # ------------------------------------------------------------------

    def on_assoc(self, payload: events.AssocPayload) -> None:
        """Handles `Assoc`: records the association attempt.

        Runs above the authentication and device-registry listeners, so
        the attempt is recorded even when a lower-priority listener later
        rejects the association.

        :param payload: association acceptor and request parameters
        :type payload: events.AssocPayload
        """
        self._guard(self._record_assoc, payload)

    def _record_assoc(self, payload: events.AssocPayload) -> None:
        assoc = payload.assoc
        context = assoc_context.current()
        identity_type, presented = self._presented_identity(assoc)
        # Recorded *before* authentication/device-policy listeners run, so
        # this is a pre-authentication attempt: the username is the
        # (unverified) presented identity and the status is ``pending`` —
        # the authoritative accept/reject outcome comes from the
        # ``AssocReleased``/``AssocRejected`` records, which share the
        # correlation id.
        self._write(
            category='assoc', event='attempt', status='pending',
            device_aet=assoc.calling_ae_title.strip() or None,
            username=presented,
            peer=(context.peer if context is not None else None) or None,
            details={
                'called_aet': assoc.called_ae_title.strip(),
                'correlation_id':
                    context.correlation_id if context is not None else None,
                'identity_present': identity_type is not None,
                'identity_type': identity_type
            }
        )

    def on_assoc_rejected(
            self, payload: events.AssocRejectedPayload) -> None:
        """Handles `AssocRejected`: records the rejection with its reason.

        :param payload: rejected association request (may be None for an
                        invalid called AE title) and the rejection reason
        :type payload: events.AssocRejectedPayload
        """
        self._guard(self._record_assoc_rejected, payload)

    def _record_assoc_rejected(
            self, payload: events.AssocRejectedPayload) -> None:
        assoc = payload.assoc
        context = assoc_context.current()
        if assoc is not None:
            calling_aet = assoc.calling_ae_title.strip()
            called_aet = assoc.called_ae_title.strip()
            identity_type, presented = self._presented_identity(assoc)
        else:
            calling_aet = context.calling_aet if context is not None else ''
            called_aet = context.called_aet if context is not None else ''
            identity_type, presented = None, None
        peer = context.peer if context is not None else ''
        self._write(
            category='assoc', event='rejected', status='rejected',
            device_aet=calling_aet or None,
            username=presented,
            peer=peer or None,
            details={
                'called_aet': called_aet,
                'correlation_id':
                    context.correlation_id if context is not None else None,
                'reason': payload.reason,
                'identity_type': identity_type
            }
        )

    def on_assoc_released(self, context: AssocContext) -> None:
        """Handles `AssocReleased`: records the association teardown.

        :param context: association context; carries the authenticated
                        principal when one was established
        :type context: AssocContext
        """
        self._guard(self._record_assoc_released, context)

    def _record_assoc_released(self, context: AssocContext) -> None:
        self._write(
            category='assoc', event='released', status='success',
            device_aet=context.calling_aet or None,
            username=context.username,
            peer=context.peer or None,
            details={
                'called_aet': context.called_aet,
                'correlation_id': context.correlation_id
            }
        )

    # ------------------------------------------------------------------
    # Service requests and outcomes
    # ------------------------------------------------------------------

    def on_store(self, payload: events.StorePayload) -> statuses.Status:
        """Handles `Store`: records the C-STORE request.

        Returns the neutral success status; audit never influences the
        C-STORE result.

        :param payload: presentation context, incoming dataset and session
        :type payload: events.StorePayload
        :return: C-STORE success status
        :rtype: statuses.Status
        """
        self._guard(self._record_service, payload.session, 'store',
                    {'sop_class': str(payload.context.sop_class)})
        return statuses.SUCCESS

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone`: records the successful C-STORE outcome.

        :param ds: stored dataset
        :type ds: pydicom.Dataset
        """
        self._guard(self._record_store_outcome, ds, 'store-done', 'success')

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure`: records the failed C-STORE outcome.

        :param ds: dataset that failed to store
        :type ds: pydicom.Dataset
        """
        self._guard(self._record_store_outcome, ds, 'store-failure',
                    'failure')

    def on_find(self, payload: events.FindPayload
                ) -> Iterable[tuple[pydicom.Dataset, statuses.Status]]:
        """Handles `Find`: records the C-FIND request.

        Returns an empty iterable so the query results of the PACS
        component are unaffected.

        :param payload: presentation context, request dataset and session
        :type payload: events.FindPayload
        :return: empty result iterable
        :rtype: Iterable[tuple[pydicom.Dataset, statuses.Status]]
        """
        self._guard(self._record_service, payload.session, 'find', {
            'sop_class': str(payload.context.sop_class),
            'level': _ds_value(payload.ds, 'QueryRetrieveLevel')
        })
        return ()

    def on_move(self, payload: events.MovePayload
                ) -> list[events.StoredFile]:
        """Handles `Move`: records the C-MOVE request.

        Returns an empty list so the files resolved by the PACS component
        are unaffected.

        :param payload: presentation context, request dataset, destination
                        and session
        :type payload: events.MovePayload
        :return: empty stored-file list
        :rtype: list[events.StoredFile]
        """
        self._guard(self._record_service, payload.session, 'move', {
            'sop_class': str(payload.context.sop_class),
            'level': _ds_value(payload.ds, 'QueryRetrieveLevel'),
            'destination': _clip(_text(payload.destination),
                                 MAX_DETAILS_VALUE)
        })
        return []

    def on_get(self, payload: events.GetPayload) -> list[events.StoredFile]:
        """Handles `Get`: records the C-GET request.

        Returns an empty list so the files resolved by the PACS component
        are unaffected.

        :param payload: presentation context, request dataset and session
        :type payload: events.GetPayload
        :return: empty stored-file list
        :rtype: list[events.StoredFile]
        """
        self._guard(self._record_service, payload.session, 'get', {
            'sop_class': str(payload.context.sop_class),
            'level': _ds_value(payload.ds, 'QueryRetrieveLevel')
        })
        return []

    def on_commitment(
            self, uids: list[tuple[uid.UID, uid.UID]]
    ) -> tuple[list[tuple[uid.UID, uid.UID]], list[tuple[uid.UID, uid.UID]]]:
        """Handles `Commitment`: records the Storage Commitment request.

        Returns two empty lists so the committed/failed instances reported
        by the PACS component are unaffected.

        :param uids: SOP Class / SOP Instance UID tuples to verify
        :type uids: list[tuple[uid.UID, uid.UID]]
        :return: empty success and failure lists
        :rtype: tuple
        """
        self._guard(self._record_commitment, len(uids))
        return [], []

    def _record_store_outcome(
            self, ds: pydicom.Dataset, event: str, status: str) -> None:
        self._record_service(
            assoc_context.current(), event,
            {
                'sop_class': _ds_value(ds, 'SOPClassUID'),
                'sop_instance': _ds_value(ds, 'SOPInstanceUID')
            },
            status=status
        )

    def _record_commitment(self, instances: int) -> None:
        # The Commitment payload carries no session; attribute through the
        # association context (the request is handled on its thread)
        self._record_service(
            assoc_context.current(), 'commitment', {'instances': instances}
        )

    def _record_service(
            self,
            session: AssocContext | None,
            event: str,
            details: dict[str, Any],
            status: str = 'success'
    ) -> None:
        """Records a DIMSE service request, attributed to its session.

        The session travels on the payload; when absent (e.g. a
        non-DIMSE store pipeline) the association context of the handling
        thread is used.
        """
        context = session or assoc_context.current()
        self._write(
            category='service', event=event, status=status,
            device_aet=context.calling_aet if context is not None else None,
            username=context.username if context is not None else None,
            peer=(context.peer if context is not None else None) or None,
            details=details
        )

    # ------------------------------------------------------------------
    # Administrative records from other components
    # ------------------------------------------------------------------

    def on_audit_record(self, payload: events.AuditRecordPayload) -> None:
        """Handles `AuditRecord`: records an administrative/opaque action.

        :param payload: record fields emitted by another component
        :type payload: events.AuditRecordPayload
        """
        self._guard(self._record_audit_record, payload)

    def _record_audit_record(
            self, payload: events.AuditRecordPayload) -> None:
        self._write(
            category=payload.category, event=payload.event,
            status=payload.status,
            device_aet=payload.device_aet,
            username=payload.username,
            peer=None,
            details=dict(payload.details)
        )

    # ------------------------------------------------------------------
    # Query API
    # ------------------------------------------------------------------

    def on_query(
            self, payload: events.AuditFilter) -> list[AuditEventModel]:
        """Handles `AuditQuery`: returns the matching records.

        :param payload: query filter
        :type payload: events.AuditFilter
        :return: matching records, oldest first
        :rtype: list[AuditEventModel]
        """
        return list(self._filtered(payload))

    def on_count(self, payload: events.AuditFilter) -> int:
        """Handles `AuditCount`: counts the matching records.

        The count reflects every matching record, independent of the
        pagination (``limit``/``offset``) that :meth:`on_query` applies.

        :param payload: query filter
        :type payload: events.AuditFilter
        :return: number of matching records
        :rtype: int
        """
        return self._predicates(AuditEventModel.select(), payload).count()

    def on_stats(self, _: None = None) -> dict[str, Any]:
        """Handles `AuditStats`: returns aggregate trail statistics.

        :return: total row count, the recorded time range and the row
                 counts per category, event and status
        :rtype: dict[str, Any]
        """
        oldest = AuditEventModel.select()\
            .order_by(AuditEventModel.timestamp).first()
        newest = AuditEventModel.select()\
            .order_by(AuditEventModel.timestamp.desc()).first()
        return {
            'total': AuditEventModel.select().count(),
            'oldest': oldest.timestamp if oldest is not None else None,
            'newest': newest.timestamp if newest is not None else None,
            'by_category': self._group_counts(AuditEventModel.category),
            'by_event': self._group_counts(AuditEventModel.event),
            'by_status': self._group_counts(AuditEventModel.status)
        }

    def _predicates(
            self,
            query: 'peewee.ModelSelect[AuditEventModel]',
            payload: events.AuditFilter
    ) -> 'peewee.ModelSelect[AuditEventModel]':
        """Applies the filter predicates only (no ordering, no pagination).

        Shared by :meth:`_filtered` and :meth:`on_count` so the count and
        the page always agree on *what* matches.

        :param query: the query to add predicates to
        :param payload: query filter
        :return: the constrained query
        :rtype: peewee.ModelSelect[AuditEventModel]
        """
        if payload.categories:
            query = query.where(AuditEventModel.category << payload.categories)
        if payload.events:
            query = query.where(AuditEventModel.event << payload.events)
        if payload.device_aet:
            query = query.where(
                AuditEventModel.device_aet == payload.device_aet)
        if payload.username:
            query = query.where(AuditEventModel.username == payload.username)
        if payload.since is not None:
            query = query.where(AuditEventModel.timestamp >= payload.since)
        if payload.until is not None:
            query = query.where(AuditEventModel.timestamp <= payload.until)
        return query

    def _filtered(
            self, payload: events.AuditFilter
    ) -> 'peewee.ModelSelect[AuditEventModel]':
        """Translates an :class:`~tiny_pacs.events.AuditFilter` to a query.

        The result page is always bounded: a non-positive ``limit`` and any
        value above :data:`MAX_QUERY_ROWS` are clamped, so a query can never
        hydrate the entire (only-growing) table in one call.

        :param payload: query filter
        :type payload: events.AuditFilter
        :return: records matching the filter, oldest first
        :rtype: peewee.ModelSelect[AuditEventModel]
        """
        query = self._predicates(AuditEventModel.select(), payload)
        query = query.order_by(AuditEventModel.timestamp, AuditEventModel.id)
        limit = payload.limit if payload.limit > 0 else MAX_QUERY_ROWS
        query = query.limit(min(limit, MAX_QUERY_ROWS))
        if payload.offset and payload.offset > 0:
            query = query.offset(payload.offset)
        return query

    @staticmethod
    def _group_counts(field: Any) -> dict[str, int]:
        """Counts rows grouped by a column.

        :param field: the :class:`AuditEventModel` column to group by
        :return: column value to row count
        :rtype: dict[str, int]
        """
        rows = cast(
            Iterable[Any],
            AuditEventModel.select(
                field,
                peewee.fn.COUNT(AuditEventModel.id).alias('count')
            ).group_by(field).dicts()
        )
        return {str(row[field.name]): int(row['count']) for row in rows}

    # ------------------------------------------------------------------
    # Retention
    # ------------------------------------------------------------------

    def on_cleanup(self, options: AuditCleanupOptions) -> int:
        """Handles `AuditCleanup`: deletes records past the age threshold.

        Unlike the DICOM-path handlers this one lets failures surface, so
        the requesting operator sees them instead of a silent no-op.

        :param options: cleanup threshold
        :type options: AuditCleanupOptions
        :return: number of deleted records
        :rtype: int
        """
        removed = self.purge_older_than(options.older_than_days)
        self.log_info(
            'Audit cleanup: removed %d record(s) older than %d day(s)',
            removed, options.older_than_days
        )
        return removed

    def purge_older_than(self, days: int) -> int:
        """Deletes audit records older than the given age.

        Used by the ``audit cleanup`` CLI command; the operation always
        applies (there is no dry run for a deletion the operator explicitly
        requested with a threshold) and database failures propagate so the
        operator sees them.

        :param days: age threshold in days
        :type days: int
        :return: number of deleted records
        :rtype: int
        :raises ValueError: raised when ``days`` is less than 1
        """
        if days < 1:
            raise ValueError('--older-than must be at least 1 day')
        cutoff = models._utcnow() - datetime.timedelta(days=days)
        return self._delete_before(cutoff)

    def _delete_before(self, cutoff: datetime.datetime) -> int:
        """Deletes records older than ``cutoff`` in bounded batches.

        Both the startup retention pass and the ``audit cleanup`` command
        use this single implementation, so they always delete by the same
        predicate. Batching (:data:`DELETE_CHUNK` rows per transaction)
        keeps the single-writer SQLite lock held only briefly and bounds the
        rollback journal, which matters when purging a large backlog on a
        live server.

        Database errors propagate so each caller decides policy: the cleanup
        CLI surfaces them, while startup retention swallows and logs them.

        :param cutoff: earliest timestamp to keep
        :type cutoff: datetime.datetime
        :return: total number of deleted records
        :rtype: int
        """
        deleted = 0
        while True:
            with self.atomic():
                batch = int(AuditEventModel.delete().where(
                    AuditEventModel.id.in_(
                        AuditEventModel.select(AuditEventModel.id)
                        .where(AuditEventModel.timestamp < cutoff)
                        .limit(DELETE_CHUNK)
                    )
                ).execute())
            deleted += batch
            if batch < DELETE_CHUNK:
                return deleted

    # ------------------------------------------------------------------
    # Failure isolation and writes
    # ------------------------------------------------------------------

    def _guard(
            self, action: Callable[..., TR], *args: Any) -> TR | None:
        """Runs an audit action, swallowing (and logging) any failure.

        The heartbeat of failure isolation: no handler body may let an
        exception escape into the DICOM service path or abort an event
        broadcast.

        :param action: the recording callable to run
        :type action: Callable
        :param args: positional arguments forwarded to ``action``
        :return: whatever ``action`` returned, or None when it raised
        """
        try:
            return action(*args)
        except Exception:
            self.log_exception('Audit recording failed')
            return None

    def _write(
            self,
            *,
            category: str,
            event: str,
            status: str,
            device_aet: str | None = None,
            username: str | None = None,
            peer: str | None = None,
            details: dict[str, Any] | None = None
    ) -> None:
        """Inserts one audit record.

        Column values are clipped to their declared widths and the details
        blob is dropped when :attr:`AuditLogConfig.record_details` is
        False.

        :param category: record category
        :param event: record event
        :param status: record status
        :param device_aet: device the record relates to
        :param username: acting or authenticated user
        :param peer: peer address of the association
        :param details: additional JSON-serializable information
        """
        self._insert({
            'timestamp': models._utcnow(),
            'category': _clip(category, 16),
            'event': _clip(event, 32),
            'status': _clip(status, 16),
            'device_aet': _clip(device_aet, 16),
            'username': _clip(username, 64),
            'peer': _clip(peer, 64),
            'details': self._details_json(details)
        })

    def _details_json(self, details: dict[str, Any] | None) -> str:
        """Serializes the details blob, honoring ``record_details``.

        Even though individual values are width-bounded, the whole blob is
        capped at :data:`MAX_DETAILS_BLOB` so an emitter supplying many keys
        cannot grow a single record without limit.
        """
        if not self.config.record_details or not details:
            return '{}'
        try:
            blob = json.dumps(details, default=str, sort_keys=True)
        except (TypeError, ValueError):
            # Details are best-effort; a non-serializable value never fails
            # the record itself
            self.log_warning('Audit details were not JSON-serializable')
            return '{}'
        if len(blob) > MAX_DETAILS_BLOB:
            self.log_warning(
                'Audit details exceeded %d bytes; truncated', MAX_DETAILS_BLOB)
            return '{"truncated": true}'
        return blob

    def _insert(self, row: dict[str, Any]) -> None:
        """Inserts a record, retrying a locked SQLite database once.

        A single-row INSERT is already atomic, so no explicit transaction
        wrapper is used — it would only add a bus dispatch and a
        BEGIN/COMMIT on the DICOM hot path for every audited action.
        """
        try:
            AuditEventModel.create(**row)
        except peewee.OperationalError as error:
            # A locked SQLite database is retried once, then dropped
            self.log_warning('Audit write locked, retrying once: %s', error)
            try:
                AuditEventModel.create(**row)
            except Exception as retry_error:
                self.log_error(
                    'Dropping audit record after retry: %s', retry_error)
        except Exception as error:
            self.log_error('Dropping audit record: %s', error)

    def atomic(self) -> Any:
        """Context manager for handling simple transactions

        :return: atomic transaction
        """
        return self.send_one(events.Atomic, None)

    @staticmethod
    def _presented_identity(
            assoc: pdu.AAssociateRqPDU
    ) -> tuple[int | None, str | None]:
        """Summarizes the presented user identity as ``(type, username)``.

        Only the username-bearing identity types (1 and 2) contribute a
        username. The secondary field (the password) is never read, and the
        opaque credentials of the not-yet-supported types 3-5 are not
        recorded either.

        :param assoc: association request parameters
        :type assoc: pdu.AAssociateRqPDU
        :return: identity type (None when no sub-item) and presented
                 username (None when absent or not username-bearing)
        :rtype: tuple[int or None, str or None]
        """
        item = core_identity.get_user_identity(assoc)
        if item is None:
            return None, None
        identity_type = item.user_identity_type
        username: str | None = None
        if identity_type in _USERNAME_IDENTITY_TYPES:
            username = _text(item.primary_field)
        return identity_type, username
