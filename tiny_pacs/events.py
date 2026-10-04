"""Event definitions for the tiny_pacs event system.

Events are built on top of :mod:`trolleybus`: every event is a class carrying
its payload type and listener result type. Events are never instantiated —
the event class itself is broadcast together with a single payload value.
Multi-value payloads are packed into small dataclasses.

Lifecycle events are provided by :mod:`trolleybus` itself
(:class:`trolleybus.OnStart`, :class:`trolleybus.OnStarted` and
:class:`trolleybus.OnExit`) and are emitted by
:meth:`trolleybus.EventBus.start` / :meth:`trolleybus.EventBus.stop`.
"""
import datetime
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, BinaryIO, TypeAlias

import peewee
import pydantic
import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import asceprovider, fsm, pdu, statuses

from .assoc_context import AssocContext

if TYPE_CHECKING:
    from . import client, devices, identity, schema  # noqa: F401


#: Single stored file: SOP Class UID, Transfer Syntax UID and either a file
#: name, a dataset or a file object. UID values can come either as
#: ``pydicom.uid.UID`` objects or as plain strings (e.g. from the database).
StoredFile: TypeAlias = tuple[
    str | uid.UID,
    str | uid.UID,
    str | pydicom.Dataset | BinaryIO
]


# ---------------------------------------------------------------------------
# AE events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AssocPayload:
    """Payload of the :class:`Assoc` event."""

    #: Association acceptor handling the incoming connection
    asce: asceprovider.AssociationAcceptor

    #: Association request parameters
    assoc: pdu.AAssociateRqPDU


class Assoc(trolleybus.Event[AssocPayload, None]):
    """Incoming association request."""


@dataclass(frozen=True)
class AssocRejectedPayload:
    """Payload of the :class:`AssocRejected` event."""

    #: Association request parameters; None when the called AE title
    #: itself is invalid
    assoc: pdu.AAssociateRqPDU | None

    #: Human-readable rejection reason
    reason: str


class AssocRejected(trolleybus.Event[AssocRejectedPayload, None]):
    """Incoming association request has been rejected.

    Broadcast by the AE both when the called AE title is invalid and when
    an :class:`Assoc` listener raises
    :class:`pynetdicom2.exceptions.AssociationRejectedError`.
    """


class AssocReleased(trolleybus.Event[AssocContext, None]):
    """An accepted association has ended.

    Payload is the association context; the event is broadcast by the AE
    at association teardown whatever the reason (release, abort, timeout
    or error).
    """


@dataclass(frozen=True)
class StorePayload:
    """Payload of the :class:`Store` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: Incoming dataset: file object (when SOP Class is stored in file) or
    #: raw encoded bytes
    ds: BinaryIO | bytes

    #: Association context the request belongs to, when known
    session: AssocContext | None = None


class Store(trolleybus.Event[StorePayload, statuses.Status]):
    """Incoming C-STORE request. Handling result is a C-STORE status."""


@dataclass(frozen=True)
class FindPayload:
    """Payload of the :class:`Find` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: C-FIND request dataset
    ds: pydicom.Dataset

    #: Association context the request belongs to, when known
    session: AssocContext | None = None


class Find(trolleybus.Event[FindPayload,
                            Iterable[tuple[pydicom.Dataset,
                                           statuses.Status]]]):
    """Incoming C-FIND request.

    Handling result is an iterable of ``(dataset, status)`` tuples.
    """


@dataclass(frozen=True)
class MovePayload:
    """Payload of the :class:`Move` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: C-MOVE request dataset
    ds: pydicom.Dataset

    #: C-MOVE destination AE title
    destination: str

    #: Association context the request belongs to, when known
    session: AssocContext | None = None


class Move(trolleybus.Event[MovePayload, list[StoredFile]]):
    """Incoming C-MOVE request. Handling result is a list of stored files."""


@dataclass(frozen=True)
class GetPayload:
    """Payload of the :class:`Get` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: C-GET request dataset
    ds: pydicom.Dataset

    #: Association context the request belongs to, when known
    session: AssocContext | None = None


class Get(trolleybus.Event[GetPayload, list[StoredFile]]):
    """Incoming C-GET request. Handling result is a list of stored files."""


@dataclass(frozen=True)
class StoreDatasetPayload:
    """Payload of the :class:`StoreDataset` event."""

    #: Decoded dataset to record in the archive
    ds: pydicom.Dataset

    #: Transfer Syntax UID the dataset arrived with
    transfer_syntax: str

    #: Where the dataset came from: ``dimse`` for C-STORE requests
    #: handled by the AE, other values for non-DIMSE sources (e.g.
    #: ``stow`` for DICOMweb STOW-RS)
    origin: str = 'dimse'


class StoreDataset(trolleybus.Event[StoreDatasetPayload, None]):
    """Decoded dataset to feed into the store pipeline.

    Broadcast by the AE after decoding an incoming C-STORE dataset and
    directly by non-DIMSE sources (e.g. DICOMweb STOW-RS). The PACS
    component records the database rows and broadcasts
    :class:`StoreDone` / :class:`StoreFailure` accordingly; storage
    components keep their existing subscriptions. Handling failures are
    signalled by raising from the handler.
    """


class Commitment(
    trolleybus.Event[list[tuple[uid.UID, uid.UID]],
                     tuple[list[tuple[uid.UID, uid.UID]],
                           list[tuple[uid.UID, uid.UID]]]]):
    """Storage Commitment request.

    Payload is a list of ``(SOP Class UID, SOP Instance UID)`` tuples; handling
    result is a tuple of two lists — successfully stored instances and
    failures.
    """


@dataclass(frozen=True)
class GetFilePayload:
    """Payload of the :class:`GetFile` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: Command dataset of the received message
    command_set: pydicom.Dataset

    #: Transfer Syntax UID to record; used by non-DIMSE sources that have
    #: no real presentation context. When None, storage components use
    #: ``context.supported_ts``.
    transfer_syntax: str | None = None


class GetFile(trolleybus.Event[GetFilePayload, tuple[BinaryIO, int]]):
    """Request a file object to store incoming dataset.

    Handling result is a tuple of the file object and the starting position
    of the dataset stream within it.
    """


class MainAET(trolleybus.Event[None, str]):
    """Request the main AE Title of the service."""


@dataclass(frozen=True)
class ServiceHook:
    """Additional SOP classes contributed to the AE by an extension.

    :ivar name: short hook name, e.g. ``mwl``
    :ivar sop_classes: abstract syntaxes the hook serves
    :ivar on_find: event class broadcast instead of :class:`Find` for
                   C-FIND requests on the hook's SOP classes; the event
                   is declared by the contributing extension, the AE only
                   broadcasts the class it is given
    """

    #: Short hook name
    name: str

    #: Abstract syntaxes served by the hook
    sop_classes: list[str]

    #: C-FIND event class of the hook, when it serves queries
    on_find: type[trolleybus.Event[Any, Any]] | None = None


class ServicesRegistry(trolleybus.Event[None, list[ServiceHook]]):
    """Request additional service hooks from components.

    Broadcast by the AE after its built-in SCPs are registered; every
    contributed hook adds SOP classes to the AE. Hooks may only add
    services, never replace the built-ins.
    """


# ---------------------------------------------------------------------------
# Shared HTTP server events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HttpAppHook:
    """One WSGI application contributed to the shared HTTP server.

    :ivar name: providing component name (logging/introspection)
    :ivar prefix: mount prefix; ``/`` is the root/catch-all application
    :ivar app: WSGI callable (no framework assumed)
    """

    #: Providing component name
    name: str

    #: Mount prefix, ``/`` for the root/catch-all app
    prefix: str

    #: WSGI callable serving the mount
    app: Callable[[dict, Callable], Iterable[bytes]]


class HttpAppsRegistry(trolleybus.Event[None, list[HttpAppHook]]):
    """Request the WSGI applications to mount on the shared HTTP server.

    Broadcast by the :class:`~tiny_pacs.http.HttpServer` component once
    the bus has started (:class:`trolleybus.OnStarted`); every HTTP
    front-end component answers with its hooks — an empty list when it
    refuses to mount itself (failed self-checks, headless run), which
    never affects the shared server or the other mounts.
    """


class HttpMountsQuery(trolleybus.Event[None, list[tuple[str, str]]]):
    """Request the HTTP applications mounted on the shared server.

    Answered by the :class:`~tiny_pacs.http.HttpServer` component with
    ``(component name, prefix)`` tuples for every mounted application;
    the result is empty while the server is dormant (nothing bound).
    """


# ---------------------------------------------------------------------------
# Storage events
# ---------------------------------------------------------------------------

class StoreDone(trolleybus.Event[pydicom.Dataset, None]):
    """Dataset has been stored successfully. Payload is the stored dataset."""


class StoreFailure(trolleybus.Event[pydicom.Dataset, None]):
    """Dataset storage has failed. Payload is the dataset that failed."""


class GetFiles(trolleybus.Event[list[str], Iterable[StoredFile]]):
    """Request stored files by a list of SOP Instance UIDs."""


class StoreVerify(trolleybus.Event[list[tuple[uid.UID, uid.UID]],
                                   tuple[frozenset[tuple[uid.UID, uid.UID]],
                                         frozenset[tuple[uid.UID,
                                                         uid.UID]]]]):
    """Verify that provided SOP Instance UIDs are stored.

    Payload is a list of ``(SOP Class UID, SOP Instance UID)`` tuples;
    handling result is a tuple of two frozensets — successes and failures.
    """


@dataclass(frozen=True)
class StorageStatsReport:
    """Result of the :class:`StorageStatsQuery` event.

    :ivar records_total: total number of storage records
    :ivar records_stored: records successfully stored
    :ivar records_failed: records with ``is_stored == False``
    :ivar oldest: oldest ``added`` timestamp, None for an empty storage
    :ivar newest: newest ``added`` timestamp, None for an empty storage
    :ivar per_sop_class: record counts by SOP Class UID
    :ivar storage_dir: storage directory, None for non-file backends
    :ivar file_count: files on disk, None without a directory
    :ivar file_bytes: total bytes on disk, None without a directory
    :ivar per_day_bytes: bytes per ``%Y%m%d`` day folder, None without a
                         directory
    """

    records_total: int = 0
    records_stored: int = 0
    records_failed: int = 0
    oldest: datetime.datetime | None = None
    newest: datetime.datetime | None = None
    per_sop_class: dict[str, int] = field(default_factory=dict)
    storage_dir: str | None = None
    file_count: int | None = None
    file_bytes: int | None = None
    per_day_bytes: dict[str, int] | None = None


@dataclass(frozen=True)
class StorageVerifyReport:
    """Result of the :class:`StorageVerifyQuery` event.

    :ivar missing_files: stored records whose file is gone (relative to
                         the storage directory)
    :ivar orphan_files: files under the storage directory no record
                        references
    :ivar stuck_records: SOP Instance UIDs with ``is_stored == False``
    :ivar file_backend: whether the answering component has a directory
    """

    missing_files: list[str] = field(default_factory=list)
    orphan_files: list[str] = field(default_factory=list)
    stuck_records: list[str] = field(default_factory=list)
    file_backend: bool = False


class StorageCleanupOptions(pydantic.BaseModel):
    """Payload of the :class:`StorageCleanupCommand` event.

    :ivar delete_missing_records: delete records whose file is gone
    :ivar delete_orphans: delete files no record references
    :ivar failed_older_than_days: delete stuck (``is_stored == False``)
                                  records older than this many days;
                                  None keeps them; 0 is refused — a
                                  running server may legitimately have
                                  in-progress stores
    :ivar apply: perform the deletions; False is a dry run
    """

    model_config = pydantic.ConfigDict(extra='forbid')

    delete_missing_records: bool = False
    delete_orphans: bool = False
    failed_older_than_days: int | None = pydantic.Field(default=None, ge=1)
    apply: bool = False


@dataclass(frozen=True)
class StorageCleanupReport:
    """Result of the :class:`StorageCleanupCommand` event.

    :ivar records_removed: storage records deleted (applied runs)
    :ivar files_removed: files deleted (applied runs)
    :ivar would_remove_records: records a dry run would delete
    :ivar would_remove_files: files a dry run would delete
    :ivar errors: messages of operations that failed
    """

    records_removed: int = 0
    files_removed: int = 0
    would_remove_records: int = 0
    would_remove_files: int = 0
    errors: list[str] = field(default_factory=list)


class StorageStatsQuery(trolleybus.Event[None, StorageStatsReport]):
    """Request storage usage statistics.

    Answered by the storage components.
    """


class StorageVerifyQuery(trolleybus.Event[None, StorageVerifyReport]):
    """Request the storage consistency report.

    Answered by the storage components: records whose file is missing,
    orphan files and stuck in-progress records.
    """


class StorageCleanupCommand(
    trolleybus.Event[StorageCleanupOptions, StorageCleanupReport]
):
    """Run a storage cleanup.

    Dry run by default (``StorageCleanupOptions.apply``); applied cleanups
    are followed by an :class:`AuditRecord` broadcast from the storage
    component.
    """


# ---------------------------------------------------------------------------
# Database events
# ---------------------------------------------------------------------------

class Atomic(trolleybus.Event[None, Any]):
    """Request an atomic transaction context manager."""


class Tables(trolleybus.Event[None, list[type[peewee.Model]]]):
    """Request a list of database tables from components."""


class Migrations(trolleybus.Event[None, 'schema.ComponentMigrations']):
    """Request schema migrations from components.

    Handling result is the component's schema name, its tables and its
    migration list (see :class:`tiny_pacs.schema.ComponentMigrations`).
    """


class StringAgg(trolleybus.Event[None, Callable[..., peewee.Function]]):
    """Request the string aggregate SQL function for the current DB driver."""


class SchemaVersions(trolleybus.Event[None, dict[str, int]]):
    """Request the applied schema version of every component.

    Answered by the database component with a mapping of component schema
    names to their applied migration versions.
    """


class TableCounts(trolleybus.Event[None, dict[str, int]]):
    """Request the row count of every database table.

    Answered by the database component with a mapping of table names to
    their row counts.
    """


class CloseConnection(trolleybus.Event[None, None]):
    """Close the database connection of the calling thread.

    peewee tracks connections per thread, so this event must be handled in
    (and broadcast from) the thread that performed the queries. With the
    pooled DB drivers the connection is checked back into the pool for
    reuse; a worker thread that exits without it leaks its connection
    until the pool limit is reached. Sent at every thread work-unit
    boundary: incoming DICOM association threads, the per-association DUL
    thread and each served HTTP request.
    """


# ---------------------------------------------------------------------------
# Device events
# ---------------------------------------------------------------------------

class DeviceByAE(trolleybus.Event[str, 'devices.DeviceConfig | None']):
    """Request device settings by AE Title."""


class DeviceConfigs(
    trolleybus.Event[None, "dict[str, 'devices.DeviceConfig']"]
):
    """Request the configured remote devices.

    Answered by device registry components with a mapping of AE titles to
    device configurations, e.g. so database-backed registries can import
    the YAML-configured devices.
    """


class DeviceList(trolleybus.Event[None, list[Any]]):
    """Request every registered device, ordered by AE title.

    Handled by the DB-backed device registry; results are its device
    records.
    """


class DeviceAdd(trolleybus.Event[dict[str, Any], Any]):
    """Register a new device.

    Payload is a device field mapping that must contain at least ``aet``
    and ``address``; the port and the identity policy default to the
    registry's configured defaults when omitted. The result is the
    created device record.
    """


class DeviceUpdate(trolleybus.Event[dict[str, Any], Any]):
    """Update an existing device.

    Payload is a device field mapping that must contain ``aet``; only the
    provided fields are changed. The result is the updated device record.
    """


class DeviceRemove(trolleybus.Event[str, bool]):
    """Remove a device by AE title.

    The result is whether the device existed (and was removed).
    """


class AutoAddIdentity(
    trolleybus.Event[None, 'identity.IdentityPolicy | None']
):
    """Request the identity policy auto-added devices receive.

    Answered by the DB-backed device registry with its configured
    default; the result is None when no DB-backed device registry
    participates, so consumers can detect whether auto-added devices are
    persisted at all.
    """


# ---------------------------------------------------------------------------
# User events
# ---------------------------------------------------------------------------

class UserByName(trolleybus.Event[str, Any]):
    """Request a user by login name.

    Handled by the user registry; the result is the user record, or None
    for unknown usernames.
    """


class UserVerify(trolleybus.Event[dict[str, Any], Any]):
    """Verify credentials and return the matching user, or None.

    Payload is a mapping with ``username`` and ``password``; a ``None``
    password is a username-only check (DICOM user identity type 1). The
    handler runs exactly one password proof — against a dummy hash for
    unknown or inactive users — so the timing never reveals whether the
    username exists, and records a successful authentication (e.g. the
    ``last_login`` timestamp).
    """


class UserList(trolleybus.Event[None, list[Any]]):
    """Request every user, ordered by username."""


class UserAdd(trolleybus.Event[dict[str, Any], Any]):
    """Register a new user.

    Payload is a mapping with ``username`` and ``password``; the password
    is hashed before storage. The result is the created user record.
    """


class UserSetPassword(trolleybus.Event[dict[str, Any], Any]):
    """Change the password of an existing user.

    Payload is a mapping with ``username`` and ``password``. The result
    is the updated user record.
    """


class UserRemove(trolleybus.Event[str, bool]):
    """Remove a user by login name.

    The result is whether the user existed (and was removed).
    """


class UserSetActive(trolleybus.Event[dict[str, Any], Any]):
    """Activate or deactivate a user.

    Payload is a mapping with ``username`` and ``is_active``. The result
    is the updated user record.
    """


# ---------------------------------------------------------------------------
# Archive query events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ArchiveFilter:
    """Query filter of the archive query events.

    :ivar patient_id: exact Patient ID match
    :ivar patient_name: Patient's Name substring match
    :ivar patient_birth_date: exact Patient's Birth Date (DA) match
    :ivar accession_number: exact Accession Number match
    :ivar study_date_from: earliest Study Date (DA), inclusive
    :ivar study_date_to: latest Study Date (DA), inclusive
    :ivar modality: filter to levels containing this Series Modality
    :ivar study_instance_uid: exact Study Instance UID match
    :ivar series_instance_uid: exact Series Instance UID match
    :ivar sop_instance_uid: exact SOP Instance UID match
    :ivar limit: maximum number of items returned
    :ivar offset: number of items skipped (pagination)

    Filter semantics are uniform across the queried levels: a filter
    naming a level below the queried one restricts the results to the
    rows *owning* at least one matching lower-level row (e.g. a Study
    Date range on a PATIENT query matches patients through their
    studies, and the Series Modality filter has always worked this way).
    """

    patient_id: str | None = None
    patient_name: str | None = None
    patient_birth_date: str | None = None
    accession_number: str | None = None
    study_date_from: str | None = None
    study_date_to: str | None = None
    modality: str | None = None
    study_instance_uid: str | None = None
    series_instance_uid: str | None = None
    sop_instance_uid: str | None = None
    limit: int = 100
    offset: int = 0


@dataclass(frozen=True)
class ArchiveItem:
    """One archive query result.

    :ivar uids: identifiers of the item: ``patient_id`` and the study,
                series and instance UIDs down to the queried level
    :ivar fields: DB-level attributes of the queried level
                  (attribute name to value)
    :ivar attributes: DICOM view of the level: tag to ``(VR, value)``,
                      built by the PACS component from its model
                      mappings so consumers never re-derive DICOM
                      semantics
    :ivar total: total number of matches before pagination
    """

    uids: dict[str, str] = field(default_factory=dict)
    fields: dict[str, Any] = field(default_factory=dict)
    attributes: dict[int, tuple[str, Any]] = field(default_factory=dict)
    total: int = 0


class ArchivePatientQuery(
    trolleybus.Event[ArchiveFilter, list[ArchiveItem]]
):
    """Request patients of the archive. Answered by the PACS component."""


class ArchiveStudyQuery(
    trolleybus.Event[ArchiveFilter, list[ArchiveItem]]
):
    """Request studies of the archive. Answered by the PACS component."""


class ArchiveSeriesQuery(
    trolleybus.Event[ArchiveFilter, list[ArchiveItem]]
):
    """Request series of the archive. Answered by the PACS component."""


class ArchiveInstanceQuery(
    trolleybus.Event[ArchiveFilter, list[ArchiveItem]]
):
    """Request instances of the archive. Answered by the PACS component."""


# ---------------------------------------------------------------------------
# Audit events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AuditRecordPayload:
    """Payload of the :class:`AuditRecord` event.

    Emitted by every component performing operator-visible mutations
    (device/user management, storage maintenance and the like) so an
    installed audit component records them without any coupling between
    the extensions. Emitters never include secrets in :attr:`details`.

    :ivar category: record category, e.g. ``admin`` or ``storage``
    :ivar event: record event, e.g. ``device-add`` or ``user-passwd``
    :ivar device_aet: device the record relates to, when applicable
    :ivar username: acting user, when known
    :ivar status: ``success`` or ``failure``
    :ivar details: additional JSON-serializable information
    """

    #: Record category, e.g. ``admin`` or ``storage``
    category: str

    #: Record event, e.g. ``device-add`` or ``user-passwd``
    event: str

    #: Device the record relates to, when applicable
    device_aet: str | None = None

    #: Acting user, when known
    username: str | None = None

    #: ``success`` or ``failure``
    status: str = 'success'

    #: Additional JSON-serializable information
    details: dict[str, Any] = field(default_factory=dict)


class AuditRecord(trolleybus.Event[AuditRecordPayload, None]):
    """Administrative or opaque action to be recorded by the audit trail.

    Fire-and-forget: emitters broadcast it after their own mutation
    succeeded; no listener is required.
    """


@dataclass(frozen=True)
class AuditFilter:
    """Filter of the audit query events.

    :ivar categories: restrict to these record categories
    :ivar events: restrict to these record events
    :ivar device_aet: restrict to this device
    :ivar username: restrict to this user
    :ivar since: earliest record timestamp, inclusive
    :ivar until: latest record timestamp, inclusive
    :ivar limit: maximum number of records returned
    :ivar offset: number of records skipped (pagination)
    """

    categories: list[str] | None = None
    events: list[str] | None = None
    device_aet: str | None = None
    username: str | None = None
    since: datetime.datetime | None = None
    until: datetime.datetime | None = None
    limit: int = 100
    offset: int = 0


class AuditQuery(trolleybus.Event[AuditFilter, list[Any]]):
    """Request audit records matching the filter.

    Answered by the audit component with its audit records; the result is
    empty when no audit component is installed.
    """


class AuditCount(trolleybus.Event[AuditFilter, int]):
    """Count audit records matching the filter.

    Answered by the audit component; 0 when it is not installed.
    """


class AuditStats(trolleybus.Event[None, dict[str, Any]]):
    """Request aggregate audit statistics.

    Answered by the audit component with counts per category/event plus
    the recorded time range; empty when it is not installed.
    """


# ---------------------------------------------------------------------------
# Client events
# ---------------------------------------------------------------------------

class GetClient(trolleybus.Event[str, 'client.DICOMClient']):
    """Request a DICOM client for the provided AE Title."""
