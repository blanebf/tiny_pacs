"""Storage components

Module provides various implementation of storage components.
"""
import datetime
import enum
import io
import os
import pathlib
import shutil
import tempfile
import threading
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, BinaryIO, TypeVar, cast

import peewee
import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import applicationentity, statuses

from . import component, events, questions, schema


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class StorageConfig(component.ComponentConfig):
    """Configuration common to every storage component.

    :ivar overwrite: when true an incoming C-STORE whose SOP Instance UID is
                     already stored replaces the existing instance; when false
                     (the default) such a store is refused with a failure
                     status and the already-stored instance is left untouched.
                     A replacement is only destructive once it succeeded: the
                     stored copy is kept until the incoming dataset is fully
                     stored, and a failed replacement rolls the record back to
                     it. Overwriting replaces the stored dataset and its
                     storage record only; the PACS catalogue keeps the
                     attributes it recorded on the first store of that SOP
                     Instance UID (the archive is keyed by UID, which is
                     unchanged by an overwrite), so this is intended for
                     re-pushing the same instance rather than editing its
                     indexed attributes.
    """

    overwrite: bool = False


TStorageConfig = TypeVar('TStorageConfig', bound=StorageConfig)


class _DuplicateAction(enum.Enum):
    """What to do with an incoming store of an already-known instance."""

    #: Not stored yet: proceed normally
    NEW = 'new'

    #: Stored and ``overwrite`` is on: store the new copy, then swap the
    #: record onto it (the stored copy is only removed once the replacement
    #: is durable)
    OVERWRITE = 'overwrite'

    #: Stored and ``overwrite`` is off: refuse the store
    REJECT = 'reject'


@dataclass(frozen=True)
class _OverwriteSnapshot:
    """Record state saved while an overwrite is in flight.

    Lets :meth:`StorageBase.file_stored` remove the replaced copy once the
    new one is durable and :meth:`StorageBase.rollback_overwrite` restore it
    when the replacement fails.

    :ivar file_name: physical file name of the replaced (old) copy
    :ivar transfer_syntax: transfer syntax recorded for the old copy
    :ivar is_stored: stored flag of the old copy
    :ivar added: recorded add time of the old copy
    """

    file_name: str
    transfer_syntax: str
    is_stored: bool
    added: datetime.datetime


#: C-STORE-RSP status returned when a duplicate store is refused because
#: ``overwrite`` is disabled. There is no dedicated "already exists" status in
#: PS3.4, so the generic "Refused: Out of Resources" failure is used; the peer
#: learns the instance was not (re)stored.
DUPLICATE_REJECTED_STATUS = statuses.C_STORE_OUT_OF_RESOURCES


class _RejectedStore(io.BytesIO):
    """Discard buffer returned when a C-STORE is refused.

    ``get_file`` must always hand the DIMSE decoder a writable stream, even
    when the store is refused; raising from there would abort the association
    instead of answering with a C-STORE-RSP. The decoder writes the incoming
    dataset into this throwaway buffer and the flag below lets the storage's
    :class:`~tiny_pacs.events.Store` handler surface
    :data:`DUPLICATE_REJECTED_STATUS` back to the AE.
    """

    #: Marker read by :meth:`StorageBase.on_store`
    store_rejected = True


class StorageFiles(peewee.Model):
    """Storage model

    Table for stored files.
    """

    #: SOP Instance UID of a stored file
    sop_instance_uid = peewee.CharField(max_length=64, unique=True)

    #: SOP Class UID of a stored file
    sop_class_uid = peewee.CharField(max_length=64, index=True)

    #: Transfer Syntax of a stored file
    transfer_syntax = peewee.CharField(max_length=64)

    #: File name
    file_name = peewee.TextField()

    #: When the file was added to the storage
    added = peewee.DateTimeField(default=_utcnow, index=True)

    #: Whether the file has been stored successfully
    is_stored = peewee.BooleanField(index=True, default=False)


#: Tables owned by the storage components
TABLES: list[type[peewee.Model]] = [StorageFiles]

#: Schema migrations of the storage tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create storage tables')
]

#: Grace window for orphan-file cleanup: a live C-STORE creates the file
#: *before* its database record exists, so files at least this fresh are
#: never orphan candidates for deletion (an in-store dataset would be
#: indistinguishable from an orphan during that window).
_ORPHAN_GRACE = datetime.timedelta(minutes=5)

#: Number of records removed by a cleanup within a single DELETE statement
_DELETE_CHUNK = 500


class StorageBase(component.Component[TStorageConfig]):
    """Abstract storage component.

    Provides basic storage functionality, common for all storage components.
    """

    #: All storage implementations share the same tables and thus the same
    #: schema version
    schema_name = 'Storage'

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: TStorageConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        Subscribes to all storage-related events.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: TStorageConfig or dict
        """
        super().__init__(bus, config)

        #: In-flight overwrites by SOP Instance UID: snapshot of the record
        #: state the replaced (old) copy must be restored to when the
        #: replacement store fails, and removed once it succeeds.
        self._pending_overwrite: dict[str, _OverwriteSnapshot] = {}
        self._overwrite_lock = threading.Lock()

        self.subscribe(events.Store, self.on_store)
        self.subscribe(events.GetFile, self.on_get_file)
        self.subscribe(events.StoreDone, self.on_store_done)
        self.subscribe(events.StoreFailure, self.on_store_failure)
        self.subscribe(events.GetFiles, self.on_store_get_files)
        self.subscribe(events.StoreVerify, self.verify)
        self.subscribe(events.StorageStatsQuery, self.on_storage_stats)
        self.subscribe(events.StorageVerifyQuery, self.on_storage_verify)
        self.subscribe(events.StorageCleanupCommand, self.on_storage_cleanup)
        self.subscribe(events.Migrations, self.migrations)

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), TABLES, MIGRATIONS
        )

    def atomic(self) -> Any:
        """Opens transaction

        :return: transaction context manager
        """
        return self.send_one(events.Atomic, None)

    def on_store(self, payload: events.StorePayload) -> statuses.Status:
        """Handles `Store` event: surfaces a refused-store decision.

        A duplicate C-STORE that has been refused (because ``overwrite`` is
        off) reaches the DIMSE decoder as a :class:`_RejectedStore` buffer;
        the flag on it is translated here into the refusal status so the peer
        receives a proper C-STORE-RSP instead of the association aborting.

        :param payload: presentation context, dataset and association context
        :type payload: events.StorePayload
        :return: refusal status for a rejected store, success otherwise
        :rtype: statuses.Status
        """
        if getattr(payload.ds, 'store_rejected', False):
            return DUPLICATE_REJECTED_STATUS
        return statuses.SUCCESS

    def existing_file(self, sop_instance_uid: str) -> StorageFiles | None:
        """Returns the record of an already-stored instance, if any.

        :param sop_instance_uid: SOP Instance UID to look up
        :type sop_instance_uid: str
        :return: the existing record or None
        :rtype: StorageFiles or None
        """
        return StorageFiles.get_or_none(
            StorageFiles.sop_instance_uid == sop_instance_uid
        )

    def check_duplicate(self, sop_instance_uid: str) -> _DuplicateAction:
        """Decides how to treat a store of an already-known instance.

        When the instance is unknown the store proceeds. When it exists and
        ``overwrite`` is enabled the caller must swap the record onto the new
        copy with :meth:`replace_file` (the stored copy itself is only
        removed once the replacement is durable). Otherwise the store is
        refused.

        :param sop_instance_uid: SOP Instance UID of the incoming dataset
        :type sop_instance_uid: str
        :return: the action the caller must take
        :rtype: _DuplicateAction
        """
        existing = self.existing_file(sop_instance_uid)
        if existing is None:
            return _DuplicateAction.NEW
        if self.config.overwrite:
            self.log_info(
                'Overwriting existing instance, SOP Instance UID: %s',
                sop_instance_uid
            )
            return _DuplicateAction.OVERWRITE
        self.log_warning(
            'Instance already stored and overwrite is disabled, refusing '
            'C-STORE, SOP Instance UID: %s', sop_instance_uid
        )
        return _DuplicateAction.REJECT

    def record_store(
            self,
            action: _DuplicateAction,
            sop_instance_uid: str,
            sop_class_uid: str,
            transfer_syntax: str | uid.UID,
            file_name: str
    ) -> StorageFiles:
        """Creates or swaps the storage record for an incoming dataset.

        Must be called with the action decided by :meth:`check_duplicate`;
        the record write is the only step that can lose a creation race
        against a parallel store of the same SOP Instance UID, which surfaces
        as a :class:`peewee.DatabaseError` the caller must translate into a
        refusal instead of letting it escape into the DIMSE decoder (an
        exception there aborts the association).

        :param action: the duplicate action for this store
        :type action: _DuplicateAction
        :param sop_instance_uid: SOP Instance UID of the dataset
        :type sop_instance_uid: str
        :param sop_class_uid: SOP Class UID of the dataset
        :type sop_class_uid: str
        :param transfer_syntax: original Transfer Syntax UID of the dataset
        :type transfer_syntax: str or uid.UID
        :param file_name: file name of the new copy in the storage
        :type file_name: str
        :return: the created or updated record
        :rtype: StorageFiles
        :raises peewee.DatabaseError: on a lost race or a vanished record
        """
        if action is _DuplicateAction.OVERWRITE:
            return self.replace_file(
                sop_instance_uid, sop_class_uid, transfer_syntax, file_name
            )
        return self.new_file(
            sop_instance_uid, sop_class_uid, transfer_syntax, file_name
        )

    def replace_file(
            self,
            sop_instance_uid: str,
            sop_class_uid: str,
            transfer_syntax: str | uid.UID,
            file_name: str
    ) -> StorageFiles:
        """Swaps an existing record onto the new copy of an overwrite.

        The old record state is snapshotted so :meth:`file_stored` can remove
        the replaced copy once the new one is durable and
        :meth:`rollback_overwrite` can restore it when the replacement
        fails; until then the stored copy is never touched.

        :param sop_instance_uid: SOP Instance UID of the dataset
        :type sop_instance_uid: str
        :param sop_class_uid: SOP Class UID of the dataset
        :type sop_class_uid: str
        :param transfer_syntax: original Transfer Syntax UID of the dataset
        :type transfer_syntax: str or uid.UID
        :param file_name: file name of the new copy in the storage
        :type file_name: str
        :return: the updated record
        :rtype: StorageFiles
        """
        with self.atomic():
            record = StorageFiles.get(
                StorageFiles.sop_instance_uid == sop_instance_uid
            )
            snapshot = _OverwriteSnapshot(
                file_name=record.file_name,
                transfer_syntax=record.transfer_syntax,
                is_stored=record.is_stored,
                added=record.added
            )
            record.sop_class_uid = sop_class_uid
            record.transfer_syntax = transfer_syntax
            record.file_name = file_name
            record.added = _utcnow()
            record.is_stored = False
            record.save()
        with self._overwrite_lock:
            self._pending_overwrite[sop_instance_uid] = snapshot
        self.log_info(
            'Replacing stored file '
            'SOP Instance UID: %(sop_instance_uid)s '
            'SOP Class UID: %(sop_class_uid)s '
            'Transfer Syntax UID: %(transfer_syntax)s '
            'File Name: %(file_name)s ',
            {
                'sop_instance_uid': sop_instance_uid,
                'sop_class_uid': sop_class_uid,
                'transfer_syntax': transfer_syntax,
                'file_name': file_name
            }
        )
        return record

    def _take_pending(self, sop_instance_uid: str
                      ) -> _OverwriteSnapshot | None:
        """Removes and returns the in-flight overwrite snapshot, if any."""
        with self._overwrite_lock:
            return self._pending_overwrite.pop(sop_instance_uid, None)

    def rollback_overwrite(
            self,
            sop_instance_uid: str
    ) -> tuple[_OverwriteSnapshot, str] | None:
        """Restores the record of a failed in-flight overwrite.

        The replaced (old) copy was never removed, so restoring the record
        fields makes the stored instance whole again.

        :param sop_instance_uid: SOP Instance UID of the failed store
        :type sop_instance_uid: str
        :return: the restored snapshot and the file name of the incomplete
                 new copy (for physical removal by the caller), or None when
                 no overwrite was in flight
        :rtype: tuple[_OverwriteSnapshot, str] or None
        """
        snapshot = self._take_pending(sop_instance_uid)
        if snapshot is None:
            return None
        partial_file_name = ''
        with self.atomic():
            record = StorageFiles.get_or_none(
                StorageFiles.sop_instance_uid == sop_instance_uid
            )
            if record is not None:
                partial_file_name = record.file_name
                record.file_name = snapshot.file_name
                record.transfer_syntax = snapshot.transfer_syntax
                record.is_stored = snapshot.is_stored
                record.added = snapshot.added
                record.save()
        self.log_info(
            'Rolled back failed overwrite, SOP Instance UID: %s',
            sop_instance_uid
        )
        return snapshot, partial_file_name

    def finish_overwrite(self, snapshot: _OverwriteSnapshot) -> None:
        """Removes the replaced copy after a successful overwrite.

        :param snapshot: state of the record before the overwrite
        :type snapshot: _OverwriteSnapshot
        """
        self.remove_physical(snapshot.file_name)

    def discard_replacement(self, file_name: str) -> None:
        """Removes the incomplete new copy of a failed overwrite.

        :param file_name: file name of the new copy in the storage
        :type file_name: str
        """
        self.remove_physical(file_name)

    def remove_physical(self, file_name: str) -> None:
        """Removes the physical storage of ``file_name``.

        File-backed components delete the file on disk; the base is a no-op
        for components without a durable physical medium.

        :param file_name: file name as recorded on the storage record
        :type file_name: str
        """

    def reject_buffer(
            self,
            command_set: pydicom.Dataset,
            transfer_syntax: uid.UID
    ) -> tuple[BinaryIO, int]:
        """Builds the throwaway stream for a refused C-STORE.

        :param command_set: command dataset of the received message
        :type command_set: pydicom.Dataset
        :param transfer_syntax: negotiated transfer syntax
        :type transfer_syntax: uid.UID
        :return: the flagged discard buffer and its dataset start position
        :rtype: tuple[BinaryIO, int]
        """
        fp = _RejectedStore()
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, transfer_syntax)
        return fp, start

    def on_get_file(
            self,
            payload: events.GetFilePayload
    ) -> tuple[BinaryIO, int]:
        """Handles `GetFile` event from AE

        :param payload: presentation context and command Dataset
        :type payload: events.GetFilePayload
        :return: file object to store the incoming dataset in and the start
                 position of the dataset stream within it
        :rtype: tuple[BinaryIO, int]
        """
        raise NotImplementedError()

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event

        :param ds: successfully stored dataset
        :type ds: pydicom.Dataset
        :raises NotImplementedError: always, subclasses must implement this
        """
        raise NotImplementedError()

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event

        :param ds: dataset that failed to store
        :type ds: pydicom.Dataset
        :raises NotImplementedError: always, subclasses must implement this
        """
        raise NotImplementedError()

    def on_store_get_files(
            self,
            sop_instance_uids: list[str]
    ) -> Iterable[events.StoredFile]:
        """Handles `GetFiles` event

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :yield: stored files: tuples of SOP Class UID, Transfer Syntax UID
                and either a file name, a dataset or a file object
        :raises NotImplementedError: always, subclasses must implement this
        """
        raise NotImplementedError()

    def new_file(
            self,
            sop_instance_uid: str,
            sop_class_uid: str,
            transfer_syntax: str | uid.UID,
            file_name: str
    ) -> StorageFiles:
        """Adds new file record to the database

        :param sop_instance_uid: file SOP Instance UID
        :type sop_instance_uid: str
        :param sop_class_uid: file SOP Class UID
        :type sop_class_uid: str
        :param transfer_syntax: file original Transfer Syntax UID
        :type transfer_syntax: str or uid.UID
        :param file_name: file name in storage
        :type file_name: str
        :return: new file record
        :rtype: StorageFiles
        """
        self.log_info(
            'Storing new file '
            'SOP Instance UID: %(sop_instance_uid)s '
            'SOP Class UID: %(sop_class_uid)s '
            'Transfer Syntax UID: %(transfer_syntax)s '
            'File Name: %(file_name)s ',
            {
                'sop_instance_uid': sop_instance_uid,
                'sop_class_uid': sop_class_uid,
                'transfer_syntax': transfer_syntax,
                'file_name': file_name
            }
        )
        with self.atomic():
            return StorageFiles.create(
                sop_instance_uid=sop_instance_uid,
                sop_class_uid=sop_class_uid,
                transfer_syntax=transfer_syntax,
                file_name=file_name
            )

    def file_stored(self, sop_instance_uid: str) -> None:
        """Set file with specific SOP Instance UID as successfully stored

        When this completes an in-flight overwrite, the replaced (old) copy
        is removed here — only once the new copy is durable.

        :param sop_instance_uid: file SOP Instance UID
        :type sop_instance_uid: str
        """
        with self.atomic():
            stored_file = StorageFiles.get(
                StorageFiles.sop_instance_uid == sop_instance_uid
            )
            stored_file.is_stored = True
            stored_file.save()
        self.log_info('Successfully stored file in DB, SOP Instance UID: %s',
                      sop_instance_uid)
        snapshot = self._take_pending(sop_instance_uid)
        if snapshot is not None:
            self.finish_overwrite(snapshot)

    def remove_file(self, sop_instance_uid: str) -> str:
        """Remove file record from database with specific SOP Instance UID

        :param sop_instance_uid: file SOP Instance UID
        :type sop_instance_uid: str
        :return: removed file name
        :rtype: str
        """
        with self.atomic():
            stored_file = StorageFiles.get(
                StorageFiles.sop_instance_uid == sop_instance_uid
            )
            file_name = stored_file.file_name
            stored_file.delete_instance()
        self.log_info('Removed stored file from DB, SOP Instance UID: %s',
                      sop_instance_uid)
        return file_name

    def verify(
            self,
            instances: list[tuple[uid.UID, uid.UID]]
    ) -> tuple[frozenset[tuple[uid.UID, uid.UID]],
               frozenset[tuple[uid.UID, uid.UID]]]:
        """Verifies that the provided SOP Instance UIDs are successfully
        stored

        :param instances: list of tuples (SOP Class UID, SOP Instance UID)
        :type instances: list
        :return: tuple of two sets - one for successes and one for failures
        :rtype: tuple[frozenset, frozenset]
        """
        self.log_debug('Verifying instances: %r', instances)
        sop_instance_uids = [i for _, i in instances]
        query = self.find_files(sop_instance_uids)
        stored_instances = frozenset(
            (r.sop_class_uid, r.sop_instance_uid) for r in query
        )
        _instances = frozenset(instances)
        success = _instances & stored_instances
        failure = _instances - stored_instances
        self.log_debug('Verification, stored successfully: %r', success)
        self.log_debug('Verification, missing from storage: %r', failure)
        return success, failure

    def find_files(
            self,
            sop_instance_uids: Sequence[str]
    ) -> 'peewee.ModelSelect[StorageFiles]':
        """Find stored files based on a list of SOP Instance UIDs

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :return: query to iterate over
        :rtype: peewee.ModelSelect
        """
        # ``== True`` on a peewee field builds a SQL expression, it is not a
        # Python singleton comparison; peewee 4 has no ``.is_()`` alternative.
        query = StorageFiles.select()\
            .where(
                (StorageFiles.sop_instance_uid << sop_instance_uids) &
                (StorageFiles.is_stored == True)  # noqa: E712
            )
        return query

    def remove_nothrow(self, file_name: str) -> None:
        """Safely removes file from disc without raising an exception

        :param file_name: filename to remove
        :type file_name: str
        """
        try:
            os.remove(file_name)
        except Exception as error:
            self.log_exception(f'Failed to remove file {file_name}: {error}')

    # ------------------------------------------------------------------
    # Storage maintenance (answer the core maintenance events)
    # ------------------------------------------------------------------

    def storage_directory(self) -> str | None:
        """Returns the storage directory of file-backed components.

        Components without a directory (in-memory, temporary files) return
        None and answer the maintenance events with the DB-side sections
        only.

        :return: storage directory or None
        :rtype: str or None
        """
        return None

    def on_storage_stats(self, _: None = None) -> events.StorageStatsReport:
        """Handles `StorageStatsQuery` event

        :return: storage usage statistics
        :rtype: events.StorageStatsReport
        """
        # A single aggregate pass for the record totals (COUNT + SUM of
        # the stored flag), so a dashboard polling this event never runs
        # two full table scans per request
        aggregate_rows = cast(
            Iterable[Any],
            StorageFiles.select(
                peewee.fn.COUNT(StorageFiles.sop_instance_uid).alias('total'),
                peewee.fn.SUM(peewee.Case(
                    None,
                    [(StorageFiles.is_stored == True, 1)],  # noqa: E712
                    0
                )).alias('stored')
            ).dicts()
        )
        aggregate = next(iter(aggregate_rows))
        total = int(aggregate['total'])
        stored = int(aggregate['stored'] or 0)
        oldest = StorageFiles.select().order_by(StorageFiles.added).first()
        newest = StorageFiles.select().order_by(StorageFiles.added.desc())\
            .first()
        per_sop_class_rows = cast(
            Iterable[Any],
            StorageFiles.select(
                StorageFiles.sop_class_uid,
                peewee.fn.COUNT(StorageFiles.sop_instance_uid).alias('count')
            ).group_by(StorageFiles.sop_class_uid).dicts()
        )
        per_sop_class = {
            row['sop_class_uid']: int(row['count'])
            for row in per_sop_class_rows
        }
        storage_dir = self.storage_directory()
        file_count = file_bytes = per_day_bytes = None
        if storage_dir is not None:
            file_count, file_bytes, per_day_bytes = self._walk_storage_dir()
        return events.StorageStatsReport(
            records_total=total,
            records_stored=stored,
            records_failed=total - stored,
            oldest=oldest.added if oldest is not None else None,
            newest=newest.added if newest is not None else None,
            per_sop_class=per_sop_class,
            storage_dir=storage_dir,
            file_count=file_count,
            file_bytes=file_bytes,
            per_day_bytes=per_day_bytes
        )

    def on_storage_verify(self, _: None = None) -> events.StorageVerifyReport:
        """Handles `StorageVerifyQuery` event

        :return: storage consistency report
        :rtype: events.StorageVerifyReport
        """
        missing, orphans = self._classify_files()
        stuck = [
            record.sop_instance_uid
            for record in StorageFiles.select()
            .where(StorageFiles.is_stored == False)  # noqa: E712
        ]
        return events.StorageVerifyReport(
            missing_files=[
                os.path.normpath(record.file_name) for record in missing
            ],
            orphan_files=orphans,
            stuck_records=stuck,
            file_backend=self.storage_directory() is not None
        )

    def on_storage_cleanup(
            self,
            options: events.StorageCleanupOptions
    ) -> events.StorageCleanupReport:
        """Handles `StorageCleanupCommand` event

        Dry run by default: ``options.apply`` must be set to actually
        delete. Orphan files are additionally gated for live servers:
        a file younger than :data:`_ORPHAN_GRACE` or referenced by a
        record that appeared since the classification is never deleted
        (an in-flight C-STORE creates its file before the record).
        Applied cleanups are broadcast as
        :class:`~tiny_pacs.events.AuditRecord` (category ``storage``).

        :param options: cleanup options
        :type options: events.StorageCleanupOptions
        :return: cleanup report
        :rtype: events.StorageCleanupReport
        """
        errors: list[str] = []
        missing, orphans = self._classify_files()
        stuck_records = self._stuck_records(options.failed_older_than_days)
        root = self._resolved_root()
        wanted_records = list(dict.fromkeys(
            ([record.sop_instance_uid for record in missing]
             if options.delete_missing_records and root is not None else [])
            + [record.sop_instance_uid for record in stuck_records]
        ))
        orphan_files = (
            orphans if options.delete_orphans and root is not None else []
        )
        stuck_files: list[str] = []
        if root is not None:
            for record in stuck_records:
                full_name = self._contained(record.file_name, root)
                if full_name is not None and os.path.exists(full_name):
                    stuck_files.append(record.file_name)
        if not options.apply:
            removable_orphans = [
                file_name for file_name in orphan_files
                if self._removable_orphan(file_name, root)
            ]
            return events.StorageCleanupReport(
                would_remove_records=len(wanted_records),
                would_remove_files=len(set(map(
                    os.path.normpath, removable_orphans + stuck_files
                ))),
                errors=errors
            )

        records_removed = 0
        with self.atomic():
            for index in range(0, len(wanted_records), _DELETE_CHUNK):
                chunk = wanted_records[index:index + _DELETE_CHUNK]
                try:
                    removed = StorageFiles.delete().where(
                        StorageFiles.sop_instance_uid << chunk
                    ).execute()
                    records_removed += removed
                except Exception as error:
                    errors.append(
                        f'failed to remove {len(chunk)} records: {error}'
                    )
        files_removed = 0
        removed_paths: set[str] = set()
        for file_name in orphan_files:
            if not self._removable_orphan(file_name, root):
                continue
            if self._remove_contained_file(file_name, root, removed_paths,
                                           errors):
                files_removed += 1
        for file_name in stuck_files:
            if self._remove_contained_file(file_name, root, removed_paths,
                                           errors):
                files_removed += 1

        self.broadcast_nothrow(
            events.AuditRecord,
            events.AuditRecordPayload(
                category='storage',
                event='cleanup',
                details={
                    'records_removed': records_removed,
                    'files_removed': files_removed,
                    'errors': errors
                }
            )
        )
        return events.StorageCleanupReport(
            records_removed=records_removed,
            files_removed=files_removed,
            errors=errors
        )

    def _classify_files(self) -> tuple[list[StorageFiles], list[str]]:
        """Classifies stored files into missing and orphan sets.

        The disk is walked *before* the DB records are snapshotted: a
        live C-STORE creates the file before its record exists, so
        walking first classifies an in-store file against the newest
        record state and shrinks the window in which it can look like an
        orphan. The cleanup gates deletion further (see
        :meth:`_removable_orphan`).

        :return: records whose file is gone and normalized relative paths
                 of files no record references; empty results for
                 components without a storage directory
        :rtype: tuple[list[StorageFiles], list[str]]
        """
        root = self._resolved_root()
        if root is None:
            return [], []
        disk_files = self._disk_files()
        record_paths: set[str] = set()
        missing: list[StorageFiles] = []
        for record in StorageFiles.select():
            normalized = os.path.normpath(record.file_name)
            record_paths.add(normalized)
            full_name = self._contained(record.file_name, root)
            if full_name is None or not os.path.exists(full_name):
                missing.append(record)
        orphans: list[str] = []
        for relative in disk_files:
            normalized = os.path.normpath(relative)
            if normalized not in record_paths:
                orphans.append(normalized)
        return missing, orphans

    def _stuck_records(
            self, older_than_days: int | None
    ) -> list[StorageFiles]:
        """Returns stuck in-progress records older than the given age.

        :param older_than_days: age threshold in days; None selects no
                                records
        :rtype: list[StorageFiles]
        """
        if older_than_days is None:
            return []
        cutoff = _utcnow() - datetime.timedelta(days=older_than_days)
        return list(
            StorageFiles.select().where(
                (StorageFiles.is_stored == False)  # noqa: E712
                & (StorageFiles.added < cutoff)
            )
        )

    def _resolved_root(self) -> 'pathlib.Path | None':
        """Returns the resolved storage directory root, if any."""
        storage_dir = self.storage_directory()
        if storage_dir is None:
            return None
        return pathlib.Path(storage_dir).resolve()

    def _contained(self, file_name: str,
                   root: 'pathlib.Path | None' = None) -> str | None:
        """Joins a record file name onto the storage directory safely.

        :param file_name: file name as stored in the record
        :type file_name: str
        :param root: pre-resolved storage root (avoids re-resolving the
                     invariant directory inside loops)
        :type root: pathlib.Path or None
        :return: absolute file name inside the storage directory, or None
                 when the joined path escapes it (or the component has no
                 directory)
        :rtype: str or None
        """
        if root is None:
            root = self._resolved_root()
        if root is None:
            return None
        candidate = (root / file_name).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            return None
        return str(candidate)

    def _removable_orphan(self, file_name: str,
                          root: 'pathlib.Path | None') -> bool:
        """Applies the live-server safety gates to an orphan candidate.

        An orphan is only removable when it exists, is older than the
        grace window (an in-flight C-STORE has created the file but not
        its record yet) and no record has appeared referencing it since
        the classification snapshot.

        :param file_name: orphan file path relative to the storage dir
        :type file_name: str
        :param root: pre-resolved storage root
        :type root: pathlib.Path or None
        :return: whether the file may be deleted
        :rtype: bool
        """
        full_name = self._contained(file_name, root)
        if full_name is None:
            return False
        try:
            mtime = datetime.datetime.fromtimestamp(
                os.stat(full_name).st_mtime, datetime.timezone.utc
            )
        except OSError:
            return False
        if mtime > _utcnow() - _ORPHAN_GRACE:
            self.log_debug('Skipping recent orphan file %s', file_name)
            return False
        if StorageFiles.select().where(
                StorageFiles.file_name << (file_name,
                                           os.path.normpath(file_name))
        ).exists():
            self.log_debug(
                'Skipping orphan file %s referenced by a newer record',
                file_name
            )
            return False
        return True

    def _remove_contained_file(
            self,
            file_name: str,
            root: 'pathlib.Path | None',
            removed_paths: set[str],
            errors: list[str]
    ) -> bool:
        """Removes one cleanup target file with the containment guard.

        :param file_name: file path relative to the storage directory
        :type file_name: str
        :param root: pre-resolved storage root
        :type root: pathlib.Path or None
        :param removed_paths: normalized paths already removed
        :type removed_paths: set[str]
        :param errors: report errors list
        :type errors: list[str]
        :return: whether the file was removed
        :rtype: bool
        """
        full_name = self._contained(file_name, root)
        if full_name is None:
            errors.append(
                f'refusing to touch file outside the storage directory: '
                f'{file_name}'
            )
            return False
        normalized = os.path.normpath(file_name)
        if normalized in removed_paths or not os.path.exists(full_name):
            return False
        try:
            os.remove(full_name)
        except Exception as error:
            errors.append(f'failed to remove file {file_name}: {error}')
            return False
        removed_paths.add(normalized)
        return True

    def _walk_storage_dir(self) -> tuple[int, int, dict[str, int]]:
        """Counts the files under the storage directory.

        A single ``os.scandir`` walk provides both the listing and the
        file sizes: no path list is materialized and every file is
        stat'd at most once.

        :return: file count, total bytes and bytes per ``%Y%m%d`` day
                 folder
        :rtype: tuple[int, int, dict[str, int]]
        """
        count = 0
        total = 0
        per_day: dict[str, int] = {}
        for relative, size in self._disk_entries():
            if size is None:
                continue
            count += 1
            total += size
            parts = pathlib.PurePath(relative).parts
            if len(parts) > 1 and len(parts[0]) == 8 and parts[0].isdigit():
                per_day[parts[0]] = per_day.get(parts[0], 0) + size
        return count, total, per_day

    def _disk_files(self) -> list[str]:
        """Lists every file under the storage directory.

        :return: file paths relative to the storage directory
        :rtype: list[str]
        """
        return [relative for relative, _ in self._disk_entries()]

    def _disk_entries(self) -> Iterator[tuple[str, int | None]]:
        """Walks the storage directory with ``os.scandir``.

        :yield: tuples of file paths relative to the storage directory
                and their size in bytes (None when the stat failed)
        :rtype: Iterator[tuple[str, int or None]]
        """
        root = self._resolved_root()
        if root is None or not root.is_dir():
            return
        yield from self._scan_directory(root, '')

    def _scan_directory(
            self, directory: pathlib.Path, prefix: str
    ) -> Iterator[tuple[str, int | None]]:
        """Recursively yields the files of one directory subtree."""
        try:
            entries = list(os.scandir(directory))
        except OSError:
            return
        for entry in entries:
            relative = f'{prefix}{os.sep}{entry.name}' if prefix \
                else entry.name
            try:
                if entry.is_dir():
                    yield from self._scan_directory(
                        pathlib.Path(entry.path), relative
                    )
                    continue
                if not entry.is_file():
                    continue
                try:
                    size: int | None = entry.stat().st_size
                except OSError:
                    size = None
                yield relative, size
            except OSError:
                continue


class FileStorageConfig(StorageConfig):
    """Configuration of the :class:`FileStorage` component.

    :ivar storage_dir: directory for stored files; a temporary directory is
                       created and removed on shutdown when not provided
    :ivar overwrite: inherited from :class:`StorageConfig`; replace instances
                     that are already stored instead of refusing the store
    """

    storage_dir: str | None = None


class FileStorage(StorageBase[FileStorageConfig],
                  applicationentity.FolderStorageMixin):
    """Simple file storage implementation.

    Stores incoming datasets in a provided folder. If specific folder is not
    provided in the component configuration, temporary one is created.

    Unique file naming and storage file creation (preamble and file meta
    information) are provided by
    :class:`pynetdicom2.applicationentity.FolderStorageMixin`. Note that the
    mixin gives up after ``max_iterations`` attempts to find a free file name
    and raises ``OSError`` in that case.
    """

    config_model = FileStorageConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: FileStorageConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        Uses the configured storage directory or creates a temporary one
        that is removed on exit.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: FileStorageConfig or dict
        """
        super().__init__(bus, config)
        storage_dir = self.config.storage_dir
        if storage_dir is None:
            # TODO Gracefully remove temporary directory on shutdown
            storage_dir = tempfile.mkdtemp()
            self.subscribe(trolleybus.OnExit, self.cleanup)
        self.storage_dir = storage_dir

    @classmethod
    def interactive(cls) -> questions.Questionnaire:
        """Returns interactive questionnaire for component configuration

        :return: storage configuration questionnaire
        :rtype: questions.Questionnaire
        """
        return questions.Questionnaire([
            questions.Question(
                'storage_dir', 'Enter storage directory',
                lambda v: v, default=None, default_repr='Temp dir'
            ),
            questions.Question(
                'overwrite', 'Overwrite instances that are already stored?',
                lambda v: v.lower() == 'y', default='N'
            )
        ])

    def storage_directory(self) -> str | None:
        """Returns the configured storage directory.

        :return: storage directory of the component
        :rtype: str
        """
        return self.storage_dir

    def on_get_file(self,
                    payload: events.GetFilePayload) -> tuple[BinaryIO, int]:
        """Handles `GetFile` event: creates a new file in the storage.

        When the SOP Instance is already stored the ``overwrite`` setting
        decides between replacing it and refusing the store; a refusal hands
        the decoder a throwaway :class:`_RejectedStore` buffer (no file is
        created) that the :meth:`StorageBase.on_store` handler turns into a
        failure C-STORE-RSP. Losing the record-creation race against a
        parallel store of the same instance is refused the same way instead
        of raising into the DIMSE decoder (which would abort the
        association).

        :param payload: presentation context and command Dataset
        :type payload: events.GetFilePayload
        :return: file object and the dataset stream start position
        :rtype: tuple[BinaryIO, int]
        """
        command_set = payload.command_set
        sop_instance_uid = command_set.AffectedSOPInstanceUID
        sop_class_uid = command_set.AffectedSOPClassUID
        ts = uid.UID(payload.transfer_syntax
                     or payload.context.supported_ts)
        action = self.check_duplicate(sop_instance_uid)
        if action is _DuplicateAction.REJECT:
            return self.reject_buffer(command_set, ts)
        folder = pathlib.Path(self.get_folder_path())
        folder.mkdir(parents=True, exist_ok=True)
        # Unique file name and file creation (preamble and file meta
        # information) come from FolderStorageMixin; on overwrite the still
        # existing old copy is simply bypassed by the unique naming and only
        # removed once the replacement is durable.
        ds, start = self.get_storage_file(payload.context, command_set, folder)
        full_name = cast(io.BufferedRandom, ds).name
        file_name = os.path.relpath(full_name, self.storage_dir)
        self.log_info('Storing incoming dataset in %s', file_name)
        try:
            self.record_store(action, sop_instance_uid, sop_class_uid, ts,
                              file_name)
        except peewee.DatabaseError as error:
            self.log_warning(
                'Refusing C-STORE, SOP Instance UID %s was stored in '
                'parallel: %s', sop_instance_uid, error
            )
            ds.close()
            self.remove_physical(file_name)
            return self.reject_buffer(command_set, ts)
        return ds, start

    def remove_physical(self, file_name: str) -> None:
        """Removes a stored file from disk.

        :param file_name: file name as recorded on the storage record
        :type file_name: str
        """
        full_name = self._contained(file_name)
        if full_name is None:
            self.log_error(
                'Refusing to remove file outside the storage directory: %s',
                file_name
            )
            return
        self.remove_nothrow(full_name)

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event: marks the file as stored."""
        self.file_stored(ds.SOPInstanceUID)

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event: removes the file.

        A failure during an in-flight overwrite rolls the record back to the
        stored copy (which was never removed) and only deletes the incomplete
        new file.
        """
        rolled_back = self.rollback_overwrite(ds.SOPInstanceUID)
        if rolled_back is not None:
            _, partial_file_name = rolled_back
            if partial_file_name:
                self.discard_replacement(partial_file_name)
            return
        self.remove_physical(self.remove_file(ds.SOPInstanceUID))

    def on_store_get_files(
            self,
            sop_instance_uids: list[str]
    ) -> Iterable[events.StoredFile]:
        """Handles `GetFiles` event

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :yield: stored files
        :rtype: events.StoredFile
        """
        self.log_debug('Getting files %r', sop_instance_uids)
        for file_record in self.find_files(sop_instance_uids):
            file_name = self._contained(file_record.file_name)
            if file_name is None:
                self.log_error(
                    'Refusing to serve file outside the storage directory: '
                    '%s', file_record.file_name
                )
                continue
            yield (file_record.sop_class_uid,
                   file_record.transfer_syntax, file_name)

    def get_folder_path(self) -> str:
        """Return full path for storing an incoming file

        :return: full path for storing
        :rtype: str
        """
        now = _utcnow()
        return os.path.join(self.storage_dir, now.strftime('%Y%m%d'))

    def cleanup(self, _: None = None) -> None:
        """Cleans up storage directory.

        Called on exit, if temporary directory is used
        """
        try:
            shutil.rmtree(self.storage_dir)
        except Exception as error:
            self.log_exception(
                f'Failed to cleanup storage directory {self.storage_dir}: '
                f'{error}'
            )


class InMemoryStorage(StorageBase[StorageConfig]):
    """Simple in-memory storage component.

    Stores all incoming datasets in RAM. Intended for testing only.
    """

    config_model = StorageConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: StorageConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: StorageConfig or dict
        """
        super().__init__(bus, config)
        self._temp_files: dict[str, tuple[BinaryIO, int]] = {}
        self._stored_files: dict[str, pydicom.Dataset] = {}

    def on_get_file(
            self,
            payload: events.GetFilePayload
    ) -> tuple[BinaryIO, int]:
        """Handles `GetFile` event: stores the dataset in memory.

        A duplicate store is replaced when ``overwrite`` is on, otherwise it
        is refused through a :class:`_RejectedStore` buffer.

        :param payload: presentation context and command Dataset
        :type payload: events.GetFilePayload
        :return: file object and the dataset stream start position
        :rtype: tuple[BinaryIO, int]
        """
        command_set = payload.command_set
        sop_instance_uid = command_set.AffectedSOPInstanceUID
        sop_class_uid = command_set.AffectedSOPClassUID
        ts = uid.UID(payload.transfer_syntax
                     or payload.context.supported_ts)
        action = self.check_duplicate(sop_instance_uid)
        if action is _DuplicateAction.REJECT:
            return self.reject_buffer(command_set, ts)
        fp = io.BytesIO()
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, ts)
        try:
            self.record_store(action, sop_instance_uid, sop_class_uid, ts,
                              sop_instance_uid)
        except peewee.DatabaseError as error:
            self.log_warning(
                'Refusing C-STORE, SOP Instance UID %s was stored in '
                'parallel: %s', sop_instance_uid, error
            )
            return self.reject_buffer(command_set, ts)
        self._temp_files[sop_instance_uid] = (fp, start)
        self.log_info('Storing dataset in memory: %s', sop_instance_uid)
        return fp, start

    def finish_overwrite(self, snapshot: _OverwriteSnapshot) -> None:
        """No-op: ``on_store_done`` replaces the in-memory dataset in place.

        :param snapshot: state of the record before the overwrite
        :type snapshot: _OverwriteSnapshot
        """

    def discard_replacement(self, file_name: str) -> None:
        """Drops the incomplete in-memory buffer of a failed overwrite.

        The stored dataset itself is untouched, so a rollback cannot lose
        it; only the pending temporary buffer (keyed by SOP Instance UID)
        is dropped.

        :param file_name: SOP Instance UID key of the buffer
        :type file_name: str
        """
        self._temp_files.pop(file_name, None)

    def remove_physical(self, file_name: str) -> None:
        """Drops the in-memory dataset.

        In this component the recorded ``file_name`` is the SOP Instance UID.

        :param file_name: SOP Instance UID key of the stored dataset
        :type file_name: str
        """
        self._stored_files.pop(file_name, None)
        self._temp_files.pop(file_name, None)

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event: reads the dataset into memory."""
        sop_instance_uid = ds.SOPInstanceUID
        self.file_stored(ds.SOPInstanceUID)
        fp, start = self._temp_files[sop_instance_uid]
        fp.seek(start)
        self._stored_files[sop_instance_uid] = pydicom.dcmread(fp)
        del self._temp_files[sop_instance_uid]

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event: drops the in-memory dataset.

        A failure during an in-flight overwrite rolls the record back to the
        stored copy and only drops the incomplete buffer.
        """
        rolled_back = self.rollback_overwrite(ds.SOPInstanceUID)
        if rolled_back is not None:
            _, partial_file_name = rolled_back
            if partial_file_name:
                self.discard_replacement(partial_file_name)
            return
        file_name = self.remove_file(ds.SOPInstanceUID)
        try:
            del self._temp_files[file_name]
        except KeyError:
            pass

    def on_store_get_files(
            self,
            sop_instance_uids: list[str]
    ) -> Iterable[events.StoredFile]:
        """Handles `GetFiles` event

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :yield: stored files
        :rtype: events.StoredFile
        """
        self.log_debug('Getting files %r', sop_instance_uids)
        for file_record in self.find_files(sop_instance_uids):
            ds = self._stored_files[file_record.sop_instance_uid]
            yield file_record.sop_class_uid, file_record.transfer_syntax, ds


class TempFileStorage(StorageBase[StorageConfig]):
    """Simple storage component that uses temporary files to store incoming
    dataset.

    Intended for testing only.
    """

    config_model = StorageConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: StorageConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: StorageConfig or dict
        """
        super().__init__(bus, config)
        self._temp_files: set[str] = set()

    def on_get_file(
            self,
            payload: events.GetFilePayload
    ) -> tuple[BinaryIO, int]:
        """Handles `GetFile` event: creates a new temporary file.

        A duplicate store is replaced when ``overwrite`` is on, otherwise it
        is refused through a :class:`_RejectedStore` buffer.

        :param payload: presentation context and command Dataset
        :type payload: events.GetFilePayload
        :return: file object and the dataset stream start position
        :rtype: tuple[BinaryIO, int]
        """
        command_set = payload.command_set
        sop_instance_uid = command_set.AffectedSOPInstanceUID
        sop_class_uid = command_set.AffectedSOPClassUID
        ts = uid.UID(payload.transfer_syntax
                     or payload.context.supported_ts)
        action = self.check_duplicate(sop_instance_uid)
        if action is _DuplicateAction.REJECT:
            return self.reject_buffer(command_set, ts)
        fp = cast(BinaryIO, tempfile.NamedTemporaryFile(delete=False))
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, ts)
        try:
            self.record_store(action, sop_instance_uid, sop_class_uid, ts,
                              fp.name)
        except peewee.DatabaseError as error:
            self.log_warning(
                'Refusing C-STORE, SOP Instance UID %s was stored in '
                'parallel: %s', sop_instance_uid, error
            )
            fp.close()
            self.remove_nothrow(fp.name)
            return self.reject_buffer(command_set, ts)
        self._temp_files.add(fp.name)
        self.log_info('Storing incoming dataset in %s', fp.name)
        return fp, start

    def remove_physical(self, file_name: str) -> None:
        """Removes a temporary file.

        :param file_name: temporary file name
        :type file_name: str
        """
        self.remove_nothrow(file_name)
        self._temp_files.discard(file_name)

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event: marks the file as stored."""
        self.file_stored(ds.SOPInstanceUID)

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event: removes the temporary file.

        A failure during an in-flight overwrite rolls the record back to the
        stored copy (which was never removed) and only deletes the incomplete
        new file.
        """
        rolled_back = self.rollback_overwrite(ds.SOPInstanceUID)
        if rolled_back is not None:
            _, partial_file_name = rolled_back
            if partial_file_name:
                self.discard_replacement(partial_file_name)
            return
        self.remove_physical(self.remove_file(ds.SOPInstanceUID))

    def on_store_get_files(
            self,
            sop_instance_uids: list[str]
    ) -> Iterable[events.StoredFile]:
        """Handles `GetFiles` event

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :yield: stored files
        :rtype: events.StoredFile
        """
        self.log_debug('Getting files %r', sop_instance_uids)
        for file_record in self.find_files(sop_instance_uids):
            file_name = file_record.file_name
            yield (file_record.sop_class_uid,
                   file_record.transfer_syntax, file_name)

    def on_exit(self) -> None:
        """Removes left-over temporary files on exit."""
        super().on_exit()
        for file_name in self._temp_files:
            self.remove_nothrow(file_name)
