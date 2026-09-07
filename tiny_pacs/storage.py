"""Storage components

Module provides various implementation of storage components.
"""
import datetime
import io
import os
import pathlib
import shutil
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, BinaryIO, cast

import peewee
import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import applicationentity

from . import component, events, questions, schema
from .component import TConfig


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


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


class StorageBase(component.Component[TConfig]):
    """Abstract storage component.

    Provides basic storage functionality, common for all storage components.
    """

    #: All storage implementations share the same tables and thus the same
    #: schema version
    schema_name = 'Storage'

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: TConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        Subscribes to all storage-related events.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: TConfig or dict
        """
        super().__init__(bus, config)

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


class FileStorageConfig(component.ComponentConfig):
    """Configuration of the :class:`FileStorage` component.

    :ivar storage_dir: directory for stored files; a temporary directory is
                       created and removed on shutdown when not provided
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
        folder = pathlib.Path(self.get_folder_path())
        folder.mkdir(parents=True, exist_ok=True)
        # Unique file name and file creation (preamble and file meta
        # information) come from FolderStorageMixin.
        ds, start = self.get_storage_file(payload.context, command_set, folder)
        full_name = cast(io.BufferedRandom, ds).name
        file_name = os.path.relpath(full_name, self.storage_dir)
        self.log_info('Storing incoming dataset in %s', file_name)
        self.new_file(sop_instance_uid, sop_class_uid, ts, file_name)
        return ds, start

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event: marks the file as stored."""
        self.file_stored(ds.SOPInstanceUID)

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event: removes the file."""
        file_name = self.remove_file(ds.SOPInstanceUID)
        full_name = self._contained(file_name)
        if full_name is None:
            self.log_error(
                'Refusing to remove file outside the storage directory: %s',
                file_name
            )
            return
        self.remove_nothrow(full_name)

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


class InMemoryStorage(StorageBase[component.ComponentConfig]):
    """Simple in-memory storage component.

    Stores all incoming datasets in RAM. Intended for testing only.
    """

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: component.ComponentConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: ComponentConfig or dict
        """
        super().__init__(bus, config)
        self._temp_files: dict[str, tuple[BinaryIO, int]] = {}
        self._stored_files: dict[str, pydicom.Dataset] = {}

    def on_get_file(
            self,
            payload: events.GetFilePayload
    ) -> tuple[BinaryIO, int]:
        """Handles `GetFile` event: stores the dataset in memory.

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
        fp = io.BytesIO()
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, ts)
        self.new_file(sop_instance_uid, sop_class_uid, ts, sop_instance_uid)
        self._temp_files[sop_instance_uid] = (fp, start)
        self.log_info('Storing dataset in memory: %s', sop_instance_uid)
        return fp, start

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event: reads the dataset into memory."""
        sop_instance_uid = ds.SOPInstanceUID
        self.file_stored(ds.SOPInstanceUID)
        fp, start = self._temp_files[sop_instance_uid]
        fp.seek(start)
        self._stored_files[sop_instance_uid] = pydicom.dcmread(fp)
        del self._temp_files[sop_instance_uid]

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event: drops the in-memory dataset."""
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


class TempFileStorage(StorageBase[component.ComponentConfig]):
    """Simple storage component that uses temporary files to store incoming
    dataset.

    Intended for testing only.
    """
    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: component.ComponentConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: ComponentConfig or dict
        """
        super().__init__(bus, config)
        self._temp_files: set[str] = set()

    def on_get_file(
            self,
            payload: events.GetFilePayload
    ) -> tuple[BinaryIO, int]:
        """Handles `GetFile` event: creates a new temporary file.

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
        fp = cast(BinaryIO, tempfile.NamedTemporaryFile(delete=False))
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, ts)
        self.new_file(sop_instance_uid, sop_class_uid, ts, fp.name)
        self._temp_files.add(fp.name)
        self.log_info('Storing incoming dataset in %s', fp.name)
        return fp, start

    def on_store_done(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreDone` event: marks the file as stored."""
        self.file_stored(ds.SOPInstanceUID)

    def on_store_failure(self, ds: pydicom.Dataset) -> None:
        """Handles `StoreFailure` event: removes the temporary file."""
        file_name = self.remove_file(ds.SOPInstanceUID)
        self.remove_nothrow(file_name)
        self._temp_files.remove(file_name)

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
