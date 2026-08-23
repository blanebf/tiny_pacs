"""Storage components

Module provides various implementation of storage components.
"""
import datetime
import io
import os
import pathlib
import shutil
import tempfile
from collections.abc import Iterable, Sequence
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
        ts = payload.context.supported_ts
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
        file_name = os.path.join(self.storage_dir, file_name)
        self.remove_nothrow(file_name)

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
            file_name = os.path.join(self.storage_dir, file_record.file_name)
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
        ts = payload.context.supported_ts
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
        ts = payload.context.supported_ts
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
