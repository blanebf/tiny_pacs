# -*- coding: utf-8 -*-
"""Storage components

Module provides various implementation of storage components.
"""
import datetime
import enum
import io
import os
import shutil
import tempfile
from typing import List, Tuple, IO, Type

import peewee

import pydicom
from pynetdicom2 import applicationentity
from pynetdicom2 import asceprovider

from . import ae
from . import component
from . import db
from . import event_bus
from . import questions


class StorageChannels(enum.Enum):
    """Storage events"""

    #: Storage is done successfully
    ON_STORE_DONE = 'on-store-done'

    #: Storage has ended in a failure
    ON_STORE_FAILURE = 'on-store-failure'

    #: Files request
    ON_GET_FILES = 'on-store-get-files'

    #: Verify stored files
    ON_STORE_VERIFY = 'on-store-verify'


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

    #: When a file were added to a storage
    added = peewee.DateTimeField(default=datetime.datetime.utcnow, index=True)

    #: Is file stored successfully already
    is_stored = peewee.BooleanField(index=True, default=False)


class StorageBase(component.Component):
    """Abstract storage component.

    Provides basic storage functionality, common for all storage components.
    """
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)

        self.subscribe(ae.AEChannels.ON_GET_FILE, self.on_get_file)
        self.subscribe(StorageChannels.ON_STORE_DONE, self.on_store_done)
        self.subscribe(StorageChannels.ON_STORE_FAILURE, self.on_store_failure)
        self.subscribe(StorageChannels.ON_GET_FILES, self.on_store_get_files)
        self.subscribe(StorageChannels.ON_STORE_VERIFY, self.verify)
        self.subscribe(db.DBChannels.TABLES, self.tables)

    @staticmethod
    def tables() -> List[Type[peewee.Model]]:
        """Returns a list of tables used by the component

        :return: list of tables
        :rtype: List[peewee.Model]
        """
        return [StorageFiles]

    def atomic(self):
        """Opens transaction

        :return: transaction context manager
        """
        return self.send_one(db.DBChannels.ATOMIC)

    def on_get_file(self, context: asceprovider.PContextDef,
                    command_set: pydicom.Dataset) -> Tuple[IO, int]:
        """Handles 'on-get-file' event from AE channel

        :param context: presentation context
        :type context: [type]
        :param command_set: command Dataset
        :type command_set: pydicom.Dataset
        """
        raise NotImplementedError()

    def on_store_done(self, ds: pydicom.Dataset):
        """Handles 'on-storage-done' event

        :param ds: stored command dataset
        :type ds: pydicom.Dataset
        :raises NotImplementedError: [description]
        """
        raise NotImplementedError()

    def on_store_failure(self, ds: pydicom.Dataset):
        """Handles 'on-store-failure' event

        :param ds: dataset that failed to store
        :type ds: pydicom.Dataset
        :raises NotImplementedError: [description]
        """
        raise NotImplementedError()

    def on_store_get_files(self, sop_instance_uids: list):
        """Handles 'on-store-get-files' event

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :raises NotImplementedError: [description]
        """
        raise NotImplementedError()

    def new_file(self, sop_instance_uid: str, sop_class_uid: str,
                 transfer_syntax: str, file_name: str) -> peewee.Model:
        """Adds new file record to the database

        :param sop_instance_uid: file SOP Instance UID
        :type sop_instance_uid: str
        :param sop_class_uid: file SOP Class UID
        :type sop_class_uid: str
        :param transfer_syntax: file original Transfer Syntax UID
        :type transfer_syntax: str
        :param file_name: file name in storage
        :type file_name: str
        :return: new file record
        :rtype: StoredFiles
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

    def file_stored(self, sop_instance_uid: str):
        """Set file with specific SOP Instance UID as successfully stored

        :param sop_instance_uid: file SOP Instnace UID
        :type sop_instance_uid: str
        """
        with self.atomic():
            stored_file = StorageFiles.get(StorageFiles.sop_instance_uid == sop_instance_uid)
            stored_file.is_stored = True
            stored_file.save()
        self.log_info('Successfully stored file in DB, SOP Instance UID: %s', sop_instance_uid)

    def remove_file(self, sop_instance_uid: str) -> str:
        """Remove file record from database with specific SOP Instance UID

        :param sop_instance_uid: file SOP Instance UID
        :type sop_instance_uid: str
        :return: removed file name
        :rtype: str
        """
        with self.atomic():
            stored_file = StorageFiles.get(StorageFiles.sop_instance_uid == sop_instance_uid)
            file_name = stored_file.file_name
            stored_file.delete_instance()
        self.log_info('Removed stored file from DB, SOP Instance UID: %s', sop_instance_uid)
        return file_name

    def verify(self, instances: list) -> Tuple[frozenset, frozenset]:
        """Verify that provided list of SOP Instnace UIDs are successfully stored

        :param instances: list of SOP Instance UIDs
        :type instances: list
        :return: tuple of two sets - one for successes and one for failures
        :rtype: Tuple[frozenset, frozenset]
        """
        self.log_debug('Verifying instances: %r', instances)
        sop_instance_uids = [i for _, i in instances]
        query = self.find_files(sop_instance_uids)
        stored_instances = frozenset((r.sop_class_uid, r.sop_instance_uid) for r in query)
        _instances = frozenset(instances)
        success = _instances & stored_instances
        failure = _instances - stored_instances
        self.log_debug('Verification, stored successfully: %r', success)
        self.log_debug('Verification, missing from storage: %r', failure)
        return success, failure

    def find_files(self, sop_instance_uids: list) -> peewee.Query:
        """Find stored files based on a list of SOP Instance UIDs

        :param sop_instance_uids: list of SOP Instance UIDs
        :type sop_instance_uids: list
        :return: query to iterate over
        :rtype: peewee.Query
        """
        query = StorageFiles.select()\
            .where(
                (StorageFiles.sop_instance_uid << sop_instance_uids) &
                (StorageFiles.is_stored == True)  # pylint: disable=singleton-comparison
            )
        return query

    def remove_nothrow(self, file_name: str):
        """Safely removes file from disc without raising an exception

        :param file_name: filename to remove
        :type file_name: str
        """
        try:
            os.remove(file_name)
        except Exception as error:  # pylint: disable=broad-except
            self.log_exception(f'Failed to remove file {file_name}: {error}')


class FileStorage(StorageBase):
    """Simple file storage implementation.

    Stores incoming datasets in a provided folder. If specific folder is not
    provided in the component configuration, temporary one is created.
    """
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        storage_dir = config.get('storage_dir', None)
        if storage_dir is None:
            # TODO Gracefully remove temporary directory on shutdown
            storage_dir = tempfile.mkdtemp()
            self.subscribe(event_bus.DefaultChannels.ON_EXIT, self.cleanup)
        self.storage_dir = storage_dir

    @classmethod
    def interactive(cls):
        return questions.Questionnaire([
            questions.Question(
                'storage_dir', 'Enter storage directory',
                lambda v: v, default=None, default_repr='Temp dir'
            )
        ])

    def on_get_file(self, context: asceprovider.PContextDef,
                    command_set: pydicom.Dataset) -> Tuple[IO, int]:
        sop_instance_uid = command_set.AffectedSOPInstanceUID
        sop_class_uid = command_set.AffectedSOPClassUID
        ts = context.supported_ts
        full_name = self.get_file_name(sop_instance_uid)
        folder, file_name = os.path.split(full_name)
        folder = os.path.basename(folder)
        file_name = os.path.join(folder, file_name)
        self.log_info('Storing incoming dataset in %s', file_name)

        ds = open(full_name, 'wb')
        start = ds.tell()
        try:
            applicationentity.write_meta(ds, command_set, ts)
        except Exception:
            ds.close()
            raise
        else:
            self.new_file(sop_instance_uid, sop_class_uid, ts, file_name)
            return ds, start

    def on_store_done(self, ds: pydicom.Dataset):
        self.file_stored(ds.SOPInstanceUID)

    def on_store_failure(self, ds: pydicom.Dataset):
        file_name = self.remove_file(ds.SOPInstanceUID)
        file_name = os.path.join(self.storage_dir, file_name)
        self.remove_nothrow(file_name)

    def on_store_get_files(self, sop_instance_uids: list):
        self.log_debug('Getting files %r', sop_instance_uids)
        for file_record in self.find_files(sop_instance_uids):
            file_name = os.path.join(self.storage_dir, file_record.file_name)
            yield file_record.sop_class_uid, file_record.transfer_syntax, file_name

    def get_folder_path(self) -> str:
        """Return full path for storing an incoming file

        :return: full path for storing
        :rtype: str
        """
        now = datetime.datetime.utcnow()
        return os.path.join(self.storage_dir, now.strftime('%Y%m%d'))

    def get_file_name(self, sop_instance_uid: str) -> str:
        """Gets full file name for an incoming file

        :param sop_instance_uid: incoming file SOP Instance UID
        :type sop_instance_uid: str
        :return: full file name
        :rtype: str
        """
        folder = self.get_folder_path()
        file_name = f'{sop_instance_uid}.dcm'
        full_name = os.path.join(folder, file_name)
        i = 0
        while os.path.exists(full_name):
            i += 1
            file_name = f'{sop_instance_uid}_{i}.dcm'
            full_name = os.path.join(folder, file_name)
        return full_name

    def cleanup(self):
        """Cleans up storage directory.

        Called on exit, if temporary directory is used
        """
        try:
            shutil.rmtree(self.storage_dir)
        except Exception as error:  # pylint: disable=broad-except
            self.log_exception(
                f'Failed to cleanup storage directory {self.storage_dir}: {error}'
            )


class InMemoryStorage(StorageBase):
    """Simple in-memory storage component.

    Stores all incoming datasets in RAM. Intended for testing only.
    """

    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self._temp_files = {}
        self._stored_files = {}

    def on_get_file(self, context: asceprovider.PContextDef,
                    command_set: pydicom.Dataset) -> Tuple[IO, int]:
        sop_instance_uid = command_set.AffectedSOPInstanceUID
        sop_class_uid = command_set.AffectedSOPClassUID
        ts = context.supported_ts
        fp = io.BytesIO()
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, ts)
        self.new_file(sop_instance_uid, sop_class_uid, ts, sop_instance_uid)
        self._temp_files[sop_instance_uid] = (fp, start)
        self.log_info('Storing dataset in memory: %s', sop_instance_uid)
        return fp, start

    def on_store_done(self, ds: pydicom.Dataset):
        sop_instance_uid = ds.SOPInstanceUID
        self.file_stored(ds.SOPInstanceUID)
        fp, start = self._temp_files[sop_instance_uid]
        fp.seek(start)
        self._stored_files[sop_instance_uid] = pydicom.dcmread(fp)
        del self._temp_files[sop_instance_uid]

    def on_store_failure(self, ds: pydicom.Dataset):
        file_name = self.remove_file(ds.SOPInstanceUID)
        try:
            del self._temp_files[file_name]
        except KeyError:
            pass

    def on_store_get_files(self, sop_instance_uids: list):
        self.log_debug('Getting files %r', sop_instance_uids)
        for file_record in self.find_files(sop_instance_uids):
            ds = self._stored_files[file_record.sop_instance_uid]
            yield file_record.sop_class_uid, file_record.transfer_syntax, ds


class TempFileStorage(StorageBase):
    """Simple storage component that uses temporary files to store incoming
    dataset.

    Intended for testing only.
    """
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self._temp_files = set()

    def on_get_file(self, context: asceprovider.PContextDef,
                    command_set: pydicom.Dataset) -> Tuple[IO, int]:
        sop_instance_uid = command_set.AffectedSOPInstanceUID
        sop_class_uid = command_set.AffectedSOPClassUID
        ts = context.supported_ts
        fp = tempfile.NamedTemporaryFile(delete=False)
        start = fp.tell()
        applicationentity.write_meta(fp, command_set, context.supported_ts)
        self.new_file(sop_instance_uid, sop_class_uid, ts, fp.name)
        self._temp_files.add(fp.name)
        self.log_info('Storing incoming dataset in %s', fp.name)
        return fp, start

    def on_store_done(self, ds: pydicom.Dataset):
        self.file_stored(ds.SOPInstanceUID)

    def on_store_failure(self, ds: pydicom.Dataset):
        file_name = self.remove_file(ds.SOPInstanceUID)
        self.remove_nothrow(file_name)
        self._temp_files.remove(file_name)

    def on_store_get_files(self, sop_instance_uids: list):
        self.log_debug('Getting files %r', sop_instance_uids)
        for file_record in self.find_files(sop_instance_uids):
            file_name = file_record.file_name
            yield file_record.sop_class_uid, file_record.transfer_syntax, file_name

    def on_exit(self):
        super().on_exit()
        for file_name in self._temp_files:
            self.remove_nothrow(file_name)
