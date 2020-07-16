# -*- coding: utf-8 -*-
import datetime
import enum
import io
import os
import shutil
import tempfile

import peewee

import pydicom
from pynetdicom2 import applicationentity

from . import ae
from . import component
from . import db
from . import event_bus


class StorageChannels(enum.Enum):
    ON_STORE_DONE = 'on-store-done'
    ON_STORE_FAILURE = 'on-store-failure'
    ON_GET_FILES = 'on-store-get-files'
    ON_STORE_VERIFY = 'on-store-verify'


class StorageFiles(peewee.Model):
    sop_instance_uid = peewee.CharField(max_length=64, unique=True)
    sop_class_uid = peewee.CharField(max_length=64, unique=True)
    transfer_syntax = peewee.CharField(max_length=64)
    file_name = peewee.TextField()
    added = peewee.DateTimeField(default=datetime.datetime.utcnow, index=True)
    is_stored = peewee.BooleanField(index=True, default=False)


class StorageBase(component.Component):
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)

        self.subscribe(ae.AEChannels.ON_GET_FILE, self.on_get_file)
        self.subscribe(StorageChannels.ON_STORE_DONE, self.on_store_done)
        self.subscribe(StorageChannels.ON_STORE_FAILURE, self.on_store_failure)
        self.subscribe(StorageChannels.ON_GET_FILES, self.on_store_get_files)
        self.subscribe(StorageChannels.ON_STORE_VERIFY, self.verify)
        self.subscribe(db.DBChannels.TABLES, self.tables)

    @staticmethod
    def tables():
        return [StorageFiles]

    def atomic(self):
        return self.send_one(db.DBChannels.ATOMIC)

    def on_get_file(self, context, command_set: pydicom.Dataset):
        raise NotImplementedError()

    def on_store_done(self, ds: pydicom.Dataset):
        raise NotImplementedError()

    def on_store_failure(self, ds: pydicom.Dataset):
        raise NotImplementedError()

    def on_store_get_files(self, sop_instance_uids: list):
        raise NotImplementedError()

    def new_file(self, sop_instance_uid: str, sop_class_uid: str,
                 transfer_syntax: str, file_name: str):
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
        with self.atomic():
            stored_file = StorageFiles.get(StorageFiles.sop_instance_uid == sop_instance_uid)
            stored_file.is_stored = True
            stored_file.save()
        self.log_info('Successfully stored file in DB, SOP Instance UID: %s', sop_instance_uid)

    def remove_file(self, sop_instance_uid: str):
        with self.atomic():
            stored_file = StorageFiles.get(StorageFiles.sop_instance_uid == sop_instance_uid)
            file_name = stored_file.file_name
            stored_file.delete_instance()
        self.log_info('Removed stored file from DB, SOP Instance UID: %s', sop_instance_uid)
        return file_name

    def verify(self, instances: list):
        self.log_debug('Verifying instances: %r', instances)
        sop_instance_uids = [i for _, i in instances]
        query = self.find_files(sop_instance_uids)
        stored_instances = frozenset((r.sop_class_uid, r.sop_instance_uid) for r in query)
        instances = frozenset(instances)
        success = instances & stored_instances
        failure = instances - stored_instances
        self.log_debug('Verification, stored successfully: %r', success)
        self.log_debug('Verification, missing from storage: %r', failure)
        return success, failure

    def find_files(self, sop_instance_uids: list):
        query = StorageFiles.select()\
            .where(
                (StorageFiles.sop_instance_uid << sop_instance_uids) &
                (StorageFiles.is_stored == True)
            )
        return query

    def remove_nothrow(self, file_name):
        try:
            os.remove(file_name)
        except Exception as e:
            self.log_exception(f'Failed to remove file {file_name}: {e}')


class FileStorage(StorageBase):
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        storage_dir = config.get('storage_dir', None)
        if storage_dir is None:
            # TODO Gracefully remove temporary directory on shutdown
            storage_dir = tempfile.mkdtemp()
            self.subscribe(event_bus.DefaultChannels.ON_EXIT, self.cleanup)
        self.storage_dir = storage_dir

    def on_get_file(self, context, command_set: pydicom.Dataset):
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

    def get_folder_path(self):
        now = datetime.datetime.utcnow()
        return os.path.join(self.storage_dir, now.strftime('%Y%m%d'))

    def get_file_name(self, sop_instance_uid: str):
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
        try:
            shutil.rmtree(self.storage_dir)
        except Exception as e:
            self.log_exception(
                f'Failed to cleanup storage directory {self.storage_dir}: {e}'
            )


class InMemoryStorage(StorageBase):
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self._temp_files = {}
        self._stored_files = {}

    def on_get_file(self, context, command_set: pydicom.Dataset):
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
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self._temp_files = set()

    def on_get_file(self, context, command_set: pydicom.Dataset):
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
