import os
import pathlib
import uuid

import peewee
import pydicom
import pytest
import trolleybus
from pydicom import uid
from pynetdicom2 import dsutils, fsm

from tiny_pacs import db, events, storage


@pytest.fixture
def memory_storage() -> storage.InMemoryStorage:
    bus = trolleybus.EventBus()
    _db = db.Database(bus, {'db_name': str(uuid.uuid4())})
    _storage = storage.InMemoryStorage(bus, {})
    bus.start()
    return _storage


def test_new_file(memory_storage: storage.InMemoryStorage) -> None:
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    _file = storage.StorageFiles.get(storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert _file.sop_instance_uid == '1.2.3.4'
    assert _file.sop_class_uid == '1.2.3'
    assert _file.transfer_syntax == '1.2.3.5'
    assert _file.file_name == 'test'
    assert not _file.is_stored


def test_in_progress_storage(memory_storage: storage.InMemoryStorage) -> None:
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    _file = storage.StorageFiles.get(storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert not _file.is_stored


def test_failure_storage(memory_storage: storage.InMemoryStorage) -> None:
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    memory_storage.bus.broadcast(events.StoreFailure, ds)
    with pytest.raises(peewee.DoesNotExist):
        storage.StorageFiles.get(storage.StorageFiles.sop_instance_uid == '1.2.3.4')


def test_get_files(memory_storage: storage.InMemoryStorage) -> None:
    ts = uid.ImplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd_ds = pydicom.Dataset()
    cmd_ds.AffectedSOPClassUID = '1.2.3'
    cmd_ds.AffectedSOPInstanceUID = '1.2.3.4'
    fp, start = memory_storage.bus.send_one(
        events.GetFile, events.GetFilePayload(ctx, cmd_ds)
    )
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    ds.SOPClassUID = '1.2.3'
    ds_stream = dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian)
    fp.write(ds_stream)
    fp.seek(start)
    memory_storage.bus.broadcast(events.StoreDone, ds)
    for sop_class_uid, _ts, stored_ds in memory_storage.on_store_get_files(['1.2.3.4']):
        assert sop_class_uid == '1.2.3'
        assert _ts == ts
        assert isinstance(stored_ds, pydicom.Dataset)
        assert stored_ds.SOPInstanceUID == '1.2.3.4'


def test_get_files_empty(memory_storage: storage.InMemoryStorage) -> None:
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    results = memory_storage.on_store_get_files(['1.2.3.4'])
    assert not len(list(results))


def test_file_storage_get_files(tmp_path: pathlib.Path) -> None:
    # Mirrors the DIMSE store flow: GetFile hands out a file object, the
    # dataset bytes are written to it, then the file must be *readable*
    # (PACS re-reads it via pydicom before broadcasting StoreDone).
    bus = trolleybus.EventBus()
    _db = db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(bus, {'storage_dir': str(tmp_path)})
    bus.start()

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd_ds = pydicom.Dataset()
    cmd_ds.AffectedSOPClassUID = '1.2.3'
    cmd_ds.AffectedSOPInstanceUID = '1.2.3.4'
    fp, start = bus.send_one(events.GetFile, events.GetFilePayload(ctx, cmd_ds))

    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    ds.SOPClassUID = '1.2.3'
    fp.write(dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian))
    fp.seek(start)
    # PACS.on_store reads the stored dataset back from the same file object
    stored = pydicom.dcmread(fp, stop_before_pixels=True)
    assert stored.SOPInstanceUID == '1.2.3.4'
    fp.close()

    file_storage.bus.broadcast(events.StoreDone, ds)
    results = list(file_storage.on_store_get_files(['1.2.3.4']))
    assert len(results) == 1
    sop_class_uid, file_ts, file_name = results[0]
    assert sop_class_uid == '1.2.3'
    assert file_ts == str(ts)
    assert isinstance(file_name, str)
    full_name = os.path.join(str(tmp_path), file_name)
    assert os.path.isfile(full_name)
    on_disk = pydicom.dcmread(full_name)
    assert on_disk.SOPInstanceUID == '1.2.3.4'


def test_file_storage_unique_file_names(tmp_path: pathlib.Path) -> None:
    # If the default file name is already taken on disk, a unique name is
    # chosen (FolderStorageMixin provides the naming scheme). The SOP Instance
    # UID column is unique in the DB, so the collision is simulated by
    # pre-creating a file with the expected name.
    bus = trolleybus.EventBus()
    _db = db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(bus, {'storage_dir': str(tmp_path)})
    bus.start()

    # Pre-create the default file name so the storage must fall back to a
    # unique ``_1`` suffix.
    folder = file_storage.get_folder_path()
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, '1.2.3.4.dcm'), 'wb'):
        pass

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd_ds = pydicom.Dataset()
    cmd_ds.AffectedSOPClassUID = '1.2.3'
    cmd_ds.AffectedSOPInstanceUID = '1.2.3.4'

    fp, start = bus.send_one(events.GetFile, events.GetFilePayload(ctx, cmd_ds))
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    ds.SOPClassUID = '1.2.3'
    fp.write(dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian))
    fp.seek(start)
    fp.close()

    record = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4'
    )
    assert os.path.basename(record.file_name) == '1.2.3.4_1.dcm'
    full_name = os.path.join(str(tmp_path), record.file_name)
    assert os.path.isfile(full_name)
    on_disk = pydicom.dcmread(full_name)
    assert on_disk.SOPInstanceUID == '1.2.3.4'
