import uuid

import pydicom
import pytest
import trolleybus
from pydicom import uid
from pynetdicom2 import dsutils, fsm

from tiny_pacs import db, events, storage


@pytest.fixture
def memory_storage():
    bus = trolleybus.EventBus()
    _db = db.Database(bus, {'db_name': str(uuid.uuid4())})
    _storage = storage.InMemoryStorage(bus, {})
    bus.start()
    return _storage


def test_new_file(memory_storage: storage.InMemoryStorage):
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


def test_in_progress_storage(memory_storage: storage.InMemoryStorage):
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


def test_failure_storage(memory_storage: storage.InMemoryStorage):
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    memory_storage.bus.broadcast(events.StoreFailure, ds)
    with pytest.raises(storage.StorageFiles.DoesNotExist):
        storage.StorageFiles.get(storage.StorageFiles.sop_instance_uid == '1.2.3.4')


def test_get_files(memory_storage: storage.InMemoryStorage):
    ts = uid.ImplicitVRLittleEndian
    ctx = fsm.PContextDef(1, '1.2.3', ts)
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
    for sop_class_uid, _ts, ds in memory_storage.on_store_get_files(['1.2.3.4']):
        assert sop_class_uid == '1.2.3'
        assert _ts == ts
        assert ds.SOPInstanceUID == '1.2.3.4'


def test_get_files_empty(memory_storage: storage.InMemoryStorage):
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    results = memory_storage.on_store_get_files(['1.2.3.4'])
    assert not len(list(results))
