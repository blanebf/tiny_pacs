# -*- coding: utf-8 -*-
import pydicom
import pytest

from pynetdicom2 import asceprovider
from pydicom import uid
from pynetdicom2 import dsutils

from tiny_pacs import ae
from tiny_pacs import db
from tiny_pacs import event_bus
from tiny_pacs import storage


@pytest.fixture
def memory_storage():
    bus = event_bus.EventBus()
    _db = db.Database(bus, {})
    _storage = storage.InMemoryStorage(bus, {})
    bus.broadcast(event_bus.DefaultChannels.ON_START)
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
    assert _file.is_stored == False


def test_successful_storage(memory_storage: storage.InMemoryStorage):
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    memory_storage.bus.broadcast(storage.StorageChannels.ON_STORE_DONE, ds)
    _file = storage.StorageFiles.get(storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert _file.is_stored == True


def test_failure_storage(memory_storage: storage.InMemoryStorage):
    memory_storage.new_file(
        '1.2.3.4',
        '1.2.3',
        '1.2.3.5',
        'test'
    )
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    memory_storage.bus.broadcast(storage.StorageChannels.ON_STORE_FAILURE, ds)
    with pytest.raises(storage.StorageFiles.DoesNotExist):  # pylint: disable=no-member
        storage.StorageFiles.get(storage.StorageFiles.sop_instance_uid == '1.2.3.4')


def test_get_files(memory_storage: storage.InMemoryStorage):
    ts = uid.ImplicitVRLittleEndian
    ctx = asceprovider.PContextDef(1, '1.2.3', ts)
    cmd_ds = pydicom.Dataset()
    cmd_ds.AffectedSOPClassUID = '1.2.3'
    cmd_ds.AffectedSOPInstanceUID = '1.2.3.4'
    fp, start = memory_storage.bus.send_one(ae.AEChannels.ON_GET_FILE, ctx, cmd_ds)
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    ds.SOPClassUID = '1.2.3'
    ds_stream = dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian)
    fp.write(ds_stream)
    fp.seek(start)
    memory_storage.bus.broadcast(storage.StorageChannels.ON_STORE_DONE, ds)
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
