import os
import pathlib
import uuid
from typing import cast

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
    db.Database(bus, {'db_name': str(uuid.uuid4())})
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
    _file = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
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
    _file = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
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
        storage.StorageFiles.get(
            storage.StorageFiles.sop_instance_uid == '1.2.3.4')


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
    for sop_class_uid, _ts, stored_ds in memory_storage.on_store_get_files(
            ['1.2.3.4']):
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
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(bus, {'storage_dir': str(tmp_path)})
    bus.start()

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd_ds = pydicom.Dataset()
    cmd_ds.AffectedSOPClassUID = '1.2.3'
    cmd_ds.AffectedSOPInstanceUID = '1.2.3.4'
    fp, start = bus.send_one(events.GetFile,
                             events.GetFilePayload(ctx, cmd_ds))

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
    db.Database(bus, {'db_name': str(uuid.uuid4())})
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

    fp, start = bus.send_one(events.GetFile,
                             events.GetFilePayload(ctx, cmd_ds))
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


def _store_once(
        bus: trolleybus.EventBus,
        ctx: fsm.PContextDef,
        cmd: pydicom.Dataset,
        ts: uid.UID
) -> bool:
    """Runs one full store cycle through the bus.

    :return: whether the ``GetFile`` handler refused the store (handed back a
             reject buffer instead of a writable file)
    :rtype: bool
    """
    fp, start = bus.send_one(events.GetFile, events.GetFilePayload(ctx, cmd))
    if getattr(fp, 'store_rejected', False):
        return True
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = cmd.AffectedSOPInstanceUID
    ds.SOPClassUID = cmd.AffectedSOPClassUID
    fp.write(dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian))
    fp.seek(start)
    # Flush (not close): FileStorage must persist bytes to disk, while
    # InMemoryStorage's ``StoreDone`` handler still reads from the buffer.
    fp.flush()
    bus.broadcast(events.StoreDone, ds)
    return False


def test_file_storage_duplicate_refused(tmp_path: pathlib.Path) -> None:
    # A second store of an already-stored instance must not create anything
    # and must surface a refusal status, instead of aborting the association.
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(bus, {'storage_dir': str(tmp_path)})
    bus.start()

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd = pydicom.Dataset()
    cmd.AffectedSOPClassUID = '1.2.3'
    cmd.AffectedSOPInstanceUID = '1.2.3.4'

    assert not _store_once(bus, ctx, cmd, ts)
    record_before = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4'
    )
    files_before = len(os.listdir(os.path.join(
        str(tmp_path), os.path.dirname(record_before.file_name))))

    # The duplicate store is refused: GetFile hands back a reject buffer and
    # the storage's Store handler maps it to the failure status.
    payload = events.GetFilePayload(ctx, cmd)
    rejected_fp, _ = bus.send_one(events.GetFile, payload)
    assert getattr(rejected_fp, 'store_rejected', False) is True
    status = file_storage.on_store(
        events.StorePayload(ctx, rejected_fp, None))
    assert int(status) == int(storage.DUPLICATE_REJECTED_STATUS)

    # The already-stored instance is untouched.
    record_after = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4'
    )
    assert record_after.file_name == record_before.file_name
    assert record_after.is_stored
    assert storage.StorageFiles.select().where(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4').count() == 1
    files_after = len(os.listdir(os.path.join(
        str(tmp_path), os.path.dirname(record_before.file_name))))
    assert files_after == files_before


def test_file_storage_duplicate_overwrite(tmp_path: pathlib.Path) -> None:
    # With overwrite enabled the duplicate store replaced the existing
    # instance: exactly one record and one physical file remain, and the
    # old copy is removed once the replacement is durable.
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    storage.FileStorage(bus, {'storage_dir': str(tmp_path),
                              'overwrite': True})
    bus.start()

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd = pydicom.Dataset()
    cmd.AffectedSOPClassUID = '1.2.3'
    cmd.AffectedSOPInstanceUID = '1.2.3.4'

    assert not _store_once(bus, ctx, cmd, ts)
    first = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    first_full = os.path.join(str(tmp_path), first.file_name)
    assert os.path.isfile(first_full)

    assert not _store_once(bus, ctx, cmd, ts)

    records = list(storage.StorageFiles.select().where(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4'))
    assert len(records) == 1
    assert records[0].is_stored
    full_name = os.path.join(str(tmp_path), records[0].file_name)
    assert os.path.isfile(full_name)
    on_disk = pydicom.dcmread(full_name)
    assert on_disk.SOPInstanceUID == '1.2.3.4'
    # The replaced physical copy is gone (a no-op ``remove_physical`` would
    # leave it behind under a unique suffixed name)
    assert not os.path.isfile(first_full)
    file_count = sum(
        len(files) for _, _, files in os.walk(str(tmp_path)))
    assert file_count == 1


def test_file_storage_overwrite_failure_rolls_back(
        tmp_path: pathlib.Path) -> None:
    # A failed overwrite must leave the stored instance completely intact:
    # the old copy is never removed before the replacement is durable.
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(
        bus, {'storage_dir': str(tmp_path), 'overwrite': True})
    bus.start()

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd = pydicom.Dataset()
    cmd.AffectedSOPClassUID = '1.2.3'
    cmd.AffectedSOPInstanceUID = '1.2.3.4'

    assert not _store_once(bus, ctx, cmd, ts)
    old_record = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    old_name = old_record.file_name
    old_full = os.path.join(str(tmp_path), old_name)

    # Start an overwrite store, then fail it mid-pipeline
    fp, start = bus.send_one(events.GetFile,
                             events.GetFilePayload(ctx, cmd))
    assert not getattr(fp, 'store_rejected', False)
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    ds.SOPClassUID = '1.2.3'
    fp.write(dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian))
    fp.seek(start)
    fp.flush()
    mid_record = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert mid_record.file_name != old_name
    assert not mid_record.is_stored
    assert os.path.isfile(old_full), 'old copy must survive until StoreDone'

    bus.broadcast(events.StoreFailure, ds)

    restored = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert restored.file_name == old_name
    assert restored.is_stored
    assert os.path.isfile(old_full)
    assert not os.path.exists(os.path.join(str(tmp_path),
                                           mid_record.file_name))
    results = list(file_storage.on_store_get_files(['1.2.3.4']))
    assert len(results) == 1
    # A retry of the same instance is accepted again (overwrite still on)
    assert not _store_once(bus, ctx, cmd, ts)


def test_file_storage_parallel_duplicate_refused(
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Losing the check-then-create race against a parallel store of the same
    # instance must produce a refusal, not an exception escaping into the
    # DIMSE decoder (which would abort the association).
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(bus, {'storage_dir': str(tmp_path)})
    bus.start()

    ts = uid.ExplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd = pydicom.Dataset()
    cmd.AffectedSOPClassUID = '1.2.3'
    cmd.AffectedSOPInstanceUID = '1.2.3.4'

    assert not _store_once(bus, ctx, cmd, ts)
    record_before = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    # Simulate the race: the record appears between the duplicate check and
    # the creation, so the check still reports a brand-new instance
    monkeypatch.setattr(
        file_storage, 'check_duplicate',
        lambda _uid: storage._DuplicateAction.NEW
    )
    rejected_fp, _ = bus.send_one(events.GetFile,
                                  events.GetFilePayload(ctx, cmd))
    assert getattr(rejected_fp, 'store_rejected', False) is True
    status = file_storage.on_store(
        events.StorePayload(ctx, rejected_fp, None))
    assert int(status) == int(storage.DUPLICATE_REJECTED_STATUS)
    # The lost race left no extra file behind and kept the stored instance
    file_count = sum(
        len(files) for _, _, files in os.walk(str(tmp_path)))
    assert file_count == 1
    record_after = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert record_after.file_name == record_before.file_name
    assert record_after.is_stored


def test_in_memory_duplicate_refused() -> None:
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    storage.InMemoryStorage(bus, {})
    bus.start()

    ts = uid.ImplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd = pydicom.Dataset()
    cmd.AffectedSOPClassUID = '1.2.3'
    cmd.AffectedSOPInstanceUID = '1.2.3.4'

    assert not _store_once(bus, ctx, cmd, ts)
    rejected_fp, _ = bus.send_one(events.GetFile, events.GetFilePayload(
        ctx, cmd))
    assert getattr(rejected_fp, 'store_rejected', False) is True
    results = bus.broadcast(events.Store, events.StorePayload(
        ctx, rejected_fp, None))
    assert any(
        int(status) == int(storage.DUPLICATE_REJECTED_STATUS)
        for status in results
    )


def test_in_memory_overwrite_failure_keeps_stored_dataset() -> None:
    # A failed in-memory overwrite must leave the stored dataset intact and
    # only drop the incomplete pending buffer.
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    memory_storage = storage.InMemoryStorage(bus, {'overwrite': True})
    bus.start()

    ts = uid.ImplicitVRLittleEndian
    ctx = fsm.PContextDef(1, uid.UID('1.2.3'), ts)
    cmd = pydicom.Dataset()
    cmd.AffectedSOPClassUID = '1.2.3'
    cmd.AffectedSOPInstanceUID = '1.2.3.4'

    assert not _store_once(bus, ctx, cmd, ts)
    fp, _ = bus.send_one(events.GetFile, events.GetFilePayload(ctx, cmd))
    assert not getattr(fp, 'store_rejected', False)
    ds = pydicom.Dataset()
    ds.SOPInstanceUID = '1.2.3.4'
    ds.SOPClassUID = '1.2.3'
    fp.write(dsutils.encode(ds, ts.is_implicit_VR, ts.is_little_endian))
    fp.flush()
    bus.broadcast(events.StoreFailure, ds)

    record = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    assert record.is_stored
    results = list(memory_storage.on_store_get_files(['1.2.3.4']))
    assert len(results) == 1
    stored_ds = cast(pydicom.Dataset, results[0][2])
    assert stored_ds.SOPInstanceUID == '1.2.3.4'
