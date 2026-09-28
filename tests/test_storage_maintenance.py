"""Tests of the storage maintenance events (stats, verify, cleanup)."""
import datetime
import os
import pathlib
import time
import uuid
from typing import Any

import pydantic
import pytest
import trolleybus

from tiny_pacs import db, events, storage


@pytest.fixture
def file_env(tmp_path: pathlib.Path) -> Any:
    """Bus with ``Database`` + ``FileStorage`` over a temp directory."""
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    file_storage = storage.FileStorage(
        bus, {'storage_dir': str(storage_dir)}
    )
    bus.start()
    yield bus, file_storage, storage_dir
    bus.stop()


def _age(path: pathlib.Path, hours: int = 24) -> None:
    """Backdates a file's timestamps past the orphan grace window."""
    old = time.time() - hours * 3600
    os.utime(path, (old, old))


def _stored(bus: trolleybus.EventBus, file_storage: storage.FileStorage,
            storage_dir: pathlib.Path, sop_instance: str) -> str:
    """Creates a stored record with a real file; returns the relative name.

    The record is written the way ``FileStorage.on_get_file`` does it: a
    ``%Y%m%d`` day folder under the storage directory.
    """
    day = storage._utcnow().strftime('%Y%m%d')
    relative = os.path.join(day, f'{sop_instance}.dcm')
    full = storage_dir / relative
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(b'dicom-bytes')
    file_storage.new_file(sop_instance, '1.2.3', '1.2.840.10008.1.2',
                          relative)
    file_storage.file_stored(sop_instance)
    return relative


def test_stats_empty(file_env: Any) -> None:
    bus, _, storage_dir = file_env
    report = bus.send_one(events.StorageStatsQuery, None)
    assert report.records_total == 0
    assert report.records_stored == 0
    assert report.records_failed == 0
    assert report.oldest is None
    assert report.newest is None
    assert report.per_sop_class == {}
    assert report.storage_dir == str(storage_dir)
    assert report.file_count == 0
    assert report.file_bytes == 0
    assert report.per_day_bytes == {}


def test_stats(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    relative = _stored(bus, file_storage, storage_dir, '1.2.3.4')
    day = pathlib.PurePath(relative).parts[0]
    # A stuck record counts as failed
    file_storage.new_file('9.9.9', '1.2.3', '1.2.840.10008.1.2',
                          os.path.join(day, '9.9.9.dcm'))
    report = bus.send_one(events.StorageStatsQuery, None)
    assert report.records_total == 2
    assert report.records_stored == 1
    assert report.records_failed == 1
    assert report.per_sop_class == {'1.2.3': 2}
    assert report.oldest is not None
    assert report.newest is not None
    assert report.file_count == 1
    assert report.file_bytes == len(b'dicom-bytes')
    assert report.per_day_bytes == {day: len(b'dicom-bytes')}


def test_verify_healthy(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    _stored(bus, file_storage, storage_dir, '1.2.3.4')
    report = bus.send_one(events.StorageVerifyQuery, None)
    assert report.file_backend is True
    assert report.missing_files == []
    assert report.orphan_files == []
    assert report.stuck_records == []


def test_verify_classifies(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    missing = _stored(bus, file_storage, storage_dir, '1.2.3.4')
    (storage_dir / missing).unlink()
    orphan_dir = storage_dir / '20200101'
    orphan_dir.mkdir()
    (orphan_dir / 'orphan.dcm').write_bytes(b'junk')
    # A stuck record whose file exists on disk
    (orphan_dir / '5.5.5.dcm').write_bytes(b'dicom-bytes')
    file_storage.new_file('5.5.5', '1.2.3', '1.2.840.10008.1.2',
                          '20200101/5.5.5.dcm')
    report = bus.send_one(events.StorageVerifyQuery, None)
    assert report.missing_files == [os.path.normpath(missing)]
    assert report.orphan_files == [
        os.path.normpath(os.path.join('20200101', 'orphan.dcm'))
    ]
    assert report.stuck_records == ['5.5.5']


def test_cleanup_dry_run_is_the_default(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    missing = _stored(bus, file_storage, storage_dir, '1.2.3.4')
    (storage_dir / missing).unlink()
    audit: list[events.AuditRecordPayload] = []
    bus.subscribe(events.AuditRecord, audit.append)
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(delete_missing_records=True)
    )
    assert report.would_remove_records == 1
    assert report.records_removed == 0
    # Nothing was deleted and nothing was audited
    assert storage.StorageFiles.select().count() == 1
    assert audit == []


def test_cleanup_missing_records_and_orphans(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    missing = _stored(bus, file_storage, storage_dir, '1.2.3.4')
    (storage_dir / missing).unlink()
    orphan_dir = storage_dir / '20200101'
    orphan_dir.mkdir()
    orphan = orphan_dir / 'orphan.dcm'
    orphan.write_bytes(b'junk')
    # A live-server guard: only orphans past the grace window go
    _age(orphan)
    audit: list[events.AuditRecordPayload] = []
    bus.subscribe(events.AuditRecord, audit.append)
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(delete_missing_records=True,
                                     delete_orphans=True, apply=True)
    )
    assert report.records_removed == 1
    assert report.files_removed == 1
    assert report.errors == []
    assert storage.StorageFiles.select().count() == 0
    assert not orphan.exists()
    # The applied cleanup is broadcast for the audit trail
    assert len(audit) == 1
    assert audit[0].category == 'storage'
    assert audit[0].event == 'cleanup'
    assert audit[0].details['records_removed'] == 1
    assert audit[0].details['files_removed'] == 1


def test_cleanup_skips_recent_orphans(file_env: Any) -> None:
    """Files within the grace window are never orphan candidates.

    An in-flight C-STORE creates its file before the DB record exists;
    a fresh file is indistinguishable from such a store.
    """
    bus, _, storage_dir = file_env
    orphan_dir = storage_dir / '20200101'
    orphan_dir.mkdir()
    fresh = orphan_dir / 'in-store.dcm'
    fresh.write_bytes(b'partial-dataset')
    dry = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(delete_orphans=True)
    )
    assert dry.would_remove_files == 0
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(delete_orphans=True, apply=True)
    )
    assert report.files_removed == 0
    assert fresh.exists()


def test_removable_orphan_gates(file_env: Any) -> None:
    """The orphan safety gates: grace window and record re-check."""
    bus, file_storage, storage_dir = file_env
    orphan_dir = storage_dir / '20200101'
    orphan_dir.mkdir()
    aged = orphan_dir / 'aged.dcm'
    aged.write_bytes(b'junk')
    _age(aged)
    root = file_storage._resolved_root()
    # Aged and unreferenced: removable
    assert file_storage._removable_orphan(
        os.path.join('20200101', 'aged.dcm'), root) is True
    # A record appearing after the classification vetoes the deletion
    file_storage.new_file('8.8.8', '1.2.3', '1.2.840.10008.1.2',
                          os.path.join('20200101', 'aged.dcm'))
    assert file_storage._removable_orphan(
        os.path.join('20200101', 'aged.dcm'), root) is False
    # Fresh files are skipped regardless
    fresh = orphan_dir / 'fresh.dcm'
    fresh.write_bytes(b'junk')
    assert file_storage._removable_orphan(
        os.path.join('20200101', 'fresh.dcm'), root) is False
    # Missing files are skipped
    assert file_storage._removable_orphan(
        os.path.join('20200101', 'gone.dcm'), root) is False


def test_cleanup_stuck_records(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    relative = _stored(bus, file_storage, storage_dir, '1.2.3.4')
    # Force the record back into the stuck state and age it
    row = storage.StorageFiles.get(
        storage.StorageFiles.sop_instance_uid == '1.2.3.4')
    row.is_stored = False
    row.added = storage._utcnow() - datetime.timedelta(days=5)
    row.save()
    # Younger than the threshold: everything is kept
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(failed_older_than_days=10, apply=True)
    )
    assert report.records_removed == 0
    assert (storage_dir / relative).exists()
    # Older than the threshold: record and file go
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(failed_older_than_days=1, apply=True)
    )
    assert report.records_removed == 1
    assert report.files_removed == 1
    assert storage.StorageFiles.select().count() == 0
    assert not (storage_dir / relative).exists()


def test_cleanup_zero_days_refused() -> None:
    # A running server may legitimately have in-progress stores: age 0
    # is rejected by the options model
    with pytest.raises(pydantic.ValidationError):
        events.StorageCleanupOptions(failed_older_than_days=0)


def test_cleanup_containment_guard(file_env: Any) -> None:
    bus, file_storage, storage_dir = file_env
    outside = storage_dir.parent / 'outside.dcm'
    outside.write_bytes(b'do-not-touch')
    # A malicious record pointing outside the storage directory
    file_storage.new_file('6.6.6', '1.2.3', '1.2.840.10008.1.2',
                          '../outside.dcm')
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(delete_missing_records=True,
                                     apply=True)
    )
    # The record is dropped but the outside file is never touched
    assert report.records_removed == 1
    assert report.files_removed == 0
    assert outside.exists()


def test_non_file_backend_answers(tmp_path: pathlib.Path) -> None:
    """Backends without a directory skip the disk sections gracefully."""
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    memory_storage = storage.InMemoryStorage(bus, {})
    bus.start()
    stats = bus.send_one(events.StorageStatsQuery, None)
    assert stats.storage_dir is None
    assert stats.file_count is None
    assert stats.file_bytes is None
    assert stats.per_day_bytes is None
    verify = bus.send_one(events.StorageVerifyQuery, None)
    assert verify.file_backend is False
    assert verify.missing_files == []
    assert verify.orphan_files == []
    # Stuck records are still reported and cleaned DB-side
    memory_storage.new_file('7.7.7', '1.2.3', '1.2.840.10008.1.2', 'x')
    verify = bus.send_one(events.StorageVerifyQuery, None)
    assert verify.stuck_records == ['7.7.7']
    report = bus.send_one(
        events.StorageCleanupCommand,
        events.StorageCleanupOptions(failed_older_than_days=1, apply=True)
    )
    # The record is not old enough yet
    assert report.records_removed == 0
    bus.stop()
