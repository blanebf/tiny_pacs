"""Tests of the ``storage`` CLI subcommands (stats, verify, cleanup)."""
import datetime
import json
import os
import pathlib
import time
import uuid
from collections.abc import Callable
from typing import Any

import pytest
import trolleybus
import yaml  # type: ignore[import-untyped]
from tiny_pacs import __main__ as cli
from tiny_pacs import db as core_db
from tiny_pacs import storage as core_storage


def _run(argv: list[str]) -> Any:
    """Parses and executes a ``tiny-pacs`` subcommand."""
    args = cli.parse_args(argv)
    handler = getattr(args, 'command_handler', None)
    if handler is not None:
        return handler(args)
    raise AssertionError(f'no command handler for {argv!r}')


def _file_storage_extra(storage_dir: pathlib.Path) -> str:
    """Configuration fragment enabling only the file-backed storage."""
    return (
        '  FileStorage:\n'
        '    on: true\n'
        f'    storage_dir: {storage_dir}\n'
        '  InMemoryStorage:\n'
        '    on: false\n'
    )


def _age(path: pathlib.Path, hours: int = 24) -> None:
    """Backdates a file's timestamps past the orphan grace window."""
    old = time.time() - hours * 3600
    os.utime(path, (old, old))


def test_stats_table_empty(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    conf = config_file(extra=_file_storage_extra(storage_dir))
    _run(['storage', 'stats', '-c', conf])
    out = capsys.readouterr().out
    assert 'Storage statistics:' in out
    assert 'records' in out
    assert 'Storage directory:' in out


def test_stats_json_yaml_parity(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    (storage_dir / '20200101').mkdir(parents=True)
    (storage_dir / '20200101' / 'x.dcm').write_bytes(b'0123456789')
    conf = config_file(extra=_file_storage_extra(storage_dir))

    _run(['storage', 'stats', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert data['file_count'] == 1
    assert data['file_bytes'] == 10
    assert data['per_day_bytes'] == {'20200101': 10}
    assert data['storage_dir'] == str(storage_dir)

    _run(['storage', 'stats', '--format', 'yaml', '-c', conf])
    parsed = yaml.safe_load(capsys.readouterr().out)
    assert parsed == data


def test_stats_non_file_backend(
        config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    """The default InMemoryStorage degrades without the disk sections."""
    conf = config_file()
    _run(['storage', 'stats', '-c', conf])
    out = capsys.readouterr().out
    assert 'non-file storage' in out

    _run(['storage', 'stats', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert data['storage_dir'] is None
    assert data['file_bytes'] is None


def test_stats_quota_exceeded_exits_2(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    (storage_dir / 'big.dcm').write_bytes(b'x' * 2048)
    conf = config_file(extra=_file_storage_extra(storage_dir))
    with pytest.raises(SystemExit) as error:
        _run(['storage', 'stats', '--quota', '0.000001', '-c', conf])
    assert error.value.code == 2
    assert 'exceeds the quota' in capsys.readouterr().err


def test_stats_quota_under_limit(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    (storage_dir / 'small.dcm').write_bytes(b'x' * 10)
    conf = config_file(extra=_file_storage_extra(storage_dir))
    _run(['storage', 'stats', '--quota', '1', '-c', conf])
    out = capsys.readouterr().out
    assert 'Storage statistics:' in out


def test_stats_quota_without_file_backend_fails(
        config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit) as error:
        _run(['storage', 'stats', '--quota', '1', '-c', conf])
    assert error.value.code == 1
    assert 'file-backed storage' in capsys.readouterr().err


@pytest.mark.parametrize('quota', ('nan', 'inf', '-1', '0'))
def test_stats_quota_invalid_value_fails(
        quota: str, config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    """Non-positive/non-finite quotas are refused before any DB access.

    A NaN quota would otherwise silently disable the exit-2 signal a
    monitoring script relies on (every comparison with NaN is False).
    """
    conf = config_file()
    with pytest.raises(SystemExit) as error:
        _run(['storage', 'stats', f'--quota={quota}', '-c', conf])
    assert error.value.code == 1
    assert 'positive, finite' in capsys.readouterr().err


def test_storage_commands_skip_non_storage_components(
        config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    """Non-storage components never start (no startup writes).

    ``DeviceStore`` would import the YAML devices into the database on
    startup; running ``storage stats`` must not create its tables or
    rows, so the command stays safe against a live server's database.
    """
    import sqlite3
    extra = (
        '  Devices:\n'
        '    on: true\n'
        '    devices:\n'
        '      MRI:\n'
        '        aet: MRI\n'
        '        address: 10.0.0.1\n'
        '        port: 104\n'
        '  DeviceStore:\n'
        '    on: true\n'
    )
    conf = config_file(extra=extra)
    loaded = yaml.safe_load(pathlib.Path(conf).read_text())
    db_path = loaded['components']['Database']['db_name']

    _run(['storage', 'stats', '-c', conf])
    capsys.readouterr()

    connection = sqlite3.connect(db_path)
    try:
        tables = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()
    assert 'devicemodel' not in tables


def test_storage_disabled_fails(tmp_path: pathlib.Path,
                                capsys: pytest.CaptureFixture[str]) -> None:
    conf = tmp_path / 'nostorage.yaml'
    conf.write_text(
        'components:\n'
        '  Database:\n'
        '    on: true\n'
        f'    db_name: {tmp_path / "n.db"}\n'
        '    mode: rwc\n'
        '  FileStorage:\n'
        '    on: false\n'
        '  InMemoryStorage:\n'
        '    on: false\n'
        '  TempFileStorage:\n'
        '    on: false\n'
    )
    with pytest.raises(SystemExit) as error:
        _run(['storage', 'stats', '-c', str(conf)])
    assert error.value.code == 1
    assert 'No storage component' in capsys.readouterr().err


def test_verify_table_and_json(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    orphan_dir = storage_dir / '20200101'
    orphan_dir.mkdir(parents=True)
    (orphan_dir / 'orphan.dcm').write_bytes(b'junk')
    conf = config_file(extra=_file_storage_extra(storage_dir))

    _run(['storage', 'verify', '-c', conf])
    out = capsys.readouterr().out
    assert 'Storage verification:' in out
    assert 'orphan files:     1' in out
    assert os.path.join('20200101', 'orphan.dcm') in out

    _run(['storage', 'verify', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert data['file_backend'] is True
    assert data['orphan_files'] == [os.path.join('20200101', 'orphan.dcm')]


def test_verify_non_file_backend_degrades(
        config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    _run(['storage', 'verify', '-c', conf])
    out = capsys.readouterr().out
    assert 'non-file backend' in out


def test_verify_delete_orphans_needs_file_backend(
        config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit) as error:
        _run(['storage', 'verify', '--delete-orphans', '-c', conf])
    assert error.value.code == 1
    assert 'file-backed storage' in capsys.readouterr().err


def test_verify_delete_orphans_dry_run_then_apply(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    orphan_dir = storage_dir / '20200101'
    orphan_dir.mkdir(parents=True)
    orphan = orphan_dir / 'orphan.dcm'
    orphan.write_bytes(b'junk')
    _age(orphan)
    conf = config_file(extra=_file_storage_extra(storage_dir))

    _run(['storage', 'verify', '--delete-orphans', '-c', conf])
    out = capsys.readouterr().out
    assert 'Dry run: would remove 0 record(s) and 1 file(s)' in out
    assert orphan.exists()

    _run(['storage', 'verify', '--delete-orphans', '--apply', '-c', conf])
    out = capsys.readouterr().out
    assert 'Removed 0 record(s) and 1 file(s)' in out
    assert not orphan.exists()


def test_cleanup_refuses_zero_days(
        config_file: Callable[..., str],
        capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit) as error:
        _run(['storage', 'cleanup', '--older-than', '0', '-c', conf])
    assert error.value.code == 1
    err = capsys.readouterr().err
    assert 'at least 1 day' in err


def test_cleanup_dry_run_by_default(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    conf = config_file(extra=_file_storage_extra(storage_dir))
    _seed_stuck(conf, storage_dir, '5.5.5.5', days=5)
    _run(['storage', 'cleanup', '--older-than', '1', '-c', conf])
    out = capsys.readouterr().out
    assert 'Dry run: would remove 1 record(s) and 1 file(s)' in out
    # The dry run left everything in place
    assert (storage_dir / '20200101' / '5.5.5.5.dcm').exists()


def test_cleanup_apply_heals_killed_store(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    """Component reuse over the bus: a force-killed store is healed.

    A record with ``is_stored == False`` whose file still exists (the
    process died between ``GetFile`` and ``StoreDone``/``StoreFailure``)
    is classified by ``verify`` and removed by ``cleanup --apply``.
    """
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    conf = config_file(extra=_file_storage_extra(storage_dir))
    _seed_stuck(conf, storage_dir, '5.5.5.5', days=5)

    _run(['storage', 'verify', '-c', conf])
    out = capsys.readouterr().out
    assert 'stuck records:    1' in out
    assert '5.5.5.5' in out

    _run(['storage', 'cleanup', '--older-than', '1', '--apply', '-c', conf])
    out = capsys.readouterr().out
    assert 'Removed 1 record(s) and 1 file(s)' in out
    assert not (storage_dir / '20200101' / '5.5.5.5.dcm').exists()

    _run(['storage', 'verify', '-c', conf])
    out = capsys.readouterr().out
    assert 'stuck records:    0' in out


def test_cleanup_keeps_younger_records(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    conf = config_file(extra=_file_storage_extra(storage_dir))
    _seed_stuck(conf, storage_dir, '6.6.6.6', days=1)
    _run(['storage', 'cleanup', '--older-than', '10', '--apply', '-c', conf])
    out = capsys.readouterr().out
    assert 'Removed 0 record(s) and 0 file(s)' in out
    assert (storage_dir / '20200101' / '6.6.6.6.dcm').exists()


def test_cleanup_warns_about_recent_activity(
        config_file: Callable[..., str], tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    storage_dir = tmp_path / 'files'
    storage_dir.mkdir()
    conf = config_file(extra=_file_storage_extra(storage_dir))
    # A record added right now hints at a live server
    _seed_stuck(conf, storage_dir, '7.7.7.7', days=0)
    _run(['storage', 'cleanup', '--older-than', '1', '--apply', '-c', conf])
    captured = capsys.readouterr()
    assert 'warning: recent store activity' in captured.err
    assert 'Removed 0 record(s)' in captured.out


def _seed_stuck(config_path: str, storage_dir: pathlib.Path,
                sop_instance: str, days: int) -> None:
    """Seeds a stuck record with a real file directly over an admin bus.

    Simulates a store interrupted between ``GetFile`` and
    ``StoreDone``/``StoreFailure``: the file exists on disk while the
    record stays ``is_stored == False``, backdated past the cleanup
    threshold.
    """
    loaded = yaml.safe_load(pathlib.Path(config_path).read_text())
    db_name = loaded['components']['Database']['db_name']
    relative = os.path.join('20200101', f'{sop_instance}.dcm')
    full = storage_dir / relative
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(b'partial-dataset')
    bus = trolleybus.EventBus()
    database = core_db.Database(
        bus, {'driver': 'sqlite', 'db_name': db_name, 'uri': False}
    )
    file_storage = core_storage.FileStorage(
        bus, {'storage_dir': str(storage_dir)}
    )
    bus.start()
    try:
        file_storage.new_file(sop_instance, '1.2.3', '1.2.840.10008.1.2',
                              relative)
        row = core_storage.StorageFiles.get(
            core_storage.StorageFiles.sop_instance_uid == sop_instance
        )
        row.added = (datetime.datetime.now(datetime.timezone.utc)
                     - datetime.timedelta(days=days))
        row.save()
    finally:
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


def test_config_yaml_storage_dir_roundtrip(tmp_path: pathlib.Path) -> None:
    """Guards the fixture assumption that db_name survives the YAML."""
    path = tmp_path / 'conf.yaml'
    db_name = str(tmp_path / f'{uuid.uuid4().hex}.db')
    path.write_text(
        'components:\n'
        '  Database:\n'
        '    on: true\n'
        f'    db_name: {db_name}\n'
        '    mode: rwc\n'
    )
    loaded = yaml.safe_load(path.read_text())
    assert loaded['components']['Database']['db_name'] == db_name
