"""Tests of the ``audit`` CLI subcommands against temporary databases."""
import datetime
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from conftest import seed_rows
from tiny_pacs import __main__ as cli


def _run(argv: list[str]) -> Any:
    """Parses and executes a ``tiny-pacs`` subcommand."""
    args = cli.parse_args(argv)
    handler = getattr(args, 'command_handler', None)
    if handler is not None:
        return handler(args)
    raise AssertionError(f'no command handler for {argv!r}')


def _seed(db_path: str) -> None:
    """Seeds three records: one ancient, two recent."""
    now = datetime.datetime.now(datetime.timezone.utc)
    seed_rows(db_path, [
        {
            'timestamp': now - datetime.timedelta(days=40),
            'category': 'assoc', 'event': 'accepted',
            'device_aet': 'MODALITY', 'username': 'alice',
            'peer': '10.0.0.9', 'status': 'accepted', 'details': '{}'
        },
        {
            'timestamp': now - datetime.timedelta(hours=2),
            'category': 'service', 'event': 'store',
            'device_aet': 'MODALITY', 'username': 'alice',
            'status': 'success', 'details': '{"sop_class": "1.2.3"}'
        },
        {
            'timestamp': now - datetime.timedelta(hours=1),
            'category': 'admin', 'event': 'device-add',
            'device_aet': 'MRI', 'username': 'root',
            'status': 'success', 'details': '{}'
        },
    ])


def test_query_table(config_file: Callable[..., str], tmp_path: Path,
                     capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'query', '-c', conf])
    out = capsys.readouterr().out
    assert 'CATEGORY' in out
    assert 'MODALITY' in out
    assert 'device-add' in out


def test_query_json(config_file: Callable[..., str], tmp_path: Path,
                    capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'query', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 3
    store = [row for row in data if row['event'] == 'store'][0]
    # The details blob is emitted as a parsed object
    assert store['details'] == {'sop_class': '1.2.3'}


def test_query_category_filter(config_file: Callable[..., str], tmp_path: Path,
                               capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'query', '--category', 'assoc', '--format', 'json',
          '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert [row['event'] for row in data] == ['accepted']


def test_query_user_and_event_filters(
        config_file: Callable[..., str], tmp_path: Path,
        capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'query', '--user', 'alice', '--event', 'store',
          '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1
    assert data[0]['event'] == 'store'


def test_query_since_filter(config_file: Callable[..., str], tmp_path: Path,
                            capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    since = (datetime.datetime.now(datetime.timezone.utc)
             - datetime.timedelta(days=1)).isoformat()
    _run(['audit', 'query', '--since', since, '--format', 'json', '-c', conf])
    assert len(json.loads(capsys.readouterr().out)) == 2


def test_query_offset(config_file: Callable[..., str], tmp_path: Path,
                      capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    # Oldest-first order is accepted(40d), store(2h), device-add(1h);
    # skipping one returns the second-oldest.
    _run(['audit', 'query', '--limit', '1', '--offset', '1', '--format',
          'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1
    assert data[0]['event'] == 'store'


def test_query_does_not_purge(config_file: Callable[..., str], tmp_path: Path,
                              capsys: pytest.CaptureFixture[str]) -> None:
    """A read-only query never runs the retention purge, even when the
    config sets ``retention_days`` (regression for destructive-on-read)."""
    db = str(tmp_path / 'audit.db')
    extra = '  AuditLog:\n    on: true\n    retention_days: 1\n'
    conf = config_file(db_name=db, extra=extra)
    now = datetime.datetime.now(datetime.timezone.utc)
    seed_rows(db, [{
        'timestamp': now - datetime.timedelta(days=40),
        'category': 'assoc', 'event': 'attempt', 'device_aet': 'OLD',
        'status': 'pending', 'details': '{}'
    }])
    _run(['audit', 'query', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert [row['device_aet'] for row in data] == ['OLD']


def test_query_invalid_timestamp(config_file: Callable[..., str],
                                 tmp_path: Path,
                                 capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    with pytest.raises(SystemExit) as error:
        _run(['audit', 'query', '--since', 'not-a-date', '-c', conf])
    assert error.value.code == 1
    assert 'invalid timestamp' in capsys.readouterr().err


def test_stats_json(config_file: Callable[..., str], tmp_path: Path,
                    capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'stats', '--format', 'json', '-c', conf])
    stats = json.loads(capsys.readouterr().out)
    assert stats['total'] == 3
    assert stats['by_category']['assoc'] == 1
    assert stats['by_category']['service'] == 1


def test_stats_table(config_file: Callable[..., str], tmp_path: Path,
                     capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'stats', '-c', conf])
    out = capsys.readouterr().out
    assert 'Audit statistics:' in out
    assert 'Per category:' in out


def test_cleanup_removes_old(config_file: Callable[..., str], tmp_path: Path,
                             capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    _seed(db)
    _run(['audit', 'cleanup', '--older-than', '30', '-c', conf])
    assert 'Removed 1 audit record(s)' in capsys.readouterr().out

    _run(['audit', 'stats', '--format', 'json', '-c', conf])
    assert json.loads(capsys.readouterr().out)['total'] == 2


def test_cleanup_refuses_zero_days(config_file: Callable[..., str],
                                   tmp_path: Path,
                                   capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / 'audit.db')
    conf = config_file(db_name=db)
    with pytest.raises(SystemExit) as error:
        _run(['audit', 'cleanup', '--older-than', '0', '-c', conf])
    assert error.value.code == 1
    assert 'at least 1 day' in capsys.readouterr().err


def test_memory_database_rejected(tmp_path: Path,
                                  capsys: pytest.CaptureFixture[str]) -> None:
    conf = tmp_path / 'mem.yaml'
    conf.write_text(
        'components:\n'
        '  Database:\n'
        '    on: true\n'
        '    db_name: mem\n'
        '    mode: memory\n'
    )
    with pytest.raises(SystemExit) as error:
        _run(['audit', 'query', '-c', str(conf)])
    assert error.value.code == 1
    assert 'persistent database' in capsys.readouterr().err
