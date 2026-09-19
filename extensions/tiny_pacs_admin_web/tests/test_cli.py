"""Tests of the ``web-admin`` CLI subcommands against temporary
databases.

The commands run through the core headless admin runtime with the
``Database`` component only: no HTTP port is ever bound, the grant table
is created safely outside the migration mechanism, and a later console
start records the schema version through the regular migration path.
"""
import json
import os
from collections.abc import Callable
from typing import Any

import pytest
from conftest import sqlite_config
from tiny_pacs import __main__ as cli
from tiny_pacs import db as core_db
from tiny_pacs.admin import AdminError
from tiny_pacs.http import HEADLESS_ENV

from tiny_pacs_admin_web.models import WebGrantModel


def _run(argv: list[str]) -> Any:
    """Parses and executes a ``tiny-pacs`` subcommand."""
    args = cli.parse_args(argv)
    handler = getattr(args, 'command_handler', None)
    if handler is not None:
        return handler(args)
    raise AssertionError(f'no command handler for {argv!r}')


def test_grant_creates_table_and_row(config_file: Callable[..., str],
                                     capsys: pytest.CaptureFixture[str]
                                     ) -> None:
    conf = config_file()
    _run(['web-admin', 'grant', 'alice', '--role', 'admin', '-c', conf])
    captured = capsys.readouterr()
    assert 'Granted admin console access to alice' in captured.out
    _assert_rows(conf, [('alice', 'admin')])


def test_grant_default_role_is_viewer(config_file: Callable[..., str],
                                      capsys: pytest.CaptureFixture[str]
                                      ) -> None:
    conf = config_file()
    _run(['web-admin', 'grant', 'bob', '-c', conf])
    assert 'Granted viewer' in capsys.readouterr().out
    _assert_rows(conf, [('bob', 'viewer')])


def test_grant_updates_existing_role(config_file: Callable[..., str],
                                     capsys: pytest.CaptureFixture[str]
                                     ) -> None:
    conf = config_file()
    _run(['web-admin', 'grant', 'alice', '--role', 'viewer', '-c', conf])
    capsys.readouterr()
    _run(['web-admin', 'grant', 'alice', '--role', 'admin', '-c', conf])
    assert 'Updated the console role' in capsys.readouterr().out
    _assert_rows(conf, [('alice', 'admin')])
    # Idempotent repeat
    _run(['web-admin', 'grant', 'alice', '--role', 'admin', '-c', conf])
    assert 'already has admin' in capsys.readouterr().out
    _assert_rows(conf, [('alice', 'admin')])


def test_grant_rejects_empty_username(config_file: Callable[..., str],
                                      ) -> None:
    conf = config_file()
    with pytest.raises(SystemExit) as exit_info:
        _run(['web-admin', 'grant', '   ', '-c', conf])
    assert exit_info.value.code == 1


def test_grant_rejects_unknown_role(config_file: Callable[..., str],
                                    ) -> None:
    conf = config_file()
    with pytest.raises(SystemExit):
        _run(['web-admin', 'grant', 'alice', '--role', 'root', '-c', conf])


def test_revoke_roundtrip(config_file: Callable[..., str],
                          capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    _run(['web-admin', 'grant', 'alice', '-c', conf])
    capsys.readouterr()
    _run(['web-admin', 'revoke', 'alice', '-c', conf])
    out = capsys.readouterr().out
    assert 'Revoked web console access of alice' in out
    _assert_rows(conf, [])


def test_revoke_unknown_fails(config_file: Callable[..., str],
                              capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit) as exit_info:
        _run(['web-admin', 'revoke', 'ghost', '-c', conf])
    assert exit_info.value.code == 1
    assert 'error: Unknown grant ghost' in capsys.readouterr().err


def test_list_table_and_json(config_file: Callable[..., str],
                             capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    _run(['web-admin', 'grant', 'alice', '--role', 'admin', '-c', conf])
    _run(['web-admin', 'grant', 'bob', '--role', 'viewer', '-c', conf])
    capsys.readouterr()
    _run(['web-admin', 'list', '-c', conf])
    table = capsys.readouterr().out
    assert 'USERNAME' in table
    assert 'alice' in table and 'admin' in table
    assert 'bob' in table and 'viewer' in table
    _run(['web-admin', 'list', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert {(row['username'], row['role']) for row in data} == {
        ('alice', 'admin'), ('bob', 'viewer')
    }
    assert all(row['created'] for row in data)


def test_memory_database_refused(capsys: pytest.CaptureFixture[str]
                                 ) -> None:
    with pytest.raises(SystemExit) as exit_info:
        _run(['web-admin', 'list'])
    assert exit_info.value.code == 1
    assert 'persistent database' in capsys.readouterr().err


def test_env_guard_set_and_restored(config_file: Callable[..., str],
                                    monkeypatch: pytest.MonkeyPatch
                                    ) -> None:
    conf = config_file()
    monkeypatch.delenv(HEADLESS_ENV, raising=False)
    seen: list[str | None] = []

    original_init = core_db.Database.on_start

    def observing_on_start(self: Any) -> None:
        seen.append(os.environ.get(HEADLESS_ENV))
        original_init(self)

    monkeypatch.setattr(core_db.Database, 'on_start', observing_on_start)
    _run(['web-admin', 'grant', 'alice', '-c', conf])
    assert seen == ['1'], 'the guard is active during the headless run'
    assert os.environ.get(HEADLESS_ENV) is None, 'the guard is restored'
    outer = 'custom'
    monkeypatch.setenv(HEADLESS_ENV, outer)
    _run(['web-admin', 'grant', 'bob', '-c', conf])
    assert os.environ.get(HEADLESS_ENV) == outer


def test_adminerror_surfaces_as_exit_1(config_file: Callable[..., str],
                                       monkeypatch: pytest.MonkeyPatch,
                                       capsys: pytest.CaptureFixture[str]
                                       ) -> None:
    from contextlib import contextmanager

    from tiny_pacs_admin_web import cli as web_cli

    @contextmanager
    def boom(config: list[str]) -> Any:
        raise AdminError('configuration exploded')
        yield  # pragma: no cover - generator shape

    monkeypatch.setattr(web_cli, '_grant_context', boom)
    with pytest.raises(SystemExit) as exit_info:
        _run(['web-admin', 'list', '-c', config_file()])
    assert exit_info.value.code == 1
    assert 'configuration exploded' in capsys.readouterr().err


def _assert_rows(conf: str, expected: list[tuple[str, str]]) -> None:
    """Re-opens the database headlessly and checks the grant rows."""
    import trolleybus
    bus = trolleybus.EventBus()
    database = core_db.Database(bus, _database_config(conf))
    bus.start()
    try:
        assert database.db is not None
        WebGrantModel.bind(database.db)
        rows = [(row.username, row.role)
                for row in WebGrantModel.select()]
    finally:
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()
    assert sorted(rows) == sorted(expected)


def _database_config(conf: str) -> dict[str, Any]:
    """Extracts the Database configuration of a CLI test config file."""
    import yaml  # type: ignore[import-untyped]
    with open(conf) as fp:
        data = yaml.safe_load(fp)
    db_conf = dict(data['components']['Database'])
    db_conf.pop('on', None)
    return sqlite_config(db_conf['db_name'])
