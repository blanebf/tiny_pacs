"""Tests of the CLI subcommands against temporary configs and databases."""
import json
from collections.abc import Callable
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]
from tiny_pacs import __main__ as cli


def _run(argv: list[str]) -> Any:
    """Parses and executes a ``tiny-pacs`` subcommand."""
    args = cli.parse_args(argv)
    handler = getattr(args, 'command_handler', None)
    if handler is not None:
        return handler(args)
    if args.command == 'config':
        return cli.config_command(args)
    raise AssertionError(f'no command handler for {argv!r}')


def test_devices_add_and_list_table(
        config_file: Callable[..., str], capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    _run(['devices', 'add', 'MRI', '--address', '10.0.0.20',
          '--port', '104', '--identity', 'password', '-c', conf])
    out = capsys.readouterr().out
    assert 'Added device MRI' in out

    _run(['devices', 'list', '-c', conf])
    out = capsys.readouterr().out
    assert 'MRI' in out
    assert '10.0.0.20' in out
    assert 'password' in out


def test_devices_add_defaults(
        config_file: Callable[..., str], capsys: pytest.CaptureFixture[str]
) -> None:
    extra = (
        '  DeviceStore:\n'
        '    on: true\n'
        '    default_identity: username\n'
        '    default_port: 12345\n'
    )
    conf = config_file(extra=extra)
    _run(['devices', 'add', 'CT', '--address', '10.0.0.21', '-c', conf])
    capsys.readouterr()
    _run(['devices', 'list', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1
    # Port and identity policy come from the DeviceStore configuration
    assert data[0]['port'] == 12345
    assert data[0]['identity'] == 'username'


def test_devices_list_json_and_yaml(
        config_file: Callable[..., str], capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    _run(['devices', 'add', 'US', '--address', '10.0.0.22', '-c', conf])
    capsys.readouterr()

    _run(['devices', 'list', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert data[0]['aet'] == 'US'

    _run(['devices', 'list', '--format', 'yaml', '-c', conf])
    parsed = yaml.safe_load(capsys.readouterr().out)
    assert parsed[0]['aet'] == 'US'


def test_devices_update_and_remove(
        config_file: Callable[..., str], capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    _run(['devices', 'add', 'PET', '--address', '10.0.0.23', '-c', conf])
    capsys.readouterr()

    _run(['devices', 'update', 'PET', '--port', '9999',
          '--identity', 'password', '-c', conf])
    capsys.readouterr()
    _run(['devices', 'list', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert data[0]['port'] == 9999
    assert data[0]['identity'] == 'password'

    _run(['devices', 'remove', 'PET', '-c', conf])
    out = capsys.readouterr().out
    assert 'Removed device PET' in out
    _run(['devices', 'list', '--format', 'json', '-c', conf])
    assert json.loads(capsys.readouterr().out) == []


def test_devices_add_duplicate_fails(config_file: Callable[..., str]) -> None:
    conf = config_file()
    _run(['devices', 'add', 'DUP', '--address', '10.0.0.24', '-c', conf])
    with pytest.raises(SystemExit):
        _run(['devices', 'add', 'DUP', '--address', '10.0.0.25',
              '-c', conf])


def test_devices_remove_unknown_fails(config_file: Callable[..., str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit):
        _run(['devices', 'remove', 'GHOST', '-c', conf])


def test_devices_memory_mode_fails(tmp_path: Any,
                                   capsys: pytest.CaptureFixture[str]
                                   ) -> None:
    conf = tmp_path / 'memory.yaml'
    conf.write_text(
        'components:\n'
        '  Database:\n'
        '    on: true\n'
    )
    with pytest.raises(SystemExit):
        _run(['devices', 'add', 'X', '--address', 'h', '-c', str(conf)])
    err = capsys.readouterr().err
    assert 'persistent database' in err


def test_components_list_origin(
        capsys: pytest.CaptureFixture[str]) -> None:
    _run(['components', 'list'])
    out = capsys.readouterr().out
    assert 'NAME' in out
    assert 'ORIGIN' in out
    # Built-in components are attributed to the core
    assert 'built-in' in out
    # The DeviceStore component comes from this extension distribution
    assert 'DeviceStore' in out
    assert 'tiny_pacs_admin' in out


def test_components_list_state(
        config_file: Callable[..., str], capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file(extra=(
        '  DeviceStore:\n'
        '    on: true\n'
    ))
    _run(['components', 'list', '-c', conf])
    out = capsys.readouterr().out
    assert 'enabled' in out


def test_db_info(config_file: Callable[..., str],
                 capsys: pytest.CaptureFixture[str]) -> None:
    conf = config_file()
    _run(['devices', 'add', 'INFO', '--address', '10.0.0.26', '-c', conf])
    capsys.readouterr()

    _run(['db', 'info', '-c', conf])
    out = capsys.readouterr().out
    assert 'Schema versions' in out
    assert 'DeviceStore' in out
    assert 'Row counts' in out
    assert 'devicemodel' in out


def test_config_show(capsys: pytest.CaptureFixture[str]) -> None:
    _run(['config', 'show'])
    data = yaml.safe_load(capsys.readouterr().out)
    assert data['ae']['port'] == 11112
    assert 'Database' in data['components']


def test_devices_echo_live(
        config_file: Callable[..., str], pacs_port: int,
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    _run(['devices', 'add', 'TINY_PACS', '--address', '127.0.0.1',
          '--port', str(pacs_port), '-c', conf])
    capsys.readouterr()

    _run(['devices', 'echo', 'TINY_PACS', '-c', conf])
    out = capsys.readouterr().out
    assert 'C-ECHO to TINY_PACS succeeded' in out


def test_devices_echo_unknown_fails(config_file: Callable[..., str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit):
        _run(['devices', 'echo', 'GHOST', '-c', conf])
