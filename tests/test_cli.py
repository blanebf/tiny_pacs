import sys
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from tiny_pacs import __main__ as cli
from tiny_pacs import config, server


def test_parse_args_defaults_to_run() -> None:
    args = cli.parse_args([])
    assert args.command == 'run'
    assert args.config == []
    assert args.aet is None
    assert args.port is None
    assert args.interactive is False


def test_parse_args_legacy_invocation() -> None:
    args = cli.parse_args(['-c', 'conf.yaml', '-a', 'MY_AE', '-p', '4242',
                           '-i'])
    assert args.command == 'run'
    assert args.config == ['conf.yaml']
    assert args.aet == 'MY_AE'
    assert args.port == 4242
    assert args.interactive is True


def test_parse_args_run_command() -> None:
    args = cli.parse_args(['run', '-a', 'MY_AE', '-p', '4242'])
    assert args.command == 'run'
    assert args.aet == 'MY_AE'
    assert args.port == 4242


def test_parse_args_config_command() -> None:
    args = cli.parse_args(['config', '-o', 'conf.yaml', '-i'])
    assert args.command == 'config'
    assert args.output == 'conf.yaml'
    assert args.interactive is True


def test_parse_args_help() -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(['-h'])


def test_parse_args_unknown_command() -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(['frobnicate'])


def test_dump_yaml_roundtrip() -> None:
    conf = config.Config()
    data = yaml.safe_load(config.dump_yaml(conf))
    restored = config.Config()
    restored.update_config(data)
    assert restored == conf


def test_config_command_prints_defaults(
        capsys: pytest.CaptureFixture[str]) -> None:
    cli.config_command(cli.parse_args(['config']))
    data = yaml.safe_load(capsys.readouterr().out)
    default = config.Config()
    assert data['ae'] == default.ae.model_dump()
    assert data['log'] == default.log
    assert set(data['components']) == set(config.DEFAULT_COMPONENTS)


def test_config_command_output_file(tmp_path: Path) -> None:
    out_file = tmp_path / 'tiny_pacs.yaml'
    cli.config_command(cli.parse_args(['config', '-o', str(out_file)]))
    data = yaml.safe_load(out_file.read_text())
    assert data['ae']['ae_title'] == ['TINY_PACS']
    assert data['ae']['port'] == 11112
    assert out_file.stat().st_mode & 0o777 == 0o600


def test_config_command_interactive(tmp_path: Path,
                                    monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = ['MY_AE', '', '4242', '', 'Y', '', '',
              'N', 'N', 'N', 'N', 'N', 'N']
    monkeypatch.setattr('builtins.input', lambda prompt='': inputs.pop(0))
    out_file = tmp_path / 'tiny_pacs.yaml'
    cli.config_command(cli.parse_args(['config', '-i', '-o', str(out_file)]))
    data = yaml.safe_load(out_file.read_text())
    assert data['ae']['ae_title'] == ['MY_AE']
    assert data['ae']['port'] == 4242
    assert data['components']['Database']['on'] is True
    assert out_file.stat().st_mode & 0o777 == 0o600


def test_run_command_interactive_saves_config(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out_file = tmp_path / 'wizard.yaml'
    inputs = ['', '', '', '', '', '',
              'N', 'N', 'N', 'N', 'N', 'N',
              'Y', str(out_file), 'N']
    monkeypatch.setattr('builtins.input', lambda prompt='': inputs.pop(0))
    created: list[config.Config] = []

    class FakeServer:
        def __init__(self, _config: config.Config) -> None:
            created.append(_config)

        def start_with_block(self) -> None:
            pass

    monkeypatch.setattr(server, 'Server', FakeServer)
    cli.run_command(cli.parse_args(['run', '-i']))
    assert created == []
    data = yaml.safe_load(out_file.read_text())
    assert data['ae']['ae_title'] == ['TINY_PACS']
    assert data['ae']['port'] == 11112
    assert out_file.stat().st_mode & 0o777 == 0o600


def test_main_config_command(monkeypatch: pytest.MonkeyPatch,
                             capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(sys, 'argv', ['tiny-pacs', 'config'])
    cli.main()
    data = yaml.safe_load(capsys.readouterr().out)
    assert data['ae']['port'] == 11112


def test_main_legacy_invocation(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[config.Config] = []

    class FakeServer:
        def __init__(self, _config: config.Config) -> None:
            created.append(_config)

        def start_with_block(self) -> None:
            pass

    monkeypatch.setattr(server, 'Server', FakeServer)
    monkeypatch.setattr(sys, 'argv',
                        ['tiny-pacs', '-a', 'MY_AE', '-p', '4242'])
    cli.main()
    assert len(created) == 1
    assert created[0].ae.ae_title == ['MY_AE']
    assert created[0].ae.port == 4242
