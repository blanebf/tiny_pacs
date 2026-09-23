import argparse
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
    assert args.action == 'generate'
    assert args.config == []
    assert args.output == 'conf.yaml'
    assert args.interactive is True


def test_parse_args_config_show() -> None:
    args = cli.parse_args(['config', 'show', '-c', 'conf.yaml'])
    assert args.command == 'config'
    assert args.action == 'show'
    assert args.config == ['conf.yaml']


def test_parse_args_help() -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(['-h'])


def test_parse_args_unknown_command() -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(['frobnicate'])


def test_fail_reports_error_and_exits(
        capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.fail('boom')
    assert excinfo.value.code == 1
    assert 'error: boom' in capsys.readouterr().err


def test_format_table_alignment() -> None:
    table = cli.format_table(
        ('NAME', 'ORIGIN'), [('Database', 'built-in'), ('x', 'tiny_pacs')]
    )
    assert table.splitlines() == [
        'NAME      ORIGIN',
        '--------  ---------',
        'Database  built-in',
        'x         tiny_pacs',
    ]


def test_add_action_parser_registers_handler() -> None:
    parser = argparse.ArgumentParser()
    actions = parser.add_subparsers(dest='action')

    def handler(_args: argparse.Namespace) -> None:
        pass

    action = cli.add_action_parser(actions, 'list', 'list things', handler)
    args = parser.parse_args(['list', '-c', 'conf.yaml'])
    assert args.command_handler is handler
    assert args.config == ['conf.yaml']
    assert action.prog.endswith('list')


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
    # Generated configurations contain the built-in defaults plus every
    # available extension component, disabled by default
    extensions = config.extension_component_defaults()
    assert set(data['components']) == (set(config.DEFAULT_COMPONENTS)
                                       | set(extensions))
    for name in extensions:
        assert data['components'][name]['on'] is False


def test_config_command_output_file(tmp_path: Path) -> None:
    out_file = tmp_path / 'tiny_pacs.yaml'
    cli.config_command(cli.parse_args(['config', '-o', str(out_file)]))
    data = yaml.safe_load(out_file.read_text())
    assert data['ae']['ae_title'] == ['TINY_PACS']
    assert data['ae']['port'] == 11112
    assert out_file.stat().st_mode & 0o777 == 0o600


def test_config_command_interactive(tmp_path: Path,
                                    monkeypatch: pytest.MonkeyPatch) -> None:
    # The wizard asks "use component?" for every registered component, so
    # the number of declines depends on the installed extensions.
    config.load_component_plugins()
    n_components = len(config.COMPONENT_REGISTRY)
    inputs = (['MY_AE', '', '4242', '', 'Y', '', '']
              + ['N'] * n_components)
    monkeypatch.setattr('builtins.input', lambda prompt='': inputs.pop(0))
    out_file = tmp_path / 'tiny_pacs.yaml'
    cli.config_command(cli.parse_args(['config', '-i', '-o', str(out_file)]))
    data = yaml.safe_load(out_file.read_text())
    assert data['ae']['ae_title'] == ['MY_AE']
    assert data['ae']['port'] == 4242
    assert data['components']['Database']['on'] is True
    assert out_file.stat().st_mode & 0o777 == 0o600


def test_parse_args_config_launcher() -> None:
    assert cli.parse_args(['config']).launcher is False
    args = cli.parse_args(['config', '-o', 'conf.yaml', '--launcher'])
    assert args.launcher is True


def test_config_command_launcher_writes_scripts(tmp_path: Path) -> None:
    out_file = tmp_path / 'my.yaml'
    cli.config_command(cli.parse_args(
        ['config', '-o', str(out_file), '--launcher']))
    sh_file = tmp_path / 'cli.sh'
    cmd_file = tmp_path / 'cli.cmd'
    assert sh_file.is_file()
    assert cmd_file.is_file()
    # Both scripts reference the generated configuration by name
    assert '-c my.yaml' in sh_file.read_text()
    assert 'set "TINY_PACS_CONFIG=my.yaml"' in cmd_file.read_text()


def test_config_command_launcher_without_output(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    cli.config_command(cli.parse_args(['config', '--launcher']))
    captured = capsys.readouterr()
    # The configuration itself still goes to stdout, unpolluted by the
    # launcher note (which is printed to stderr), so stdout stays
    # pipeable into the configuration file
    yaml.safe_load(captured.out)
    assert 'Launcher scripts saved to' in captured.err
    assert (tmp_path / 'cli.cmd').is_file()
    assert '-c config.yaml' in (tmp_path / 'cli.sh').read_text()


def test_config_show_launcher_refused(
        capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.config_command(cli.parse_args(['config', 'show', '--launcher']))
    assert excinfo.value.code == 1
    assert 'error:' in capsys.readouterr().err


def test_config_command_launcher_rejects_unsafe_output_name(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out_file = tmp_path / 'evil"&calc&.yaml'
    with pytest.raises(SystemExit) as excinfo:
        cli.config_command(cli.parse_args(
            ['config', '-o', str(out_file), '--launcher']))
    assert excinfo.value.code == 1
    assert 'error:' in capsys.readouterr().err
    # Validated before anything is written: no partial folder
    assert not out_file.exists()
    assert not (tmp_path / 'cli.sh').exists()
    assert not (tmp_path / 'cli.cmd').exists()


def test_run_command_interactive_saves_config(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out_file = tmp_path / 'wizard.yaml'
    # See test_config_command_interactive: decline every registered
    # component, however many extensions contribute.
    config.load_component_plugins()
    n_components = len(config.COMPONENT_REGISTRY)
    inputs = (['', '', '', '', '', ''] + ['N'] * n_components
              + ['Y', str(out_file), 'N'])
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


def test_config_show_dumps_effective_config(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    conf_file = tmp_path / 'override.yaml'
    conf_file.write_text('ae:\n  port: 4242\n')
    cli.config_command(cli.parse_args(['config', 'show',
                                       '-c', str(conf_file)]))
    data = yaml.safe_load(capsys.readouterr().out)
    # The override is merged into the defaults
    assert data['ae']['port'] == 4242
    assert data['ae']['ae_title'] == ['TINY_PACS']
    assert set(data['components']) == set(config.DEFAULT_COMPONENTS)


def test_config_show_output_file(tmp_path: Path) -> None:
    conf_file = tmp_path / 'override.yaml'
    conf_file.write_text('ae:\n  port: 4242\n')
    out_file = tmp_path / 'effective.yaml'
    cli.config_command(cli.parse_args([
        'config', 'show', '-c', str(conf_file), '-o', str(out_file)]))
    data = yaml.safe_load(out_file.read_text())
    assert data['ae']['port'] == 4242
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
