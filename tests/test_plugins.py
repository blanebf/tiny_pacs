import logging
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from pynetdicom2 import pdu, userdataitems

from tiny_pacs import __main__ as cli
from tiny_pacs import component, config, devices, server
from tiny_pacs.identity import get_user_identity

COMPONENT_SRC = (
    'from tiny_pacs import component\n'
    '\n'
    '\n'
    'class FakeComponent(component.Component[component.ComponentConfig]):\n'
    '    pass\n'
)

CLI_SRC = (
    'def register(subparsers):\n'
    "    parser = subparsers.add_parser('frobnicate', help='fake command')\n"
    "    parser.add_argument('--flag', action='store_true')\n"
    '    parser.set_defaults(command_handler=handler)\n'
    '\n'
    '\n'
    'def handler(args):\n'
    '    pass\n'
)


@pytest.fixture
def install_plugin(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Callable[..., str]]:
    """Installs fake distributions exposing entry points into the test env.

    Each call creates an importable module and a ``.dist-info`` directory
    in a fresh ``sys.path`` entry, so ``importlib.metadata`` discovery sees
    the entry points exactly like those of a real installed package.

    :yield: installer taking a distribution name, the module source and the
            entry points of both plugin groups; returns the module name
    """
    def install(dist_name: str, module_src: str = '',
                components: dict[str, str] | None = None,
                cli_eps: dict[str, str] | None = None) -> str:
        module_name = f'fake_plugin_{uuid.uuid4().hex[:8]}'
        root = tmp_path / f'{dist_name}-{module_name}'
        root.mkdir()
        (root / f'{module_name}.py').write_text(module_src)
        normalized = dist_name.replace('-', '_')
        dist_info = root / f'{normalized}-1.0.0.dist-info'
        dist_info.mkdir()
        (dist_info / 'METADATA').write_text(
            'Metadata-Version: 2.1\n'
            f'Name: {dist_name}\n'
            'Version: 1.0.0\n'
        )
        lines: list[str] = []
        for group, eps in (('tiny_pacs.components', components),
                           ('tiny_pacs.cli', cli_eps)):
            if not eps:
                continue
            lines.append(f'[{group}]')
            for name, target in eps.items():
                if ':' not in target:
                    target = f'{module_name}:{target}'
                lines.append(f'{name} = {target}')
            lines.append('')
        (dist_info / 'entry_points.txt').write_text('\n'.join(lines))
        monkeypatch.syspath_prepend(str(root))
        return module_name

    installed: list[str] = []

    def tracked_install(*args: Any, **kwargs: Any) -> str:
        module_name = install(*args, **kwargs)
        installed.append(module_name)
        return module_name

    yield tracked_install

    # Modules are unique per install, but never leak them into other tests
    for module_name in installed:
        sys.modules.pop(module_name, None)


@pytest.fixture
def fresh_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Restores the component registry and plugin discovery state."""
    registry = dict(config.COMPONENT_REGISTRY)
    origins = dict(config.COMPONENT_ORIGINS)
    monkeypatch.setattr(config, '_plugins_loaded', False)
    yield
    config.COMPONENT_REGISTRY.clear()
    config.COMPONENT_REGISTRY.update(registry)
    config.COMPONENT_ORIGINS.clear()
    config.COMPONENT_ORIGINS.update(origins)


def test_component_entry_point_discovery(
        install_plugin: Callable[..., str], fresh_registry: None) -> None:
    install_plugin('fake-ext', COMPONENT_SRC,
                   components={'Fake': 'FakeComponent'})
    config.Config()
    assert 'Fake' in config.COMPONENT_REGISTRY
    assert config.get_component_origin('Fake') == 'fake-ext'
    assert config.get_component_origin('Database') == 'built-in'
    assert config.get_component_origin('NeverRegistered') == 'registered'


def test_third_party_component_config_validation(
        install_plugin: Callable[..., str], fresh_registry: None,
        tmp_path: Path) -> None:
    install_plugin('fake-ext', COMPONENT_SRC,
                   components={'Fake': 'FakeComponent'})
    conf_file = tmp_path / 'config.yaml'
    conf_file.write_text(
        'components:\n'
        '  Fake:\n'
        '    on: true\n'
    )
    conf = config.Config()
    conf.update_config(str(conf_file))
    assert isinstance(conf.components['Fake'], component.ComponentConfig)
    assert conf.components['Fake'].on is True
    srv = server.Server(conf)
    types = {type(c) for c in srv.components}
    assert config.COMPONENT_REGISTRY['Fake'] in types


def test_plugin_overrides_builtin(
        install_plugin: Callable[..., str], fresh_registry: None,
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('fake-ext', COMPONENT_SRC,
                   components={'Devices': 'FakeComponent'})
    with caplog.at_level(logging.INFO, logger='tiny_pacs.config'):
        config.Config()
    assert config.COMPONENT_REGISTRY['Devices'] is not devices.Devices
    assert config.get_component_origin('Devices') == 'fake-ext'
    messages = [record.getMessage() for record in caplog.records]
    assert any('built-in' in message for message in messages)


def test_duplicate_component_name_last_wins(
        install_plugin: Callable[..., str], fresh_registry: None,
        caplog: pytest.LogCaptureFixture) -> None:
    first_module = install_plugin('first-ext', COMPONENT_SRC,
                                  components={'Dup': 'FakeComponent'})
    # Prepended later entries come first in sys.path, so the second
    # distribution is discovered first and the first one is loaded last
    install_plugin('second-ext', COMPONENT_SRC,
                   components={'Dup': 'FakeComponent'})
    with caplog.at_level(logging.WARNING, logger='tiny_pacs.config'):
        config.Config()
    winner = getattr(sys.modules[first_module], 'FakeComponent')  # noqa: B009
    assert config.COMPONENT_REGISTRY['Dup'] is winner
    assert config.get_component_origin('Dup') == 'first-ext'
    messages = [record.getMessage() for record in caplog.records]
    assert any('wins' in message for message in messages)


def test_broken_component_entry_point_isolated(
        install_plugin: Callable[..., str], fresh_registry: None,
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('bad-ext', components={'Broken': 'no_such_module:Thing'})
    install_plugin('good-ext', COMPONENT_SRC,
                   components={'Good': 'FakeComponent'})
    with caplog.at_level(logging.ERROR, logger='tiny_pacs.config'):
        config.Config()
    assert 'Good' in config.COMPONENT_REGISTRY
    assert 'Broken' not in config.COMPONENT_REGISTRY
    messages = [record.getMessage() for record in caplog.records]
    assert any('Failed to load component entry point' in message
               for message in messages)


def test_non_component_entry_point_skipped(
        install_plugin: Callable[..., str], fresh_registry: None,
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('odd-ext', 'thing = 42\n',
                   components={'Odd': 'thing'})
    with caplog.at_level(logging.ERROR, logger='tiny_pacs.config'):
        config.Config()
    assert 'Odd' not in config.COMPONENT_REGISTRY
    messages = [record.getMessage() for record in caplog.records]
    assert any('not a Component subclass' in message for message in messages)


def test_plugins_loaded_once(
        install_plugin: Callable[..., str], fresh_registry: None) -> None:
    install_plugin('fake-ext', COMPONENT_SRC,
                   components={'Fake': 'FakeComponent'})
    config.Config()
    assert 'Fake' in config.COMPONENT_REGISTRY
    del config.COMPONENT_REGISTRY['Fake']
    config.Config()
    assert 'Fake' not in config.COMPONENT_REGISTRY


def test_register_component_overrides_plugin(
        install_plugin: Callable[..., str], fresh_registry: None) -> None:
    install_plugin('fake-ext', COMPONENT_SRC,
                   components={'Fake': 'FakeComponent'})
    config.Config()

    class Local(component.Component[component.ComponentConfig]):
        pass

    config.register_component('Fake', Local)
    assert config.COMPONENT_REGISTRY['Fake'] is Local
    assert config.get_component_origin('Fake') == 'registered'


def test_cli_entry_point_registration(
        install_plugin: Callable[..., str]) -> None:
    install_plugin('cli-ext', CLI_SRC, cli_eps={'frobnicate': 'register'})
    parser = cli.build_parser()
    args = parser.parse_args(['frobnicate', '--flag'])
    assert args.command == 'frobnicate'
    assert args.flag is True
    assert callable(args.command_handler)


def test_broken_cli_entry_point_isolated(
        install_plugin: Callable[..., str],
        caplog: pytest.LogCaptureFixture) -> None:
    src = ('def register(subparsers):\n'
           "    raise RuntimeError('boom')\n")
    install_plugin('bad-cli', src, cli_eps={'broken': 'register'})
    with caplog.at_level(logging.ERROR, logger='tiny_pacs.cli'):
        parser = cli.build_parser()
    args = parser.parse_args(['run'])
    assert args.command == 'run'
    messages = [record.getMessage() for record in caplog.records]
    assert any('Failed to register CLI entry point' in message
               for message in messages)


def test_reserved_cli_names_rejected(
        install_plugin: Callable[..., str],
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('sneaky', CLI_SRC,
                   cli_eps={'run': 'register', 'config': 'register',
                            'help': 'register'})
    with caplog.at_level(logging.WARNING, logger='tiny_pacs.cli'):
        parser = cli.build_parser()
    args = parser.parse_args(['config'])
    assert args.command == 'config'
    assert not hasattr(args, 'command_handler')
    messages = [record.getMessage() for record in caplog.records]
    reserved = [message for message in messages
                if 'reserved subcommand name' in message]
    assert len(reserved) == 3


def test_main_dispatches_to_plugin_handler(
        install_plugin: Callable[..., str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    src = ('calls = []\n'
           '\n'
           '\n'
           'def handler(args):\n'
           '    calls.append(args)\n'
           '\n'
           '\n'
           'def register(subparsers):\n'
           "    parser = subparsers.add_parser('frobnicate')\n"
           '    parser.set_defaults(command_handler=handler)\n')
    module_name = install_plugin('cli-ext', src,
                                 cli_eps={'frobnicate': 'register'})
    monkeypatch.setattr(sys, 'argv', ['tiny-pacs', 'frobnicate'])
    cli.main()
    calls = getattr(sys.modules[module_name], 'calls')  # noqa: B009
    assert len(calls) == 1
    assert calls[0].command == 'frobnicate'


HIJACK_SRC = (
    'def register(subparsers):\n'
    "    devices = subparsers.add_parser('devices', help='ok')\n"
    '    devices.set_defaults(command_handler=handler)\n'
    "    subparsers.add_parser('run')\n"
    "    subparsers.add_parser('config')\n"
    '\n'
    '\n'
    'def handler(args):\n'
    '    pass\n'
)


def test_cli_plugin_cannot_register_reserved_names(
        install_plugin: Callable[..., str],
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('sneaky', HIJACK_SRC, cli_eps={'sneaky': 'register'})
    with caplog.at_level(logging.WARNING, logger='tiny_pacs.cli'):
        parser = cli.build_parser()
    args = parser.parse_args(['devices'])
    assert args.command == 'devices'
    assert callable(args.command_handler)
    args = parser.parse_args(['run', '--aet', 'MY_AE'])
    assert args.command == 'run'
    assert args.aet == 'MY_AE'
    assert not hasattr(args, 'command_handler')
    args = parser.parse_args(['config', '-o', 'x.yaml'])
    assert args.command == 'config'
    assert args.output == 'x.yaml'
    messages = [record.getMessage() for record in caplog.records]
    ignored = [message for message in messages
               if 'reserved' in message and 'ignoring' in message]
    assert len(ignored) == 2


def _shared_src(marker: str) -> str:
    return (f'marker = {marker!r}\n'
            '\n'
            '\n'
            'def handler(args):\n'
            '    pass\n'
            '\n'
            '\n'
            'def register(subparsers):\n'
            "    parser = subparsers.add_parser('shared')\n"
            '    parser.set_defaults(command_handler=handler,'
            ' marker=marker)\n')


def test_cli_duplicate_subcommand_names_warned(
        install_plugin: Callable[..., str],
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('first-cli', _shared_src('first'),
                   cli_eps={'first': 'register'})
    # Prepended sys.path entries are discovered first, so the second
    # distribution registers the subcommand and the first one is rejected
    install_plugin('second-cli', _shared_src('second'),
                   cli_eps={'second': 'register'})
    with caplog.at_level(logging.WARNING, logger='tiny_pacs.cli'):
        parser = cli.build_parser()
    args = parser.parse_args(['shared'])
    assert args.marker == 'second'
    messages = [record.getMessage() for record in caplog.records]
    duplicates = [message for message in messages
                  if "'shared'" in message and 'both' in message]
    assert len(duplicates) == 1
    assert 'first' in duplicates[0]
    assert 'second' in duplicates[0]


def test_broken_cli_registration_leaves_no_partial_subcommands(
        install_plugin: Callable[..., str]) -> None:
    src = ('def register(subparsers):\n'
           "    subparsers.add_parser('harmless')\n"
           "    raise RuntimeError('boom')\n")
    install_plugin('bad-cli', src, cli_eps={'broken': 'register'})
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['harmless'])


def test_systemexit_in_cli_register_isolated(
        install_plugin: Callable[..., str]) -> None:
    src = ('def register(subparsers):\n'
           '    raise SystemExit(1)\n')
    install_plugin('exiting-cli', src, cli_eps={'exiting': 'register'})
    parser = cli.build_parser()
    args = parser.parse_args(['run'])
    assert args.command == 'run'


def test_main_errors_on_missing_command_handler(
        install_plugin: Callable[..., str],
        monkeypatch: pytest.MonkeyPatch) -> None:
    src = ('def register(subparsers):\n'
           "    subparsers.add_parser('frobnicate')\n")
    install_plugin('cli-ext', src, cli_eps={'frobnicate': 'register'})
    monkeypatch.setattr(sys, 'argv', ['tiny-pacs', 'frobnicate'])
    with pytest.raises(SystemExit):
        cli.main()


def test_builtin_commands_skip_plugin_loading(
        install_plugin: Callable[..., str],
        caplog: pytest.LogCaptureFixture) -> None:
    src = ('def register(subparsers):\n'
           "    raise RuntimeError('should never run')\n")
    install_plugin('bad-cli', src, cli_eps={'broken': 'register'})
    with caplog.at_level(logging.ERROR, logger='tiny_pacs.cli'):
        assert cli.parse_args(['config']).command == 'config'
        assert cli.parse_args(['run']).command == 'run'
        assert cli.parse_args([]).command == 'run'
        assert cli.parse_args(['-c', 'x.yaml']).command == 'run'
        assert not any('Failed to register CLI entry point'
                       in record.getMessage()
                       for record in caplog.records)
        # A non-built-in command still triggers plugin discovery
        with pytest.raises(SystemExit):
            cli.parse_args(['broken'])
        assert any('Failed to register CLI entry point'
                   in record.getMessage()
                   for record in caplog.records)


def test_programmatic_registration_wins_over_entry_point(
        install_plugin: Callable[..., str], fresh_registry: None) -> None:
    class Local(component.Component[component.ComponentConfig]):
        pass

    config.register_component('Fake', Local)
    install_plugin('fake-ext', COMPONENT_SRC,
                   components={'Fake': 'FakeComponent'})
    config.Config()
    assert config.COMPONENT_REGISTRY['Fake'] is Local
    assert config.get_component_origin('Fake') == 'registered'


def test_systemexit_in_component_entry_point_isolated(
        install_plugin: Callable[..., str], fresh_registry: None,
        caplog: pytest.LogCaptureFixture) -> None:
    install_plugin('exiting-ext', 'import sys\nsys.exit(3)\n',
                   components={'Exiting': 'Anything'})
    install_plugin('good-ext', COMPONENT_SRC,
                   components={'Good': 'FakeComponent'})
    with caplog.at_level(logging.ERROR, logger='tiny_pacs.config'):
        config.Config()
    assert 'Good' in config.COMPONENT_REGISTRY
    assert 'Exiting' not in config.COMPONENT_REGISTRY
    messages = [record.getMessage() for record in caplog.records]
    assert any('Failed to load component entry point' in message
               for message in messages)


def _make_rq(*user_data: Any) -> pdu.AAssociateRqPDU:
    items: list[Any] = []
    if user_data:
        items.append(pdu.UserInformationItem(user_data=list(user_data)))
    return pdu.AAssociateRqPDU(
        called_ae_title='TINY_PACS', calling_ae_title='CLIENT_AET',
        variable_items=items
    )


def test_get_user_identity_found() -> None:
    identity_item = userdataitems.UserIdentityNegotiationSubItem(
        'alice', 'secret', user_identity_type=2, positive_response_req=1
    )
    rq = _make_rq(userdataitems.MaximumLengthSubItem(65536), identity_item)
    assert get_user_identity(rq) is identity_item


def test_get_user_identity_absent() -> None:
    rq = _make_rq(userdataitems.MaximumLengthSubItem(65536))
    assert get_user_identity(rq) is None


def test_get_user_identity_no_user_info() -> None:
    assert get_user_identity(_make_rq()) is None


def test_get_user_identity_on_decoded_pdu() -> None:
    identity_item = userdataitems.UserIdentityNegotiationSubItem(
        'bob', 'pw', user_identity_type=2, positive_response_req=1
    )
    decoded = pdu.AAssociateRqPDU.decode(_make_rq(identity_item).encode())
    assert isinstance(decoded, pdu.AAssociateRqPDU)
    found = get_user_identity(decoded)
    assert found is not None
    assert found.primary_field == 'bob'
    assert found.user_identity_type == 2
    assert found.positive_response_req == 1
