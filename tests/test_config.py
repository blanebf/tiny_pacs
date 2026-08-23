import pathlib

import pydantic
import pytest
import trolleybus

from tiny_pacs import component, config, db, devices, pacs, server, storage


def test_yaml_unquoted_on_key(tmp_path: pathlib.Path) -> None:
    # PyYAML (YAML 1.1) parses the bare ``on`` key as boolean ``True``,
    # producing ``{True: True}`` — components must still be enabled.
    conf_file = tmp_path / 'config.yaml'
    conf_file.write_text(
        'components:\n'
        '  Database:\n'
        '    on: true\n'
        '  InMemoryStorage:\n'
        '    on: true\n'
        '  FileStorage:\n'
        '    on: false\n'
    )
    conf = config.Config()
    conf.update_config(str(conf_file))
    srv = server.Server(conf)
    types = {type(c) for c in srv.components}
    assert db.Database in types
    assert storage.InMemoryStorage in types
    assert storage.FileStorage not in types


def test_components_provide_their_config_models() -> None:
    # Every registered component declares its own pydantic config model for
    # the loader to validate against.
    for factory in config.COMPONENT_REGISTRY.values():
        assert issubclass(factory.config_model, component.ComponentConfig)
    assert db.Database.config_model is db.DatabaseConfig
    assert devices.Devices.config_model is devices.DevicesConfig
    assert pacs.PACS.config_model is pacs.PACSConfig
    assert storage.FileStorage.config_model is storage.FileStorageConfig


def test_default_config_validates_and_typing() -> None:
    conf = config.Config()
    assert isinstance(conf.ae, config.AEConfig)
    # Defaults are seeded with the default component set, all enabled
    assert isinstance(conf.components['Database'], db.DatabaseConfig)
    assert isinstance(conf.components['Devices'], devices.DevicesConfig)
    assert isinstance(conf.components['PACS'], pacs.PACSConfig)
    assert isinstance(conf.components['InMemoryStorage'],
                      component.ComponentConfig)
    assert all(c.on for c in conf.components.values())


def test_yaml_off_key_disables_component(tmp_path: pathlib.Path) -> None:
    # PyYAML (YAML 1.1) parses a bare ``off`` key as boolean ``False``,
    # producing ``{False: ...}`` — it must negate the ``on`` flag instead of
    # failing validation.
    conf_file = tmp_path / 'config.yaml'
    conf_file.write_text(
        'components:\n'
        '  Database:\n'
        '    off: true\n'
        '  InMemoryStorage:\n'
        '    off: false\n'
    )
    conf = config.Config()
    conf.update_config(str(conf_file))
    assert conf.components['Database'].on is False
    assert conf.components['InMemoryStorage'].on is True
    srv = server.Server(conf)
    types = {type(c) for c in srv.components}
    assert db.Database not in types
    assert storage.InMemoryStorage in types


def test_entry_without_on_is_disabled() -> None:
    # Components are skipped unless ``on`` is true: configuring a component
    # without an explicit ``on: true`` must not enable it.
    conf = config.Config()
    conf.update_config(
        {'components': {'FileStorage': {'storage_dir': '/tmp'}}})
    assert conf.components['FileStorage'].on is False
    assert conf.components['Database'].on is True  # untouched default


def test_component_entry_replaced_wholesale() -> None:
    # A later configuration source replaces the whole component entry; fields
    # it omits fall back to the model defaults instead of resurrecting values
    # set by an earlier source.
    conf = config.Config()
    conf.update_config({
        'components': {
            'Database': {
                'on': True, 'driver': 'postgres',
                'host': 'prod-db', 'db_name': 'prod'
            }
        }
    })
    conf.update_config(
        {'components': {'Database': {'on': True, 'driver': 'sqlite'}}})
    db_conf = conf.components['Database']
    assert isinstance(db_conf, db.DatabaseConfig)
    assert db_conf.driver is db.DBDrivers.SQLITE
    assert db_conf.host == 'localhost'
    assert db_conf.db_name is None


def test_component_config_validated_into_typed_model() -> None:
    conf = config.Config()
    conf.update_config({
        'components': {
            'Database': {'on': True, 'driver': 'sqlite',
                         'db_name': ':memory:'},
            'Devices': {'on': True, 'default_port': 11113,
                        'devices': {'WS': {'aet': 'WS', 'address': '127.0.0.1',
                                           'port': 11113}}}
        }
    })
    db_conf = conf.components['Database']
    assert isinstance(db_conf, db.DatabaseConfig)
    assert db_conf.driver is db.DBDrivers.SQLITE
    assert db_conf.db_name == ':memory:'

    devices_conf = conf.components['Devices']
    assert isinstance(devices_conf, devices.DevicesConfig)
    assert devices_conf.default_port == 11113
    assert isinstance(devices_conf.devices['WS'], devices.DeviceConfig)
    assert devices_conf.devices['WS'].port == 11113


def test_invalid_component_value_rejected() -> None:
    conf = config.Config()
    with pytest.raises(pydantic.ValidationError):
        conf.update_config({'components': {'Database': {'driver': 'mysql'}}})


def test_extra_component_field_rejected() -> None:
    conf = config.Config()
    with pytest.raises(pydantic.ValidationError):
        conf.update_config({'components': {'Database': {'unknown_field': 1}}})


def test_invalid_ae_value_rejected() -> None:
    conf = config.Config()
    with pytest.raises(pydantic.ValidationError):
        conf.update_config({'ae': {'port': 'not-a-port'}})


def test_unknown_component_skipped() -> None:
    conf = config.Config()
    # Unknown components are ignored rather than failing validation
    conf.update_config({'components': {'Nope': {'on': True}}})
    assert 'Nope' not in conf.components


def test_component_receives_validated_model() -> None:
    bus = trolleybus.EventBus()
    # Passing a plain dict is validated into the component's own model
    database = db.Database(bus, {'driver': 'sqlite', 'db_name': ':memory:'})
    assert isinstance(database.config, db.DatabaseConfig)
    assert database.config.db_name == ':memory:'


def test_register_component() -> None:
    class _ExtraConfig(component.ComponentConfig):
        option: str = 'default'

    class _ExtraComponent(component.Component[_ExtraConfig]):
        config_model = _ExtraConfig

    config.register_component('_ExtraComponent', _ExtraComponent)
    try:
        conf = config.Config()
        conf.update_config(
            {'components': {'_ExtraComponent': {'option': 'y'}}})
        extra = conf.components['_ExtraComponent']
        assert isinstance(extra, _ExtraConfig)
        assert extra.option == 'y'
    finally:
        del config.COMPONENT_REGISTRY['_ExtraComponent']
