"""Tests of the DeviceStore component."""
import json
from types import SimpleNamespace
from typing import Any

import pydantic
import pytest
from pynetdicom2 import pdu
from tiny_pacs import db as core_db
from tiny_pacs import devices as core_devices
from tiny_pacs import events as core_events

from tiny_pacs_admin import events as admin_events
from tiny_pacs_admin.models import DeviceModel, IdentityPolicy
from tiny_pacs_admin.store import DeviceStoreConfig


def _version(name: str) -> int | None:
    row = core_db.SchemaVersion.get_or_none(
        core_db.SchemaVersion.component == name
    )
    return row.version if row is not None else None


def _assoc_payload(calling_aet: str) -> core_events.AssocPayload:
    """Builds an association payload without a live network connection."""
    assoc = pdu.AAssociateRqPDU(
        called_ae_title='TINY_PACS', calling_ae_title=calling_aet,
        variable_items=[]
    )
    # No DUL socket: components must fall back to an empty peer address
    asce = SimpleNamespace(dul=SimpleNamespace(dul_socket=None))
    return core_events.AssocPayload(asce, assoc)  # type: ignore[arg-type]


def test_baseline_migration_fresh_db(admin_bus: Any) -> None:
    admin_bus()
    assert _version('DeviceStore') == 1


def test_migration_restart_is_noop(admin_bus: Any) -> None:
    _, _, database, path = admin_bus()
    database.db.close()
    admin_bus(db_path=path)
    # The restart applies no migrations and keeps the recorded version
    assert _version('DeviceStore') == 1


def test_startup_sync_imports_configured_devices(admin_bus: Any) -> None:
    devices_conf = {'devices': {
        'WITH_POLICY': {'aet': 'WITH_POLICY', 'address': '10.0.0.5',
                        'port': 104, 'identity': 'password'},
        'PLAIN': {'aet': 'PLAIN', 'address': '10.0.0.6', 'port': 105}
    }}
    admin_bus(devices_conf=devices_conf)
    rows = {row.aet: row for row in DeviceModel.select()}
    assert set(rows) == {'WITH_POLICY', 'PLAIN'}
    assert rows['WITH_POLICY'].identity == IdentityPolicy.PASSWORD.value
    assert rows['WITH_POLICY'].address == '10.0.0.5'
    assert rows['WITH_POLICY'].port == 104
    # Devices without an explicit policy get the DB default
    assert rows['PLAIN'].identity == IdentityPolicy.NONE.value


def test_startup_sync_upserts_on_restart(admin_bus: Any) -> None:
    conf_v1 = {'devices': {
        'MODALITY': {'aet': 'MODALITY', 'address': '10.0.0.7',
                     'port': 111, 'identity': 'username'}
    }}
    _, _, database, path = admin_bus(devices_conf=conf_v1)
    database.db.close()

    # The configuration stays the source of truth: changed address/port
    # are followed, but a policy omitted by the configuration is preserved
    conf_v2 = {'devices': {
        'MODALITY': {'aet': 'MODALITY', 'address': '10.0.0.8',
                     'port': 112}
    }}
    admin_bus(devices_conf=conf_v2, db_path=path)
    row = DeviceModel.get(DeviceModel.aet == 'MODALITY')
    assert row.address == '10.0.0.8'
    assert row.port == 112
    assert row.identity == IdentityPolicy.USERNAME.value


def test_policy_roundtrip_yaml_db_device_by_ae(admin_bus: Any) -> None:
    devices_conf = {'devices': {
        'SECURE': {'aet': 'SECURE', 'address': '10.0.0.9',
                   'port': 113, 'identity': 'password'}
    }}
    bus, _, _, _ = admin_bus(devices_conf=devices_conf)
    device = bus.send_any(core_events.DeviceByAE, 'SECURE')
    assert device is not None
    assert device.aet == 'SECURE'
    assert device.address == '10.0.0.9'
    # The identity policy rides on the DeviceConfig as an extra field
    assert device.model_extra['identity'] == 'password'


def test_device_by_ae_db_wins_over_devices(admin_bus: Any) -> None:
    # No configured devices: the DB device is added first, then an
    # in-memory registry advertising the same AE title with different
    # settings joins at the default (lower) priority.
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd, {
        'aet': 'SHARED', 'address': '10.0.0.2', 'port': 2
    })
    core_devices.Devices(bus, {'devices': {
        'SHARED': {'aet': 'SHARED', 'address': '10.0.0.1', 'port': 1}
    }})
    device = bus.send_any(core_events.DeviceByAE, 'SHARED')
    assert device is not None
    # The DB device wins over the in-memory one
    assert device.address == '10.0.0.2'
    assert device.port == 2


def test_device_by_ae_falls_back_to_devices(admin_bus: Any) -> None:
    devices_conf = {'devices': {
        'YAML_ONLY': {'aet': 'YAML_ONLY', 'address': '10.0.0.3',
                      'port': 3}
    }}
    bus, _, _, _ = admin_bus(devices_conf=devices_conf)
    device = bus.send_any(core_events.DeviceByAE, 'YAML_ONLY')
    assert device is not None
    assert device.address == '10.0.0.3'
    assert bus.send_any(core_events.DeviceByAE, 'UNKNOWN') is None


def test_auto_add_persists_defaults(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus(
        store_conf={'default_port': 7777, 'default_identity': 'username'}
    )
    bus.broadcast(core_events.Assoc, _assoc_payload('  NEW_DEV '))
    row = DeviceModel.get(DeviceModel.aet == 'NEW_DEV')
    assert row.port == 7777
    assert row.identity == IdentityPolicy.USERNAME.value


def test_auto_add_keeps_known_device(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd, {
        'aet': 'KNOWN', 'address': '10.0.0.4', 'port': 444,
        'identity': 'password'
    })
    bus.broadcast(core_events.Assoc, _assoc_payload('KNOWN'))
    row = DeviceModel.get(DeviceModel.aet == 'KNOWN')
    # The existing record is untouched by the auto-add
    assert row.port == 444
    assert row.identity == IdentityPolicy.PASSWORD.value
    assert DeviceModel.select().count() == 1


def test_auto_add_tandem_with_devices(admin_bus: Any) -> None:
    # Both components auto-add the same unknown device: the DB record is
    # created with the DeviceStore defaults and wins the device lookup
    devices_conf = {'auto_add': True, 'default_port': 1, 'devices': {}}
    bus, _, _, _ = admin_bus(
        devices_conf=devices_conf,
        store_conf={'default_port': 2}
    )
    bus.broadcast(core_events.Assoc, _assoc_payload('TANDEM'))
    row = DeviceModel.get(DeviceModel.aet == 'TANDEM')
    assert row.port == 2
    device = bus.send_any(core_events.DeviceByAE, 'TANDEM')
    assert device is not None
    assert device.port == 2


def test_auto_add_runs_before_default_priority(admin_bus: Any) -> None:
    bus, store, _, _ = admin_bus()
    order: list[str] = []

    def probe(_: core_events.AssocPayload) -> None:
        order.append('probe')

    bus.subscribe(core_events.Assoc, probe)
    original = store.on_assoc

    def tracked(payload: core_events.AssocPayload) -> None:
        order.append('store')
        original(payload)

    bus.unsubscribe(core_events.Assoc, store.on_assoc)
    bus.subscribe(core_events.Assoc, tracked, store.priority)
    bus.broadcast(core_events.Assoc, _assoc_payload('RACER'))
    assert order == ['store', 'probe']


def test_device_list_event(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd,
                 {'aet': 'B_DEV', 'address': 'h2', 'port': 2})
    bus.send_one(admin_events.DeviceAdd,
                 {'aet': 'A_DEV', 'address': 'h1', 'port': 1})
    rows = bus.send_one(admin_events.DeviceList, None)
    assert [row.aet for row in rows] == ['A_DEV', 'B_DEV']


def test_device_add_applies_config_defaults(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus(
        store_conf={'default_port': 5555, 'default_identity': 'password'}
    )
    row = bus.send_one(admin_events.DeviceAdd,
                       {'aet': 'DEFAULTS', 'address': '10.0.0.10'})
    assert row.port == 5555
    assert row.identity == IdentityPolicy.PASSWORD.value


def test_device_add_requires_fields(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    with pytest.raises(ValueError, match='aet is required'):
        bus.send_one(admin_events.DeviceAdd, {'address': '10.0.0.1'})
    with pytest.raises(ValueError, match='address is required'):
        bus.send_one(admin_events.DeviceAdd, {'aet': 'NO_ADDR'})


def test_device_add_rejects_duplicates(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd,
                 {'aet': 'DUP', 'address': '10.0.0.1'})
    with pytest.raises(ValueError, match='already exists'):
        bus.send_one(admin_events.DeviceAdd,
                     {'aet': 'DUP', 'address': '10.0.0.2'})


def test_device_add_validates_values(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    with pytest.raises(ValueError, match='not a valid IdentityPolicy'):
        bus.send_one(admin_events.DeviceAdd,
                     {'aet': 'X', 'address': 'h', 'identity': 'kerberos'})
    with pytest.raises(ValueError, match='port must be an integer'):
        bus.send_one(admin_events.DeviceAdd,
                     {'aet': 'X', 'address': 'h', 'port': 'high'})
    with pytest.raises(ValueError, match='port must be within'):
        bus.send_one(admin_events.DeviceAdd,
                     {'aet': 'X', 'address': 'h', 'port': 70000})


def test_device_update_partial(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd, {
        'aet': 'UPD', 'address': '10.0.0.11', 'port': 111,
        'identity': 'none'
    })
    row = bus.send_one(admin_events.DeviceUpdate,
                       {'aet': 'UPD', 'identity': 'password'})
    assert row.identity == IdentityPolicy.PASSWORD.value
    assert row.address == '10.0.0.11'
    assert row.port == 111


def test_device_update_unknown(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    with pytest.raises(ValueError, match='Unknown device'):
        bus.send_one(admin_events.DeviceUpdate,
                     {'aet': 'GHOST', 'port': 1})


def test_device_update_no_changes(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd,
                 {'aet': 'SAME', 'address': '10.0.0.12'})
    with pytest.raises(ValueError, match='Nothing to update'):
        bus.send_one(admin_events.DeviceUpdate, {'aet': 'SAME'})


def test_device_remove(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd,
                 {'aet': 'GONE', 'address': '10.0.0.13'})
    assert bus.send_one(admin_events.DeviceRemove, 'GONE') is True
    assert bus.send_one(admin_events.DeviceRemove, 'GONE') is False
    assert DeviceModel.select().count() == 0


def test_extra_fields_roundtrip(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd, {
        'aet': 'EXTRA_DEV', 'address': '10.0.0.14', 'port': 104,
        'vendor': 'ACME', 'location': 'Room 3'
    })
    row = DeviceModel.get(DeviceModel.aet == 'EXTRA_DEV')
    extra = json.loads(row.extra)
    assert extra['vendor'] == 'ACME'
    device = bus.send_any(core_events.DeviceByAE, 'EXTRA_DEV')
    assert device is not None
    assert device.model_extra['vendor'] == 'ACME'
    assert device.model_extra['location'] == 'Room 3'
    # The association layer never sees the extra keys
    remote_ae = device.to_remote_ae()
    assert not hasattr(remote_ae, 'vendor')
    assert not hasattr(remote_ae, 'location')


def test_remote_ae_never_carries_identity(admin_bus: Any) -> None:
    bus, _, _, _ = admin_bus()
    bus.send_one(admin_events.DeviceAdd, {
        'aet': 'IDENT', 'address': '10.0.0.15', 'port': 104,
        'identity': 'password', 'username': 'alice', 'password': 'secret'
    })
    device = bus.send_any(core_events.DeviceByAE, 'IDENT')
    assert device is not None
    remote_ae = device.to_remote_ae()
    # Credentials are forwarded for outgoing identity, the policy is not
    assert remote_ae.username == 'alice'
    assert remote_ae.password == 'secret'
    assert not hasattr(remote_ae, 'identity')


def test_config_model_validation() -> None:
    config = DeviceStoreConfig.model_validate(
        {'default_identity': 'password', 'default_port': 12345}
    )
    assert config.default_identity == IdentityPolicy.PASSWORD
    assert config.default_port == 12345
    with pytest.raises(pydantic.ValidationError):
        DeviceStoreConfig.model_validate({'default_identity': 'kerberos'})
    with pytest.raises(pydantic.ValidationError):
        DeviceStoreConfig.model_validate({'default_port': 70000})
