"""Tests of the UserIdentityAuth component on a headless bus."""
import logging
from typing import Any

import pydantic
import pytest
from conftest import _identity_item, assoc_payload
from pynetdicom2 import exceptions, pdu
from pynetdicom2.userdataitems import (
    UserIdentityNegotiationSubItem,
    UserIdentityNegotiationSubItemAc,
)
from tiny_pacs import assoc_context
from tiny_pacs import devices as core_devices
from tiny_pacs import events as core_events

from tiny_pacs_identity import hashing
from tiny_pacs_identity.auth import (
    UnknownDevicePolicy,
    UserIdentityAuth,
    UserIdentityAuthConfig,
)
from tiny_pacs_identity.models import UserModel


def _add_user(bus: Any, username: str = 'alice',
              password: str = 'secret') -> None:
    bus.send_one(core_events.UserAdd,
                 {'username': username, 'password': password})


def _devices_with_policy(policy: str) -> dict[str, Any]:
    """A YAML device registry carrying the given per-device policy."""
    return {'auto_add': False, 'devices': {
        'MODALITY': {'aet': 'MODALITY', 'address': '10.0.0.5',
                     'port': 104, 'identity': policy}
    }}


@pytest.mark.parametrize('identity', [
    None,                       # no identity at all
    ('alice', '', 1),           # type 1, known user
    ('alice', 'secret', 2),     # type 2, valid credentials
    ('alice', 'wrong', 2),      # type 2, wrong password: advisory only
    ('ghost', 'secret', 2),     # type 2, unknown user: advisory only
])
def test_none_policy_accepts(identity_bus: Any, identity: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('none'), auth_conf={}
    )
    _add_user(bus)
    item = None if identity is None else _identity_item(*identity)
    bus.broadcast(core_events.Assoc, assoc_payload('MODALITY', item))


def test_none_policy_advisory_updates_last_login_only_on_valid(
        identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('none'), auth_conf={}
    )
    _add_user(bus)
    bus.broadcast(core_events.Assoc,
                  assoc_payload('MODALITY', _identity_item('alice', '', 1)))
    assert UserModel.get(UserModel.username == 'alice').last_login is not None
    bus.broadcast(core_events.Assoc,
                  assoc_payload('MODALITY', _identity_item('ghost', '', 1)))
    assert UserModel.get(UserModel.username == 'alice').last_login is not None
    assert UserModel.select().count() == 1


def test_username_policy_matrix(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('username'), auth_conf={}
    )
    _add_user(bus)
    # No identity: rejected
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY', None))
    # Type 1 with a known active user: accepted
    bus.broadcast(core_events.Assoc, assoc_payload(
        'MODALITY', _identity_item('alice', '', 1)))
    # Type 1 with an unknown user: rejected
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('ghost', '', 1)))
    # Type 2 with a valid password: accepted
    bus.broadcast(core_events.Assoc, assoc_payload(
        'MODALITY', _identity_item('alice', 'secret')))
    # Type 2 with a wrong password: rejected
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', 'wrong')))


def test_password_policy_matrix(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    # No identity: rejected
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY', None))
    # Type 1 (no password): rejected even for a known user
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', '', 1)))
    # Type 2 with a valid password: accepted
    bus.broadcast(core_events.Assoc, assoc_payload(
        'MODALITY', _identity_item('alice', 'secret')))
    # Type 2 with a wrong password: rejected
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', 'wrong')))


def test_unsupported_identity_types_rejected(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('none'), auth_conf={}
    )
    _add_user(bus)
    # Kerberos, SAML and JWT identities carry opaque binary credentials
    for identity_type in (3, 4, 5):
        with pytest.raises(exceptions.AssociationRejectedError):
            bus.broadcast(core_events.Assoc, assoc_payload(
                'MODALITY',
                _identity_item(b'\xff\xfe', b'', identity_type)))


def test_non_text_identity_rejected(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('none'),
        auth_conf={'default_policy': 'username'}
    )
    _add_user(bus)
    # Non-UTF-8 username bytes cannot be a login name
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item(b'\xff\xfe', b'', 1)))


def test_unknown_user_runs_dummy_verification(
        identity_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    calls: list[str] = []
    original = hashing.verify_password

    def recording(password: str, stored: str) -> bool:
        calls.append(stored)
        return original(password, stored)

    monkeypatch.setattr(hashing, 'verify_password', recording)
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('ghost', 'whatever')))
    # One verification ran against the dummy hash: the rejection timing
    # must not reveal whether the username exists
    assert len(calls) == 1


def test_missing_users_component_fails_closed_and_warns(
        identity_bus: Any, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR, logger='UserIdentityAuth'):
        bus, users, _, _, _ = identity_bus(
            devices_conf=_devices_with_policy('password'),
            auth_conf={}, with_users=False
        )
    assert users is None
    # The misconfiguration is reported at startup...
    assert any('no Users component' in message
               for message in caplog.messages)
    # ...and the association is cleanly rejected instead of aborting
    # with an event bus error
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', 'secret')))


def test_missing_users_component_none_policy_accepts(identity_bus: Any
                                                     ) -> None:
    bus, users, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('none'),
        auth_conf={}, with_users=False
    )
    assert users is None
    # Advisory mode: unknown users are logged and accepted, no crash
    bus.broadcast(core_events.Assoc, assoc_payload(
        'MODALITY', _identity_item('alice', 'secret')))


def test_inactive_user_rejected(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('username'), auth_conf={}
    )
    _add_user(bus)
    bus.send_one(core_events.UserSetActive,
                 {'username': 'alice', 'is_active': False})
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', '', 1)))


def test_default_policy_for_devices_without_one(identity_bus: Any) -> None:
    # The YAML device omits the identity field: default_policy governs
    devices_conf = {'auto_add': False, 'devices': {
        'MODALITY': {'aet': 'MODALITY', 'address': '10.0.0.5', 'port': 104}
    }}
    bus, _, _, _, _ = identity_bus(
        devices_conf=devices_conf, auth_conf={'default_policy': 'password'}
    )
    _add_user(bus)
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY', None))
    bus.broadcast(core_events.Assoc, assoc_payload(
        'MODALITY', _identity_item('alice', 'secret')))


def test_unknown_device_policies(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf={'auto_add': False, 'devices': {}},
        auth_conf={'unknown_device_policy': 'reject'}
    )
    _add_user(bus)
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload(
            'STRANGER', _identity_item('alice', 'secret')))

    bus2, _, _, _, _ = identity_bus(
        devices_conf={'auto_add': False, 'devices': {}},
        auth_conf={'unknown_device_policy': 'password'}
    )
    _add_user(bus2)
    with pytest.raises(exceptions.AssociationRejectedError):
        bus2.broadcast(core_events.Assoc, assoc_payload('STRANGER', None))
    bus2.broadcast(core_events.Assoc, assoc_payload(
        'STRANGER', _identity_item('alice', 'secret')))


def test_default_unknown_device_policy_is_none(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf={'auto_add': False, 'devices': {}}, auth_conf={}
    )
    # No identity required for unknown devices by default
    bus.broadcast(core_events.Assoc, assoc_payload('STRANGER', None))


def test_rejected_assoc_is_not_auto_added(identity_bus: Any) -> None:
    # auto_add enabled: the rejection must abort the remaining Assoc
    # listeners, so the device registry never sees the request
    devices_conf = {'auto_add': True, 'devices': {}}
    bus, _, _, _, _ = identity_bus(
        devices_conf=devices_conf,
        auth_conf={'unknown_device_policy': 'password'}
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload('STRANGER', None))
    devices_component = bus.send_any(core_events.DeviceByAE, 'STRANGER')
    assert devices_component is None


def test_accepted_assoc_is_still_auto_added(identity_bus: Any) -> None:
    devices_conf = {'auto_add': True, 'devices': {}}
    bus, _, _, _, _ = identity_bus(devices_conf=devices_conf, auth_conf={})
    bus.broadcast(core_events.Assoc, assoc_payload('STRANGER', None))
    device = bus.send_any(core_events.DeviceByAE, 'STRANGER')
    assert device is not None


def test_rejected_assoc_not_persisted_by_device_store(
        identity_bus: Any) -> None:
    from tiny_pacs_admin.models import DeviceModel
    bus, _, _, _, _ = identity_bus(
        devices_conf={'auto_add': False, 'devices': {}},
        store_conf={'default_identity': 'none'},
        auth_conf={'unknown_device_policy': 'reject'}
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload('STRANGER', None))
    assert DeviceModel.get_or_none(DeviceModel.aet == 'STRANGER') is None


def test_auth_runs_before_device_components(identity_bus: Any) -> None:
    seen: list[str] = []

    def probe(_: core_events.AssocPayload) -> None:
        seen.append('probe')

    bus, _, auth, _, _ = identity_bus(
        devices_conf={'auto_add': True, 'devices': {}},
        auth_conf={'unknown_device_policy': 'reject'}
    )
    assert auth is not None
    bus.subscribe(core_events.Assoc, probe)
    with pytest.raises(exceptions.AssociationRejectedError):
        bus.broadcast(core_events.Assoc, assoc_payload('STRANGER', None))
    # The higher-priority auth handler aborted the remaining listeners
    assert seen == []
    assert UserIdentityAuth.priority > core_devices.Devices.priority


def test_positive_response_sub_item(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    identity_item = _identity_item('alice', 'secret', 2,
                                   positive_response_req=1)
    payload = assoc_payload('MODALITY', identity_item)
    bus.broadcast(core_events.Assoc, payload)
    user_info = payload.assoc.variable_items[-1]
    assert isinstance(user_info, pdu.UserInformationItem)
    # The request sub-item (0x58) is replaced with the response (0x59)
    assert not [item for item in user_info.user_data
                if isinstance(item, UserIdentityNegotiationSubItem)]
    ac_items = [item for item in user_info.user_data
                if isinstance(item, UserIdentityNegotiationSubItemAc)]
    assert len(ac_items) == 1
    assert ac_items[0].server_response == ''


def test_no_positive_response_removes_sub_item(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    payload = assoc_payload(
        'MODALITY', _identity_item('alice', 'secret', 2,
                                   positive_response_req=0)
    )
    bus.broadcast(core_events.Assoc, payload)
    user_info = payload.assoc.variable_items[-1]
    assert isinstance(user_info, pdu.UserInformationItem)
    # The accepted request must carry no identity sub-items at all
    assert not [item for item in user_info.user_data
                if isinstance(item, (UserIdentityNegotiationSubItem,
                                     UserIdentityNegotiationSubItemAc))]


def test_last_login_updated_on_success(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    before = UserModel.get(UserModel.username == 'alice').last_login
    assert before is None
    bus.broadcast(core_events.Assoc, assoc_payload(
        'MODALITY', _identity_item('alice', 'secret')))
    after = UserModel.get(UserModel.username == 'alice').last_login
    assert after is not None


def test_startup_warning_on_conflicting_defaults(
        identity_bus: Any, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger='UserIdentityAuth'):
        identity_bus(
            devices_conf={'auto_add': False, 'devices': {}},
            store_conf={'default_identity': 'password'},
            auth_conf={'unknown_device_policy': 'none'}
        )
    assert any('consistently' in message
               for message in caplog.messages)


def test_no_warning_on_consistent_defaults(
        identity_bus: Any, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger='UserIdentityAuth'):
        identity_bus(
            devices_conf={'auto_add': False, 'devices': {}},
            store_conf={'default_identity': 'none'},
            auth_conf={'unknown_device_policy': 'none'}
        )
    assert not caplog.messages


def test_config_model_validation() -> None:
    conf = UserIdentityAuthConfig.model_validate(
        {'default_policy': 'password', 'unknown_device_policy': 'reject'}
    )
    assert conf.default_policy.value == 'password'
    assert conf.unknown_device_policy == UnknownDevicePolicy.REJECT
    with pytest.raises(pydantic.ValidationError):
        UserIdentityAuthConfig.model_validate({'default_policy': 'reject'})
    with pytest.raises(pydantic.ValidationError):
        UserIdentityAuthConfig.model_validate(
            {'unknown_device_policy': 'kerberos'}
        )


def test_rejection_carries_reason(identity_bus: Any) -> None:
    """The rejection reason travels on the exception (AssocRejected)."""
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    with pytest.raises(exceptions.AssociationRejectedError) as info:
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', 'wrong')))
    assert str(info.value) == 'invalid credentials'


def test_authenticated_user_enriches_assoc_context(identity_bus: Any) -> None:
    """Successful authentication writes the principal into the core
    association context (audit/access-control read it from there)."""
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    payload = assoc_payload('MODALITY', _identity_item('alice', 'secret'))
    # The AE opens the context before broadcasting Assoc
    context = assoc_context.open_context(payload.asce, payload.assoc)
    try:
        bus.broadcast(core_events.Assoc, payload)
        assert context.username == 'alice'
        assert assoc_context.current() is context
    finally:
        assoc_context.close_current()


def test_rejected_auth_leaves_context_without_user(
        identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus(
        devices_conf=_devices_with_policy('password'), auth_conf={}
    )
    _add_user(bus)
    payload = assoc_payload('MODALITY', _identity_item('alice', 'wrong'))
    context = assoc_context.open_context(payload.asce, payload.assoc)
    try:
        with pytest.raises(exceptions.AssociationRejectedError):
            bus.broadcast(core_events.Assoc, payload)
        assert context.username is None
    finally:
        assoc_context.close_current()
