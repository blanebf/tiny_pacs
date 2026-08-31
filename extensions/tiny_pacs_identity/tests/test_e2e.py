"""End-to-end association authentication against a live server.

Starts real tiny_pacs servers with the ``Users`` and ``UserIdentityAuth``
components and drives them with the core DICOM client presenting (or
withholding) identities — the full enforcement matrix per device policy
crossed with every identity presentation.
"""
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from pynetdicom2 import applicationentity, asceprovider, exceptions, sopclass
from pynetdicom2 import uids as netdicom_uids
from pynetdicom2.userdataitems import (
    UserIdentityNegotiationSubItem,
    UserIdentityNegotiationSubItemAc,
)
from tiny_pacs import client as core_client
from tiny_pacs import config as core_config
from tiny_pacs import devices as core_devices
from tiny_pacs import server as core_server
from tiny_pacs_admin.models import DeviceModel

from tiny_pacs_identity import events as identity_events

#: The calling AE title used by the test client
CLIENT_AET = 'MODALITY'

#: (device policy, username, password, association accepted)
POLICY_MATRIX: list[tuple[str, str | None, str | None, bool]] = [
    ('none', None, None, True),
    ('none', 'alice', None, True),
    ('none', 'alice', 'secret', True),
    ('none', 'alice', 'wrong', True),       # advisory verification
    ('none', 'ghost', 'secret', True),      # advisory verification
    ('username', None, None, False),
    ('username', 'alice', None, True),
    ('username', 'alice', 'secret', True),
    ('username', 'alice', 'wrong', False),
    ('username', 'ghost', None, False),
    ('password', None, None, False),
    ('password', 'alice', None, False),
    ('password', 'alice', 'secret', True),
    ('password', 'alice', 'wrong', False),
    ('password', 'ghost', 'secret', False),
]


@pytest.fixture
def auth_server() -> Iterator[Callable[..., tuple[core_server.Server, int]]]:
    """Starts tiny_pacs servers with identity authentication enabled.

    :yield: factory ``(devices=None, auth_conf=None, store_conf=None,
            users=())`` returning the running server and its bound port
    """
    servers: list[core_server.Server] = []

    def start(
            devices: dict[str, Any] | None = None,
            auth_conf: dict[str, Any] | None = None,
            store_conf: dict[str, Any] | None = None,
            users: list[tuple[str, str]] | None = None
    ) -> tuple[core_server.Server, int]:
        components: dict[str, Any] = {
            'Database': {'on': True, 'db_name': str(uuid.uuid4())},
            'Devices': {'on': True, 'auto_add': False,
                        'devices': devices or {}},
            'Users': {'on': True},
            'UserIdentityAuth': {'on': True, **(auth_conf or {})}
        }
        if store_conf is not None:
            components['DeviceStore'] = {'on': True, **store_conf}
        conf = core_config.Config()
        conf.update_config({'ae': {'port': 0}, 'components': components})
        srv = core_server.Server(conf)
        srv.start()
        servers.append(srv)
        for username, password in (users or []):
            srv.bus.send_one(identity_events.UserAdd, {
                'username': username, 'password': password
            })
        assert srv.ae is not None
        port = int(srv.ae.server.server_address[1])
        return srv, port

    yield start

    for srv in reversed(servers):
        srv.exit()


def _client(port: int, username: str | None = None,
            password: str | None = None) -> core_client.DICOMClient:
    """Builds a client for the test server with the given identity."""
    device = core_devices.DeviceConfig(
        aet='TINY_PACS', address='127.0.0.1', port=port,
        username=username, password=password
    )
    return core_client.DICOMClient(CLIENT_AET, device)


def _device(port: int, policy: str) -> dict[str, Any]:
    """Device entry of the test client carrying an identity policy."""
    return {'aet': CLIENT_AET, 'address': '127.0.0.1', 'port': port,
            'identity': policy}


@pytest.mark.parametrize('policy, username, password, accepted',
                         POLICY_MATRIX)
def test_policy_matrix(auth_server: Callable[..., Any], policy: str,
                       username: str | None, password: str | None,
                       accepted: bool) -> None:
    # The device address/port are irrelevant for the authentication itself
    _, port = auth_server(
        devices={CLIENT_AET: _device(1, policy)},
        users=[('alice', 'secret')]
    )
    if accepted:
        _client(port, username, password).echo()
    else:
        with pytest.raises(exceptions.AssociationRejectedError):
            _client(port, username, password).echo()


def test_unknown_device_policy_reject(auth_server: Callable[..., Any]
                                      ) -> None:
    _, port = auth_server(
        auth_conf={'unknown_device_policy': 'reject'},
        users=[('alice', 'secret')]
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port).echo()
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port, 'alice', 'secret').echo()


def test_unknown_device_policy_username(auth_server: Callable[..., Any]
                                        ) -> None:
    _, port = auth_server(
        auth_conf={'unknown_device_policy': 'username'},
        users=[('alice', 'secret')]
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port).echo()
    _client(port, 'alice').echo()


def test_unknown_device_policy_password(auth_server: Callable[..., Any]
                                        ) -> None:
    _, port = auth_server(
        auth_conf={'unknown_device_policy': 'password'},
        users=[('alice', 'secret')]
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port, 'alice').echo()
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port).echo()
    _client(port, 'alice', 'secret').echo()


def test_unknown_device_default_is_none(
        auth_server: Callable[..., Any]) -> None:
    _, port = auth_server()
    # By default unknown devices need no identity at all
    _client(port).echo()


def test_default_policy_applies_to_policyless_device(
        auth_server: Callable[..., Any]) -> None:
    device = {'aet': CLIENT_AET, 'address': '127.0.0.1', 'port': 1}
    _, port = auth_server(
        devices={CLIENT_AET: device},
        auth_conf={'default_policy': 'password'},
        users=[('alice', 'secret')]
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port).echo()
    _client(port, 'alice', 'secret').echo()


def test_accepted_unknown_device_is_auto_added(
        auth_server: Callable[..., Any]) -> None:
    _, port = auth_server(store_conf={'default_identity': 'username'})
    assert DeviceModel.get_or_none(DeviceModel.aet == CLIENT_AET) is None
    _client(port).echo()
    row = DeviceModel.get_or_none(DeviceModel.aet == CLIENT_AET)
    assert row is not None
    # The DeviceStore auto-add default governs the next association
    assert row.identity == 'username'


def test_rejected_device_is_not_auto_added(
        auth_server: Callable[..., Any]) -> None:
    _, port = auth_server(
        store_conf={'default_identity': 'none'},
        auth_conf={'unknown_device_policy': 'reject'}
    )
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port).echo()
    assert DeviceModel.get_or_none(DeviceModel.aet == CLIENT_AET) is None


def test_positive_response_sub_item(auth_server: Callable[..., Any]
                                    ) -> None:
    _, port = auth_server(
        devices={CLIENT_AET: _device(1, 'password')},
        users=[('alice', 'secret')]
    )
    responses: list[Any] = []

    class RecordingAE(applicationentity.ClientAE):
        def on_association_response(self, response: Any) -> None:
            responses.append(response)
            super().on_association_response(response)

    aet = RecordingAE(CLIENT_AET)
    aet.add_scu(sopclass.verification_scu)
    remote = asceprovider.RemoteAEConfig(
        aet='TINY_PACS', address='127.0.0.1', port=port,
        user_data=[UserIdentityNegotiationSubItem(
            'alice', 'secret', user_identity_type=2,
            positive_response_req=1
        )]
    )
    with aet.request_association(remote) as asce:
        service = asce.get_scu(netdicom_uids.VERIFICATION_SOP_CLASS)
        status = service(1)
        assert not status.is_failure

    # The A-ASSOCIATE-AC answers with exactly one 0x59 sub-item and no
    # request sub-item (PS3.7 D.3.3.7.3)
    ac_pdu = responses[0]
    user_info = ac_pdu.variable_items[-1]
    ac_items = [item for item in user_info.user_data
                if isinstance(item, UserIdentityNegotiationSubItemAc)]
    rq_items = [item for item in user_info.user_data
                if isinstance(item, UserIdentityNegotiationSubItem)]
    assert len(ac_items) == 1
    assert ac_items[0].server_response == ''
    assert not rq_items


def test_last_login_persisted(auth_server: Callable[..., Any]) -> None:
    srv, port = auth_server(
        devices={CLIENT_AET: _device(1, 'password')},
        users=[('alice', 'secret')]
    )
    _client(port, 'alice', 'secret').echo()
    row = srv.bus.send_one(identity_events.UserByName, 'alice')
    assert row.last_login is not None
