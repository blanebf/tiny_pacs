"""End-to-end audit recording against a live server.

Starts real tiny_pacs servers with the ``AuditLog`` component (and, where
relevant, the identity components for username attribution) and drives them
with the core DICOM client, asserting the recorded trail.
"""
import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pydicom
import pytest
from pydicom import uid
from pynetdicom2 import exceptions, uids
from tiny_pacs import client as core_client
from tiny_pacs import config as core_config
from tiny_pacs import devices as core_devices
from tiny_pacs import events as core_events
from tiny_pacs import server as core_server

from tiny_pacs_audit.models import AuditEventModel

CLIENT_AET = 'MODALITY'


@pytest.fixture
def audit_server() -> Iterator[Callable[..., tuple[core_server.Server, int]]]:
    """Starts tiny_pacs servers with the audit component enabled.

    :yield: factory returning the running server and its bound port
    """
    servers: list[core_server.Server] = []

    def start(
            with_identity: bool = True,
            with_admin: bool = False,
            audit_conf: dict[str, Any] | None = None,
            users: list[tuple[str, str]] | None = None,
            policy: str = 'password',
            db: dict[str, Any] | None = None
    ) -> tuple[core_server.Server, int]:
        components: dict[str, Any] = {
            'Database': db or {'on': True, 'db_name': str(uuid.uuid4())},
            'PACS': {'on': True},
            'InMemoryStorage': {'on': True},
            'AuditLog': {'on': True, **(audit_conf or {})}
        }
        if with_identity:
            components['Devices'] = {
                'on': True, 'auto_add': False,
                'devices': {CLIENT_AET: {
                    'aet': CLIENT_AET, 'address': '127.0.0.1', 'port': 1,
                    'identity': policy}}
            }
            components['Users'] = {'on': True}
            components['UserIdentityAuth'] = {'on': True}
        if with_admin:
            components.setdefault('Devices', {'on': True, 'auto_add': False})
            components['DeviceStore'] = {'on': True}
        conf = core_config.Config()
        conf.update_config({'ae': {'port': 0}, 'components': components})
        srv = core_server.Server(conf)
        srv.start()
        servers.append(srv)
        for username, password in (users or []):
            srv.bus.send_one(core_events.UserAdd,
                             {'username': username, 'password': password})
        assert srv.ae is not None
        return srv, int(srv.ae.server.server_address[1])

    yield start

    for srv in reversed(servers):
        srv.exit()


def _client(port: int, username: str | None = None,
            password: str | None = None,
            aet: str = CLIENT_AET) -> core_client.DICOMClient:
    device = core_devices.DeviceConfig(
        aet='TINY_PACS', address='127.0.0.1', port=port,
        username=username, password=password
    )
    return core_client.DICOMClient(aet, device)


def _store_ds() -> pydicom.Dataset:
    ds = pydicom.Dataset()
    ds.PatientName = 'Test^Patient'
    ds.PatientID = 'P1'
    ds.PatientSex = 'O'
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.StudyInstanceUID = uid.generate_uid()
    ds.SeriesInstanceUID = uid.generate_uid()
    ds.SOPInstanceUID = uid.generate_uid()
    ds.SOPClassUID = uids.BASIC_TEXT_SR_STORAGE
    return ds


def _find_request() -> pydicom.Dataset:
    request = pydicom.Dataset()
    request.QueryRetrieveLevel = 'STUDY'
    request.PatientID = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    return request


def _query(srv: core_server.Server, **filters: Any
           ) -> list[AuditEventModel]:
    return srv.bus.send_one(core_events.AuditQuery,
                            core_events.AuditFilter(**filters))


def _wait_for(srv: core_server.Server, timeout: float = 5.0, **filters: Any
              ) -> list[AuditEventModel]:
    """Polls the trail until a matching row appears (teardown is async)."""
    deadline = time.time() + timeout
    rows = _query(srv, **filters)
    while not rows and time.time() < deadline:
        time.sleep(0.05)
        rows = _query(srv, **filters)
    return rows


def test_store_records_request_and_outcome(audit_server: Callable[..., Any]
                                           ) -> None:
    srv, port = audit_server(users=[('alice', 'secret')])
    client = _client(port, 'alice', 'secret')
    client.store(_store_ds(), uids.BASIC_TEXT_SR_STORAGE,
                 uid.ImplicitVRLittleEndian)

    request_rows = _query(srv, events=['store'])
    outcome_rows = _query(srv, events=['store-done'])
    assert request_rows and outcome_rows
    for row in (*request_rows, *outcome_rows):
        assert row.username == 'alice'
        assert row.device_aet == CLIENT_AET
    assert json.loads(outcome_rows[0].details)['sop_instance']


def test_find_attributed(audit_server: Callable[..., Any]) -> None:
    srv, port = audit_server(users=[('alice', 'secret')])
    list(_client(port, 'alice', 'secret').find(_find_request()))
    rows = _query(srv, events=['find'])
    assert rows
    assert rows[0].username == 'alice'
    assert rows[0].device_aet == CLIENT_AET


def test_rejected_association_recorded(audit_server: Callable[..., Any]
                                       ) -> None:
    srv, port = audit_server(users=[('alice', 'secret')])
    with pytest.raises(exceptions.AssociationRejectedError):
        _client(port, 'alice', 'wrong').echo()
    rejected = _wait_for(srv, events=['rejected'])
    assert rejected
    assert rejected[0].status == 'rejected'
    assert rejected[0].category == 'assoc'
    assert rejected[0].device_aet == CLIENT_AET


def test_invalid_called_aet_recorded(audit_server: Callable[..., Any]) -> None:
    srv, port = audit_server(users=[('alice', 'secret')])
    device = core_devices.DeviceConfig(
        aet='WRONG_AET', address='127.0.0.1', port=port,
        username='alice', password='secret')
    with pytest.raises(exceptions.AssociationRejectedError):
        core_client.DICOMClient(CLIENT_AET, device).echo()
    rejected = _wait_for(srv, events=['rejected'])
    assert any('not valid' in json.loads(row.details).get('reason', '')
               for row in rejected)


def test_released_records_authenticated_user(audit_server: Callable[..., Any]
                                             ) -> None:
    srv, port = audit_server(users=[('alice', 'secret')])
    _client(port, 'alice', 'secret').echo()
    released = _wait_for(srv, events=['released'])
    assert released
    assert released[0].username == 'alice'
    assert released[0].status == 'success'


def test_audit_failure_does_not_break_store(
        audit_server: Callable[..., Any],
        monkeypatch: pytest.MonkeyPatch) -> None:
    srv, port = audit_server(users=[('alice', 'secret')])

    def boom(**kwargs: Any) -> Any:
        raise RuntimeError('audit db down')

    monkeypatch.setattr(AuditEventModel, 'create', boom)
    # A failing audit write must never surface as a C-STORE failure
    _client(port, 'alice', 'secret').store(
        _store_ds(), uids.BASIC_TEXT_SR_STORAGE, uid.ImplicitVRLittleEndian)


def test_admin_record_from_device_store(audit_server: Callable[..., Any]
                                        ) -> None:
    """A device mutation in the admin extension reaches the audit trail."""
    srv, _ = audit_server(with_identity=False, with_admin=True)
    srv.bus.send_one(core_events.DeviceAdd,
                     {'aet': 'MRI_01', 'address': '10.0.0.20'})
    rows = _query(srv, categories=['admin'])
    assert any(row.event == 'device-add' and row.device_aet == 'MRI_01'
               for row in rows)


def test_concurrent_associations_attributed(
        audit_server: Callable[..., Any], tmp_path: Path) -> None:
    """Every service row carries the correct device under concurrency."""
    count = 6
    aets = [f'MOD{i}' for i in range(count)]
    srv, port = audit_server(
        with_identity=False,
        db={'on': True, 'db_name': str(tmp_path / 'thread.db'),
            'mode': 'rwc', 'uri': False})
    barrier = threading.Barrier(count)
    errors: list[BaseException] = []

    def work(aet: str) -> None:
        try:
            barrier.wait()
            list(_client(port, aet=aet).find(_find_request()))
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    threads = [threading.Thread(target=work, args=(aet,)) for aet in aets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    find_rows = _query(srv, events=['find'])
    assert len(find_rows) == count
    assert sorted(row.device_aet or '' for row in find_rows) == sorted(aets)
