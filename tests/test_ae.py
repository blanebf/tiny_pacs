import io
from collections.abc import Iterator
from typing import cast

import pytest
import trolleybus
from pydicom import dataset, uid
from pydicom.uid import ImplicitVRLittleEndian
from pynetdicom2 import asceprovider, exceptions, fsm, pdu, statuses, uids

from tiny_pacs import ae, client, config, devices, events


@pytest.fixture
def ae_title() -> Iterator[ae.AE]:
    bus = trolleybus.EventBus()
    # Tests exercise message handling only, so no need to bind the port;
    # dataset dumping is disabled because the test streams are not real DICOM
    _ae = ae.AE(bus, config.AEConfig(dump_ds=False), bind_and_activate=False)
    yield _ae


def test_assoc(ae_title: ae.AE) -> None:
    def callback(payload: events.AssocPayload) -> None:
        # Fill with proper assoc object
        assert payload.assoc.calling_ae_title == 'TEST'
        assert payload.assoc.called_ae_title == 'TINY_PACS'
    ae_title.bus.subscribe(events.Assoc, callback)
    asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
    ae_title.on_association_request(
        cast(asceprovider.AssociationAcceptor, None), asce_rq)


def test_find(ae_title: ae.AE) -> None:
    def callback(
            payload: events.FindPayload
    ) -> list[tuple[dataset.Dataset, statuses.Status]]:
        assert ctx == payload.context
        assert ds == payload.ds
        return [(dataset.Dataset(), statuses.C_FIND_PENDING),
                (dataset.Dataset(), statuses.C_FIND_PENDING)]

    ctx = fsm.PContextDef(1, uids.STUDY_ROOT_FIND_SOP_CLASS,
                          ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    ae_title.bus.subscribe(events.Find, callback)
    results = ae_title.on_receive_find(ctx, ds)
    for _ds, status in results:
        assert status.is_pending
        assert _ds is not None


def test_store_success(ae_title: ae.AE) -> None:
    def callback(payload: events.StorePayload) -> statuses.Status:
        assert ctx == payload.context
        assert buf == payload.ds
        return statuses.SUCCESS

    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    buf = io.BytesIO(b'dataset stream')
    ae_title.bus.subscribe(events.Store, callback)
    status = ae_title.on_receive_store(ctx, buf)
    assert status.is_success


def test_store_failure(ae_title: ae.AE) -> None:
    def callback(payload: events.StorePayload) -> statuses.Status:
        assert ctx == payload.context
        assert buf == payload.ds
        return statuses.C_MOVE_UNABLE_TO_PROCESS

    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    buf = io.BytesIO(b'dataset stream')
    ae_title.bus.subscribe(events.Store, callback)
    status = ae_title.on_receive_store(ctx, buf)
    assert status.is_failure


def test_move(ae_title: ae.AE) -> None:
    def callback(payload: events.MovePayload) -> list[events.StoredFile]:
        assert payload.destination == 'REMOTE_PACS'
        assert ctx == payload.context
        assert ds == payload.ds
        return [
            (uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian,
             dataset.Dataset()),
            (uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian,
             dataset.Dataset()),
        ]

    ctx = fsm.PContextDef(1, uids.STUDY_ROOT_MOVE_SOP_CLASS,
                          ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    devices.Devices(
        ae_title.bus,
        {
            'devices': {
                'REMOTE_PACS': {
                    'address': '127.0.0.1', 'port': 11112, 'aet': 'REMOTE_PACS'
                }
            }
        }
    )
    ae_title.bus.subscribe(events.Move, callback)
    remote_ae, nop, results = ae_title.on_receive_move(ctx, ds, 'REMOTE_PACS')
    assert nop == 2
    assert len(list(results)) == 2
    assert remote_ae.aet == 'REMOTE_PACS'
    assert remote_ae.address == '127.0.0.1'
    assert remote_ae.port == 11112


def test_move_with_user_authentication(ae_title: ae.AE) -> None:
    def callback(payload: events.MovePayload) -> list[events.StoredFile]:
        return [
            (uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian,
             dataset.Dataset()),
        ]

    ctx = fsm.PContextDef(1, uids.STUDY_ROOT_MOVE_SOP_CLASS,
                          ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    devices.Devices(
        ae_title.bus,
        {
            'devices': {
                'REMOTE_PACS': {
                    'address': '127.0.0.1', 'port': 11112,
                    'aet': 'REMOTE_PACS',
                    'username': 'dicom_user', 'password': 'secret'
                }
            }
        }
    )
    ae_title.bus.subscribe(events.Move, callback)
    remote_ae, nop, _ = ae_title.on_receive_move(ctx, ds, 'REMOTE_PACS')
    assert nop == 1
    # DICOM user authentication parameters flow through to the remote AE config
    assert remote_ae.username == 'dicom_user'
    assert remote_ae.password == 'secret'


@pytest.mark.parametrize('tls_config', [True, {}, {'key': '/nonexistent.key'}])
def test_tls_invalid_config(tls_config: object) -> None:
    bus = trolleybus.EventBus()
    with pytest.raises(ValueError):
        ae.AE(bus, {'tls': tls_config, 'port': 0}, bind_and_activate=False)


def test_echo(ae_title: ae.AE) -> None:
    ctx = fsm.PContextDef(1, uids.VERIFICATION_SOP_CLASS,
                          ImplicitVRLittleEndian)
    status = ae_title.on_receive_echo(ctx)
    assert status.is_success


def test_commitment_response(ae_title: ae.AE) -> None:
    success = [(uid.UID('1.2.3'), uid.UID('1.2.3.4'))]
    failure = [(uid.UID('1.2.3'), uid.UID('1.2.3.5'), 0x0112)]
    ae_title.on_commitment_response(uid.UID('1.2.3.4.5'), success, failure)


def test_association_response(ae_title: ae.AE) -> None:
    response = pdu.AAssociateAcPDU('TINY_PACS', 'TEST', [])
    ae_title.on_association_response(response)


def test_abort_and_timeout(ae_title: ae.AE) -> None:
    ae_title.on_abort(cast(asceprovider.Association, None),
                      exceptions.AssociationAbortedError(1, 0))
    ae_title.on_dcm_timeout(cast(asceprovider.Association, None),
                            exceptions.DCMTimeoutError())


def test_client_with_user_authentication(ae_title: ae.AE) -> None:
    devices.Devices(
        ae_title.bus,
        {
            'devices': {
                'REMOTE_PACS': {
                    'address': '127.0.0.1', 'port': 11112,
                    'aet': 'REMOTE_PACS',
                    'username': 'dicom_user', 'password': 'secret'
                }
            }
        }
    )
    dicom_client = client.Client(ae_title.bus, {}).get('REMOTE_PACS')
    # Device settings, including DICOM user authentication, are passed
    # through to the client untouched.
    assert dicom_client.remote_ae.username == 'dicom_user'
    assert dicom_client.remote_ae.password == 'secret'
