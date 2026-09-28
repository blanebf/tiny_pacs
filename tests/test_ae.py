import io
from collections.abc import Iterator
from typing import cast

import pydicom
import pytest
import trolleybus
from pydicom import dataset, uid
from pydicom.uid import ImplicitVRLittleEndian
from pynetdicom2 import asceprovider, exceptions, fsm, pdu, statuses, uids

from tiny_pacs import ae, assoc_context, client, config, devices, events


@pytest.fixture
def ae_title() -> Iterator[ae.AE]:
    bus = trolleybus.EventBus()
    # Tests exercise message handling only, so no need to bind the port;
    # dataset dumping is disabled because the test streams are not real
    # DICOM
    _ae = ae.AE(bus, config.AEConfig(dump_ds=False), bind_and_activate=False)
    yield _ae
    # Association contexts opened on this thread must not leak into
    # subsequent tests
    while assoc_context.close_current() is not None:
        pass


def _dicom_stream() -> io.BytesIO:
    """Builds a decodable DICOM dataset stream (preamble + file meta)."""
    dcm = pydicom.Dataset()
    dcm.PatientName = 'Test^Test^Test'
    dcm.SOPClassUID = uids.BASIC_TEXT_SR_STORAGE
    dcm.SOPInstanceUID = '1.2.3.4'
    meta = dataset.FileMetaDataset()
    meta.MediaStorageSOPClassUID = dcm.SOPClassUID
    meta.MediaStorageSOPInstanceUID = dcm.SOPInstanceUID
    meta.TransferSyntaxUID = ImplicitVRLittleEndian
    dcm.file_meta = meta
    buf = io.BytesIO()
    pydicom.dcmwrite(buf, dcm, enforce_file_format=True)
    buf.seek(0)
    return buf


def test_assoc(ae_title: ae.AE) -> None:
    def callback(payload: events.AssocPayload) -> None:
        # Fill with proper assoc object
        assert payload.assoc.calling_ae_title == 'TEST'
        assert payload.assoc.called_ae_title == 'TINY_PACS'
        # The context is opened before the broadcast, so listeners can
        # attribute their work
        context = assoc_context.current()
        assert context is not None
        assert context.calling_aet == 'TEST'
        assert context.called_aet == 'TINY_PACS'
    ae_title.bus.subscribe(events.Assoc, callback)
    asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
    ae_title.on_association_request(
        cast(asceprovider.AssociationAcceptor, None), asce_rq)


def test_assoc_rejected_invalid_called_aet(ae_title: ae.AE) -> None:
    payloads: list[events.AssocRejectedPayload] = []
    ae_title.bus.subscribe(events.AssocRejected, payloads.append)
    asce_rq = pdu.AAssociateRqPDU('OTHER_AET', 'TEST', [])
    with pytest.raises(exceptions.AssociationRejectedError):
        ae_title.on_association_request(
            cast(asceprovider.AssociationAcceptor, None), asce_rq)
    assert len(payloads) == 1
    assert payloads[0].assoc is None
    assert 'OTHER_AET' in payloads[0].reason
    assert assoc_context.current() is None


def test_assoc_rejected_by_listener(ae_title: ae.AE) -> None:
    payloads: list[events.AssocRejectedPayload] = []

    def rejector(_: events.AssocPayload) -> None:
        raise exceptions.AssociationRejectedError(1, 1, 7,
                                                  'invalid credentials')

    ae_title.bus.subscribe(events.Assoc, rejector)
    ae_title.bus.subscribe(events.AssocRejected, payloads.append)
    asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
    with pytest.raises(exceptions.AssociationRejectedError):
        ae_title.on_association_request(
            cast(asceprovider.AssociationAcceptor, None), asce_rq)
    assert len(payloads) == 1
    assert payloads[0].assoc is asce_rq
    assert payloads[0].reason == 'invalid credentials'
    # The context is dropped without a release event
    assert assoc_context.current() is None


def test_assoc_released(ae_title: ae.AE) -> None:
    released: list[assoc_context.AssocContext] = []
    ae_title.bus.subscribe(events.AssocReleased, released.append)
    asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
    ae_title.on_association_request(
        cast(asceprovider.AssociationAcceptor, None), asce_rq)
    context = assoc_context.current()
    assert context is not None
    ae_title.on_association_end()
    assert released == [context]
    assert assoc_context.current() is None
    # Without an open context the teardown is a silent no-op
    ae_title.on_association_end()
    assert released == [context]


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


def test_service_hook_find_routing() -> None:
    """A contributed hook routes C-FIND on its SOP class to its event."""

    class HookFind(trolleybus.Event[
        events.FindPayload,
        list[tuple[dataset.Dataset, statuses.Status]]
    ]):
        pass

    hook = events.ServiceHook(
        name='test', sop_classes=['1.2.3.99'], on_find=HookFind
    )
    received: list[events.FindPayload] = []

    def on_hook(payload: events.FindPayload
                ) -> list[tuple[dataset.Dataset, statuses.Status]]:
        received.append(payload)
        return [(dataset.Dataset(), statuses.C_FIND_PENDING)]

    bus = trolleybus.EventBus()
    bus.subscribe(events.ServicesRegistry, lambda _: [hook])
    bus.subscribe(HookFind, on_hook)
    _ae = ae.AE(bus, config.AEConfig(dump_ds=False), bind_and_activate=False)
    # The hook's SOP class is served by the AE...
    assert uid.UID('1.2.3.99') in _ae.supported_scp
    # ...and its C-FIND requests are broadcast as the hook event
    ctx = fsm.PContextDef(1, uid.UID('1.2.3.99'), ImplicitVRLittleEndian)
    results = list(_ae.on_receive_find(ctx, dataset.Dataset()))
    assert len(received) == 1
    assert len(results) == 1
    # Built-in SOP classes still route to the built-in Find event
    builtin_ctx = fsm.PContextDef(3, uids.STUDY_ROOT_FIND_SOP_CLASS,
                                  ImplicitVRLittleEndian)
    seen: list[events.FindPayload] = []

    def on_find(payload: events.FindPayload
                ) -> list[tuple[dataset.Dataset, statuses.Status]]:
        seen.append(payload)
        return [(dataset.Dataset(), statuses.C_FIND_PENDING)]

    bus.subscribe(events.Find, on_find)
    results = list(_ae.on_receive_find(builtin_ctx, dataset.Dataset()))
    assert len(seen) == 1
    assert len(results) == 1


def test_service_hook_cannot_replace_builtins(ae_title: ae.AE) -> None:
    """Hooks may only add SOP classes, never replace built-in services."""
    hook = events.ServiceHook(
        name='test',
        sop_classes=[str(uids.STUDY_ROOT_FIND_SOP_CLASS)],
        on_find=events.Find
    )
    ae_title._add_service_hook(hook)
    assert ae_title.find_hooks == {}


def test_store_success(ae_title: ae.AE) -> None:
    stored: list[events.StoreDatasetPayload] = []

    def callback(payload: events.StorePayload) -> statuses.Status:
        assert ctx == payload.context
        assert buf == payload.ds
        # No association is open in this unit test
        assert payload.session is None
        return statuses.SUCCESS

    ae_title.bus.subscribe(events.StoreDataset, stored.append)
    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    buf = _dicom_stream()
    ae_title.bus.subscribe(events.Store, callback)
    status = ae_title.on_receive_store(ctx, buf)
    assert status.is_success
    # The decoded dataset feeds the store pipeline
    assert len(stored) == 1
    assert stored[0].ds.SOPInstanceUID == '1.2.3.4'
    assert stored[0].transfer_syntax == str(ImplicitVRLittleEndian)
    assert stored[0].origin == 'dimse'


def test_store_failure(ae_title: ae.AE) -> None:
    stored: list[events.StoreDatasetPayload] = []

    def callback(payload: events.StorePayload) -> statuses.Status:
        assert ctx == payload.context
        assert buf == payload.ds
        return statuses.C_MOVE_UNABLE_TO_PROCESS

    ae_title.bus.subscribe(events.StoreDataset, stored.append)
    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    buf = io.BytesIO(b'dataset stream')
    ae_title.bus.subscribe(events.Store, callback)
    status = ae_title.on_receive_store(ctx, buf)
    assert status.is_failure
    # A rejected Store never reaches the dataset pipeline
    assert stored == []


def test_store_undecodable_dataset(ae_title: ae.AE) -> None:
    """An undecodable dataset yields the failure status, not a crash."""
    stored: list[events.StoreDatasetPayload] = []
    ae_title.bus.subscribe(events.StoreDataset, stored.append)
    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    status = ae_title.on_receive_store(ctx, io.BytesIO(b'not a dicom file'))
    assert status == statuses.C_STORE_CANNOT_UNDERSTAND
    assert stored == []


def test_store_pipeline_failure_status(ae_title: ae.AE) -> None:
    """A raising StoreDataset handler maps to the failure status."""

    def failing(_: events.StoreDatasetPayload) -> None:
        raise RuntimeError('db down')

    ae_title.bus.subscribe(events.StoreDataset, failing)
    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    status = ae_title.on_receive_store(ctx, _dicom_stream())
    assert status == statuses.C_STORE_CANNOT_UNDERSTAND


def test_store_session(ae_title: ae.AE) -> None:
    """Service payloads carry the association context of the AE."""

    def open_assoc() -> assoc_context.AssocContext:
        asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
        ae_title.on_association_request(
            cast(asceprovider.AssociationAcceptor, None), asce_rq)
        return cast(assoc_context.AssocContext, assoc_context.current())

    sessions: list[object] = []

    def store_callback(payload: events.StorePayload) -> statuses.Status:
        sessions.append(payload.session)
        return statuses.SUCCESS

    def find_callback(
            payload: events.FindPayload
    ) -> list[tuple[dataset.Dataset, statuses.Status]]:
        sessions.append(payload.session)
        return []

    context = open_assoc()
    ae_title.bus.subscribe(events.Store, store_callback)
    ae_title.bus.subscribe(events.Find, find_callback)
    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE,
                          ImplicitVRLittleEndian)
    ae_title.on_receive_store(ctx, _dicom_stream())
    find_ctx = fsm.PContextDef(3, uids.STUDY_ROOT_FIND_SOP_CLASS,
                               ImplicitVRLittleEndian)
    list(ae_title.on_receive_find(find_ctx, dataset.Dataset()))
    assert sessions == [context, context]
    # Authentication components enrich the context in-place
    context.username = 'alice'
    assert assoc_context.current() is context


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
