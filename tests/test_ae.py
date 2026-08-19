import pytest
import trolleybus
from pydicom import dataset
from pydicom.uid import ImplicitVRLittleEndian
from pynetdicom2 import fsm, pdu, statuses, uids

from tiny_pacs import ae, devices, events


@pytest.fixture
def ae_title():
    bus = trolleybus.EventBus()
    # Tests exercise message handling only, so no need to bind the port
    _ae = ae.AE(bus, {}, bind_and_activate=False)
    yield _ae


def test_assoc(ae_title: ae.AE):
    def callback(payload: events.AssocPayload):
        # Fill with proper assoc object
        assert payload.assoc.calling_ae_title == 'TEST'
        assert payload.assoc.called_ae_title == 'TINY_PACS'
    ae_title.bus.subscribe(events.Assoc, callback)
    asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
    ae_title.on_association_request(None, asce_rq)


def test_find(ae_title: ae.AE):
    def callback(payload: events.FindPayload):
        assert ctx == payload.context
        assert ds == payload.ds
        return [(dataset.Dataset(), statuses.C_FIND_PENDING),
                (dataset.Dataset(), statuses.C_FIND_PENDING)]

    ctx = fsm.PContextDef(1, uids.STUDY_ROOT_FIND_SOP_CLASS, ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    ae_title.bus.subscribe(events.Find, callback)
    results = ae_title.on_receive_find(ctx, ds)
    for _ds, status in results:
        assert status.is_pending
        assert _ds is not None


def test_store_success(ae_title: ae.AE):
    def callback(payload: events.StorePayload):
        assert ctx == payload.context
        assert ds == payload.ds
        return statuses.SUCCESS

    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    ae_title.bus.subscribe(events.Store, callback)
    status = ae_title.on_receive_store(ctx, ds)
    assert status.is_success


def test_store_failure(ae_title: ae.AE):
    def callback(payload: events.StorePayload):
        assert ctx == payload.context
        assert ds == payload.ds
        return statuses.C_MOVE_UNABLE_TO_PROCESS

    ctx = fsm.PContextDef(1, uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    ae_title.bus.subscribe(events.Store, callback)
    status = ae_title.on_receive_store(ctx, ds)
    assert status.is_failure


def test_move(ae_title: ae.AE):
    def callback(payload: events.MovePayload):
        assert payload.destination == 'REMOTE_PACS'
        assert ctx == payload.context
        assert ds == payload.ds
        return [dataset.Dataset(), dataset.Dataset()]

    ctx = fsm.PContextDef(1, uids.STUDY_ROOT_MOVE_SOP_CLASS, ImplicitVRLittleEndian)
    ds = dataset.Dataset()
    _devices = devices.Devices(
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
