# -*- coding: utf-8 -*-
import pytest

from pydicom.uid import ImplicitVRLittleEndian
from pydicom import dataset
from pynetdicom2.asceprovider import PContextDef
from pynetdicom2 import uids
from pynetdicom2 import statuses
from pynetdicom2 import pdu

from tiny_pacs import ae
from tiny_pacs import devices
from tiny_pacs import event_bus


@pytest.fixture
def ae_title():
    bus = event_bus.EventBus()
    return ae.AE(bus, {})


def test_assoc(ae_title: ae.AE):
    def callback(asce, assoc: pdu.AAssociateRqPDU):
        # Fill with proper assoc object
        assert assoc.calling_ae_title == 'TEST'
        assert assoc.called_ae_title == 'TINY_PACS'
    ae_title.bus.subscribe(ae.AEChannels.ASSOC, callback)
    asce_rq = pdu.AAssociateRqPDU('TINY_PACS', 'TEST', [])
    ae_title.on_association_request(None, asce_rq)


def test_find(ae_title: ae.AE):
    def callback(context, ds):
        assert ctx == context
        assert ds == _ds
        return [(statuses.C_FIND_PENDING, dataset.Dataset()),
                (statuses.C_FIND_PENDING, dataset.Dataset())]

    ctx = PContextDef(1, uids.STUDY_ROOT_FIND_SOP_CLASS, ImplicitVRLittleEndian)
    _ds = dataset.Dataset()
    ae_title.bus.subscribe(ae.AEChannels.FIND, callback)
    results = ae_title.on_receive_find(ctx, _ds)
    for status, ds in results:
        assert status.is_pending
        assert ds is not None


def test_store_success(ae_title: ae.AE):
    def callback(context, ds):
        assert ctx == context
        assert ds == _ds
        return statuses.SUCCESS

    ctx = PContextDef(1, uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian)
    _ds = dataset.Dataset()
    ae_title.bus.subscribe(ae.AEChannels.STORE, callback)
    status = ae_title.on_receive_store(ctx, _ds)
    assert status.is_success


def test_store_failure(ae_title: ae.AE):
    def callback(context, ds):
        assert ctx == context
        assert ds == _ds
        return statuses.C_MOVE_UNABLE_TO_PROCESS

    ctx = PContextDef(1, uids.BASIC_TEXT_SR_STORAGE, ImplicitVRLittleEndian)
    _ds = dataset.Dataset()
    ae_title.bus.subscribe(ae.AEChannels.STORE, callback)
    status = ae_title.on_receive_store(ctx, _ds)
    assert status.is_failure


def test_move(ae_title: ae.AE):
    def callback(context, ds, destination):
        assert destination == 'REMOTE_PACS'
        assert ctx == context
        assert ds == _ds
        return [dataset.Dataset(), dataset.Dataset()]

    ctx = PContextDef(1, uids.STUDY_ROOT_MOVE_SOP_CLASS, ImplicitVRLittleEndian)
    _ds = dataset.Dataset()
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
    ae_title.bus.subscribe(ae.AEChannels.MOVE, callback)
    results = ae_title.on_receive_move(ctx, _ds, 'REMOTE_PACS')
    assert len(list(results)) == 2
