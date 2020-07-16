# -*- coding: utf-8 -*-
import pydicom
import pytest

from pydicom import uid
from pynetdicom2 import applicationentity
from pynetdicom2 import sopclass
from pynetdicom2 import statuses
from pynetdicom2 import uids

from tiny_pacs import ae
from tiny_pacs import client
from tiny_pacs import config
from tiny_pacs import devices
from tiny_pacs import event_bus
from tiny_pacs import server

@pytest.fixture
def pacs():
    conf = config.Config()
    conf.update_config({
        'ae': {'port': 11113}
    })
    _pacs = server.Server(conf)
    _pacs.start()
    yield _pacs
    _pacs.exit()


@pytest.fixture
def pacs_client():
    def main_aet():
        return 'TEST_CLIENT'
    bus = event_bus.EventBus()
    bus.subscribe(ae.AEChannels.MAIN_AET, main_aet)
    _devices = devices.Devices(bus, {
        'devices': {
            'TINY_PACS': {'aet': 'TINY_PACS', 'address': '127.0.0.1', 'port': 11113}
        }
    })
    _client = client.Client(bus, {})
    return _client.get('TINY_PACS')


@pytest.fixture
def test_ds():
    ds = pydicom.Dataset()
    ds.PatientName = 'Test^Test^Test'
    ds.PatientSex = 'M'
    ds.PatientID = 'auto1'
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.StudyInstanceUID = uid.generate_uid()
    ds.SeriesInstanceUID = uid.generate_uid()
    ds.SOPInstanceUID = uid.generate_uid()
    ds.SOPClassUID = uids.BASIC_TEXT_SR_STORAGE
    return ds


def test_startup(pacs: server.Server, pacs_client: client.DICOMClient):
    pacs_client.echo()


def test_find_empty(pacs: server.Server, pacs_client: client.DICOMClient):
    request = pydicom.Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.NumberOfPatientRelatedStudies = None
    results = pacs_client.find(request)
    assert not list(results)


def test_move_empty(pacs: server.Server, pacs_client: client.DICOMClient):
    request = pydicom.Dataset()
    request.StudyInstanceUID = '1.2.3'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    pacs_client.move(request)


def test_storage(pacs: server.Server, pacs_client: client.DICOMClient, test_ds: pydicom.Dataset):
    pacs_client.store(test_ds, uids.BASIC_TEXT_SR_STORAGE, uid.ImplicitVRLittleEndian)


class CStoreAE(applicationentity.AE):
    def __init__(self, rq, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rq = rq

    def on_receive_store(self, context, ds):
        d = pydicom.dcmread(ds)
        assert context.sop_class == self.rq.SOPClassUID
        assert d.PatientName == self.rq.PatientName
        assert d.StudyInstanceUID == self.rq.StudyInstanceUID
        assert d.SeriesInstanceUID == self.rq.SeriesInstanceUID
        assert d.SOPInstanceUID == self.rq.SOPInstanceUID
        assert d.SOPClassUID == self.rq.SOPClassUID
        return statuses.SUCCESS


def test_full_cycle(pacs: server.Server, pacs_client: client.DICOMClient, test_ds: pydicom.Dataset):
    test_storage(pacs, pacs_client, test_ds)
    find_request = pydicom.Dataset()
    find_request.QueryRetrieveLevel = 'IMAGE'
    find_request.StudyInstanceUID = None
    find_request.SeriesInstanceUID = None
    find_request.SOPInstanceUID = None
    results = list(pacs_client.find(find_request))
    assert len(results) == 1
    ae = CStoreAE(test_ds, 'TEST_CLIENT', 11112)
    ae.add_scp(sopclass.storage_scp)
    with ae:
        move_request = pydicom.Dataset()
        move_request.QueryRetrieveLevel = 'IMAGE'
        move_request.StudyInstanceUID = test_ds.StudyInstanceUID
        move_request.SeriesInstanceUID = test_ds.SeriesInstanceUID
        move_request.SOPInstanceUID = test_ds.SOPInstanceUID
        pacs_client.move(move_request)
