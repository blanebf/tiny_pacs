import uuid
from collections.abc import Iterator
from typing import Any, BinaryIO

import pydicom
import pytest
import trolleybus
from pydicom import uid
from pynetdicom2 import applicationentity, fsm, sopclass, statuses, uids

from tiny_pacs import client, config, devices, events, server


@pytest.fixture
def pacs() -> Iterator[server.Server]:
    # Port 0 lets the OS pick a free port, so no fixed TCP ports are needed
    conf = config.Config()
    conf.update_config({
        'ae': {'port': 0},
        'components': {
            'Database': {'on': True, 'db_name': str(uuid.uuid4())}
        }
    })
    _pacs = server.Server(conf)
    _pacs.start()
    yield _pacs
    _pacs.exit()


def pacs_port(_pacs: server.Server) -> int:
    """Actual port the server AE has bound (port 0 = OS-assigned)."""
    assert _pacs.ae is not None
    return int(_pacs.ae.server.server_address[1])


def _client_for(_pacs: server.Server) -> client.DICOMClient:
    """Builds a DICOM client connected to a running server."""
    def main_aet(_: None) -> str:
        return 'TEST_CLIENT'
    bus = trolleybus.EventBus()
    bus.subscribe(events.MainAET, main_aet)
    devices.Devices(bus, {
        'devices': {
            'TINY_PACS': {
                'aet': 'TINY_PACS',
                'address': '127.0.0.1',
                'port': pacs_port(_pacs)
            }
        }
    })
    _client = client.Client(bus, {})
    return _client.get('TINY_PACS')


@pytest.fixture
def pacs_client(pacs: server.Server) -> client.DICOMClient:
    return _client_for(pacs)


@pytest.fixture
def test_ds() -> pydicom.Dataset:
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


def test_startup(pacs: server.Server, pacs_client: client.DICOMClient) -> None:
    pacs_client.echo()


def test_find_empty(pacs: server.Server,
                    pacs_client: client.DICOMClient) -> None:
    request = pydicom.Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.NumberOfPatientRelatedStudies = None
    results = pacs_client.find(request)
    assert not list(results)


def test_move_empty(pacs: server.Server,
                    pacs_client: client.DICOMClient) -> None:
    request = pydicom.Dataset()
    request.StudyInstanceUID = '1.2.3'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    pacs_client.move(request)


def test_storage(pacs: server.Server, pacs_client: client.DICOMClient,
                 test_ds: pydicom.Dataset) -> None:
    pacs_client.store(test_ds, uids.BASIC_TEXT_SR_STORAGE,
                      uid.ImplicitVRLittleEndian)


@pytest.fixture
def small_pool_pacs() -> Iterator[server.Server]:
    """Server whose pool has just one slot beyond the main thread's.

    The startup migrations leave the main thread holding one pooled
    connection, so with ``max_conn: 2`` only a single association thread
    fits at a time: associations succeed only while every per-association
    thread returns its connection to the pool when it is done.
    """
    conf = config.Config()
    conf.update_config({
        'ae': {'port': 0},
        'components': {
            'Database': {
                'on': True,
                'db_name': str(uuid.uuid4()),
                'max_conn': 2
            }
        }
    })
    _pacs = server.Server(conf)
    _pacs.start()
    yield _pacs
    _pacs.exit()


def test_store_releases_pool_connections(
        small_pool_pacs: server.Server,
        test_ds: pydicom.Dataset) -> None:
    """Stores over more associations than the pool has slots must work.

    Every C-STORE runs on fresh threads (the connection thread and the
    per-association DUL thread) that both check out pooled connections.
    Leaked connections used to abort the store with
    ``playhouse.pool.MaxConnectionsExceeded`` once the pool filled up.
    """
    pacs_client = _client_for(small_pool_pacs)
    for _ in range(4):
        ds = test_ds.copy()
        ds.SOPInstanceUID = uid.generate_uid()
        pacs_client.store(ds, uids.BASIC_TEXT_SR_STORAGE,
                          uid.ImplicitVRLittleEndian)


class CStoreAE(applicationentity.AE):
    def __init__(self, rq: pydicom.Dataset, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.rq = rq

    def on_receive_store(self, context: fsm.PContextDef,
                         ds: BinaryIO | bytes) -> statuses.Status:
        d = pydicom.dcmread(ds)
        assert context.sop_class == self.rq.SOPClassUID
        assert d.PatientName == self.rq.PatientName
        assert d.StudyInstanceUID == self.rq.StudyInstanceUID
        assert d.SeriesInstanceUID == self.rq.SeriesInstanceUID
        assert d.SOPInstanceUID == self.rq.SOPInstanceUID
        assert d.SOPClassUID == self.rq.SOPClassUID
        return statuses.SUCCESS


def test_full_cycle(pacs: server.Server, pacs_client: client.DICOMClient,
                    test_ds: pydicom.Dataset) -> None:
    # The storage AE binds an OS-assigned port up front; the server
    # auto-adds the TEST_CLIENT device on the first association and uses
    # its ``default_port`` for C-MOVE sub-operations, so point it at the
    # actual bound port before any association is made.
    ae = CStoreAE(test_ds, 'TEST_CLIENT', 0)
    ae.add_scp(sopclass.storage_scp)
    _devices = next(
        c for c in pacs.components if isinstance(c, devices.Devices))
    _devices.default_port = ae.server.server_address[1]
    with ae:
        test_storage(pacs, pacs_client, test_ds)
        find_request = pydicom.Dataset()
        find_request.QueryRetrieveLevel = 'IMAGE'
        find_request.StudyInstanceUID = None
        find_request.SeriesInstanceUID = None
        find_request.SOPInstanceUID = None
        results = list(pacs_client.find(find_request))
        assert len(results) == 1
        move_request = pydicom.Dataset()
        move_request.QueryRetrieveLevel = 'IMAGE'
        move_request.StudyInstanceUID = test_ds.StudyInstanceUID
        move_request.SeriesInstanceUID = test_ds.SeriesInstanceUID
        move_request.SOPInstanceUID = test_ds.SOPInstanceUID
        pacs_client.move(move_request)
