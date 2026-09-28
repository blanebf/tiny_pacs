"""End-to-end DICOMweb tests against live tiny_pacs servers.

Starts real servers (AE + event bus + file storage + the core shared
``HttpServer``) with the ``DICOMWeb`` component and — where relevant —
the real ``Users``/``AuditLog`` extension components providing the
listeners (imported by the test process only), then drives the service
over HTTP with ``urllib`` and the archive over real DIMSE associations
with the core ``DICOMClient``.
"""
import json
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pydicom
import pytest
from pydicom import uid as pydicom_uid
from pynetdicom2 import uids
from tiny_pacs import client as core_client
from tiny_pacs import config as core_config
from tiny_pacs import devices as core_devices
from tiny_pacs import events as core_events
from tiny_pacs import http as core_http
from tiny_pacs import server as core_server

from .conftest import (
    RawHTTPClient,
    make_dataset,
    parse_multipart,
    stow_push,
)

MAIN_AET = 'TINY_PACS'


@pytest.fixture
def dicomweb_server(tmp_path: Path) -> Iterator[Callable[..., Any]]:
    """Starts live servers with DICOMweb enabled.

    Factory options:

    * ``config``: ``DICOMWeb`` config overrides;
    * ``http_config``: ``HttpServer`` overrides (default ephemeral
      port);
    * ``with_users`` / ``with_audit``: start the real identity/audit
      extension components;
    * ``users``: seeded ``(username, password)`` accounts.

    :yield: factory returning a namespace with ``srv``, ``client`` (an
            :class:`RawHTTPClient` against the shared HTTP server),
            ``http``, ``port``, ``ae_port``, ``storage_dir`` and a
            ``dicom`` factory for DIMSE clients
    """
    servers: list[core_server.Server] = []

    def start(
            config: dict[str, Any] | None = None,
            http_config: dict[str, Any] | None = None,
            with_users: bool = False,
            with_audit: bool = False,
            users: tuple[tuple[str, str], ...] = ()
    ) -> SimpleNamespace:
        storage_dir = tmp_path / f'storage_{uuid.uuid4().hex}'
        components: dict[str, Any] = {
            'Database': {
                'on': True,
                'db_name': str(tmp_path / f'{uuid.uuid4().hex}.db'),
                'mode': 'rwc', 'uri': False
            },
            'PACS': {'on': True},
            'FileStorage': {'on': True, 'storage_dir': str(storage_dir)},
            'InMemoryStorage': {'on': False},
            'TempFileStorage': {'on': False},
            'Devices': {'on': True, 'auto_add': False},
            'HttpServer': {'on': True, 'port': 0, **(http_config or {})},
            'DICOMWeb': {'on': True, **(config or {})}
        }
        if with_users:
            components['Users'] = {'on': True}
        if with_audit:
            components['AuditLog'] = {'on': True}
        conf = core_config.Config()
        conf.update_config(
            {'ae': {'port': 0}, 'components': components}
        )
        srv = core_server.Server(conf)
        srv.start()
        servers.append(srv)
        for username, password in users:
            srv.bus.send_one(core_events.UserAdd, {
                'username': username, 'password': password
            })
        http_server = next(
            component for component in srv.components
            if isinstance(component, core_http.HttpServer)
        )
        port = http_server.web_port
        assert port is not None and port != 0
        assert srv.ae is not None
        ae_port = int(srv.ae.server.server_address[1])

        def dicom_client(local_ae: str = 'MODALITY'
                         ) -> core_client.DICOMClient:
            device = core_devices.DeviceConfig(
                aet=MAIN_AET, address='127.0.0.1', port=ae_port
            )
            return core_client.DICOMClient(local_ae, device)

        return SimpleNamespace(
            srv=srv,
            http=http_server,
            port=port,
            client=RawHTTPClient(port),
            ae_port=ae_port,
            storage_dir=storage_dir,
            dicom=dicom_client
        )

    yield start

    for srv in reversed(servers):
        srv.exit()


def _stored_bytes(bus: Any, ds: pydicom.Dataset) -> bytes:
    """Reads the stored file of one dataset through ``GetFiles``."""
    stored = list(bus.send_any(
        core_events.GetFiles, [str(ds.SOPInstanceUID)]
    ))
    assert len(stored) == 1, 'exactly one stored file'
    with open(str(stored[0][2]), 'rb') as handle:
        return handle.read()


# -----------------------------------------------------------------------
# DIMSE store -> WADO/QIDO retrieval round-trip
# -----------------------------------------------------------------------

def test_dimse_store_then_wado_round_trip(
        dicomweb_server: Callable[..., Any]) -> None:
    """A dataset pushed through a real C-STORE comes back byte-identical
    through WADO-RS and WADO-URI (the DIMSE→HTTP round-trip)."""
    env = dicomweb_server()
    ds = make_dataset(patient_id='E2E1', study_date='20240102')
    client = env.dicom()
    client.store(ds, uids.BASIC_TEXT_SR_STORAGE,
                 pydicom_uid.ImplicitVRLittleEndian)
    stored = _stored_bytes(env.srv.bus, ds)

    response = env.client.get(
        f'/dicomweb/studies/{ds.StudyInstanceUID}/series/'
        f'{ds.SeriesInstanceUID}/instances/{ds.SOPInstanceUID}'
    )
    assert response.status == 200
    assert response.body == stored, 'WADO-RS byte-compares'

    response = env.client.get(
        f'/dicomweb/wado?requestType=WADO'
        f'&studyUID={ds.StudyInstanceUID}'
        f'&seriesUID={ds.SeriesInstanceUID}'
        f'&objectUID={ds.SOPInstanceUID}'
        f'&contentType=application/dicom'
    )
    assert response.status == 200
    assert response.body == stored, 'WADO-URI byte-compares'

    # study-level retrieval streams the same object in a multipart body
    response = env.client.get(f'/dicomweb/studies/{ds.StudyInstanceUID}')
    assert response.status == 200
    assert parse_multipart(response) == [stored]


def test_dimse_store_then_qido_round_trip(
        dicomweb_server: Callable[..., Any]) -> None:
    env = dicomweb_server()
    ds = make_dataset(patient_id='E2E2', patient_name='Live^Server')
    env.dicom().store(ds, uids.BASIC_TEXT_SR_STORAGE,
                      pydicom_uid.ImplicitVRLittleEndian)
    response = env.client.get('/dicomweb/studies?PatientID=E2E2')
    assert response.status == 200
    items = response.json
    assert len(items) == 1
    assert items[0]['00100020']['Value'] == ['E2E2']
    assert items[0]['00100010']['Value'] \
        == [{'Alphabetic': 'Live^Server'}]
    response = env.client.get(
        f'/dicomweb/studies/{ds.StudyInstanceUID}/series/'
        f'{ds.SeriesInstanceUID}/instances'
    )
    assert response.status == 200
    assert len(response.json) == 1


# -----------------------------------------------------------------------
# STOW over live HTTP -> DIMSE C-FIND
# -----------------------------------------------------------------------

def test_stow_is_visible_to_c_find(
        dicomweb_server: Callable[..., Any]) -> None:
    env = dicomweb_server()
    ds = make_dataset(patient_id='E2E3', accession='STOWE2E')
    response = stow_push(env.client, [ds], prefix='/dicomweb')
    assert response.status == 200
    assert len(response.json.get('00081199', {}).get('Value', [])) == 1

    identifier = pydicom.Dataset()
    identifier.QueryRetrieveLevel = 'STUDY'
    identifier.PatientID = 'E2E3'
    identifier.AccessionNumber = None
    identifier.StudyInstanceUID = None
    found = list(env.dicom().find(identifier))
    studies = {str(row.StudyInstanceUID) for row in found
               if row.get('StudyInstanceUID')}
    assert studies == {str(ds.StudyInstanceUID)}

    # the stored file is served back byte-identically over WADO
    stored = _stored_bytes(env.srv.bus, ds)
    response = env.client.get(
        f'/dicomweb/studies/{ds.StudyInstanceUID}/series/'
        f'{ds.SeriesInstanceUID}/instances/{ds.SOPInstanceUID}'
    )
    assert response.status == 200 and response.body == stored


# -----------------------------------------------------------------------
# Authentication against the real user registry
# -----------------------------------------------------------------------

def _basic(username: str, password: str) -> dict[str, str]:
    import base64
    token = base64.b64encode(
        f'{username}:{password}'.encode()
    ).decode('ascii')
    return {'Authorization': f'Basic {token}'}


def test_basic_auth_with_the_real_user_registry(
        dicomweb_server: Callable[..., Any]) -> None:
    env = dicomweb_server(
        config={'auth': 'basic'}, with_users=True,
        users=(('alice', 's3cret-pass'),)
    )
    client = env.client
    assert client.get('/dicomweb/studies').status == 401
    assert client.get('/dicomweb/studies',
                      headers=_basic('alice', 'wrong')).status == 401
    response = client.get('/dicomweb/studies',
                          headers=_basic('alice', 's3cret-pass'))
    assert response.status == 204

    ds = make_dataset(patient_id='E2E4')
    assert stow_push(client, [ds], prefix='/dicomweb').status == 401
    assert stow_push(client, [ds], prefix='/dicomweb',
                     headers=_basic('alice', 's3cret-pass')).status == 200


def test_token_auth_end_to_end(
        dicomweb_server: Callable[..., Any]) -> None:
    env = dicomweb_server(config={'auth': 'token',
                                  'tokens': ['e2e-token']})
    assert env.client.get('/dicomweb/studies').status == 401
    assert env.client.get(
        '/dicomweb/studies',
        headers={'Authorization': 'Bearer e2e-token'}
    ).status == 204


# -----------------------------------------------------------------------
# Audit trail integration
# -----------------------------------------------------------------------

def test_stow_is_recorded_by_the_audit_log(
        dicomweb_server: Callable[..., Any]) -> None:
    env = dicomweb_server(
        config={'auth': 'basic'}, with_users=True, with_audit=True,
        users=(('bob', 'pw-bob'),)
    )
    ds = make_dataset(patient_id='E2E5')
    assert stow_push(env.client, [ds], prefix='/dicomweb',
                     headers=_basic('bob', 'pw-bob')).status == 200
    env.client.get('/dicomweb/studies', headers=_basic('bob', 'pw-bob'))

    rows = env.srv.bus.send_one(
        core_events.AuditQuery,
        core_events.AuditFilter(categories=['service'])
    )
    stow_rows = [row for row in rows if row.event == 'stow']
    qido_rows = [row for row in rows if row.event == 'qido']
    assert len(stow_rows) == 1
    details = json.loads(stow_rows[0].details)
    assert details['origin'] == 'stow'
    assert details['instances'] == 1
    assert stow_rows[0].username == 'bob'
    assert qido_rows and all(row.username == 'bob' for row in qido_rows)


# -----------------------------------------------------------------------
# Shared HTTP server integration
# -----------------------------------------------------------------------

def test_mount_is_reported_next_to_other_front_ends(
        dicomweb_server: Callable[..., Any]) -> None:
    """DICOMweb shares the single core ``HttpServer``; the mounts query
    reports the prefix the console would discover."""
    env = dicomweb_server(config={'prefix': '/dcm'})
    assert env.client.get('/dcm/studies').status == 204
    assert env.client.get('/dicomweb/studies').status == 404
    mounts = env.srv.bus.send_one(core_events.HttpMountsQuery, None)
    assert ('DICOMWeb', '/dcm') in mounts
