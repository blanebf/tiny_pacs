"""End-to-end console tests against live tiny_pacs servers.

Starts real servers (AE + event bus + the core shared ``HttpServer``)
with the ``AdminWeb`` component and — where relevant — the real
``DeviceStore``/``Users`` extension components providing the listeners
(imported by the test process only), then drives the console over HTTP
with ``urllib``.
"""
import logging
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pydicom
import pytest
from pydicom import uid
from pynetdicom2 import uids
from tiny_pacs import client as core_client
from tiny_pacs import config as core_config
from tiny_pacs import devices as core_devices
from tiny_pacs import events as core_events
from tiny_pacs import http as core_http
from tiny_pacs import server as core_server

from tiny_pacs_admin_web import web as web_module
from tiny_pacs_admin_web.component import AdminWeb
from tiny_pacs_admin_web.models import WebGrantModel, _utcnow

from .conftest import HttpClient, csrf_token

MAIN_AET = 'TINY_PACS'


@pytest.fixture
def web_server(tmp_path: Path) -> Iterator[Callable[..., SimpleNamespace]]:
    """Starts live servers with the console enabled.

    Factory options:

    * ``extensions`` (default True): enable the real ``DeviceStore`` and
      ``Users`` components;
    * ``components``: full replacement of the components section;
    * ``console``: ``AdminWeb`` config overrides;
    * ``http_config``: ``HttpServer`` config overrides (default:
      ephemeral port);
    * ``users`` / ``grants``: seeded PACS users and console grants.

    :yield: factory returning a namespace with ``srv``, ``web`` (the
            ``AdminWeb`` instance), ``http`` (the ``HttpServer``
            instance), ``console`` (an :class:`HttpClient`) and
            ``ae_port``
    """
    servers: list[core_server.Server] = []

    def start(
            extensions: bool = True,
            components: dict[str, Any] | None = None,
            console: dict[str, Any] | None = None,
            http_config: dict[str, Any] | None = None,
            users: Sequence[tuple[str, str]] = (),
            grants: Sequence[tuple[str, str]] = ()
    ) -> SimpleNamespace:
        if components is None:
            components = {
                'Database': {
                    'on': True, 'db_name':
                    str(tmp_path / f'{uuid.uuid4().hex}.db'),
                    'mode': 'rwc', 'uri': False
                },
                'PACS': {'on': True},
                'InMemoryStorage': {'on': True},
                'Devices': {'on': True, 'auto_add': False},
                'HttpServer': {'on': True, 'port': 0,
                               **(http_config or {})},
                'AdminWeb': {'on': True, **(console or {})}
            }
            if extensions:
                components['DeviceStore'] = {'on': True}
                components['Users'] = {'on': True}
        conf = core_config.Config()
        conf.update_config({'ae': {'port': 0}, 'components': components})
        srv = core_server.Server(conf)
        srv.start()
        servers.append(srv)
        web = next(
            component for component in srv.components
            if isinstance(component, AdminWeb)
        )
        http_server = next(
            (component for component in srv.components
             if isinstance(component, core_http.HttpServer)), None
        )
        for username, password in users:
            srv.bus.send_one(core_events.UserAdd, {
                'username': username, 'password': password
            })
        for username, role in grants:
            WebGrantModel.create(username=username, role=role,
                                 created=_utcnow())
        assert srv.ae is not None
        web_port = http_server.web_port if http_server is not None else None
        return SimpleNamespace(
            srv=srv,
            web=web,
            http=http_server,
            console=(HttpClient(web_port) if web_port is not None else None),
            ae_port=int(srv.ae.server.server_address[1])
        )

    yield start

    for srv in reversed(servers):
        srv.exit()


def _login(env: SimpleNamespace, username: str = 'alice',
           password: str = 'secret') -> HttpClient:
    client: HttpClient = env.console
    assert client is not None
    response = client.post('/login',
                           {'username': username, 'password': password})
    assert response.status == 200
    assert 'Dashboard' in response.text
    return client


def _admin_env(start: Callable[..., Any]) -> SimpleNamespace:
    env: SimpleNamespace = start(
        users=[('alice', 'secret')], grants=[('alice', 'admin')]
    )
    return env


@contextmanager
def component_log(name: str = 'AdminWeb'
                  ) -> Iterator[list[logging.LogRecord]]:
    """Captures records of one component logger around a server start.

    ``Server`` applies ``dictConfig`` at construction, which replaces the
    root handlers (including pytest's caplog handler); attaching directly
    to the component logger survives that.
    """
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(name)
    handler = _Capture(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


def _store_ds() -> pydicom.Dataset:
    ds = pydicom.Dataset()
    ds.PatientName = 'Test^Patient'
    ds.PatientID = 'P1'
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.StudyInstanceUID = uid.generate_uid()
    ds.SeriesInstanceUID = uid.generate_uid()
    ds.SOPInstanceUID = uid.generate_uid()
    ds.SOPClassUID = uids.BASIC_TEXT_SR_STORAGE
    return ds


# -----------------------------------------------------------------------
# Happy paths
# -----------------------------------------------------------------------

def test_login_and_dashboard(web_server: Callable[..., Any]) -> None:
    env = _admin_env(web_server)
    client = _login(env)
    page = client.get('/')
    assert page.status == 200
    assert 'Dashboard' in page.text
    assert 'AdminWeb' in page.text
    assert 'tiny_pacs_admin_web' in page.text, 'component origins shown'
    assert 'DeviceStore' in page.text and 'Users' in page.text


def test_dashboard_reflects_stored_dataset(
        web_server: Callable[..., Any]) -> None:
    env = _admin_env(web_server)
    client = _login(env)
    assert 'Storage statistics are not available' not in client.get('/').text
    device = core_devices.DeviceConfig(
        aet=MAIN_AET, address='127.0.0.1', port=env.ae_port
    )
    dicom = core_client.DICOMClient('MODALITY', device)
    dicom.store(_store_ds(), uids.BASIC_TEXT_SR_STORAGE,
                uid.ImplicitVRLittleEndian)
    page = client.get('/')
    assert '1 / 1 / 0' in page.text, 'stored record headline'


def test_device_crud_through_console(web_server: Callable[..., Any]
                                     ) -> None:
    env = _admin_env(web_server)
    client = _login(env)
    token = csrf_token(client.get('/devices').text)
    added = client.post('/devices/add', {
        'csrf_token': token, 'aet': 'MRI_01', 'address': '10.0.0.20',
        'port': '11112', 'identity': 'password', 'username': 'pacs',
        'password': 'out-secret'
    })
    assert added.status == 200
    assert 'MRI_01' in added.text
    assert 'out-secret' not in added.text
    rows = env.srv.bus.send_one(core_events.DeviceList, None)
    assert [(row.aet, row.address, row.port) for row in rows] \
        == [('MRI_01', '10.0.0.20', 11112)]
    # Edit through the console
    detail = client.get('/devices/MRI_01')
    edited = client.post('/devices/MRI_01/edit', {
        'csrf_token': csrf_token(detail.text), 'address': '10.0.0.21',
        'port': '11113', 'identity': 'password', 'username': 'pacs'
    })
    assert edited.status == 200
    config = env.srv.bus.send_any(core_events.DeviceByAE, 'MRI_01')
    assert config is not None
    assert config.address == '10.0.0.21' and config.port == 11113
    assert config.password == 'out-secret', 'untouched password kept'
    # Delete through the console
    token = csrf_token(client.get('/devices/MRI_01').text)
    client.post('/devices/MRI_01/delete', {'csrf_token': token})
    assert env.srv.bus.send_one(core_events.DeviceList, None) == []


# A connection refusal makes pynetdicom2's internal DUL thread raise an
# unhandled NetDICOMError of its own (library behaviour for any failed
# SCU association, independent of this extension); the console reports
# the failure through the JSON API, which is what this test asserts.
@pytest.mark.filterwarnings(
    'ignore::pytest.PytestUnhandledThreadExceptionWarning'
)
def test_echo_against_the_running_ae(web_server: Callable[..., Any],
                                     ) -> None:
    env = _admin_env(web_server)
    # The client component answers GetClient; installed by the test
    # process only (it is not part of the default registries)
    core_client.Client(env.srv.bus, {})
    env.srv.bus.send_one(core_events.DeviceAdd, {
        'aet': MAIN_AET, 'address': '127.0.0.1', 'port': env.ae_port
    })
    client = _login(env)
    token = csrf_token(client.get('/').text)
    response = client.post(f'/api/echo/{MAIN_AET}', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 200
    payload = response.json
    assert payload['ok'] is True
    assert payload['elapsed_ms'] >= 0
    # A device that refuses connections reports a failure, not a crash
    env.srv.bus.send_one(core_events.DeviceAdd, {
        'aet': 'DEAD_PEER', 'address': '127.0.0.1', 'port': 1
    })
    response = client.post('/api/echo/DEAD_PEER', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 502
    assert response.json['ok'] is False


def test_user_management_through_console(web_server: Callable[..., Any]
                                         ) -> None:
    env = _admin_env(web_server)
    client = _login(env)
    page = client.get('/users/')
    token = csrf_token(page.text)
    added = client.post('/users/add', {
        'csrf_token': token, 'username': 'carol', 'password': 'initial'
    })
    assert added.status == 200
    assert 'carol' in added.text
    user = env.srv.bus.send_one(core_events.UserByName, 'carol')
    assert user is not None and user.is_active
    # Rotate the password through the console
    page = client.get('/users/carol/password')
    client.post('/users/carol/password', {
        'csrf_token': csrf_token(page.text), 'password': 'rotated'
    })
    # The rotation is visible to the registry: the new password
    # verifies, the old one no longer does
    verifier = env.srv.bus.send_any(
        core_events.UserVerify,
        {'username': 'carol', 'password': 'rotated'}
    )
    assert verifier is not None
    assert env.srv.bus.send_any(
        core_events.UserVerify,
        {'username': 'carol', 'password': 'initial'}
    ) is None


# -----------------------------------------------------------------------
# Degradation
# -----------------------------------------------------------------------

def test_devices_page_degrades_without_registry(
        web_server: Callable[..., Any], tmp_path: Path) -> None:
    """Users registry present (login works), no DeviceStore: the devices
    page degrades to "not available" instead of failing."""
    env = web_server(
        components={
            'Database': {
                'on': True,
                'db_name': str(tmp_path / f'{uuid.uuid4().hex}.db'),
                'mode': 'rwc', 'uri': False
            },
            'PACS': {'on': True},
            'InMemoryStorage': {'on': True},
            'Users': {'on': True},
            'HttpServer': {'on': True, 'port': 0},
            'AdminWeb': {'on': True}
        },
        users=[('alice', 'secret')],
        grants=[('alice', 'admin')]
    )
    client = _login(env)
    page = client.get('/devices/')
    assert page.status == 200
    assert 'not available' in page.text
    assert 'Traceback' not in page.text
    # The users section keeps working
    assert 'alice' in client.get('/users/').text


def test_console_disabled_without_user_registry_ae_keeps_serving(
        web_server: Callable[..., Any]) -> None:
    with component_log() as records:
        env = web_server(extensions=False)
    # The console contributes no app ("disabled" = "not mounted"): the
    # shared HTTP server has zero hooks and stays dormant
    assert env.srv.bus.send_one(core_events.HttpMountsQuery, None) == []
    assert env.http is not None
    assert env.http.web_port is None
    assert env.console is None
    assert any(record.levelno >= logging.WARNING
               and 'UserVerify' in record.getMessage()
               for record in records)
    # The AE still serves C-ECHO
    device = core_devices.DeviceConfig(
        aet=MAIN_AET, address='127.0.0.1', port=env.ae_port
    )
    core_client.DICOMClient('MODALITY', device).echo()


def test_port_conflict_console_disabled_ae_keeps_serving(
        web_server: Callable[..., Any]) -> None:
    first = _admin_env(web_server)
    assert first.http is not None
    taken_port = first.http.web_port
    assert taken_port is not None
    with component_log('HttpServer') as records:
        second = web_server(
            extensions=True, http_config={'port': taken_port},
            users=[('alice', 'secret')], grants=[('alice', 'admin')]
        )
    assert second.http is not None
    assert second.http.web_port is None
    assert second.console is None
    assert any(record.levelno >= logging.CRITICAL
               and 'Cannot bind' in record.getMessage()
               for record in records)
    # The second AE is untouched and serves C-ECHO
    device = core_devices.DeviceConfig(
        aet=MAIN_AET, address='127.0.0.1', port=second.ae_port
    )
    core_client.DICOMClient('MODALITY', device).echo()
    # The first console is still serving
    assert first.console is not None
    assert first.console.get('/login').status == 200


def test_role_enforcement_end_to_end(web_server: Callable[..., Any]
                                     ) -> None:
    env = web_server(
        users=[('alice', 'secret'), ('bob', 'wonder')],
        grants=[('alice', 'admin'), ('bob', 'viewer')]
    )
    assert env.http is not None and env.http.web_port is not None
    viewer = HttpClient(env.http.web_port)
    response = viewer.post('/login',
                           {'username': 'bob', 'password': 'wonder'})
    assert response.status == 200
    assert 'viewer' in response.text
    page = viewer.get('/devices/')
    assert page.status == 200
    assert 'Add device' not in page.text
    assert viewer.get('/users/', follow=False).status == 403
    token = csrf_token(page.text)
    mutation = viewer.post(
        '/devices/add',
        {'csrf_token': token, 'aet': 'X', 'address': '10.0.0.5'},
        follow=False
    )
    assert mutation.status == 403
    assert env.srv.bus.send_one(core_events.DeviceList, None) == []


# -----------------------------------------------------------------------
# Archive browser through the shared HTTP server
# -----------------------------------------------------------------------

def _store_via_dicom(env: SimpleNamespace, patient_id: str,
                     name: str = 'Test^Patient',
                     study_date: str = '20200115') -> pydicom.Dataset:
    """Stores one dataset through a real C-STORE association."""
    ds = pydicom.Dataset()
    ds.PatientName = name
    ds.PatientID = patient_id
    ds.PatientBirthDate = '19800101'
    ds.PatientSex = 'M'
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.StudyDate = study_date
    ds.AccessionNumber = f'ACC-{patient_id}'
    ds.StudyDescription = 'Chest CT'
    ds.Modality = 'CT'
    ds.SeriesNumber = '1'
    ds.InstanceNumber = '1'
    ds.StudyInstanceUID = uid.generate_uid()
    ds.SeriesInstanceUID = uid.generate_uid()
    ds.SOPInstanceUID = uid.generate_uid()
    ds.SOPClassUID = uids.BASIC_TEXT_SR_STORAGE
    device = core_devices.DeviceConfig(
        aet=MAIN_AET, address='127.0.0.1', port=env.ae_port
    )
    core_client.DICOMClient('MODALITY', device).store(
        ds, uids.BASIC_TEXT_SR_STORAGE, uid.ImplicitVRLittleEndian
    )
    return ds


def test_archive_through_shared_server(web_server: Callable[..., Any],
                                       monkeypatch: pytest.MonkeyPatch
                                       ) -> None:
    env = _admin_env(web_server)
    client = _login(env)
    first = _store_via_dicom(env, 'P1')
    _store_via_dicom(env, 'P2', name='Smith^Ann', study_date='20210601')

    # The patients table shows both with drill-down links
    page = client.get('/archive/')
    assert page.status == 200
    assert 'P1' in page.text and 'P2' in page.text
    assert '/archive/studies/P1' in page.text
    assert '2 match(es)' in page.text

    # Substring search on the patient name
    page = client.get('/archive/?patient_name=Smi')
    assert 'P2' in page.text
    assert '/archive/studies/P1' not in page.text

    # Study date range search (the form submits YYYY-MM-DD dates)
    page = client.get('/archive/?study_date_from=2021-01-01')
    assert 'P2' in page.text
    assert '/archive/studies/P1' not in page.text

    # Drill-down: studies -> series -> instances with pre-filled UIDs
    studies = client.get('/archive/studies/P1')
    assert studies.status == 200
    assert first.StudyInstanceUID in studies.text
    assert f'/archive/series/{first.StudyInstanceUID}' in studies.text
    series = client.get(f'/archive/series/{first.StudyInstanceUID}')
    assert series.status == 200
    assert first.SeriesInstanceUID in series.text
    assert f'/archive/instances/{first.SeriesInstanceUID}' in series.text
    assert 'value="' + first.StudyInstanceUID + '"' in series.text, \
        'the pinned UID filter is pre-filled'
    instances = client.get(f'/archive/instances/{first.SeriesInstanceUID}')
    assert instances.status == 200
    assert first.SOPInstanceUID in instances.text

    # Pagination against ArchiveItem.total with a fixed page size
    monkeypatch.setattr(web_module, 'ARCHIVE_PAGE_SIZE', 1)
    page = client.get('/archive/')
    assert 'P1' in page.text and 'P2' not in page.text
    assert '2 match(es) — page 1 of 2' in page.text
    page = client.get('/archive/?offset=1')
    assert 'P2' in page.text
    assert 'page 2 of 2' in page.text
