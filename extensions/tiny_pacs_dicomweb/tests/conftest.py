"""Shared fixtures and test clients for the DICOMweb extension tests.

Two drivers are provided:

* :class:`RawWSGIClient` — calls the built application in-process with
  hand-built WSGI environs (unit-level QIDO/WADO/STOW/auth suites, no
  sockets involved);
* :class:`RawHTTPClient` — drives a live service over ``urllib`` against
  the core shared ``HttpServer`` on an ephemeral port (component and
  E2E suites).

Both keep raw bodies intact (DICOM parts are binary) and never follow
redirects automatically.
"""
import http.cookiejar
import io
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pydicom
import pytest
import trolleybus
from pydicom import uid as pydicom_uid
from pynetdicom2 import uids
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events
from tiny_pacs import pacs as core_pacs
from tiny_pacs import storage as core_storage

from tiny_pacs_dicomweb.component import DICOMWeb

#: SOP class used by the generated test datasets
TEST_SOP_CLASS = uids.BASIC_TEXT_SR_STORAGE


def sqlite_config(db_path: str) -> dict[str, Any]:
    """File-based SQLite configuration for offline-style runs."""
    return {'driver': 'sqlite', 'db_name': db_path, 'uri': False}


@dataclass
class Response:
    """One captured HTTP/WSGI response."""

    status: int
    headers: list[tuple[str, str]]
    body: bytes

    @property
    def text(self) -> str:
        """Body decoded as UTF-8."""
        return self.body.decode('utf-8', 'replace')

    @property
    def json(self) -> Any:
        """Body parsed as JSON."""
        import json as json_module
        return json_module.loads(self.body.decode('utf-8'))

    def header(self, name: str) -> str | None:
        """First header value by (case-insensitive) name."""
        wanted = name.lower()
        for key, value in self.headers:
            if key.lower() == wanted:
                return value
        return None


# The Raw* prefix is deliberate: the admin-web conftest exposes clients
# with the historic names WSGIClient/HttpClient that take form mappings,
# follow redirects and carry cookies. These clients post raw bodies and
# never follow, so sharing the names would let code copied between the
# two suites silently change behavior.
class RawWSGIClient:
    """Drives a WSGI callable in-process (raw bodies, no redirects)."""

    def __init__(self, app: Any, remote_addr: str = '203.0.113.7') -> None:
        self.app = app
        self.remote_addr = remote_addr

    def get(self, path: str,
            headers: dict[str, str] | None = None) -> Response:
        """Performs a GET request (query string allowed in ``path``)."""
        return self.request('GET', path, None, None, headers)

    def post(self, path: str, body: bytes | None = None,
             content_type: str | None = None,
             headers: dict[str, str] | None = None) -> Response:
        """Performs a POST request with a raw body."""
        return self.request('POST', path, body, content_type, headers)

    def request(
            self,
            method: str,
            path: str,
            body: bytes | None,
            content_type: str | None,
            headers: dict[str, str] | None
    ) -> Response:
        """Performs one request and captures the complete response."""
        parts = urllib.parse.urlsplit(path)
        environ: dict[str, Any] = {
            'REQUEST_METHOD': method,
            'SCRIPT_NAME': '/dicomweb',
            'PATH_INFO': urllib.parse.unquote(parts.path),
            'QUERY_STRING': parts.query,
            'SERVER_NAME': 'localhost',
            'SERVER_PORT': '80',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'REMOTE_ADDR': self.remote_addr,
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': 'http',
            'wsgi.input': io.BytesIO(body or b''),
            'wsgi.errors': io.BytesIO(),
            'wsgi.multithread': True,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,
            'HTTP_HOST': 'localhost',
        }
        if body is not None:
            environ['CONTENT_LENGTH'] = str(len(body))
        if content_type is not None:
            environ['CONTENT_TYPE'] = content_type
        for key, value in (headers or {}).items():
            environ[f'HTTP_{key.upper().replace("-", "_")}'] = value
        captured: dict[str, Any] = {}

        def start_response(status: str, response_headers: Any,
                           exc_info: Any = None) -> Callable[[bytes], Any]:
            captured['status'] = status
            captured['headers'] = list(response_headers)
            return lambda chunk: None

        result = self.app(environ, start_response)
        payload = b''.join(result)
        close = getattr(result, 'close', None)
        if close is not None:
            close()
        return Response(
            status=int(captured['status'].split(' ', 1)[0]),
            headers=captured['headers'],
            body=payload
        )


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Redirect handler that surfaces 3xx responses instead of following
    them."""

    def redirect_request(
            self, req: Any, fp: Any, code: int, msg: str, headers: Any,
            newurl: str
    ) -> None:
        return None


class RawHTTPClient:
    """Drives a live service over ``urllib`` (raw bodies supported)."""

    def __init__(self, port: int) -> None:
        self.base = f'http://127.0.0.1:{port}'
        jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar), _NoRedirect()
        )

    def get(self, path: str,
            headers: dict[str, str] | None = None) -> Response:
        """Performs a GET request."""
        return self.request('GET', path, None, None, headers)

    def post(self, path: str, body: bytes | None = None,
             content_type: str | None = None,
             headers: dict[str, str] | None = None) -> Response:
        """Performs a POST request with a raw body."""
        return self.request('POST', path, body, content_type, headers)

    def request(
            self,
            method: str,
            path: str,
            body: bytes | None,
            content_type: str | None,
            headers: dict[str, str] | None
    ) -> Response:
        """Performs one request; redirects are surfaced, not followed."""
        url = path if path.startswith('http') else f'{self.base}{path}'
        request = urllib.request.Request(url, data=body, method=method)
        if content_type is not None:
            request.add_header('Content-Type', content_type)
        for key, value in (headers or {}).items():
            request.add_header(key, value)
        try:
            with self.opener.open(request, timeout=20) as raw:
                return Response(
                    status=raw.status,
                    headers=list(raw.headers.items()),
                    body=raw.read()
                )
        except urllib.error.HTTPError as error:
            return Response(
                status=error.code,
                headers=list(error.headers.items()),
                body=error.read()
            )


# -----------------------------------------------------------------------
# DICOM fixtures and multipart helpers
# -----------------------------------------------------------------------

def make_dataset(
        patient_id: str = 'P1',
        patient_name: str = 'Test^Patient',
        study_uid: str | None = None,
        series_uid: str | None = None,
        sop_uid: str | None = None,
        study_date: str = '20240102',
        modality: str = 'CT',
        accession: str = 'ACC1'
) -> pydicom.Dataset:
    """Builds one storable test dataset."""
    ds = pydicom.Dataset()
    ds.PatientName = patient_name
    ds.PatientID = patient_id
    ds.StudyDate = study_date
    ds.AccessionNumber = accession
    ds.Modality = modality
    ds.StudyInstanceUID = study_uid or pydicom_uid.generate_uid()
    ds.SeriesInstanceUID = series_uid or pydicom_uid.generate_uid()
    ds.SOPInstanceUID = sop_uid or pydicom_uid.generate_uid()
    ds.SOPClassUID = TEST_SOP_CLASS
    return ds


def dicom_file_bytes(
        ds: pydicom.Dataset,
        transfer_syntax: pydicom_uid.UID =
        pydicom_uid.ExplicitVRLittleEndian
) -> bytes:
    """Serializes a dataset as a PS3.10 DICOM file (bytes)."""
    meta = pydicom.dataset.FileMetaDataset()
    meta.MediaStorageSOPClassUID = pydicom_uid.UID(str(ds.SOPClassUID))
    meta.MediaStorageSOPInstanceUID = pydicom_uid.UID(
        str(ds.SOPInstanceUID)
    )
    meta.TransferSyntaxUID = transfer_syntax
    ds.file_meta = meta
    buffer = io.BytesIO()
    pydicom.dcmwrite(buffer, ds, enforce_file_format=True)
    return buffer.getvalue()


def multipart_body(
        parts: list[tuple[bytes, str]],
        boundary: str = 'tiny-pacs-test-boundary'
) -> tuple[bytes, str]:
    """Builds a ``multipart/related`` request body.

    :param parts: ``(payload, media type)`` pairs
    :param boundary: multipart boundary token
    :return: body bytes and the matching ``Content-Type`` header value
    """
    body = b''
    for payload, media in parts:
        body += f'--{boundary}\r\n'.encode('ascii')
        body += f'Content-Type: {media}\r\n\r\n'.encode('ascii')
        body += payload + b'\r\n'
    body += f'--{boundary}--\r\n'.encode('ascii')
    return body, (f'multipart/related; type="application/dicom"; '
                  f'boundary="{boundary}"')


def parse_multipart(response: Response) -> list[bytes]:
    """Extracts the part payloads of a ``multipart/related`` response.

    :param response: the captured response
    :return: payload bytes of every part (empty parts dropped)
    """
    content_type = response.header('Content-Type') or ''
    assert content_type.startswith('multipart/related'), content_type
    message = BytesParser().parsebytes(
        f'Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n'
        .encode('latin-1') + response.body
    )
    payloads: list[bytes] = []
    for part in message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        payload = part.get_payload(decode=True)
        if payload:
            payloads.append(bytes(payload))
    return payloads


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path
        ) -> Iterator[Callable[..., SimpleNamespace]]:
    """Builds headless DICOMweb environments on a real core stack.

    Factory options:

    * ``storage`` (default ``'file'``): ``'file'`` for a ``FileStorage``
      in a fresh temp directory, ``'memory'`` for ``InMemoryStorage``,
      ``'none'`` for no storage component;
    * ``pacs`` (default True): start the real core ``PACS`` component;
    * ``user_verify`` (default True): subscribe a fake ``UserVerify``
      listener accepting ``password='secret'`` for any username;
    * ``dicomweb`` (default True): construct the ``DICOMWeb`` component
      and take the app from its ``HttpAppsRegistry`` answer;
    * ``overwrite`` (default False): ``FileStorage`` overwrite policy;
    * ``config``: ``DICOMWeb`` configuration overrides;
    * ``collect_audit`` (default False): subscribe an ``AuditRecord``
      collector (``env.audit`` list).

    The started bus is returned as a namespace: ``bus``, ``database``,
    ``component``, ``app``, ``client`` (:class:`RawWSGIClient`),
    ``storage_dir`` and ``audit`` records. ``TINY_PACS_HEADLESS`` is
    removed for every build so the component contributes its app.
    """
    monkeypatch.delenv('TINY_PACS_HEADLESS', raising=False)
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []
    counter = {'n': 0}

    def build(
            storage: str = 'file',
            pacs: bool = True,
            user_verify: bool = True,
            dicomweb: bool = True,
            collect_audit: bool = False,
            overwrite: bool = False,
            config: dict[str, Any] | None = None
    ) -> SimpleNamespace:
        counter['n'] += 1
        run_id = f'{counter["n"]}_{uuid.uuid4().hex}'
        db_path = str(tmp_path / f'dicomweb_{run_id}.db')
        storage_dir = tmp_path / f'storage_{run_id}'
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, sqlite_config(db_path))
        storage_component: Any = None
        if storage == 'file':
            storage_component = core_storage.FileStorage(
                bus, {'on': True, 'storage_dir': str(storage_dir),
                      'overwrite': overwrite}
            )
        elif storage == 'memory':
            storage_component = core_storage.InMemoryStorage(
                bus, {'on': True}
            )
        if pacs:
            core_pacs.PACS(bus, {'on': True})
        if user_verify:
            bus.subscribe(
                core_events.UserVerify,
                lambda payload: (
                    SimpleNamespace(username=str(payload.get('username')))
                    if payload.get('password') == 'secret' else None
                )
            )
        audit_log: list[Any] = []
        if collect_audit:
            audit_log = []
            bus.subscribe(core_events.AuditRecord, audit_log.append)
        component = None
        app = None
        if dicomweb:
            component = DICOMWeb(bus, {'on': True, **(config or {})})
        bus.start()
        started.append((bus, database))
        if component is not None:
            hooks = component.http_apps()
            app = hooks[0].app if hooks else None
        return SimpleNamespace(
            bus=bus, database=database, component=component, app=app,
            client=(RawWSGIClient(app) if app is not None else None),
            storage=storage_component, storage_dir=storage_dir,
            db_path=db_path, audit=audit_log
        )

    yield build

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


def seed_archive(bus: trolleybus.EventBus, ds: pydicom.Dataset,
                 transfer_syntax: str = '1.2.840.10008.1.2.1') -> None:
    """Records one dataset in the archive without materializing a file.

    Mirrors what the AE broadcasts after decoding a C-STORE, minus the
    file materialization — used by the QIDO suites, which only need the
    database rows.
    """
    bus.broadcast(
        core_events.StoreDataset,
        core_events.StoreDatasetPayload(ds, transfer_syntax)
    )


def stow_push(client: Any, datasets: list[pydicom.Dataset],
              study_uid: str | None = None,
              headers: dict[str, str] | None = None,
              prefix: str = '') -> Response:
    """Pushes datasets through the STOW-RS endpoint.

    :param client: :class:`RawWSGIClient` or :class:`RawHTTPClient`
    :param datasets: datasets serialized as PS3.10 parts
    :param study_uid: target Study Instance UID (service form when None)
    :param headers: extra request headers (e.g. ``Authorization``)
    :param prefix: mount prefix to prepend (live-server requests)
    :return: the captured response
    """
    parts = [(dicom_file_bytes(ds), 'application/dicom')
             for ds in datasets]
    body, content_type = multipart_body(parts)
    path = (f'{prefix}/studies' if study_uid is None
            else f'{prefix}/studies/{study_uid}')
    response: Response = client.post(path, body, content_type, headers)
    return response
