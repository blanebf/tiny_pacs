"""Shared fixtures and test clients for the admin-web extension tests.

Two drivers are provided:

* :class:`WSGIClient` — calls the built application in-process with
  hand-built WSGI environs (unit-level routing, sessions, CSRF, roles
  and escaping regressions, no sockets involved);
* :class:`HttpClient` — drives a live console over ``urllib`` against an
  ephemeral port (component and E2E suites).

Both keep a cookie jar and never follow cross-origin redirects. Console
sessions require a user registry answering ``UserVerify`` plus a
``WebGrant`` row; the helpers below seed both.
"""
import http.cookiejar
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import trolleybus
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events
from tiny_pacs_admin import store as admin_store
from tiny_pacs_identity.users import Users

from tiny_pacs_admin_web import web as web_module
from tiny_pacs_admin_web.component import AdminWeb
from tiny_pacs_admin_web.models import WebGrantModel, _utcnow

#: Matches the per-session CSRF token rendered into every page
CSRF_RE = re.compile(
    r'<meta name="csrf-token" content="([^"]+)"'
)


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

    def headers_named(self, name: str) -> list[str]:
        """Every header value by (case-insensitive) name."""
        wanted = name.lower()
        return [value for key, value in self.headers
                if key.lower() == wanted]


def _form_encode(data: Mapping[str, Any] | None) -> bytes | None:
    if data is None:
        return None
    return urllib.parse.urlencode(
        {key: str(value) for key, value in data.items()}
    ).encode('utf-8')


class WSGIClient:
    """Drives a WSGI callable in-process with a persistent cookie jar."""

    def __init__(self, app: Any, remote_addr: str = '203.0.113.7') -> None:
        self.app = app
        self.remote_addr = remote_addr
        self.cookies: dict[str, str] = {}

    def request(
            self,
            method: str,
            path: str,
            data: Mapping[str, Any] | None = None,
            headers: Mapping[str, str] | None = None,
            follow: bool = True
    ) -> Response:
        """Performs one request, following same-origin redirects when
        asked to."""
        response = self._once(method, path, data, headers)
        hops = 0
        while (follow and response.status in (301, 302, 303, 307, 308)
               and hops < 10):
            location = response.header('Location')
            if not location:
                break
            # 303/302 after POST switch to GET; this app only issues 303
            method = 'GET'
            data = None
            path = location
            response = self._once(method, path, None, headers)
            hops += 1
        return response

    def _once(
            self,
            method: str,
            path: str,
            data: Mapping[str, Any] | None,
            headers: Mapping[str, str] | None
    ) -> Response:
        parts = urllib.parse.urlsplit(path)
        body = _form_encode(data)
        environ: dict[str, Any] = {
            'REQUEST_METHOD': method,
            'SCRIPT_NAME': '',
            'PATH_INFO': urllib.parse.unquote(parts.path),
            'QUERY_STRING': parts.query,
            'SERVER_NAME': 'localhost',
            'SERVER_PORT': '80',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'REMOTE_ADDR': self.remote_addr,
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': 'http',
            'wsgi.input': BytesIO(body or b''),
            'wsgi.errors': BytesIO(),
            'wsgi.multithread': True,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,
        }
        if body is not None:
            environ['CONTENT_TYPE'] = 'application/x-www-form-urlencoded'
            environ['CONTENT_LENGTH'] = str(len(body))
        if self.cookies:
            environ['HTTP_COOKIE'] = '; '.join(
                f'{name}={value}' for name, value in self.cookies.items()
            )
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
        response = Response(
            status=int(captured['status'].split(' ', 1)[0]),
            headers=captured['headers'],
            body=payload
        )
        self._absorb_cookies(response)
        return response

    def _absorb_cookies(self, response: Response) -> None:
        for value in response.headers_named('Set-Cookie'):
            name, _, rest = value.partition('=')
            token, _, _ = rest.partition(';')
            if token:
                self.cookies[name.strip()] = token
            else:
                self.cookies.pop(name.strip(), None)

    def get(self, path: str, **kwargs: Any) -> Response:
        """Performs a GET request."""
        return self.request('GET', path, **kwargs)

    def post(self, path: str, data: Mapping[str, Any] | None = None,
             **kwargs: Any) -> Response:
        """Performs a POST request with urlencoded form data."""
        return self.request('POST', path, data=data, **kwargs)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Redirect handler that surfaces 3xx responses instead of following
    them."""

    def redirect_request(
            self, req: Any, fp: Any, code: int, msg: str, headers: Any,
            newurl: str
    ) -> None:
        return None


class HttpClient:
    """Drives a live console over ``urllib`` with a persistent cookie
    jar; redirects are never followed automatically."""

    def __init__(self, port: int) -> None:
        self.base = f'http://127.0.0.1:{port}'
        jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar), _NoRedirect()
        )
        self.jar = jar

    def request(
            self,
            method: str,
            path: str,
            data: Mapping[str, Any] | None = None,
            headers: Mapping[str, str] | None = None,
            follow: bool = True
    ) -> Response:
        """Performs one request; ``follow`` walks same-path redirects
        with GET like a browser form flow does."""
        response = self._once(method, path, data, headers)
        hops = 0
        while (follow and response.status in (301, 302, 303, 307, 308)
               and hops < 10):
            location = response.header('Location')
            if not location:
                break
            response = self._once('GET', location, None, headers)
            hops += 1
        return response

    def _once(
            self,
            method: str,
            path: str,
            data: Mapping[str, Any] | None,
            headers: Mapping[str, str] | None
    ) -> Response:
        url = path if path.startswith('http') else f'{self.base}{path}'
        body = _form_encode(data)
        request = urllib.request.Request(url, data=body, method=method)
        if body is not None:
            request.add_header(
                'Content-Type', 'application/x-www-form-urlencoded'
            )
        for key, value in (headers or {}).items():
            request.add_header(key, value)
        try:
            with self.opener.open(request, timeout=15) as raw:
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

    def get(self, path: str, **kwargs: Any) -> Response:
        """Performs a GET request."""
        return self.request('GET', path, **kwargs)

    def post(self, path: str, data: Mapping[str, Any] | None = None,
             **kwargs: Any) -> Response:
        """Performs a POST request with urlencoded form data."""
        return self.request('POST', path, data=data, **kwargs)


def csrf_token(html: str) -> str:
    """Extracts the per-session CSRF token from a rendered page."""
    match = CSRF_RE.search(html)
    assert match is not None, 'page carries no CSRF meta tag'
    return match.group(1)


def grant(username: str, role: str = 'admin') -> None:
    """Creates (or updates) a console grant on the bound model."""
    row = WebGrantModel.get_or_none(WebGrantModel.username == username)
    if row is None:
        WebGrantModel.create(
            username=username, role=role, created=_utcnow()
        )
    else:
        row.role = role
        row.save()


def add_user(bus: trolleybus.EventBus, username: str,
             password: str) -> None:
    """Adds a PACS user through the core event (registry must listen)."""
    bus.send_one(
        core_events.UserAdd, {'username': username, 'password': password}
    )


@pytest.fixture
def console(monkeypatch: pytest.MonkeyPatch, tmp_path: Path
            ) -> Iterator[Callable[..., SimpleNamespace]]:
    """Builds headless consoles with optional registry extensions.

    Returns a factory with keyword options:

    * ``with_registries`` (default True): starts the real ``DeviceStore``
      and ``Users`` components (imported by the test process only) so the
      device/user features have production listeners;
    * ``user_verify`` (default True): with registries off, a fake
      ``UserVerify`` listener accepting ``password='secret'`` for any
      username is installed instead;
    * ``web_config``: ``AdminWeb`` configuration overrides;
    * ``state_kwargs``: ``AppState`` overrides (e.g. ``session_ttl``).

    The started bus is an ``env`` namespace: ``bus``, ``database``,
    ``web`` (the headless ``AdminWeb`` component), ``state``, ``app`` and
    ``client`` (a :class:`WSGIClient`). ``TINY_PACS_HEADLESS`` is set for
    every build, so the component never binds a port in unit tests.
    """
    monkeypatch.setenv('TINY_PACS_HEADLESS', '1')
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []
    counter = {'n': 0}

    def build(
            with_registries: bool = True,
            user_verify: bool = True,
            web_config: dict[str, Any] | None = None,
            **state_kwargs: Any
    ) -> SimpleNamespace:
        counter['n'] += 1
        db_path = str(tmp_path / f'unit_{counter["n"]}_{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, sqlite_config(db_path))
        web = AdminWeb(bus, {'on': True, **(web_config or {})})
        if with_registries:
            admin_store.DeviceStore(bus, {})
            Users(bus, {})
        elif user_verify:
            bus.subscribe(
                core_events.UserVerify,
                lambda payload: (SimpleNamespace(
                    username=str(payload.get('username')))
                    if payload.get('password') == 'secret' else None)
            )
        bus.start()
        started.append((bus, database))
        state = web_module.AppState(
            bus=bus,
            logger=logging.getLogger('test.admin_web'),
            **state_kwargs
        )
        app = web_module.build_app(state)
        return SimpleNamespace(
            bus=bus, database=database, web=web, state=state, app=app,
            client=WSGIClient(app), db_path=db_path
        )

    yield build

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


@pytest.fixture
def config_file(tmp_path: Path) -> Iterator[Callable[..., str]]:
    r"""Writes console configuration files for CLI tests.

    :yield: factory ``(extra='', db_name=None)`` returning the path of a
            YAML configuration with a file-based SQLite database
    """
    counter = {'n': 0}

    def write(extra: str = '', db_name: str | None = None) -> str:
        counter['n'] += 1
        name = db_name or str(tmp_path / f'web_{counter["n"]}.db')
        path = tmp_path / f'conf_{counter["n"]}.yaml'
        path.write_text(
            'components:\n'
            '  Database:\n'
            '    on: true\n'
            f'    db_name: {name}\n'
            '    mode: rwc\n'
            f'{extra}'
        )
        return str(path)

    yield write
