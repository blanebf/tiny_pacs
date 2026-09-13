"""Unit tests of the console application driven in-process.

Routing, sessions, CSRF, login throttling, role enforcement, degraded
features, template escaping and the security headers/leak regressions —
all against the WSGI callable itself (no sockets). Device and user
mutations run against the *real* ``DeviceStore``/``Users`` components,
imported by the test process only.
"""
import logging
import time
import urllib.parse
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
import trolleybus
from conftest import (
    Response,
    WSGIClient,
    add_user,
    csrf_token,
    grant,
)
from tiny_pacs import events as core_events

from tiny_pacs_admin_web.sessions import SessionStore

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


def _location(response: Response) -> str:
    """Path portion of a redirect target (bottle builds absolute URLs)."""
    location = response.header('Location')
    assert location is not None
    return urllib.parse.urlsplit(location).path


def _admin_client(env: SimpleNamespace, username: str = 'alice',
                  password: str = 'secret') -> Any:
    """Creates a user + admin grant and returns a logged-in client."""
    add_user(env.bus, username, password)
    grant(username, 'admin')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': username, 'password': password})
    assert response.status == 200
    assert 'Dashboard' in response.text
    return client


def _viewer_client(env: SimpleNamespace, username: str = 'bob',
                   password: str = 'wonder') -> Any:
    add_user(env.bus, username, password)
    grant(username, 'viewer')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': username, 'password': password})
    assert response.status == 200
    return client


def _add_device(client: Any, **fields: Any) -> Any:
    token = csrf_token(client.get('/devices/').text)
    return client.post('/devices/add', data={
        'csrf_token': token,
        'aet': fields.pop('aet', 'MRI_01'),
        'address': fields.pop('address', '10.0.0.20'),
        'port': fields.pop('port', '11112'),
        'identity': fields.pop('identity', 'none'),
        'username': fields.pop('username', ''),
        'password': fields.pop('password', ''),
        **fields,
    }, follow=True)


# -----------------------------------------------------------------------
# Routing and authentication flow
# -----------------------------------------------------------------------

def test_anonymous_pages_redirect_to_login(console: Callable[..., Any]
                                           ) -> None:
    env = console()
    for path in ('/', '/devices/', '/users/'):
        response = env.client.get(path, follow=False)
        assert response.status == 303, path
        assert _location(response) == '/login', path
    # The bare section prefix redirects to the section root first
    response = env.client.get('/devices', follow=False)
    assert response.status == 303
    assert _location(response) == '/devices/'


def test_login_page_renders_form(console: Callable[..., Any]) -> None:
    env = console()
    response = env.client.get('/login')
    assert response.status == 200
    assert 'name="username"' in response.text
    assert 'autocomplete="current-password"' in response.text
    assert 'Set-Cookie' not in [key for key, _ in response.headers], \
        'the login page must not create any session'


def test_unknown_page_renders_generic_404(console: Callable[..., Any]
                                          ) -> None:
    env = console()
    response = env.client.get('/does-not-exist')
    assert response.status == 404
    assert 'Traceback' not in response.text
    assert 'does not exist' in response.text


def test_logout_requires_post(console: Callable[..., Any]) -> None:
    env = console()
    _admin_client(env)
    response = env.client.get('/logout', follow=False)
    assert response.status == 405


def test_login_logout_roundtrip(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    # Session cookie carries the strict flags
    cookies = client.cookies
    assert 'tpaw_session' in cookies
    # Log out through the nav form (CSRF protected)
    token = csrf_token(client.get('/').text)
    response = client.post('/logout', {'csrf_token': token}, follow=False)
    assert response.status == 303
    assert _location(response) == '/login'
    expiry = response.header('Set-Cookie')
    assert expiry is not None and 'Max-Age=0' in expiry
    # The destroyed session no longer passes the guard
    response = client.get('/', follow=False)
    assert response.status == 303
    assert 'tpaw_session' not in client.cookies


def test_login_sets_strict_cookie(console: Callable[..., Any]) -> None:
    env = console()
    add_user(env.bus, 'alice', 'secret')
    grant('alice', 'admin')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': 'alice', 'password': 'secret'},
                           follow=False)
    assert response.status == 303
    cookie = response.header('Set-Cookie')
    assert cookie is not None
    assert 'HttpOnly' in cookie
    assert 'SameSite=Strict' in cookie
    assert 'Path=/' in cookie
    assert 'Secure' not in cookie, 'loopback default is not TLS'


def test_secure_cookie_flag_configured(console: Callable[..., Any]) -> None:
    env = console(secure_cookie=True)
    add_user(env.bus, 'alice', 'secret')
    grant('alice', 'admin')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': 'alice', 'password': 'secret'},
                           follow=False)
    cookie = response.header('Set-Cookie')
    assert cookie is not None and 'Secure' in cookie


def test_login_failure_is_generic(console: Callable[..., Any]) -> None:
    env = console()
    add_user(env.bus, 'alice', 'secret')
    grant('alice', 'admin')
    client = WSGIClient(env.app)
    wrong = client.post('/login',
                        {'username': 'alice', 'password': 'nope'})
    assert wrong.status == 200
    assert 'Invalid username or password' in wrong.text
    assert 'tpaw_session' not in client.cookies
    unknown = client.post('/login',
                          {'username': 'mallory', 'password': 'secret'})
    assert 'Invalid username or password' in unknown.text
    env.bus.send_one(core_events.UserSetActive,
                     {'username': 'alice', 'is_active': False})
    inactive = client.post('/login',
                           {'username': 'alice', 'password': 'secret'})
    assert 'Invalid username or password' in inactive.text


def test_no_session_without_grant(console: Callable[..., Any]) -> None:
    """Valid credentials but no WebGrant row: identical generic error."""
    env = console()
    add_user(env.bus, 'alice', 'secret')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': 'alice', 'password': 'secret'})
    assert response.status == 200
    assert 'Invalid username or password' in response.text
    assert 'tpaw_session' not in client.cookies


def test_revoked_grant_destroys_live_session(console: Callable[..., Any]
                                             ) -> None:
    env = console()
    client = _admin_client(env)
    assert client.get('/').status == 200
    grant('alice', 'admin')  # sanity: still granted
    from tiny_pacs_admin_web.models import WebGrantModel
    WebGrantModel.delete().where(WebGrantModel.username == 'alice').execute()
    response = client.get('/', follow=False)
    assert response.status == 303
    assert _location(response) == '/login'
    assert 'tpaw_session' not in client.cookies


def test_session_expiry(console: Callable[..., Any]) -> None:
    env = console()
    clock = {'now': 0.0}
    env.state.sessions = SessionStore(
        ttl=60, max_sessions=10, clock=lambda: clock['now']
    )
    client = _admin_client(env)
    # Activity inside the window keeps the (sliding) session alive
    clock['now'] += 30
    assert client.get('/').status == 200
    # Once the idle window passes without activity it expires
    clock['now'] += 61
    response = client.get('/', follow=False)
    assert response.status == 303
    assert _location(response) == '/login'


def test_login_throttling_blocks_attempts(console: Callable[..., Any]
                                          ) -> None:
    calls = {'n': 0}

    def counting_verify(payload: dict[str, Any]) -> Any:
        calls['n'] += 1
        return None

    env = console(user_verify=False)
    env.bus.subscribe(core_events.UserVerify, counting_verify)
    client = WSGIClient(env.app)
    for _ in range(env.state.max_login_failures):
        response = client.post(
            '/login', {'username': 'alice', 'password': 'wrong'}
        )
        assert 'Invalid username or password' in response.text
    assert calls['n'] == env.state.max_login_failures
    # Still inside the backoff window: the attempt is refused *without*
    # touching the user registry
    response = client.post(
        '/login', {'username': 'alice', 'password': 'wrong'}
    )
    assert 'Invalid username or password' in response.text
    assert calls['n'] == env.state.max_login_failures


# -----------------------------------------------------------------------
# CSRF
# -----------------------------------------------------------------------

def test_post_without_csrf_rejected(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    response = client.post('/devices/add', {
        'aet': 'MRI_01', 'address': '10.0.0.20'
    }, follow=False)
    assert response.status == 403
    assert 'Traceback' not in response.text


def test_post_with_foreign_csrf_rejected(console: Callable[..., Any]
                                         ) -> None:
    env = console()
    client = _admin_client(env)
    response = client.post('/devices/add', {
        'csrf_token': 'not-the-token',
        'aet': 'MRI_01', 'address': '10.0.0.20'
    }, follow=False)
    assert response.status == 403


def test_logout_without_csrf_rejected(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    response = client.post('/logout', {}, follow=False)
    assert response.status == 403
    # The session survived the rejected request
    assert client.get('/').status == 200


def test_api_csrf_header(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    page = client.get('/devices/')
    good = csrf_token(page.text)
    anonymous = WSGIClient(env.app).post('/api/echo/ANY', {})
    assert anonymous.status == 401
    assert anonymous.json == {'ok': False,
                              'error': 'authentication required'}
    missing = client.post('/api/echo/ANY', {})
    assert missing.status == 403
    wrong = client.post('/api/echo/ANY', {},
                        headers={'X-CSRF-Token': 'bad'})
    assert wrong.status == 403
    # With the right header the request reaches the feature check
    # (no GetClient listener on this bus -> 409 JSON)
    accepted = client.post('/api/echo/ANY', {},
                           headers={'X-CSRF-Token': good})
    assert accepted.status == 409
    assert accepted.json['ok'] is False


# -----------------------------------------------------------------------
# Roles
# -----------------------------------------------------------------------

def test_viewer_read_only(console: Callable[..., Any]) -> None:
    env = console()
    client = _viewer_client(env)
    page = client.get('/devices/')
    assert page.status == 200
    assert 'Add device' not in page.text
    assert 'Log out' in page.text
    token = csrf_token(client.get('/').text)
    mutation = client.post(
        '/devices/add',
        {'csrf_token': token, 'aet': 'X', 'address': '10.0.0.5'},
        follow=False
    )
    assert mutation.status == 403


def test_viewer_hidden_pages(console: Callable[..., Any]) -> None:
    env = console()
    admin = _admin_client(env)
    client = _viewer_client(env)
    for path in ('/devices/add', '/users/add'):
        assert admin.get(path).status == 200, path
        assert client.get(path, follow=False).status == 403, path


def test_users_section_gated_for_viewers(console: Callable[..., Any]
                                         ) -> None:
    env = console()
    viewer = _viewer_client(env)
    assert viewer.get('/users/', follow=False).status == 403
    env.state.expose_users_to_viewer = True
    page = viewer.get('/users/')
    assert page.status == 200
    assert 'Read-only view' in page.text
    assert 'Add user' not in page.text


def test_role_change_takes_effect_on_next_request(
        console: Callable[..., Any]) -> None:
    env = console()
    client = _viewer_client(env)
    assert client.get('/devices/add', follow=False).status == 403
    grant('bob', 'admin')
    assert client.get('/devices/add').status == 200


# -----------------------------------------------------------------------
# Devices CRUD (real DeviceStore listener)
# -----------------------------------------------------------------------

def test_device_crud_roundtrip(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    response = _add_device(client, aet='MRI_01', address='10.0.0.20',
                           port='11113', identity='password',
                           username='pacs', password='out-secret')
    assert response.status == 200
    assert 'MRI_01' in response.text
    # The list shows the device with the password masked; the secret is
    # never rendered anywhere
    assert 'out-secret' not in response.text
    assert '********' in response.text
    # The registry really has it
    rows = env.bus.send_one(core_events.DeviceList, None)
    assert [row.aet for row in rows] == ['MRI_01']
    # Detail page
    detail = client.get('/devices/MRI_01')
    assert detail.status == 200
    assert '10.0.0.20' in detail.text
    assert 'out-secret' not in detail.text
    # Edit: address change + set-new password flow keeps the password
    token = csrf_token(detail.text)
    edit_page = client.get('/devices/MRI_01/edit')
    assert 'out-secret' not in edit_page.text
    edited = client.post('/devices/MRI_01/edit', {
        'csrf_token': csrf_token(edit_page.text),
        'address': '10.0.0.21', 'port': '11113', 'identity': 'password',
        'username': 'pacs', 'new_password': ''
    }, follow=True)
    assert edited.status == 200
    config = env.bus.send_any(core_events.DeviceByAE, 'MRI_01')
    assert config is not None
    assert config.address == '10.0.0.21'
    assert config.password == 'out-secret', 'empty field keeps stored'
    # Set a new outgoing password through the dedicated field
    client.post('/devices/MRI_01/edit', {
        'csrf_token': token, 'address': '10.0.0.21', 'port': '11113',
        'identity': 'password', 'username': 'pacs',
        'new_password': 'rotated'
    }, follow=True)
    config = env.bus.send_any(core_events.DeviceByAE, 'MRI_01')
    assert config.password == 'rotated'
    # Delete
    token = csrf_token(client.get('/devices/MRI_01').text)
    deleted = client.post(
        '/devices/MRI_01/delete', {'csrf_token': token}, follow=True
    )
    assert deleted.status == 200
    assert 'MRI_01' not in deleted.text
    assert env.bus.send_one(core_events.DeviceList, None) == []


def test_device_add_validation_error_rerenders_form(
        console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    _add_device(client, aet='MRI_01')
    page = client.get('/devices/')
    duplicate = client.post('/devices/add', {
        'csrf_token': csrf_token(page.text),
        'aet': 'MRI_01', 'address': '10.0.0.20'
    })
    assert duplicate.status == 200
    assert 'already exists' in duplicate.text
    assert 'value="MRI_01"' in duplicate.text, 'form keeps submitted data'


def test_unknown_device_404(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    response = client.get('/devices/NOPE')
    assert response.status == 404
    assert 'Traceback' not in response.text


def test_device_password_never_in_edit_form(console: Callable[..., Any]
                                            ) -> None:
    env = console()
    client = _admin_client(env)
    _add_device(client, aet='CT_01', password='sup3rs3cret')
    page = client.get('/devices/CT_01/edit')
    assert 'sup3rs3cret' not in page.text
    assert '********' in page.text


# -----------------------------------------------------------------------
# Users CRUD (real Users listener)
# -----------------------------------------------------------------------

def test_user_add_and_toggle_roundtrip(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    page = client.get('/users/')
    assert page.status == 200
    response = client.post('/users/add', {
        'csrf_token': csrf_token(page.text),
        'username': 'carol', 'password': 'initial'
    }, follow=True)
    assert response.status == 200
    assert 'carol' in response.text
    user = env.bus.send_one(core_events.UserByName, 'carol')
    assert user is not None and user.is_active
    # Toggle deactivates
    token = csrf_token(client.get('/users/').text)
    client.post('/users/carol/toggle', {'csrf_token': token}, follow=True)
    user = env.bus.send_one(core_events.UserByName, 'carol')
    assert not user.is_active
    listing = client.get('/users/')
    assert 'inactive' in listing.text
    # Toggle activates again
    token = csrf_token(listing.text)
    client.post('/users/carol/toggle', {'csrf_token': token}, follow=True)
    user = env.bus.send_one(core_events.UserByName, 'carol')
    assert user.is_active


def test_user_add_validation_rerenders(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    page = client.get('/users/')
    response = client.post('/users/add', {
        'csrf_token': csrf_token(page.text), 'username': '', 'password': ''
    })
    assert response.status == 200
    assert 'username must not be empty' in response.text


def test_password_change_invalidates_other_sessions(
        console: Callable[..., Any]) -> None:
    env = console()
    first = WSGIClient(env.app)
    second = WSGIClient(env.app)
    third = WSGIClient(env.app)
    add_user(env.bus, 'alice', 'secret')
    grant('alice', 'admin')
    for client in (first, second, third):
        response = client.post('/login',
                               {'username': 'alice', 'password': 'secret'},
                               follow=False)
        assert response.status == 303
    token = csrf_token(third.get('/users/').text)
    changed = third.post('/users/alice/password', {
        'csrf_token': token, 'password': 'rotated'
    }, follow=True)
    assert changed.status == 200
    # The acting session lives; the others are gone
    assert third.get('/').status == 200
    for client in (first, second):
        response = client.get('/', follow=False)
        assert response.status == 303
        assert _location(response) == '/login'


# -----------------------------------------------------------------------
# Dashboard and degraded features
# -----------------------------------------------------------------------

def test_dashboard_sections(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    page = client.get('/')
    assert page.status == 200
    assert 'Components' in page.text
    assert 'Database' in page.text
    assert 'Schema version' in page.text
    assert 'webgrantmodel' in page.text, \
        'DB row counts include the grant table'
    assert 'Storage statistics are not available' in page.text


def test_dashboard_storage_headlines(console: Callable[..., Any]) -> None:
    env = console()

    def stats(_: None) -> Any:
        return core_events.StorageStatsReport(
            records_total=7, records_stored=6, records_failed=1,
            storage_dir='/var/lib/tiny_pacs/store',
            file_count=6, file_bytes=3 * 1024 * 1024
        )

    env.bus.subscribe(core_events.StorageStatsQuery, stats)
    client = _admin_client(env)
    page = client.get('/')
    assert '7 / 6 / 1' in page.text
    assert '3.0 MiB' in page.text
    assert '/var/lib/tiny_pacs/store' in page.text
    assert 'Storage statistics are not available' not in page.text


def test_missing_devices_listener_page_not_available(
        console: Callable[..., Any]) -> None:
    env = console(with_registries=False)
    grant('alice', 'admin')
    client = WSGIClient(env.app)
    # The fake UserVerify listener accepts any user with 'secret'
    client.post('/login', {'username': 'alice', 'password': 'secret'})
    page = client.get('/devices/')
    assert page.status == 200
    assert 'not available' in page.text
    assert 'Traceback' not in page.text
    users = client.get('/users/')
    assert users.status == 200
    assert 'not available' in users.text


def test_listener_failure_degrades_page(console: Callable[..., Any],
                                        caplog: pytest.LogCaptureFixture
                                        ) -> None:
    env = console()

    def boom(_: None) -> Any:
        raise RuntimeError('registry exploded')

    # Replaces the healthy DeviceList answer with a raising listener on
    # top of the real one: send_any takes the first non-None answer, so
    # the raiser must run first (higher priority)
    env.bus.subscribe(
        core_events.DeviceList, boom, trolleybus.DEFAULT_PRIORITY + 40
    )
    client = _admin_client(env)
    with caplog.at_level(logging.ERROR):
        page = client.get('/devices/')
    assert page.status == 200
    assert 'not available' in page.text
    assert any('registry exploded' in record.getMessage()
               for record in caplog.records)


# -----------------------------------------------------------------------
# Echo API
# -----------------------------------------------------------------------

class _FakeClient:
    """DICOM client stand-in for the GetClient event."""

    def __init__(self, behaviour: str) -> None:
        self.behaviour = behaviour

    def echo(self) -> None:
        if self.behaviour == 'fail':
            raise RuntimeError('association refused')
        if self.behaviour == 'slow':
            time.sleep(30)


def test_echo_success_and_failure(console: Callable[..., Any],
                                  monkeypatch: pytest.MonkeyPatch) -> None:
    from tiny_pacs_admin_web import web as web_module
    monkeypatch.setattr(web_module, 'ECHO_TIMEOUT_SECONDS', 0.5)
    env = console()
    behaviour = {'mode': 'ok'}

    def client_factory(aet: str) -> Any:
        if aet == 'UNKNOWN':
            return None
        return _FakeClient(behaviour['mode'])

    env.bus.subscribe(core_events.GetClient, client_factory)
    client = _admin_client(env)
    token = csrf_token(client.get('/').text)
    response = client.post('/api/echo/MRI', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 200
    payload = response.json
    assert payload['ok'] is True
    assert payload['elapsed_ms'] >= 0
    behaviour['mode'] = 'fail'
    response = client.post('/api/echo/MRI', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 502
    assert 'association refused' in response.json['error']
    response = client.post('/api/echo/UNKNOWN', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 502
    assert 'unknown device' in response.json['error']
    behaviour['mode'] = 'slow'
    response = client.post('/api/echo/MRI', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 504
    assert 'timed out' in response.json['error']


def test_echo_viewer_rejected(console: Callable[..., Any]) -> None:
    env = console()
    env.bus.subscribe(core_events.GetClient, lambda aet: _FakeClient('ok'))
    client = _viewer_client(env)
    token = csrf_token(client.get('/').text)
    response = client.post('/api/echo/MRI', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 403
    assert response.json['error'] == 'admin role required'


def test_echo_without_client_component(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    token = csrf_token(client.get('/').text)
    response = client.post('/api/echo/MRI', {},
                           headers={'X-CSRF-Token': token})
    assert response.status == 409
    assert response.json['ok'] is False


# -----------------------------------------------------------------------
# Static assets and security headers
# -----------------------------------------------------------------------

def test_static_assets(console: Callable[..., Any]) -> None:
    env = console()
    css = env.client.get('/static/style.css')
    assert css.status == 200
    assert 'text/css' in (css.header('Content-Type') or '')
    assert css.body
    script = env.client.get('/static/app.js')
    assert script.status == 200
    assert 'javascript' in (script.header('Content-Type') or '')
    missing = env.client.get('/static/../../models.py')
    assert missing.status == 404


@pytest.mark.parametrize('path', ['/', '/login', '/nope'])
def test_security_headers_everywhere(console: Callable[..., Any],
                                     path: str) -> None:
    env = console()
    client = _admin_client(env) if path == '/' else env.client
    response = client.get(path, follow=False)
    csp = response.header('Content-Security-Policy')
    assert csp is not None
    assert "default-src 'none'" in csp
    assert "script-src 'self'" in csp
    assert 'unsafe-inline' not in csp
    assert response.header('X-Content-Type-Options') == 'nosniff'
    assert response.header('Referrer-Policy') == 'no-referrer'
    assert response.header('X-Frame-Options') == 'DENY'
    assert response.header('Cache-Control') == 'no-store'


def test_no_credentials_in_logs(console: Callable[..., Any],
                                caplog: pytest.LogCaptureFixture) -> None:
    env = console()
    # The console's own logging runs at INFO; DEBUG-level ORM/bus dumps
    # are core behaviour configured through the ``log`` section and not
    # part of this regression
    with caplog.at_level(logging.INFO):
        add_user(env.bus, 'alice', 'secret')
        grant('alice', 'admin')
        client = WSGIClient(env.app)
        client.post('/login',
                    {'username': 'alice', 'password': 'secret'})
        token = csrf_token(client.get('/').text)
        _add_device(client, aet='MRI_01', password='out-secret')
        client.post('/users/add', {
            'csrf_token': token, 'username': 'carol', 'password': 'pw2'
        })
    captured = '\n'.join(record.getMessage() for record in caplog.records)
    for secret in ('secret', 'out-secret', 'pw2'):
        # The raw credentials must never appear in a log message
        assert f"'{secret}'" not in captured
        assert f'"{secret}"' not in captured
        assert f'={secret}' not in captured
        assert f' {secret}' not in captured
    assert any('POST /login -> 303' in record.getMessage()
               for record in caplog.records), 'requests are logged'


def test_request_log_has_no_body(console: Callable[..., Any],
                                 caplog: pytest.LogCaptureFixture) -> None:
    env = console()
    with caplog.at_level(logging.INFO):
        add_user(env.bus, 'alice', 'secret')
        grant('alice', 'admin')
        client = WSGIClient(env.app)
        client.post('/login',
                    {'username': 'alice', 'password': 'hunter2-secret'})
    captured = '\n'.join(record.getMessage() for record in caplog.records)
    assert 'hunter2-secret' not in captured
    assert 'password=' not in captured


# -----------------------------------------------------------------------
# Template escaping
# -----------------------------------------------------------------------

def test_template_escaping(console: Callable[..., Any]) -> None:
    env = console()
    client = _admin_client(env)
    evil = '<script>alert(1)</script>'
    _add_device(client, aet='EVIL', address=evil)
    page = client.get('/devices/')
    assert '<script>alert(1)</script>' not in page.text
    assert '&lt;script&gt;' in page.text
    detail = client.get('/devices/EVIL')
    assert '<script>alert(1)</script>' not in detail.text
    client.post('/users/add', {
        'csrf_token': csrf_token(page.text),
        'username': 'eve<img src=x>', 'password': 'x'
    }, follow=True)
    users = client.get('/users/')
    assert '<img src=x>' not in users.text
    assert '&lt;img src=x&gt;' in users.text


def test_format_bytes() -> None:
    from tiny_pacs_admin_web.web import format_bytes
    assert format_bytes(None) == '-'
    assert format_bytes(512) == '512.0 B'
    assert format_bytes(2048) == '2.0 KiB'
    assert format_bytes(5 * 1024 ** 3) == '5.0 GiB'
