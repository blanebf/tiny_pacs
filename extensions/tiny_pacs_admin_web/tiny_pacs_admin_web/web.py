"""The administration console WSGI application.

A bottle application assembled from blueprinted sub-apps — one per
console section (root/login/dashboard, devices, users, JSON API) — served
by waitress. Templates ship as package data and are loaded through
:mod:`importlib.resources`; styles are one hand-written CSS file and the
little JavaScript needed (device echo, delete confirmations) is vanilla
and bundled. No CDNs, no build step.

Security properties enforced here (see the proposal checklist):

* sessions are server-side (:mod:`~tiny_pacs_admin_web.sessions`); the
  cookie is ``HttpOnly``, ``SameSite=Strict``, ``Path=/`` and — with
  ``secure_cookie`` — ``Secure``;
* every state-changing request carries the per-session CSRF token (hidden
  form field or ``X-CSRF-Token`` header), compared in constant time;
* login failures are throttled per (username, peer) with exponential
  backoff; every login failure renders the identical generic error;
* responses carry a ``self``-only Content-Security-Policy without
  inline scripts or styles, ``X-Content-Type-Options: nosniff``,
  ``Referrer-Policy: no-referrer`` and ``X-Frame-Options: DENY``;
* the request log records method, path, status and duration only — never
  form bodies;
* error pages are generic; bottle's debug mode is forced off, so
  tracebacks never reach the client.

The module imports the core only: every administration action travels
over the event bus (:func:`AppState.bus`), feature availability comes
from ``bus.has_listeners`` and missing listeners degrade pages to
"not available" instead of raising.
"""
import functools
import json
import logging
import secrets
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from importlib import resources
from typing import Any
from urllib.parse import quote

import bottle  # type: ignore[import-untyped]
import trolleybus
from tiny_pacs import config as core_config
from tiny_pacs import events
from tiny_pacs.identity import IdentityPolicy

from .models import ROLE_ADMIN, ROLES, WebGrantModel
from .sessions import (
    LOCKOUT_MAX_SECONDS,
    SESSION_COOKIE,
    LoginThrottle,
    Session,
    SessionStore,
)

#: Upper bound for a login password: longer inputs are refused without
#: running a (deliberately expensive) password proof against them
MAX_PASSWORD_LENGTH = 4096

#: Upper bound for a login username: longer inputs are refused instead of
#: truncated, matching the rejection semantics of the user registry
#: (``tiny_pacs_identity.users.MAX_USERNAME_LENGTH``) and the
#: ``WebGrantModel.username`` column width
MAX_USERNAME_LENGTH = 64

#: Upper bound for a device C-ECHO performed by ``/api/echo/{aet}``
ECHO_TIMEOUT_SECONDS = 10.0

#: Maximum number of C-ECHOes running at the same time, including echoed
#: attempts abandoned after their timeout; further requests are refused
#: with 503 instead of piling up threads
MAX_CONCURRENT_ECHOES = 2

#: Minimum seconds between two full scans of the login-failure counters
THROTTLE_PRUNE_SECONDS = 60.0

#: Response headers added to every console response by the middleware
SECURITY_HEADERS: list[tuple[str, str]] = [
    ('Content-Security-Policy',
     "default-src 'none'; script-src 'self'; style-src 'self'; "
     "img-src 'self'; connect-src 'self'; form-action 'self'; "
     "frame-ancestors 'none'; base-uri 'none'"),
    ('X-Content-Type-Options', 'nosniff'),
    ('Referrer-Policy', 'no-referrer'),
    ('X-Frame-Options', 'DENY'),
    ('Cache-Control', 'no-store'),
]

#: Feature name -> event whose listeners provide the feature. Missing
#: listeners degrade the matching pages to "not available"; they never
#: produce stack traces.
FEATURE_EVENTS: dict[str, type[trolleybus.Event[Any, Any]]] = {
    'devices': events.DeviceList,
    'users': events.UserList,
    'echo': events.GetClient,
    'storage': events.StorageStatsQuery,
    'db': events.TableCounts,
}

#: Generic message for every refused login, whatever the reason (wrong
#: password, unknown user, inactive user, no web grant, lockout)
LOGIN_ERROR = 'Invalid username or password'

#: Generic error page text by HTTP status code
ERROR_MESSAGES: dict[int, str] = {
    400: 'The request could not be understood.',
    401: 'Authentication is required.',
    403: 'This action is not permitted for your role.',
    404: 'The requested page does not exist.',
    405: 'This HTTP method is not allowed here.',
    413: 'The request is too large.',
    500: 'The console failed to handle the request. '
         'See the server log for details.',
}

#: Static assets served from the package data (never from disk paths
#: built from request input)
STATIC_ASSETS: dict[str, str] = {
    'style.css': 'text/css; charset=utf-8',
    'app.js': 'application/javascript; charset=utf-8',
}

#: Bytes multiplier names for the dashboard storage headlines
_SIZE_UNITS = ('B', 'KiB', 'MiB', 'GiB', 'TiB')


def _url_quote(value: str) -> str:
    """Percent-quotes one path segment of a record identifier.

    :param value: AE title, username or other identifier
    :type value: str
    :return: value safe to embed into a URL path
    :rtype: str
    """
    return quote(str(value), safe='')


def format_bytes(value: int | None) -> str:
    """Renders a byte count for humans.

    :param value: number of bytes, None stays ``-``
    :type value: int or None
    :return: human-readable size
    :rtype: str
    """
    if value is None:
        return '-'
    size = float(value)
    for unit in _SIZE_UNITS:
        if size < 1024.0 or unit == _SIZE_UNITS[-1]:
            return f'{size:.1f} {unit}'
        size /= 1024.0
    return f'{size:.1f} {_SIZE_UNITS[-1]}'  # pragma: no cover


@dataclass
class AppState:
    """Shared state of the console application.

    Built by the :class:`~tiny_pacs_admin_web.component.AdminWeb`
    component from validated configuration values; the module itself
    never sees the config model, so there is no import cycle between the
    component and the application.

    :ivar bus: the live event bus every administration action travels on
    :ivar logger: component logger used for request logging and warnings
    :ivar host: configured bind address (displayed on the dashboard)
    :ivar secure_cookie: adds the ``Secure`` flag to the session cookie
    :ivar session_ttl: session idle timeout in seconds (also the cookie
                       ``Max-Age``)
    :ivar expose_users_to_viewer: lets the viewer role see the users page
    :ivar sessions: in-memory session store
    :ivar throttle: login failure throttling
    :ivar templates: compiled page templates by name
    :ivar echo_lock: guards :attr:`echo_in_flight`
    :ivar echo_in_flight: device echoes currently running, including ones
                          abandoned after their request timed out
    """

    bus: trolleybus.EventBus
    logger: logging.Logger
    host: str = '127.0.0.1'
    secure_cookie: bool = False
    session_ttl: int = 3600
    max_sessions: int = 100
    max_login_failures: int = 5
    expose_users_to_viewer: bool = False
    sessions: SessionStore = field(init=False)
    throttle: LoginThrottle = field(init=False)
    templates: dict[str, Any] = field(init=False, default_factory=dict)
    echo_lock: threading.Lock = field(
        init=False, default_factory=threading.Lock)
    echo_in_flight: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self.sessions = SessionStore(
            ttl=self.session_ttl, max_sessions=self.max_sessions
        )
        self.throttle = LoginThrottle(self.max_login_failures)
        self.templates = _load_templates()

    def has_feature(self, name: str) -> bool:
        """Whether the listeners of a feature are present on the bus.

        Evaluated per request so an installed-but-disabled registry is
        reflected without a console restart.

        :param name: feature name, a key of :data:`FEATURE_EVENTS`
        :type name: str
        :return: True when at least one listener answers the feature's
                 event
        :rtype: bool
        """
        event = FEATURE_EVENTS.get(name)
        return event is not None and self.bus.has_listeners(event)

    def features(self) -> dict[str, bool]:
        """Availability of every console feature.

        :return: feature name to availability
        :rtype: dict[str, bool]
        """
        return {name: self.has_feature(name) for name in FEATURE_EVENTS}


def _load_templates() -> dict[str, Any]:
    """Compiles the package-data templates.

    :return: template name to compiled ``SimpleTemplate``
    :rtype: dict
    """
    lookup = [str(resources.files('tiny_pacs_admin_web') / 'templates')]
    names = (
        '_base', 'login', 'dashboard', 'error', 'not_available',
        'devices_list', 'device_form', 'device_detail',
        'users_list', 'user_add', 'user_password'
    )
    loaded: dict[str, Any] = {}
    for name in names:
        loaded[name] = bottle.SimpleTemplate(
            name=f'{name}.tpl', lookup=lookup
        )
    return loaded


def _static_asset(name: str) -> bytes:
    """Reads a bundled static asset.

    :param name: file name inside the package's ``static`` directory
    :type name: str
    :return: asset bytes
    :rtype: bytes
    """
    traversable = resources.files('tiny_pacs_admin_web') / 'static' / name
    return traversable.read_bytes()


class SecurityMiddleware:
    """WSGI middleware adding security headers, request logging and a
    last-resort generic 500 handler.

    Wraps the whole application (root app and every mounted sub-app), so
    the guarantees hold for every response regardless of which section
    produced it. The log line carries method, path, status and duration
    only — never query strings or form bodies.
    """

    def __init__(self, app: Any, logger: logging.Logger) -> None:
        """Initializes the middleware.

        :param app: wrapped WSGI callable
        :param logger: logger receiving the request log lines
        """
        self.app = app
        self.logger = logger

    def __call__(
            self,
            environ: dict[str, Any],
            start_response: Callable[..., Any]
    ) -> Iterable[bytes]:
        """Serves one request with logging and guaranteed headers."""
        method = environ.get('REQUEST_METHOD', '-')
        path = environ.get('PATH_INFO', '-')
        started = time.monotonic()
        status_box: list[str] = []

        def wrapped_start_response(
                status: str,
                headers: list[tuple[str, str]],
                exc_info: Any = None
        ) -> Any:
            status_box.append(status)
            return start_response(
                status, [*headers, *SECURITY_HEADERS], exc_info
            )

        try:
            result = self.app(environ, wrapped_start_response)
            body = b''.join(result)
            close = getattr(result, 'close', None)
            if close is not None:
                close()
        except Exception:
            self.logger.exception(
                'Unhandled error serving %s %s', method, path
            )
            if status_box:
                raise
            status_box.append('500 Internal Server Error')
            body = self._error_body(500)
            start_response(
                status_box[0],
                [('Content-Type', 'text/html; charset=utf-8'),
                 *SECURITY_HEADERS]
            )
        elapsed = (time.monotonic() - started) * 1000.0
        self.logger.info(
            '%s %s -> %s (%.1f ms)', method, path,
            status_box[0] if status_box else '-', elapsed
        )
        return [body]

    @staticmethod
    def _error_body(status: int) -> bytes:
        """Renders the fallback generic error page (no templates)."""
        message = ERROR_MESSAGES.get(status, 'Request failed.')
        page = (
            '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
            f'<title>{status} tiny_pacs admin</title></head><body>'
            f'<main><p><strong>{status}</strong> {message}</p></body></html>'
        )
        return page.encode('utf-8')


def _redirect_to_section(prefix: str) -> Any:
    """Redirects the bare section prefix to the mounted section root."""
    return bottle.redirect(f'{prefix}/')


def build_app(state: AppState) -> Any:
    """Assembles the console WSGI application.

    Blueprinted sub-apps per section are mounted on the root app; bottle's
    debug mode is forced off so tracebacks never reach a client, and
    every app renders generic error pages.

    :param state: shared application state
    :type state: AppState
    :return: WSGI callable to hand to waitress
    """
    bottle.DEBUG = False
    root = bottle.Bottle()
    root.catchall = True
    _install_error_pages(state, root)
    _register_root(state, root)

    for prefix, register in (
        ('/devices', _register_devices),
        ('/users', _register_users),
        ('/api', _register_api)
    ):
        section = bottle.Bottle()
        section.catchall = True
        _install_error_pages(state, section)
        register(state, section)
        # Bottle 0.13 wants trailing-slash mount prefixes; the bare
        # prefix gets an explicit redirect to the section root
        root.route(prefix, 'GET',
                   functools.partial(_redirect_to_section, prefix))
        root.mount(f'{prefix}/', section)

    return SecurityMiddleware(root, state.logger)


# -----------------------------------------------------------------------
# Rendering and request guards
# -----------------------------------------------------------------------

def _render(state: AppState, session: Session | None,
            template: str, **context: Any) -> str:
    """Renders a page inside the shared layout.

    The content template is rendered first (with automatic HTML escaping)
    and injected into ``_base`` as pre-rendered markup; user-controlled
    values are therefore escaped exactly once, at the place they are
    used.

    :param state: shared application state
    :param session: authenticated session, None on the login page
    :param template: content template name without extension
    :param context: template variables
    :return: rendered HTML page
    :rtype: str
    """
    context = dict(context)
    base_context: dict[str, Any] = {
        'title': context.pop('title', 'Console'),
        'session': session,
        'csrf': session.csrf_token if session is not None else '',
        'is_admin': session is not None and session.role == ROLE_ADMIN,
        'features': state.features(),
        'show_users_to_viewer': state.expose_users_to_viewer,
        'console_host': state.host,
        # URLs of records whose identifiers come from the database (AE
        # titles, usernames) are built through this helper only
        'quote': _url_quote,
    }
    content = state.templates[template].render(**base_context, **context)
    page: str = state.templates['_base'].render(**base_context, body=content)
    return page


def _set_session_cookie(state: AppState, token: str) -> None:
    """Sets the session cookie with the strict flags."""
    parts = [
        f'{SESSION_COOKIE}={token}', 'Path=/', 'HttpOnly',
        'SameSite=Strict', f'Max-Age={int(state.session_ttl)}'
    ]
    if state.secure_cookie:
        parts.append('Secure')
    bottle.response.set_header('Set-Cookie', '; '.join(parts))


def _clear_session_cookie(state: AppState) -> None:
    """Expires the session cookie (logout)."""
    parts = [
        f'{SESSION_COOKIE}=', 'Path=/', 'HttpOnly', 'SameSite=Strict',
        'Max-Age=0'
    ]
    if state.secure_cookie:
        parts.append('Secure')
    bottle.response.set_header('Set-Cookie', '; '.join(parts))


def _grant_role(state: AppState, username: str) -> str | None:
    """Reads the console role of a user from the grant table.

    Re-checked on every authenticated request, so a CLI ``revoke`` or
    role change takes effect immediately, without waiting for a session
    to expire.

    :param state: shared application state
    :param username: login name of the user
    :type username: str
    :return: ``admin``, ``viewer`` or None without a usable grant
    :rtype: str or None
    """
    try:
        row = WebGrantModel.get_or_none(WebGrantModel.username == username)
    except Exception:
        state.logger.exception('Cannot read the web grant table')
        return None
    if row is None or row.role not in ROLES:
        return None
    return str(row.role)


def _current_session(state: AppState) -> Session | None:
    """Resolves the session of the current request.

    Unknown, expired and cookie-less requests return None; a session
    whose grant disappeared (CLI ``revoke``) is destroyed. The stored
    role is refreshed from the grant table on every lookup, and the
    session cookie is re-set so the browser's expiry follows the
    server-side idle window ``SessionStore.get`` refreshes on the same
    call.
    """
    session = state.sessions.get(bottle.request.get_cookie(SESSION_COOKIE))
    if session is None:
        return None
    role = _grant_role(state, session.username)
    if role is None:
        state.sessions.delete(session.token)
        return None
    session.role = role
    _set_session_cookie(state, session.token)
    return session


def _check_csrf(session: Session) -> bool:
    """Verifies the CSRF token of a state-changing request.

    Accepts the hidden ``csrf_token`` form field or the ``X-CSRF-Token``
    header (JSON endpoints); the comparison is constant time.
    """
    provided = (bottle.request.forms.get('csrf_token')
                or bottle.request.headers.get('X-CSRF-Token') or '')
    return bool(provided) and secrets.compare_digest(
        session.csrf_token, provided
    )


def _guard_page(state: AppState, admin: bool = False,
                require_csrf: bool = False) -> Session:
    """Session/role/CSRF gate of an HTML page handler.

    Anonymous requests are redirected to the login page; ``admin=True``
    and every state-changing request are additionally rejected with a
    generic 403 page for viewers or a missing/invalid CSRF token.

    :param state: shared application state
    :param admin: require the admin role
    :type admin: bool
    :param require_csrf: verify the per-session CSRF token
    :type require_csrf: bool
    :return: the authenticated session
    :rtype: Session
    """
    session = _current_session(state)
    if session is None:
        _clear_session_cookie(state)
        bottle.redirect('/login')
    assert session is not None
    if admin and session.role != ROLE_ADMIN:
        bottle.abort(403, 'Administrator role required')
    if require_csrf and not _check_csrf(session):
        bottle.abort(403, 'CSRF validation failed')
    return session


def _guard_users_page(state: AppState, session: Session) -> None:
    """Gate of the users section honouring ``expose_users_to_viewer``."""
    if session.role != ROLE_ADMIN and not state.expose_users_to_viewer:
        bottle.abort(403, 'The users section is not exposed to viewers')


def _json(code: int, payload: dict[str, Any]) -> bottle.HTTPResponse:
    """Builds a JSON response with an explicit status code."""
    body = json.dumps(payload)
    return bottle.HTTPResponse(
        body=body, status=code,
        headers={'Content-Type': 'application/json'}
    )


def _guard_api(state: AppState, require_csrf: bool = True
               ) -> Session | None:
    """Session/role/CSRF gate of a JSON endpoint.

    :param state: shared application state
    :param require_csrf: verify the ``X-CSRF-Token`` header
    :type require_csrf: bool
    :return: the authenticated admin session; viewers and anonymous
             requests receive the JSON error response instead (the
             function then never returns)
    :rtype: Session or None
    """
    session = _current_session(state)
    if session is None:
        raise _json(401, {'ok': False, 'error': 'authentication required'})
    if session.role != ROLE_ADMIN:
        # Every API endpoint is state-changing (POST): viewers are
        # rejected like on the HTML pages
        raise _json(403, {'ok': False, 'error': 'admin role required'})
    if require_csrf and not _check_csrf(session):
        raise _json(403, {'ok': False, 'error': 'CSRF validation failed'})
    return session


def _install_error_pages(state: AppState, app: Any) -> None:
    """Registers the generic error page on one (sub-)application."""
    def handler(error: Any) -> str:
        status = int(getattr(error, 'status_code', 500) or 500)
        bottle.response.status = status
        return _render(
            state, _current_session(state), 'error',
            title=f'{status}', status=status,
            message=ERROR_MESSAGES.get(status, 'Request failed.')
        )

    for code in ERROR_MESSAGES:
        app.error(code)(handler)


def _bus_call(state: AppState, event: Any, payload: Any) -> Any:
    """Calls a bus event for display purposes.

    Listener failures are logged and mapped to None so a page degrades
    to "not available" instead of failing with a stack trace. Page
    handlers must only use it for read-only queries; mutations run
    through :func:`_bus_mutation` instead.
    """
    try:
        return state.bus.send_any(event, payload)
    except Exception:
        state.logger.exception(
            'Bus call %s failed', getattr(event, '__name__', event)
        )
        return None


def _bus_mutation(state: AppState, event: Any, payload: Any) -> Any:
    """Runs a mutation event, letting validation errors through.

    :raises ValueError: raised by the handling component for invalid
                        payloads (re-rendered as form errors)
    :raises RuntimeError: raised when no listener answered the event
    """
    result = state.bus.send_any(event, payload)
    if result is None:
        raise RuntimeError(
            f'No component answered {getattr(event, "__name__", event)}'
        )
    return result


def _not_available(state: AppState, session: Session, feature: str
                   ) -> str:
    """Renders the "feature not available" page."""
    return _render(
        state, session, 'not_available', title='Not available',
        feature=feature,
        detail=(
            'No installed component provides this feature. Enable the '
            'extension that serves it (see the tutorial) and restart the '
            'server.'
        )
    )


# -----------------------------------------------------------------------
# Root section: login, logout, dashboard, static assets
# -----------------------------------------------------------------------

def _register_root(state: AppState, app: Any) -> None:
    """Registers the root section routes (login/dashboard/static)."""

    @app.get('/')
    def dashboard() -> str:
        return _dashboard_page(state)

    @app.get('/login')
    def login_page() -> str:
        session = _current_session(state)
        if session is not None:
            bottle.redirect('/')
        return _render(state, None, 'login', title='Sign in', error=None)

    @app.post('/login')
    def login_submit() -> Any:
        return _login(state)

    @app.post('/logout')
    def logout() -> Any:
        # Logout is state-changing like every other POST: the per-session
        # CSRF token is required
        session = _guard_page(state, require_csrf=True)
        state.sessions.delete(session.token)
        _clear_session_cookie(state)
        bottle.redirect('/login')

    @app.get('/static/style.css')
    def style() -> Any:
        bottle.response.content_type = STATIC_ASSETS['style.css']
        return _static_asset('style.css')

    @app.get('/static/app.js')
    def script() -> Any:
        bottle.response.content_type = STATIC_ASSETS['app.js']
        return _static_asset('app.js')


def _login(state: AppState) -> Any:
    """Handles a login form submission.

    Every refusal — throttled pair, unknown user, wrong password,
    inactive user, missing web grant — renders the identical generic
    error, so the response never reveals which part failed. Credentials
    are never logged.
    """
    username = (bottle.request.forms.get('username') or '').strip()
    password = bottle.request.forms.get('password') or ''
    peer = bottle.request.remote_addr or '-'
    # Bound the throttle key even for an arbitrarily long presented
    # username; oversized usernames are refused below instead of
    # truncated, matching the rejection semantics of the user registry
    throttle_key = username[:MAX_USERNAME_LENGTH]

    def refuse() -> str:
        bottle.response.status = 200
        return _render(state, None, 'login', title='Sign in',
                       error=LOGIN_ERROR)

    # Lazily bound the failure counters: expired pairs are dropped at
    # most once a minute and the dict itself is capped, so a username
    # spray can neither grow it without limit nor make every login
    # perform a full scan
    state.throttle.maybe_prune(max_age=4 * LOCKOUT_MAX_SECONDS,
                               interval=THROTTLE_PRUNE_SECONDS)
    if state.throttle.wait_seconds(throttle_key, peer) > 0:
        state.logger.warning(
            'Login throttled for %r from %s', throttle_key, peer
        )
        return refuse()
    if (not username or len(username) > MAX_USERNAME_LENGTH
            or not password or len(password) > MAX_PASSWORD_LENGTH):
        state.throttle.record_failure(throttle_key, peer)
        return refuse()
    user = None
    try:
        user = state.bus.send_any(
            events.UserVerify, {'username': username, 'password': password}
        )
    except Exception:
        state.logger.exception('UserVerify listener failed')
    if user is None:
        state.throttle.record_failure(throttle_key, peer)
        return refuse()
    # The credentials are valid: stop penalizing this pair even when the
    # grant check below fails (which must not look different anyway)
    state.throttle.reset(throttle_key, peer)
    role = _grant_role(state, username)
    if role is None:
        state.logger.warning(
            'User %r authenticated but has no web console grant; '
            'refusing the login', username
        )
        return refuse()
    session = state.sessions.create(username, role)
    _set_session_cookie(state, session.token)
    state.logger.info('User %r signed in with role %s', username, role)
    bottle.redirect('/')


def _dashboard_page(state: AppState) -> str:
    """Renders the dashboard: components, schema versions, DB counts."""
    session = _guard_page(state)
    components = [
        {'name': name, 'origin': core_config.get_component_origin(name)}
        for name in sorted(core_config.COMPONENT_REGISTRY)
    ]
    schema_versions = _bus_call(state, events.SchemaVersions, None) or {}
    table_counts = _bus_call(state, events.TableCounts, None) or {}
    main_aet = _bus_call(state, events.MainAET, None)
    stats = _bus_call(state, events.StorageStatsQuery, None)
    storage: dict[str, Any] | None = None
    if stats is not None:
        storage = {
            'records_total': stats.records_total,
            'records_stored': stats.records_stored,
            'records_failed': stats.records_failed,
            'oldest': stats.oldest,
            'newest': stats.newest,
            'storage_dir': stats.storage_dir,
            'file_count': stats.file_count,
            'file_bytes': format_bytes(stats.file_bytes),
        }
    return _render(
        state, session, 'dashboard', title='Dashboard',
        components=components,
        schema_versions=sorted(schema_versions.items()),
        table_counts=sorted(table_counts.items()),
        main_aet=main_aet or '-',
        storage=storage
    )


# -----------------------------------------------------------------------
# Devices section
# -----------------------------------------------------------------------

def _device_row(row: Any) -> dict[str, Any]:
    """Converts a device record of a registry into a display mapping.

    The registry's record type is not imported (event-only design); the
    fields are read defensively, and the outgoing password is reduced to
    the fact that one is set — its value is never displayed.
    """
    return {
        'aet': str(getattr(row, 'aet', '') or ''),
        'address': str(getattr(row, 'address', '') or ''),
        'port': str(getattr(row, 'port', '') or ''),
        'identity': str(getattr(row, 'identity', '') or '-'),
        'username': str(getattr(row, 'username', '') or '-'),
        'password_set': bool(getattr(row, 'password', None)),
        'created': str(getattr(row, 'created', '') or '-'),
        'updated': str(getattr(row, 'updated', '') or '-'),
    }


def _register_devices(state: AppState, app: Any) -> None:
    """Registers the devices section routes."""

    @app.get('/')
    def devices_list() -> str:
        session = _guard_page(state)
        if not state.has_feature('devices'):
            return _not_available(state, session, 'devices')
        rows = _bus_call(state, events.DeviceList, None)
        if rows is None:
            # The registry is installed but currently failing: degrade
            # like a missing listener instead of pretending the registry
            # is empty (both are answered by a list, never None)
            return _not_available(state, session, 'devices')
        return _render(
            state, session, 'devices_list', title='Devices',
            devices=[_device_row(row) for row in rows],
            identity_policies=[policy.value for policy in IdentityPolicy]
        )

    @app.get('/add')
    def device_add_form() -> str:
        session = _guard_page(state, admin=True)
        if not state.has_feature('devices'):
            return _not_available(state, session, 'devices')
        return _device_form(state, session, mode='add', aet=None,
                            values={}, error=None)

    @app.post('/add')
    def device_add_submit() -> Any:
        session = _guard_page(state, admin=True, require_csrf=True)
        values = _device_form_values()
        try:
            _bus_mutation(state, events.DeviceAdd, values)
        except ValueError as error:
            return _device_form(state, session, mode='add', aet=None,
                                values=values, error=str(error))
        except Exception:
            state.logger.exception('DeviceAdd failed')
            bottle.abort(500)
        bottle.redirect('/devices/')

    @app.get('/<aet>')
    def device_detail(aet: str) -> str:
        session = _guard_page(state)
        if not state.has_feature('devices'):
            return _not_available(state, session, 'devices')
        device = _device_config(state, aet)
        if device is None:
            raise bottle.HTTPError(404, 'Unknown device')
        return _render(
            state, session, 'device_detail', title=f'Device {aet}',
            device=device, echo=state.has_feature('echo')
        )

    @app.get('/<aet>/edit')
    def device_edit_form(aet: str) -> str:
        session = _guard_page(state, admin=True)
        if not state.has_feature('devices'):
            return _not_available(state, session, 'devices')
        device = _device_config(state, aet)
        if device is None:
            raise bottle.HTTPError(404, 'Unknown device')
        return _device_form(state, session, mode='edit', aet=aet,
                            values=device, error=None)

    @app.post('/<aet>/edit')
    def device_edit_submit(aet: str) -> Any:
        session = _guard_page(state, admin=True, require_csrf=True)
        values = _device_form_values()
        values['aet'] = aet
        # The stored outgoing password is never round-tripped through the
        # form: only the dedicated "set new" field may change it, and
        # leaving it empty keeps the stored value untouched
        values.pop('password', None)
        new_password = bottle.request.forms.get('new_password') or ''
        if new_password:
            values['password'] = new_password
        if not values.get('username'):
            values.pop('username', None)
        try:
            _bus_mutation(state, events.DeviceUpdate, values)
        except ValueError as error:
            return _device_form(state, session, mode='edit', aet=aet,
                                values=values, error=str(error))
        except Exception:
            state.logger.exception('DeviceUpdate failed')
            bottle.abort(500)
        bottle.redirect(f'/devices/{_url_quote(aet)}')

    @app.post('/<aet>/delete')
    def device_delete(aet: str) -> Any:
        _guard_page(state, admin=True, require_csrf=True)
        try:
            removed = state.bus.send_any(events.DeviceRemove, aet)
        except Exception:
            state.logger.exception('DeviceRemove failed')
            bottle.abort(500)
        if not removed:
            bottle.abort(404, 'Unknown device')
        bottle.redirect('/devices/')


def _device_config(state: AppState, aet: str) -> dict[str, Any] | None:
    """Reads a device through ``DeviceByAE`` for display.

    Extra registry fields (e.g. the identity policy) come along in the
    mapping; the outgoing password is reduced to ``********`` and its
    value is never rendered.
    """
    config = _bus_call(state, events.DeviceByAE, aet)
    if config is None:
        return None
    try:
        data = dict(config.model_dump())
    except AttributeError:  # pragma: no cover - non-pydantic registry
        data = dict(config)
    password = data.pop('password', None)
    device: dict[str, Any] = {
        key: ('' if value is None else str(value))
        for key, value in data.items()
    }
    device['password_set'] = bool(password)
    return device


def _device_form_values() -> dict[str, Any]:
    """Collects the device add/edit form fields.

    Empty optional fields are dropped so the registry applies its own
    defaults; passwords are only forwarded from the dedicated field.
    """
    forms = bottle.request.forms
    values: dict[str, Any] = {}
    for name in ('aet', 'address', 'username', 'password', 'identity'):
        raw = (forms.get(name) or '').strip()
        if raw:
            values[name] = raw
    port = (forms.get('port') or '').strip()
    if port:
        values['port'] = port
    return values


def _device_form(state: AppState, session: Session, mode: str,
                 aet: str | None, values: Mapping[str, Any],
                 error: str | None) -> str:
    """Renders the shared device add/edit form."""
    return _render(
        state, session, 'device_form',
        title='Add device' if mode == 'add' else f'Edit device {aet}',
        mode=mode, aet=aet or '', values=dict(values), error=error,
        identity_policies=[policy.value for policy in IdentityPolicy]
    )


# -----------------------------------------------------------------------
# Users section
# -----------------------------------------------------------------------

def _user_row(row: Any, state: AppState) -> dict[str, Any]:
    """Converts a user record of a registry into a display mapping."""
    return {
        'username': str(getattr(row, 'username', '') or ''),
        'is_active': bool(getattr(row, 'is_active', False)),
        'created': str(getattr(row, 'created', '') or '-'),
        'last_login': str(getattr(row, 'last_login', '') or 'never'),
        'console_role': _grant_role(state, str(
            getattr(row, 'username', '') or '')) or '-',
    }


def _register_users(state: AppState, app: Any) -> None:
    """Registers the users section routes."""

    @app.get('/')
    def users_list() -> str:
        session = _guard_page(state)
        _guard_users_page(state, session)
        if not state.has_feature('users'):
            return _not_available(state, session, 'users')
        rows = _bus_call(state, events.UserList, None)
        if rows is None:
            return _not_available(state, session, 'users')
        return _render(
            state, session, 'users_list', title='Users',
            users=[_user_row(row, state) for row in rows]
        )

    @app.get('/add')
    def user_add_form() -> str:
        session = _guard_page(state, admin=True)
        if not state.has_feature('users'):
            return _not_available(state, session, 'users')
        return _render(state, session, 'user_add', title='Add user',
                       error=None, username='')

    @app.post('/add')
    def user_add_submit() -> Any:
        session = _guard_page(state, admin=True, require_csrf=True)
        username = (bottle.request.forms.get('username') or '').strip()
        password = bottle.request.forms.get('password') or ''
        try:
            _bus_mutation(state, events.UserAdd,
                          {'username': username, 'password': password})
        except ValueError as error:
            return _render(state, session, 'user_add', title='Add user',
                           error=str(error), username=username)
        except Exception:
            state.logger.exception('UserAdd failed')
            bottle.abort(500)
        bottle.redirect('/users/')

    @app.get('/<name>/password')
    def user_password_form(name: str) -> str:
        session = _guard_page(state, admin=True)
        if not state.has_feature('users'):
            return _not_available(state, session, 'users')
        return _render(state, session, 'user_password',
                       title=f'Password of {name}', username=name,
                       error=None)

    @app.post('/<name>/password')
    def user_password_submit(name: str) -> Any:
        session = _guard_page(state, admin=True, require_csrf=True)
        password = bottle.request.forms.get('password') or ''
        try:
            _bus_mutation(
                state, events.UserSetPassword,
                {'username': name, 'password': password}
            )
        except ValueError as error:
            return _render(state, session, 'user_password',
                           title=f'Password of {name}', username=name,
                           error=str(error))
        except Exception:
            state.logger.exception('UserSetPassword failed')
            bottle.abort(500)
        # A password change invalidates the user's other console sessions
        destroyed = state.sessions.delete_for(name, keep=session.token)
        if destroyed:
            state.logger.info(
                'Password change invalidated %d other session(s) of %r',
                destroyed, name
            )
        bottle.redirect('/users/')

    @app.post('/<name>/toggle')
    def user_toggle(name: str) -> Any:
        _guard_page(state, admin=True, require_csrf=True)
        user = _bus_call(state, events.UserByName, name)
        if user is None:
            bottle.abort(404, 'Unknown user')
        was_active = bool(getattr(user, 'is_active', False))
        try:
            _bus_mutation(
                state, events.UserSetActive,
                {'username': name, 'is_active': not was_active}
            )
        except ValueError:
            bottle.abort(404, 'Unknown user')
        except Exception:
            state.logger.exception('UserSetActive failed')
            bottle.abort(500)
        if was_active:
            # The user is now inactive: destroy their live console
            # sessions immediately (including an admin's own when they
            # deactivate themselves), matching how a revoked grant or a
            # password change invalidates access right away
            destroyed = state.sessions.delete_for(name)
            if destroyed:
                state.logger.info(
                    'Deactivation invalidated %d live session(s) of %r',
                    destroyed, name
                )
        bottle.redirect('/users/')


# -----------------------------------------------------------------------
# JSON API section
# -----------------------------------------------------------------------

def _register_api(state: AppState, app: Any) -> None:
    """Registers the JSON API routes."""

    @app.post('/echo/<aet>')
    def echo(aet: str) -> Any:
        _guard_api(state)
        return _run_echo(state, aet)


def _run_echo(state: AppState, aet: str) -> Any:
    """Runs a device C-ECHO in a worker thread with a bounded timeout.

    The DICOM association runs off the request thread pool so a hanging
    device can never exhaust waitress workers; the request returns the
    outcome (or a timeout) after at most :data:`ECHO_TIMEOUT_SECONDS`.
    At most :data:`MAX_CONCURRENT_ECHOES` echoes run at the same time —
    an echo abandoned after its timeout keeps its slot until its thread
    really finishes, so hung threads cannot accumulate without bound;
    further requests are refused with 503 while every slot is busy.
    """
    if not state.has_feature('echo'):
        return _json(409, {'ok': False,
                           'error': 'echo is not available on this server'})
    with state.echo_lock:
        busy = state.echo_in_flight >= MAX_CONCURRENT_ECHOES
        if not busy:
            state.echo_in_flight += 1
    if busy:
        state.logger.warning(
            'C-ECHO to %s refused: %d echo(es) already running', aet,
            MAX_CONCURRENT_ECHOES
        )
        return _json(503, {
            'ok': False,
            'error': (f'the console already runs {MAX_CONCURRENT_ECHOES} '
                      'echoes; try again in a moment')
        })
    outcome: dict[str, Any] = {}

    def attempt() -> None:
        try:
            client = state.bus.send_any(events.GetClient, aet)
            if client is None:
                outcome['error'] = f'unknown device {aet}'
                return
            client.echo()
            outcome['ok'] = True
        except Exception as error:
            outcome['error'] = _echo_error(error)
        finally:
            with state.echo_lock:
                state.echo_in_flight -= 1

    started = time.monotonic()
    thread = threading.Thread(
        target=attempt, name=f'AdminWeb-echo-{aet}', daemon=True
    )
    thread.start()
    thread.join(timeout=ECHO_TIMEOUT_SECONDS)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if thread.is_alive():
        state.logger.warning('C-ECHO to %s timed out', aet)
        return _json(504, {'ok': False, 'elapsed_ms': elapsed_ms,
                           'error': (f'C-ECHO timed out after '
                                     f'{int(ECHO_TIMEOUT_SECONDS)} s')})
    if outcome.get('ok'):
        state.logger.info('C-ECHO to %s succeeded in %d ms', aet,
                          elapsed_ms)
        return _json(200, {'ok': True, 'elapsed_ms': elapsed_ms})
    return _json(502, {'ok': False, 'elapsed_ms': elapsed_ms,
                       'error': outcome.get('error', 'C-ECHO failed')})


def _echo_error(error: Exception) -> str:
    """Maps a client exception onto a short message without internals."""
    name = type(error).__name__
    if name == 'DestinationUnknownError':
        return 'device is not registered'
    if name == 'CEchoError':
        return 'the device answered with a failure status'
    text = str(error).strip()
    if len(text) > 120:
        text = text[:117] + '...'
    return text or f'echo failed ({name})'
