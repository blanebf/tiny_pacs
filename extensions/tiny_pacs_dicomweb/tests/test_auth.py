"""Authentication matrix tests: none/basic/token × authorized, wrong
and missing credentials, plus the fail-closed behaviour of basic auth
without a user registry and the header-logging redaction.
"""
import base64
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from tiny_pacs import events as core_events

from .conftest import make_dataset, stow_push


def basic(username: str, password: str) -> dict[str, str]:
    """Builds an HTTP basic ``Authorization`` header value."""
    token = base64.b64encode(
        f'{username}:{password}'.encode()
    ).decode('ascii')
    return {'Authorization': f'Basic {token}'}


def bearer(token: str) -> dict[str, str]:
    """Builds a bearer ``Authorization`` header."""
    return {'Authorization': f'Bearer {token}'}


@contextmanager
def component_log(name: str = 'DICOMWeb'
                  ) -> Iterator[list[logging.LogRecord]]:
    """Captures records of the component logger (DEBUG and up)."""
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


# -----------------------------------------------------------------------
# auth: none
# -----------------------------------------------------------------------

def test_auth_none_accepts_everything(env: Any) -> None:
    environment = env(storage='memory')
    assert environment.client.get('/studies').status == 204
    assert stow_push(environment.client, [make_dataset()]).status == 200


# -----------------------------------------------------------------------
# auth: token
# -----------------------------------------------------------------------

def test_token_matrix(env: Any) -> None:
    environment = env(storage='memory',
                      config={'auth': 'token', 'tokens': ['t0ken']})
    client = environment.client
    assert client.get('/studies').status == 401, 'missing credential'
    assert client.get('/studies', headers=bearer('wrong')).status == 401
    response = client.get('/studies', headers=bearer('t0ken'))
    assert response.status == 204
    # mutations are guarded identically
    assert stow_push(client, [make_dataset()]).status == 401
    assert stow_push(client, [make_dataset()],
                     headers=bearer('t0ken')).status == 200


def test_token_failure_carries_no_details(env: Any) -> None:
    environment = env(storage='memory',
                      config={'auth': 'token', 'tokens': ['t0ken']})
    response = environment.client.get('/studies')
    assert response.status == 401
    assert response.body == b''
    assert response.header('WWW-Authenticate') \
        == 'Bearer realm="tiny_pacs"'


def test_non_ascii_bearer_credential_answers_401(env: Any) -> None:
    """WSGI decodes headers as latin-1; a non-ASCII credential must
    still produce the uniform empty-body 401 (``hmac.compare_digest``
    refuses non-ASCII ``str`` operands with a TypeError that must not
    escape the auth middleware as a 500)."""
    environment = env(storage='memory',
                      config={'auth': 'token', 'tokens': ['t0ken']})
    response = environment.client.get(
        '/studies', headers={'Authorization': 'Bearer t\u00ffken'}
    )
    assert response.status == 401
    assert response.body == b''


def test_multiple_tokens_are_accepted(env: Any) -> None:
    environment = env(
        storage='memory',
        config={'auth': 'token', 'tokens': ['first', 'second']}
    )
    assert environment.client.get('/studies',
                                  headers=bearer('first')).status == 204
    assert environment.client.get('/studies',
                                  headers=bearer('second')).status == 204


# -----------------------------------------------------------------------
# auth: basic
# -----------------------------------------------------------------------

def test_basic_matrix(env: Any) -> None:
    environment = env(storage='memory', config={'auth': 'basic'})
    client = environment.client
    assert client.get('/studies').status == 401, 'missing credential'
    assert client.get('/studies', headers=basic('alice', 'wrong')) \
        .status == 401
    response = client.get('/studies', headers=basic('alice', 'secret'))
    assert response.status == 204
    assert stow_push(client, [make_dataset()],
                     headers=basic('alice', 'secret')).status == 200
    assert stow_push(client, [make_dataset()]).status == 401


def test_basic_malformed_headers_refused_identically(env: Any) -> None:
    environment = env(storage='memory', config={'auth': 'basic'})
    client = environment.client
    for header in ('Basic not-base64!!', 'Basic ', 'Bearer whatever',
                   'Digest username="x"', 'Basic ' + 'AQ=='):
        assert client.get('/studies',
                          headers={'Authorization': header}).status == 401


def test_basic_failure_carries_challenge_without_body(env: Any) -> None:
    environment = env(storage='memory', config={'auth': 'basic'})
    response = environment.client.get('/studies')
    assert response.status == 401
    assert response.body == b''
    assert response.header('WWW-Authenticate') \
        == 'Basic realm="tiny_pacs"'


def test_basic_without_user_registry_fails_closed(env: Any) -> None:
    """The component contributes no app at all (see the component
    suite); an app built manually on such a bus still refuses every
    request."""
    environment = env(storage='memory', user_verify=False,
                      dicomweb=False, config={'auth': 'basic'})
    assert environment.component is None
    from tiny_pacs_dicomweb import web as web_module
    from tiny_pacs_dicomweb.common import AppState
    state = AppState(bus=environment.bus,
                     logger=logging.getLogger('test.dicomweb'),
                     auth='basic')
    from .conftest import RawWSGIClient
    client = RawWSGIClient(web_module.build_app(state))
    response = client.get('/studies', headers=basic('alice', 'secret'))
    assert response.status == 401
    assert response.body == b''


def test_basic_authenticates_through_user_verify(env: Any) -> None:
    """The audit trail sees the verified username of basic-auth pushes."""
    environment = env(storage='memory', collect_audit=True,
                      config={'auth': 'basic'})
    assert stow_push(environment.client, [make_dataset()],
                     headers=basic('bob', 'secret')).status == 200
    records = [record for record in environment.audit
               if record.event == 'stow']
    assert records and records[0].username == 'bob'


def test_basic_failures_are_throttled(env: Any) -> None:
    """After MAX_FAILURES consecutive failures the (username, peer) pair
    is locked out: even the valid credential is refused until the backoff
    window elapses (same policy as the admin-web console)."""
    from tiny_pacs_dicomweb.auth import MAX_FAILURES
    environment = env(storage='memory', config={'auth': 'basic'})
    client = environment.client
    for _ in range(MAX_FAILURES):
        assert client.get('/studies',
                          headers=basic('alice', 'wrong')).status == 401
    assert client.get('/studies',
                      headers=basic('alice', 'secret')).status == 401, \
        'the locked-out pair must be refused even with valid credentials'


def test_throttle_backoff_uses_injected_clock() -> None:
    from tiny_pacs_dicomweb.auth import LOCKOUT_BASE_SECONDS, LoginThrottle
    now = [0.0]
    throttle = LoginThrottle(max_failures=2, clock=lambda: now[0])
    assert throttle.wait_seconds('alice', 'peer') == 0.0
    throttle.record_failure('alice', 'peer')
    assert throttle.wait_seconds('alice', 'peer') == 0.0
    throttle.record_failure('alice', 'peer')
    assert throttle.wait_seconds('alice', 'peer') == LOCKOUT_BASE_SECONDS
    now[0] += LOCKOUT_BASE_SECONDS
    assert throttle.wait_seconds('alice', 'peer') == 0.0
    # the window doubles with every further failure
    throttle.record_failure('alice', 'peer')
    assert throttle.wait_seconds('alice', 'peer') \
        == 2 * LOCKOUT_BASE_SECONDS
    throttle.reset('alice', 'peer')
    assert throttle.wait_seconds('alice', 'peer') == 0.0
    # pairs are independent
    throttle.record_failure('alice', 'other')
    assert throttle.wait_seconds('alice', 'peer') == 0.0


def test_throttle_cap_evicts_oldest_pairs() -> None:
    from tiny_pacs_dicomweb.auth import LoginThrottle
    now = [0.0]
    throttle = LoginThrottle(max_failures=5, clock=lambda: now[0],
                             max_entries=2)
    throttle.record_failure('a', 'p')
    now[0] += 1.0
    throttle.record_failure('b', 'p')
    now[0] += 1.0
    throttle.record_failure('c', 'p')
    now[0] += 121.0
    throttle.maybe_prune(max_age=3600.0, interval=60.0)
    assert throttle.record_failure('a', 'p') == 1, \
        'the longest-idle pair is evicted beyond the cap'
    assert throttle.record_failure('b', 'p') == 2


def test_verify_errors_are_not_leaked(env: Any) -> None:
    """A raising user registry fails closed: the generic 401, no
    details, no 500."""
    environment = env(storage='memory', user_verify=False,
                      dicomweb=False)

    def broken(_: Any) -> Any:
        raise RuntimeError('registry exploded')

    environment.bus.subscribe(core_events.UserVerify, broken)
    from tiny_pacs_dicomweb import web as web_module
    from tiny_pacs_dicomweb.common import AppState

    from .conftest import RawWSGIClient
    state = AppState(bus=environment.bus,
                     logger=logging.getLogger('test.dicomweb'),
                     auth='basic')
    client = RawWSGIClient(web_module.build_app(state))
    response = client.get('/studies', headers=basic('alice', 'secret'))
    assert response.status == 401
    assert response.body == b''
    assert 'registry exploded' not in response.text


# -----------------------------------------------------------------------
# Logging redaction
# -----------------------------------------------------------------------

def test_authorization_header_is_never_logged(env: Any) -> None:
    environment = env(storage='memory', dicomweb=False,
                      config={'auth': 'token', 'tokens': ['t0ken']})
    from tiny_pacs_dicomweb import web as web_module
    from tiny_pacs_dicomweb.common import AppState

    from .conftest import RawWSGIClient
    with component_log('test.dicomweb') as records:
        state = AppState(
            bus=environment.bus,
            logger=logging.getLogger('test.dicomweb'),
            auth='token', tokens=('t0ken',)
        )
        client = RawWSGIClient(web_module.build_app(state))
        client.get('/studies', headers=bearer('t0ken'))
        client.post('/studies', b'x',
                    'multipart/related; boundary="x"',
                    headers={'Authorization': 'Bearer t0ken'})
    text = '\n'.join(record.getMessage() for record in records)
    assert 't0ken' not in text
    assert 'Authorization' not in text
    assert 'authorization' not in text
    assert any('headers' in record.getMessage()
               for record in records
               if record.levelno == logging.DEBUG), \
        'headers are logged at DEBUG (redacted)'


def test_request_log_has_no_bodies(env: Any) -> None:
    environment = env(storage='memory', dicomweb=False)
    from tiny_pacs_dicomweb import web as web_module
    from tiny_pacs_dicomweb.common import AppState

    from .conftest import RawWSGIClient
    with component_log('test.dicomweb') as records:
        state = AppState(bus=environment.bus,
                         logger=logging.getLogger('test.dicomweb'))
        client = RawWSGIClient(web_module.build_app(state))
        ds = make_dataset(patient_name='Secret^Person')
        stow_push(client, [ds])
    info = '\n'.join(record.getMessage() for record in records
                     if record.levelno >= logging.INFO)
    assert 'Secret^Person' not in info
    assert 'Secret' not in info
    assert 'DICM' not in info
