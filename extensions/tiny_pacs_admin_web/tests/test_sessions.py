"""Unit tests of the session store and the login throttle."""
from tiny_pacs_admin_web.sessions import (
    LOCKOUT_BASE_SECONDS,
    LOCKOUT_MAX_SECONDS,
    LoginThrottle,
    SessionStore,
    new_token,
)


class FakeClock:
    """Manually advanced monotonic clock."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_tokens_are_urlsafe_and_unique() -> None:
    tokens = {new_token() for _ in range(100)}
    assert len(tokens) == 100
    for token in tokens:
        assert len(token) >= 32
        assert all(character.isalnum() or character in '-_'
                   for character in token)


def test_create_and_get_roundtrip() -> None:
    clock = FakeClock()
    store = SessionStore(ttl=60, max_sessions=10, clock=clock)
    session = store.create('alice', 'admin')
    assert store.get(session.token) is session
    assert session.username == 'alice'
    assert session.role == 'admin'
    assert session.csrf_token
    assert store.get(None) is None
    assert store.get('bogus') is None


def test_idle_expiry_and_lazy_pruning() -> None:
    clock = FakeClock()
    store = SessionStore(ttl=60, max_sessions=10, clock=clock)
    first = store.create('alice', 'admin')
    second = store.create('bob', 'viewer')
    clock.advance(30)
    # Touching the first session refreshes its idle window only for it
    assert store.get(first.token) is not None
    clock.advance(45)
    assert store.get(first.token) is not None, 'recently used session lives'
    assert store.get(second.token) is None, 'idle session expired'
    assert store.count() == 1


def test_expiry_boundary_is_inclusive_of_ttl() -> None:
    clock = FakeClock()
    store = SessionStore(ttl=60, max_sessions=10, clock=clock)
    store.create('alice', 'admin')
    # Exactly at the TTL the session is still alive (pruning via count()
    # does not refresh the idle window like get() does)
    clock.advance(60)
    assert store.count() == 1
    clock.advance(0.001)
    assert store.count() == 0


def test_max_sessions_evicts_least_recently_used() -> None:
    clock = FakeClock()
    store = SessionStore(ttl=3600, max_sessions=2, clock=clock)
    oldest = store.create('alice', 'admin')
    middle = store.create('bob', 'admin')
    clock.advance(1)
    store.get(middle.token)  # middle becomes the most recently used
    clock.advance(1)
    newest = store.create('carol', 'admin')
    assert store.get(oldest.token) is None, 'LRU session is evicted'
    assert store.get(middle.token) is not None
    assert store.get(newest.token) is not None
    assert store.count() == 2


def test_delete_and_delete_for() -> None:
    clock = FakeClock()
    store = SessionStore(ttl=3600, max_sessions=10, clock=clock)
    keep = store.create('alice', 'admin')
    other = store.create('alice', 'admin')
    foreign = store.create('bob', 'admin')
    assert store.delete_for('alice', keep=keep.token) == 1
    assert store.get(keep.token) is not None
    assert store.get(other.token) is None
    assert store.get(foreign.token) is not None
    store.delete(foreign.token)
    assert store.get(foreign.token) is None
    assert store.delete_for('nobody') == 0


def test_throttle_allows_below_the_limit() -> None:
    clock = FakeClock()
    throttle = LoginThrottle(max_failures=3, clock=clock)
    assert throttle.wait_seconds('alice', '10.0.0.1') == 0.0
    throttle.record_failure('alice', '10.0.0.1')
    throttle.record_failure('alice', '10.0.0.1')
    assert throttle.wait_seconds('alice', '10.0.0.1') == 0.0
    assert throttle.failures('alice', '10.0.0.1') == 2


def test_throttle_locks_with_exponential_backoff() -> None:
    clock = FakeClock()
    throttle = LoginThrottle(max_failures=2, clock=clock)
    throttle.record_failure('alice', '10.0.0.1')
    throttle.record_failure('alice', '10.0.0.1')
    # Lockout starts immediately after the second failure
    assert throttle.wait_seconds('alice', '10.0.0.1') > 0.0
    clock.advance(LOCKOUT_BASE_SECONDS)
    assert throttle.wait_seconds('alice', '10.0.0.1') == 0.0
    # The next failure doubles the window
    throttle.record_failure('alice', '10.0.0.1')
    assert throttle.wait_seconds('alice', '10.0.0.1') \
        == LOCKOUT_BASE_SECONDS * 2
    clock.advance(LOCKOUT_BASE_SECONDS * 2)
    assert throttle.wait_seconds('alice', '10.0.0.1') == 0.0


def test_throttle_backoff_is_capped() -> None:
    clock = FakeClock()
    throttle = LoginThrottle(max_failures=1, clock=clock)
    for _ in range(40):
        throttle.record_failure('alice', '10.0.0.1')
    assert throttle.wait_seconds('alice', '10.0.0.1') <= LOCKOUT_MAX_SECONDS


def test_throttle_counts_pairs_independently() -> None:
    clock = FakeClock()
    throttle = LoginThrottle(max_failures=1, clock=clock)
    throttle.record_failure('alice', '10.0.0.1')
    assert throttle.wait_seconds('alice', '10.0.0.1') > 0.0
    assert throttle.wait_seconds('alice', '10.0.0.2') == 0.0
    assert throttle.wait_seconds('bob', '10.0.0.1') == 0.0


def test_throttle_reset_and_prune() -> None:
    clock = FakeClock()
    throttle = LoginThrottle(max_failures=1, clock=clock)
    throttle.record_failure('alice', '10.0.0.1')
    assert throttle.wait_seconds('alice', '10.0.0.1') > 0.0
    throttle.reset('alice', '10.0.0.1')
    assert throttle.wait_seconds('alice', '10.0.0.1') == 0.0
    assert throttle.failures('alice', '10.0.0.1') == 0
    throttle.record_failure('bob', '10.0.0.9')
    clock.advance(1000)
    throttle.record_failure('carol', '10.0.0.9')
    throttle.prune(max_age=500)
    assert throttle.failures('bob', '10.0.0.9') == 0
    assert throttle.failures('carol', '10.0.0.9') == 1
