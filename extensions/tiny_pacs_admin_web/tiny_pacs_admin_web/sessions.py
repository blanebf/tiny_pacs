"""Session store and login throttling primitives.

Server-side, in-memory and guarded by a lock — the console is a
single-process application by design (see the proposal): sessions reset
when the server restarts and are never shared between nodes.

Both primitives take an injectable monotonic clock so tests can drive
expiry and backoff windows deterministically.
"""
import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

#: Cookie carrying the session token
SESSION_COOKIE = 'tpaw_session'

#: Length in bytes of the random session and CSRF tokens
TOKEN_BYTES = 32

#: Base of the exponential login backoff, in seconds
LOCKOUT_BASE_SECONDS = 5.0

#: Upper bound of the exponential login backoff, in seconds
LOCKOUT_MAX_SECONDS = 3600.0


def new_token() -> str:
    """Generates a fresh opaque token.

    :return: URL-safe random token
    :rtype: str
    """
    return secrets.token_urlsafe(TOKEN_BYTES)


@dataclass
class Session:
    """One authenticated console session.

    :ivar token: opaque session token carried by the cookie
    :ivar username: authenticated PACS user
    :ivar role: console role captured at login time; the authoritative
                role is re-read from the grant table on every request, so
                revocations and role changes take effect immediately
    :ivar csrf_token: per-session CSRF token, required on every
                      state-changing request
    :ivar created: creation time on the store's clock
    :ivar last_seen: time of the last authenticated request
    """

    token: str
    username: str
    role: str
    csrf_token: str = field(default_factory=new_token)
    created: float = 0.0
    last_seen: float = 0.0


class SessionStore:
    """Lock-guarded in-memory session store with lazy expiry pruning.

    Sessions expire :data:`ttl` seconds after their last use (a sliding
    idle window). Expired entries are pruned lazily on every store
    operation; the store never grows past :data:`max_sessions` — the
    least recently used session is evicted when a new one is created.

    :ivar ttl: session idle timeout in seconds
    :ivar max_sessions: maximum number of concurrent sessions
    """

    def __init__(
            self,
            ttl: float,
            max_sessions: int,
            clock: Callable[[], float] = time.monotonic
    ) -> None:
        """Initializes the store.

        :param ttl: session idle timeout in seconds
        :type ttl: float
        :param max_sessions: maximum number of concurrent sessions
        :type max_sessions: int
        :param clock: monotonic clock used for expiry, injectable for
                      tests
        :type clock: Callable[[], float]
        """
        self.ttl = ttl
        self.max_sessions = max_sessions
        self._clock = clock
        self._lock = threading.Lock()
        self._sessions: dict[str, Session] = {}

    def create(self, username: str, role: str) -> Session:
        """Creates a session for an authenticated user.

        Prunes expired sessions first and evicts the least recently used
        session while the store is at capacity.

        :param username: authenticated PACS user
        :type username: str
        :param role: console role of the user
        :type role: str
        :return: the new session
        :rtype: Session
        """
        now = self._clock()
        session = Session(
            token=new_token(), username=username, role=role,
            created=now, last_seen=now
        )
        with self._lock:
            self._prune(now)
            while len(self._sessions) >= self.max_sessions:
                oldest = min(self._sessions.values(),
                             key=lambda s: s.last_seen)
                del self._sessions[oldest.token]
            self._sessions[session.token] = session
        return session

    def get(self, token: str | None) -> Session | None:
        """Looks a session up by its token.

        Refreshes the idle window of a live session; an expired or
        unknown token returns None (and expired entries are pruned).

        :param token: session token from the request cookie
        :type token: str or None
        :return: the live session or None
        :rtype: Session or None
        """
        if not token:
            return None
        now = self._clock()
        with self._lock:
            self._prune(now)
            session = self._sessions.get(token)
            if session is None:
                return None
            session.last_seen = now
            return session

    def delete(self, token: str) -> None:
        """Destroys one session (logout).

        :param token: session token
        :type token: str
        """
        with self._lock:
            self._sessions.pop(token, None)

    def delete_for(self, username: str, keep: str | None = None) -> int:
        """Destroys every session of a user except one.

        Used when the user's password changes: the acting session stays
        alive, every other session of the same user in this process is
        invalidated.

        :param username: login name whose sessions are invalidated
        :type username: str
        :param keep: session token to keep alive, None destroys all
        :type keep: str or None
        :return: number of destroyed sessions
        :rtype: int
        """
        with self._lock:
            doomed = [
                token for token, session in self._sessions.items()
                if session.username == username and token != keep
            ]
            for token in doomed:
                del self._sessions[token]
        return len(doomed)

    def count(self) -> int:
        """Number of live sessions (for dashboards and tests).

        :return: live session count
        :rtype: int
        """
        with self._lock:
            self._prune(self._clock())
            return len(self._sessions)

    def _prune(self, now: float) -> None:
        """Removes expired sessions; the lock must be held."""
        expired = [
            token for token, session in self._sessions.items()
            if now - session.last_seen > self.ttl
        ]
        for token in expired:
            del self._sessions[token]


class LoginThrottle:
    """Exponential-backoff throttling of failed login attempts.

    Failures are counted per ``(username, peer)`` pair; after
    :data:`max_failures` consecutive failures every further attempt from
    the pair is refused until the backoff window elapses. A successful
    login resets the counter. Counters are pruned lazily and the
    structure is capped at :data:`max_entries` pairs (the oldest entries
    are evicted beyond it), so a username spray can neither grow it
    without bound nor make pruning unbounded work.

    :ivar max_failures: failures tolerated before the backoff starts
    """

    def __init__(
            self,
            max_failures: int,
            clock: Callable[[], float] = time.monotonic,
            max_entries: int = 4096
    ) -> None:
        """Initializes the throttle.

        :param max_failures: failures tolerated before the backoff starts
        :type max_failures: int
        :param clock: monotonic clock, injectable for tests
        :type clock: Callable[[], float]
        :param max_entries: hard cap of tracked ``(username, peer)``
                            pairs; the oldest entries are evicted beyond
                            it
        :type max_entries: int
        """
        self.max_failures = max(1, max_failures)
        self.max_entries = max(1, max_entries)
        self._clock = clock
        self._lock = threading.Lock()
        self._failures: dict[tuple[str, str], tuple[int, float]] = {}
        self._last_prune = clock()

    def wait_seconds(self, username: str, peer: str) -> float:
        """Remaining lockout of a ``(username, peer)`` pair.

        :param username: presented login name
        :type username: str
        :param peer: request peer address
        :type peer: str
        :return: seconds the pair is still locked out; 0 when the pair
                 may attempt a login now
        :rtype: float
        """
        with self._lock:
            entry = self._failures.get((username, peer))
            if entry is None:
                return 0.0
            failures, last = entry
            if failures < self.max_failures:
                return 0.0
            window = min(
                LOCKOUT_BASE_SECONDS
                * 2.0 ** (failures - self.max_failures),
                LOCKOUT_MAX_SECONDS
            )
            remaining = last + window - self._clock()
            return remaining if remaining > 0 else 0.0

    def record_failure(self, username: str, peer: str) -> int:
        """Records a failed login attempt.

        :param username: presented login name
        :type username: str
        :param peer: request peer address
        :type peer: str
        :return: consecutive failures of the pair so far
        :rtype: int
        """
        with self._lock:
            failures, _ = self._failures.get((username, peer), (0, 0.0))
            failures += 1
            self._failures[(username, peer)] = (failures, self._clock())
            return failures

    def reset(self, username: str, peer: str) -> None:
        """Clears the failure counter after a successful login.

        :param username: authenticated login name
        :type username: str
        :param peer: request peer address
        :type peer: str
        """
        with self._lock:
            self._failures.pop((username, peer), None)

    def failures(self, username: str, peer: str) -> int:
        """Consecutive failures of a pair (for tests and dashboards).

        :param username: presented login name
        :type username: str
        :param peer: request peer address
        :type peer: str
        :return: recorded consecutive failures
        :rtype: int
        """
        with self._lock:
            return self._failures.get((username, peer), (0, 0.0))[0]

    def prune(self, max_age: float) -> None:
        """Drops counters whose backoff window expired long ago.

        Also enforces the :attr:`max_entries` cap: when more pairs are
        tracked than the cap allows (an aggressive username spray within
        the retention window), the entries idle for the longest time are
        evicted.

        :param max_age: counters idle for at least this many seconds are
                        dropped
        :type max_age: float
        """
        now = self._clock()
        with self._lock:
            stale = [
                key for key, (_, last) in self._failures.items()
                if now - last > max_age
            ]
            for key in stale:
                del self._failures[key]
            overflow = len(self._failures) - self.max_entries
            if overflow > 0:
                oldest = sorted(
                    self._failures.items(), key=lambda item: item[1][1]
                )[:overflow]
                for key, _ in oldest:
                    del self._failures[key]
            self._last_prune = now

    def maybe_prune(self, max_age: float, interval: float) -> None:
        """Prunes at most once per ``interval`` seconds.

        Callers on a request path use this instead of :meth:`prune` so
        the full-dictionary scan runs on a time interval rather than on
        every request.

        :param max_age: counters idle for at least this many seconds are
                        dropped
        :type max_age: float
        :param interval: minimum seconds between two prune runs
        :type interval: float
        """
        with self._lock:
            if self._clock() - self._last_prune < interval:
                return
        self.prune(max_age)
