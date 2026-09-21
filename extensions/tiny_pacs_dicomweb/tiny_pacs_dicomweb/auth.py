"""Request authentication middleware.

The three documented modes (see the proposal):

* ``auth: none`` — the default and a valid choice: many deployments run
  DICOMweb behind an authenticating reverse proxy;
* ``auth: basic`` — HTTP basic credentials verified through the core
  :class:`~tiny_pacs.events.UserVerify` event; fails closed (every
  request is refused with 401) when no user registry listens;
* ``auth: token`` — static bearer tokens from the configuration,
  compared in constant time (the service-to-service pattern).

Failures answer 401 with an empty body — no detail about *why* a
credential was refused — plus the scheme's ``WWW-Authenticate`` header.
Password verification (including timing equalization) is the user
registry's responsibility; this middleware never hashes or stores
credentials and never logs the ``Authorization`` header.

Basic-auth failures are throttled per ``(username, peer)`` pair with the
same exponential backoff the admin-web console applies (the two mounts
share one user registry, so an unthrottled DICOMweb endpoint would
otherwise reopen the brute-force path the console closes).

The authenticated username (when available) is published on the WSGI
environ as ``tiny_pacs.username`` for the service handlers' audit
emission (:func:`environ_username`).
"""
import base64
import binascii
import hmac
import threading
import time
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

import bottle  # type: ignore[import-untyped]
from tiny_pacs import events

if TYPE_CHECKING:  # pragma: no cover - typing only
    from . import common

#: WSGI environ key carrying the authenticated username (or None)
USERNAME_ENVIRON = 'tiny_pacs.username'

#: Upper bound for a basic-auth password: longer inputs are refused
#: without running a (deliberately expensive) password proof against them
MAX_PASSWORD_LENGTH = 4096

#: Bound of the throttle key for an arbitrarily long presented username
#: (mirrors the user registry's username limit used by admin-web)
MAX_USERNAME_LENGTH = 64

#: Consecutive basic-auth failures tolerated per ``(username, peer)``
#: pair before the backoff starts (admin-web's default, kept identical
#: so both mounts of the same registry throttle alike)
MAX_FAILURES = 5

#: Exponential backoff window of the failure throttle, in seconds
LOCKOUT_BASE_SECONDS = 5.0
LOCKOUT_MAX_SECONDS = 3600.0

#: Minimum seconds between two lazy prune runs of the failure counters
THROTTLE_PRUNE_SECONDS = 60.0

#: Hard cap of tracked ``(username, peer)`` pairs (oldest evicted):
#: a username spray can neither grow the counters without bound nor
#: make pruning unbounded work
THROTTLE_MAX_ENTRIES = 4096

#: ``WWW-Authenticate`` challenge of the basic mode
BASIC_CHALLENGE = 'Basic realm="tiny_pacs"'

#: ``WWW-Authenticate`` challenge of the token mode
BEARER_CHALLENGE = 'Bearer realm="tiny_pacs"'


def environ_username() -> str | None:
    """The authenticated username of the current request, when known.

    :return: username published by the authentication middleware on the
             WSGI environ, None when absent
    :rtype: str or None
    """
    value = bottle.request.environ.get(USERNAME_ENVIRON)
    return str(value) if value else None


class _Refused(Exception):
    """Internal signal: answer the request with an empty-body 401."""

    def __init__(self, challenge: str) -> None:
        """Records the ``WWW-Authenticate`` challenge to send.

        :param challenge: value of the ``WWW-Authenticate`` header
        :type challenge: str
        """
        super().__init__(challenge)
        self.challenge = challenge


class LoginThrottle:
    """Exponential-backoff throttling of failed basic-auth attempts.

    Failures are counted per ``(username, peer)`` pair; after
    :attr:`max_failures` consecutive failures every further attempt from
    the pair is refused until the backoff window elapses. A successful
    authentication resets the counter. Semantics mirror the admin-web
    console's ``LoginThrottle`` so both mounts of the same user registry
    enforce the identical policy. Counters are pruned lazily and the
    structure is capped at :attr:`max_entries` pairs.

    :ivar max_failures: failures tolerated before the backoff starts
    """

    def __init__(
            self,
            max_failures: int = MAX_FAILURES,
            clock: Callable[[], float] = time.monotonic,
            max_entries: int = THROTTLE_MAX_ENTRIES
    ) -> None:
        """Initializes the throttle.

        :param max_failures: failures tolerated before the backoff starts
        :type max_failures: int
        :param clock: monotonic clock, injectable for tests
        :type clock: Callable[[], float]
        :param max_entries: hard cap of tracked pairs; the oldest
                            entries are evicted beyond it
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

        :param username: throttle key of the presented login name
        :type username: str
        :param peer: request peer address
        :type peer: str
        :return: seconds the pair is still locked out; 0 when the pair
                 may attempt authentication now
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
        """Records a failed authentication attempt.

        :param username: throttle key of the presented login name
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
        """Clears the failure counter after a successful authentication.

        :param username: throttle key of the authenticated login name
        :type username: str
        :param peer: request peer address
        :type peer: str
        """
        with self._lock:
            self._failures.pop((username, peer), None)

    def maybe_prune(self, max_age: float, interval: float) -> None:
        """Prunes expired counters at most once per ``interval`` seconds.

        Also enforces the :attr:`max_entries` cap by evicting the
        longest-idle pairs beyond it.

        :param max_age: counters idle for at least this many seconds are
                        dropped
        :type max_age: float
        :param interval: minimum seconds between two prune runs
        :type interval: float
        """
        with self._lock:
            if self._clock() - self._last_prune < interval:
                return
            self._last_prune = self._clock()
            now = self._clock()
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


class AuthMiddleware:
    """WSGI middleware enforcing the configured authentication mode.

    Runs inside the logging middleware and outside the bottle app, so
    refused requests never reach a service handler.
    """

    def __init__(self, app: Any, state: 'common.AppState',
                 throttle: LoginThrottle | None = None) -> None:
        """Initializes the middleware.

        :param app: wrapped WSGI callable (the bottle application)
        :param state: shared application state (mode, tokens, bus)
        :param throttle: basic-auth failure throttle, injectable for
                         tests; defaults to a fresh policy instance
        """
        self.app = app
        self.state = state
        self.throttle = throttle if throttle is not None \
            else LoginThrottle()

    def __call__(
            self,
            environ: dict[str, Any],
            start_response: Callable[..., Any]
    ) -> Iterable[bytes]:
        """Authenticates one request and passes it to the app."""
        try:
            environ[USERNAME_ENVIRON] = self._authenticate(environ)
        except _Refused as refused:
            headers = [('Content-Type', 'text/plain; charset=utf-8'),
                       ('Content-Length', '0'),
                       ('WWW-Authenticate', refused.challenge)]
            start_response('401 Unauthorized', headers)
            return []
        result: Iterable[bytes] = self.app(environ, start_response)
        return result

    def _authenticate(self, environ: dict[str, Any]) -> str | None:
        """Verifies the credentials of one request.

        :param environ: WSGI environment of the current request
        :type environ: dict
        :return: the authenticated username, None when the mode carries
                 no identity (``none``/``token``)
        :rtype: str or None
        :raises _Refused: raised when the request must be answered 401
        """
        if self.state.auth == 'none':
            return None
        header = str(environ.get('HTTP_AUTHORIZATION') or '')
        if self.state.auth == 'token':
            if _bearer_ok(header, self.state.tokens):
                return None
            raise _Refused(BEARER_CHALLENGE)
        if not self.state.has_feature(events.UserVerify):
            self.state.logger.warning(
                'Refusing a basic-auth request: no component answers '
                'UserVerify (no user registry installed)'
            )
            raise _Refused(BASIC_CHALLENGE)
        username, password = _basic_credentials(header)
        peer = str(environ.get('REMOTE_ADDR') or '-')
        # Bound the throttle key even for an arbitrarily long presented
        # username; the registry refuses oversized names anyway
        key = username[:MAX_USERNAME_LENGTH]
        # Lazily bound the failure counters: expired pairs are dropped
        # at most once a minute and the dict itself is capped
        self.throttle.maybe_prune(max_age=4 * LOCKOUT_MAX_SECONDS,
                                  interval=THROTTLE_PRUNE_SECONDS)
        if self.throttle.wait_seconds(key, peer) > 0:
            self.state.logger.warning(
                'Basic auth throttled for %r from %s', key, peer
            )
            raise _Refused(BASIC_CHALLENGE)
        if password is None:
            self.throttle.record_failure(key, peer)
            raise _Refused(BASIC_CHALLENGE)
        try:
            user = self.state.bus.send_any(
                events.UserVerify,
                {'username': username, 'password': password}
            )
        except Exception:
            # A failing user registry must not leak details into the
            # response: basic auth fails closed with the generic 401
            self.state.logger.exception(
                'UserVerify failed while checking basic credentials'
            )
            self.throttle.record_failure(key, peer)
            raise _Refused(BASIC_CHALLENGE) from None
        if user is None:
            self.throttle.record_failure(key, peer)
            raise _Refused(BASIC_CHALLENGE)
        self.throttle.reset(key, peer)
        return str(getattr(user, 'username', None) or username)


def _bearer_ok(header: str, tokens: tuple[str, ...]) -> bool:
    """Constant-time check of a bearer credential.

    :param header: raw ``Authorization`` header value
    :type header: str
    :param tokens: configured bearer tokens (ASCII, validated by the
                   component configuration)
    :type tokens: tuple[str, ...]
    :return: True when the header carries one of the configured tokens
    :rtype: bool
    """
    scheme, _, credential = header.partition(' ')
    if scheme.strip().lower() != 'bearer' or not credential.strip():
        return False
    # Compared as bytes: WSGI decodes headers as latin-1, and
    # hmac.compare_digest refuses non-ASCII ``str`` operands with a
    # TypeError that would escape the middleware as a 500
    provided = credential.strip().encode('utf-8', 'surrogateescape')
    return any(
        hmac.compare_digest(provided, token.encode('utf-8'))
        for token in tokens
    )


def _basic_credentials(header: str) -> tuple[str, str | None]:
    """Decodes an HTTP basic credential.

    :param header: raw ``Authorization`` header value
    :type header: str
    :return: username and password; the password is None when the header
             is absent, malformed or over-long (all refused identically)
    :rtype: tuple[str, str or None]
    """
    scheme, _, encoded = header.partition(' ')
    if scheme.strip().lower() != 'basic' or not encoded.strip():
        return '', None
    try:
        decoded = base64.b64decode(encoded.strip(), validate=True)
        text = decoded.decode('utf-8')
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return '', None
    username, separator, password = text.partition(':')
    if not separator or not username or \
            len(password) > MAX_PASSWORD_LENGTH:
        return '', None
    return username, password
