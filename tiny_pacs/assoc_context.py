"""Process-wide registry of live incoming associations.

The :class:`AE <tiny_pacs.ae.AE>` opens an :class:`AssocContext` for every
incoming association before broadcasting
:class:`~tiny_pacs.events.Assoc`, so every listener of that event — and
every DIMSE handler running afterwards — can attribute its work to the
association through :func:`current`. The registry is indexed by thread id:
an association is accepted on its connection thread and all DIMSE requests
of that association are handled synchronously on the same thread.

Authentication components set :attr:`AssocContext.username` on successful
authentication; audit and access-control extensions read it without any
coupling to the authenticating component.
"""
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any

from pynetdicom2 import pdu


@dataclass
class AssocContext:
    """Context of one live incoming association.

    :ivar correlation_id: unique id, stable for the lifetime of the
                          association
    :ivar calling_aet: calling AE title of the association request
    :ivar called_aet: called AE title of the association request
    :ivar peer: peer address of the connection, empty when unknown
    :ivar username: authenticated principal; set by the authentication
                    listener on success, None when no user is known
    """

    #: Unique correlation id, stable for the lifetime of the association
    correlation_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    #: Calling AE title of the association request
    calling_aet: str = ''

    #: Called AE title of the association request
    called_aet: str = ''

    #: Peer address of the connection, empty when unknown
    peer: str = ''

    #: Authenticated principal, set by the authentication listener
    username: str | None = None


class _Registry:
    """Thread-indexed registry of live association contexts."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_thread: dict[int, AssocContext] = {}
        self._by_id: dict[str, int] = {}

    def add(self, context: AssocContext) -> None:
        with self._lock:
            self._by_thread[threading.get_ident()] = context
            self._by_id[context.correlation_id] = threading.get_ident()

    def current(self) -> AssocContext | None:
        with self._lock:
            return self._by_thread.get(threading.get_ident())

    def close(self, correlation_id: str) -> AssocContext | None:
        with self._lock:
            thread_id = self._by_id.pop(correlation_id, None)
            if thread_id is None:
                return None
            context = self._by_thread.pop(thread_id, None)
            if (context is not None
                    and context.correlation_id != correlation_id):
                # Defensive: the thread mapping must agree with the id
                self._by_thread[thread_id] = context
                self._by_id[context.correlation_id] = thread_id
                return None
            return context

    def close_current(self) -> AssocContext | None:
        with self._lock:
            thread_id = threading.get_ident()
            context = self._by_thread.pop(thread_id, None)
            if context is not None:
                self._by_id.pop(context.correlation_id, None)
            return context


_registry = _Registry()


def current() -> AssocContext | None:
    """Returns the association context of the current thread.

    :return: the context opened on this thread, or None when no
             association is being handled here
    :rtype: AssocContext or None
    """
    return _registry.current()


def open_context(asce: Any, assoc: pdu.AAssociateRqPDU) -> AssocContext:
    """Opens the association context of an incoming association request.

    Called by the AE on the accepting thread before broadcasting
    :class:`~tiny_pacs.events.Assoc`.

    :param asce: association acceptor handling the incoming connection
    :param assoc: association request parameters
    :return: the opened context
    :rtype: AssocContext
    """
    context = AssocContext(
        calling_aet=assoc.calling_ae_title.strip(),
        called_aet=assoc.called_ae_title.strip(),
        peer=_peer_address(asce)
    )
    _registry.add(context)
    return context


def close_context(correlation_id: str) -> AssocContext | None:
    """Closes the context of the association with the given correlation id.

    :param correlation_id: correlation id of the association
    :type correlation_id: str
    :return: the closed context, or None when no such association is
             registered
    :rtype: AssocContext or None
    """
    return _registry.close(correlation_id)


def close_current() -> AssocContext | None:
    """Closes the context registered for the current thread.

    Used by the AE at association teardown, whatever the reason
    (release, abort, timeout or error).

    :return: the closed context, or None when this thread has none
    :rtype: AssocContext or None
    """
    return _registry.close_current()


def _peer_address(asce: Any) -> str:
    """Extracts the peer address from the association acceptor.

    The association no longer exposes the peer address directly; it is
    obtained from the DUL provider socket. Missing sockets (e.g. in
    tests) yield an empty address.
    """
    try:
        dul_socket = asce.dul.dul_socket
        if dul_socket is None:
            return ''
        return str(dul_socket.getpeername()[0])
    except (AttributeError, OSError):
        return ''
