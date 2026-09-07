"""Tests of the association context registry."""
import threading
from types import SimpleNamespace

from pynetdicom2 import pdu

from tiny_pacs import assoc_context


def _assoc(calling: str = 'TEST',
           called: str = 'TINY_PACS') -> pdu.AAssociateRqPDU:
    return pdu.AAssociateRqPDU(called, calling, [])


def _asce(peer: str = '10.0.0.5') -> SimpleNamespace:
    sock = SimpleNamespace(getpeername=lambda: (peer, 40000))
    return SimpleNamespace(dul=SimpleNamespace(dul_socket=sock))


def setup_function() -> None:
    while assoc_context.close_current() is not None:
        pass


def teardown_function() -> None:
    while assoc_context.close_current() is not None:
        pass


def test_open_and_current() -> None:
    assert assoc_context.current() is None
    context = assoc_context.open_context(_asce(), _assoc())
    try:
        assert assoc_context.current() is context
        assert context.calling_aet == 'TEST'
        assert context.called_aet == 'TINY_PACS'
        assert context.peer == '10.0.0.5'
        assert context.username is None
        assert context.correlation_id
    finally:
        assoc_context.close_current()


def test_ae_titles_are_stripped() -> None:
    context = assoc_context.open_context(_asce(), _assoc('PADDED        '))
    try:
        assert context.calling_aet == 'PADDED'
    finally:
        assoc_context.close_current()


def test_missing_socket_yields_empty_peer() -> None:
    context = assoc_context.open_context(
        SimpleNamespace(dul=SimpleNamespace(dul_socket=None)), _assoc()
    )
    try:
        assert context.peer == ''
    finally:
        assoc_context.close_current()
    # A totally broken acceptor must not crash the association setup
    context = assoc_context.open_context(None, _assoc())
    try:
        assert context.peer == ''
    finally:
        assoc_context.close_current()


def test_username_enrichment() -> None:
    context = assoc_context.open_context(_asce(), _assoc())
    try:
        context.username = 'alice'
        current = assoc_context.current()
        assert current is not None
        assert current.username == 'alice'
    finally:
        assoc_context.close_current()


def test_close_by_correlation_id() -> None:
    context = assoc_context.open_context(_asce(), _assoc())
    assert assoc_context.close_context(context.correlation_id) is context
    assert assoc_context.current() is None
    # Closing twice is a no-op
    assert assoc_context.close_context(context.correlation_id) is None
    # Unknown ids close nothing
    assert assoc_context.close_context('no-such-id') is None


def test_thread_isolation() -> None:
    """The registry is indexed per thread: concurrent associations do not
    see each other's contexts through ``current()``."""
    main_context = assoc_context.open_context(_asce('10.0.0.1'), _assoc())
    results: list[bool] = []

    def worker() -> None:
        assert assoc_context.current() is None
        context = assoc_context.open_context(
            _asce('10.0.0.2'), _assoc('OTHER'))
        try:
            assert assoc_context.current() is context
            results.append(
                context.correlation_id != main_context.correlation_id
            )
        finally:
            assert assoc_context.close_current() is context
        assert assoc_context.current() is None

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    assert results == [True]
    # The main thread context survived
    assert assoc_context.current() is main_context
    # Closing by correlation id works cross-thread
    done: list[object] = []

    def closer() -> None:
        done.append(assoc_context.close_context(main_context.correlation_id))

    thread = threading.Thread(target=closer)
    thread.start()
    thread.join()
    assert done == [main_context]
    assert assoc_context.current() is None
