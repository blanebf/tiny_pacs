import pytest
import trolleybus


class SimpleEvent(trolleybus.Event[None, int]):
    """Test event without payload"""


class PayloadEvent(trolleybus.Event[str, str]):
    """Test event with payload"""


class OptionalEvent(trolleybus.Event[None, int | None]):
    """Test event whose listeners may skip by returning None"""


@pytest.fixture
def bus() -> trolleybus.EventBus:
    return trolleybus.EventBus()


def test_event_not_instantiable() -> None:
    with pytest.raises(RuntimeError):
        SimpleEvent()


def test_empty_bus(bus: trolleybus.EventBus) -> None:
    assert not bus.has_listeners(SimpleEvent)
    # Broadcasting an event without listeners is not an error
    assert bus.broadcast(SimpleEvent, None) == []


def test_subscription(bus: trolleybus.EventBus) -> None:
    def callback(_: None) -> int:
        return 1

    bus.subscribe(SimpleEvent, callback)
    assert bus.has_listeners(SimpleEvent)


def test_unsubscribe(bus: trolleybus.EventBus) -> None:
    def callback(_: None) -> int:
        return 1

    bus.subscribe(SimpleEvent, callback)
    bus.unsubscribe(SimpleEvent, callback)
    assert not bus.has_listeners(SimpleEvent)


def test_payload(bus: trolleybus.EventBus) -> None:
    def callback(payload: str) -> str:
        return payload.upper()

    bus.subscribe(PayloadEvent, callback)
    assert bus.send_one(PayloadEvent, 'test') == 'TEST'


def test_send_one(bus: trolleybus.EventBus) -> None:
    def callback(_: None) -> int:
        return 1

    bus.subscribe(SimpleEvent, callback)
    result = bus.send_one(SimpleEvent, None)
    assert result == 1


def test_send_one_no_listeners(bus: trolleybus.EventBus) -> None:
    with pytest.raises(trolleybus.NoListenersError):
        bus.send_one(SimpleEvent, None)


def test_send_one_priority(bus: trolleybus.EventBus) -> None:
    """Higher priority value runs first."""
    def callback1(_: None) -> int:
        return 1

    def callback2(_: None) -> int:
        return 2

    bus.subscribe(SimpleEvent, callback1, 60)
    bus.subscribe(SimpleEvent, callback2, 40)
    result = bus.send_one(SimpleEvent, None)

    assert result == 1


def test_send_any(bus: trolleybus.EventBus) -> None:
    def callback1(_: None) -> int | None:
        return None

    def callback2(_: None) -> int | None:
        return 1

    bus.subscribe(OptionalEvent, callback1)
    bus.subscribe(OptionalEvent, callback2)
    result = bus.send_any(OptionalEvent, None)

    assert result == 1


def test_broadcast(bus: trolleybus.EventBus) -> None:
    def callback1(_: None) -> int:
        return 1

    def callback2(_: None) -> int:
        return 2

    bus.subscribe(SimpleEvent, callback1)
    bus.subscribe(SimpleEvent, callback2)
    results = bus.broadcast(SimpleEvent, None)

    assert 1 in results
    assert 2 in results


def test_broadcast_priorities(bus: trolleybus.EventBus) -> None:
    """Higher priority value runs first."""
    def callback1(_: None) -> int:
        return 1

    def callback2(_: None) -> int:
        return 2

    bus.subscribe(SimpleEvent, callback1, 60)
    bus.subscribe(SimpleEvent, callback2, 40)
    results = bus.broadcast(SimpleEvent, None)

    assert results[0] == 1
    assert results[1] == 2


def test_broadcast_exception(bus: trolleybus.EventBus) -> None:
    def callback1(_: None) -> int:
        raise ValueError()

    def callback2(_: None) -> int:
        # Shouldn't be called
        raise AssertionError('callback2 must not be called')

    bus.subscribe(SimpleEvent, callback1, 60)
    bus.subscribe(SimpleEvent, callback2, 40)
    with pytest.raises(ValueError):
        bus.broadcast(SimpleEvent, None)


def test_broadcast_nothrow(bus: trolleybus.EventBus) -> None:
    def callback1(_: None) -> int:
        raise ValueError()

    def callback2(_: None) -> int:
        return 1

    # Higher priority value runs first, so the successful listener is first
    bus.subscribe(SimpleEvent, callback1, 40)
    bus.subscribe(SimpleEvent, callback2, 60)
    results = bus.broadcast_nothrow(SimpleEvent, None)

    assert results[0].ok
    assert results[0].value == 1
    assert not results[1].ok
    assert isinstance(results[1].error, ValueError)


def test_lifecycle(bus: trolleybus.EventBus) -> None:
    fired: list[str] = []
    bus.subscribe(trolleybus.OnStart, lambda _: fired.append('start'))
    bus.subscribe(trolleybus.OnStarted, lambda _: fired.append('started'))
    bus.subscribe(trolleybus.OnExit, lambda _: fired.append('exit'))

    bus.start()
    assert fired == ['start', 'started']

    results = bus.stop()
    assert fired == ['start', 'started', 'exit']
    assert all(r.ok for r in results)


def test_stop_nothrow(bus: trolleybus.EventBus) -> None:
    def callback(_: None) -> None:
        raise ValueError()

    bus.subscribe(trolleybus.OnExit, callback)
    results = bus.stop()
    assert len(results) == 1
    assert not results[0].ok
    assert isinstance(results[0].error, ValueError)
