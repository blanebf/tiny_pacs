# -*- coding: utf-8 -*-
import pytest
from tiny_pacs import event_bus


@pytest.fixture
def bus():
    return event_bus.EventBus()

def test_empty_bus(bus: event_bus.EventBus):
    assert event_bus.DefaultChannels.ON_START in bus.listeners
    assert event_bus.DefaultChannels.ON_STARTED in bus.listeners
    assert event_bus.DefaultChannels.ON_EXIT in bus.listeners

def test_subscription_default_channel(bus: event_bus.EventBus):
    def callback():
        pass
    bus.subscribe(event_bus.DefaultChannels.ON_START, callback)
    callbacks = bus.listeners[event_bus.DefaultChannels.ON_START]
    assert callback in callbacks

def test_subscription_custom_channel(bus: event_bus.EventBus):
    def callback():
        pass
    bus.subscribe('test-channel', callback)
    callbacks = bus.listeners['test-channel']
    assert callback in callbacks


def test_send_one(bus: event_bus.EventBus):
    def callback():
        return 1

    bus.subscribe('test-channel', callback)
    result = bus.send_one('test-channel')
    assert result == 1


def test_send_one_priority(bus: event_bus.EventBus):
    def callback1():
        return 1

    def callback2():
        return 2

    bus.subscribe('test-channel', callback1, 60)
    bus.subscribe('test-channel', callback2, 40)
    result = bus.send_one('test-channel')

    assert result == 2


def test_send_any(bus: event_bus.EventBus):
    def callback1():
        return None

    def callback2():
        return 1

    bus.subscribe('test-channel', callback1)
    bus.subscribe('test-channel', callback2)
    result = bus.send_any('test-channel')

    assert result == 1


def test_broadcast(bus: event_bus.EventBus):
    def callback1():
        return 1

    def callback2():
        return 2

    bus.subscribe('test-channel', callback1)
    bus.subscribe('test-channel', callback2)
    results = bus.broadcast('test-channel')

    assert 1 in results
    assert 2 in results


def test_broadcast_priorities(bus: event_bus.EventBus):
    def callback1():
        return 1

    def callback2():
        return 2

    bus.subscribe('test-channel', callback1, 60)
    bus.subscribe('test-channel', callback2, 40)
    results = bus.broadcast('test-channel')

    assert results[0] == 2
    assert results[1] == 1


def test_broadcast_exception(bus: event_bus.EventBus):
    def callback1():
        raise ValueError()

    def callback2():
        # Shouldn't be called
        assert False

    bus.subscribe('test-channel', callback1, 40)
    bus.subscribe('test-channel', callback2, 60)
    with pytest.raises(ValueError):
        bus.broadcast('test-channel')


def test_broadcast_nothrow(bus: event_bus.EventBus):
    def callback1():
        raise ValueError()

    def callback2():
        return 1

    bus.subscribe('test-channel', callback1, 40)
    bus.subscribe('test-channel', callback2, 60)
    results = bus.broadcast_nothrow('test-channel')

    error, is_failure_1 = results[0]
    value, is_failure_2 = results[1]
    assert is_failure_1
    assert isinstance(error, ValueError)
    assert value == 1
    assert not is_failure_2
