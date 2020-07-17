# -*- coding: utf-8 -*-
import enum
import operator
import logging


class DefaultChannels(enum.Enum):
    """Default events. Every component probably should support them"""

    #: Fired when server is starting
    ON_START = 'on-start'

    #: Fired when server has started
    ON_STARTED = 'on-started'

    #: Fired when server is exiting
    ON_EXIT = 'on-exit'


class NoListenersError(Exception):
    """Raised when there are no listeners for requested event.

    This error is not raised when using broadcast method."""
    pass


class EventBus:
    default_channels = [
        DefaultChannels.ON_START,
        DefaultChannels.ON_STARTED,
        DefaultChannels.ON_EXIT
    ]

    @classmethod
    def name(cls):
        return cls.__name__

    def __init__(self):
        self.listeners = {
            channel: set()
            for channel in self.default_channels
        }
        self._priorities = {}
        self.log = logging.getLogger(self.name())

    def subscribe(self, channel: str, callback, priority=50):
        callbacks = self.listeners.setdefault(channel, set())
        callbacks.add(callback)

        if priority is None:
            priority = getattr(callback, 'priority', 50)
        self._priorities[(channel, callback)] = priority

    def unsubscribe(self, channel, callback):
        listeners = self.listeners.get(channel)
        if listeners and callback in listeners:
            listeners.discard(callback)
            del self._priorities[(channel, callback)]

    def broadcast(self, channel: str, *args, **kwargs):
        results = []
        if channel not in self.listeners:
            return results

        for _, listener in self._sort_listeners(channel):
            result = listener(*args, **kwargs)
            results.append(result)
        return results

    def broadcast_nothrow(self, channel: str, *args, **kwargs):
        results = []
        if channel not in self.listeners:
            return results

        for _, listener in self._sort_listeners(channel):
            try:
                result = listener(*args, **kwargs)
            except Exception as e:
                results.append((e, True))
            else:
                results.append((result, False))
        return results

    def send_one(self, channel: str, *args, **kwargs):
        try:
            listener = self._sort_listeners(channel)[0][1]
        except IndexError:
            msg = f'No listeners for {channel}'
            self.log.error(msg)
            raise NoListenersError(msg)
        return listener(*args, **kwargs)

    def send_any(self, channel: str, *args, **kwargs):
        for _, listener in self._sort_listeners(channel):
            result = listener(*args, **kwargs)
            if result is not None:
                return result
        return None

    def _sort_listeners(self, channel):
        unsorted = (
            (self._priorities[(channel, l)], l)
            for l in self.listeners[channel]
        )
        return sorted(unsorted, key=operator.itemgetter(0))
