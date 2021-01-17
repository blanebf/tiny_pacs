# -*- coding: utf-8 -*-
"""
Evemt bus module.

Provides simple implementation of publish/subscibire model.

`EventBus` class provides all essential methods for publishing events and
subscribing to them.

Module also proveds `DefaultChannels` enum for common events such as:

    * 'on-start' - for when service is starting
    * 'on-stated' - when all subscribers have been properly initialized
    * 'on-exit' - when service is stopping (usually used for clean up)

"""
import enum
import operator
import logging

from typing import Any, List, Tuple, Union


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


class EventBus:
    """Event bus for implementing simple publish/subscribe model.

    Class provides essential method for subscribing to events and broadcasting
    them. All handling is done synchronously according to subscriber's priority.

    :ivar listeners: mapping event -> listeners
    :ivar log: event bus logger
    """
    default_channels = [
        DefaultChannels.ON_START,
        DefaultChannels.ON_STARTED,
        DefaultChannels.ON_EXIT
    ]

    @classmethod
    def name(cls) -> str:
        """Component name. Used in logging

        :return: component name
        :rtype: [str]
        """
        return cls.__name__

    def __init__(self):
        """Event bus initialization"""

        self.listeners = {
            channel: set()
            for channel in self.default_channels
        }
        self._priorities = {}
        self.log = logging.getLogger(self.name())

    def subscribe(self, channel: str, callback, priority=50):
        """Subscribes a listener to the event channel

        :param channel: event channel name
        :type channel: str
        :param callback: callback that will be called when event is fired
        :type callback: function
        :param priority: subscriber priority, defaults to 50
        :type priority: int, optional
        """
        callbacks = self.listeners.setdefault(channel, set())
        callbacks.add(callback)

        if priority is None:
            priority = getattr(callback, 'priority', 50)
        self._priorities[(channel, callback)] = priority

    def unsubscribe(self, channel: str, callback):
        """Unsubscribes a listener from event channel

        :param channel: channel name
        :type channel: str
        :param callback: subscriber
        :type callback: function
        """
        listeners = self.listeners.get(channel)
        if listeners and callback in listeners:
            listeners.discard(callback)
            del self._priorities[(channel, callback)]

    def broadcast(self, channel: str, *args, **kwargs) -> List[Any]:
        """Broadcast the event to all listeners on the channel.

        Event is handled according to listeners priority.

        :param channel: event name
        :type channel: str
        :return: list of results from all listeners
        :rtype: List[Any]
        """
        results = []
        if channel not in self.listeners:
            return results

        for _, listener in self._sort_listeners(channel):
            result = listener(*args, **kwargs)
            results.append(result)
        return results

    def broadcast_nothrow(self, channel: str, *args, **kwargs) -> List[Tuple[Any, bool]]:
        """Broadcast the event to all listeners on the channel.

        Event is handled according to listeners priority. In case one of the
        listeners would rise an exception, it won't be raised, but instead it
        would be appended to results

        :param channel: event name
        :type channel: str
        :return: list of tuples. Each tuple would contain result from the
                 listener or exception, if it occured. Second value of the tuple
                 would be either `True` (if no exception occured) or `False` (
                 if exception did occur)
        :rtype: List[Tuple[Any, bool]]
        """
        results = []
        if channel not in self.listeners:
            return results

        for _, listener in self._sort_listeners(channel):
            try:
                result = listener(*args, **kwargs)
            except Exception as error:  # pylint: disable=broad-except
                results.append((error, True))
            else:
                results.append((result, False))
        return results

    def send_one(self, channel: str, *args, **kwargs) -> Any:
        """Sends event to one listiner with highest priority

        :param channel: event name
        :type channel: str
        :raises NoListenersError: raised when no listener found for specified event
        :return: result from a listiner with highest priority
        :rtype: Any
        """
        try:
            listener = self._sort_listeners(channel)[0][1]
        except IndexError:
            msg = f'No listeners for {channel}'
            self.log.error(msg)
            raise NoListenersError(msg)
        return listener(*args, **kwargs)

    def send_any(self, channel: str, *args, **kwargs) -> Union[Any, None]:
        """Broadcast the specfied event and returns firts none `None` result

        If all listeners return `None` or no listeners present for specfied event
        method returns `None`

        :param channel: event name
        :type channel: str
        :return: first none `None` result or `None`
        :rtype: Union[Any, None]
        """
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
