# -*- coding: utf-8 -*-
"""Base component implementation"""
import logging
from typing import Any, Dict, Hashable, List, Tuple

from . import event_bus
from . import questions


class Component:
    """Base component class

    Subscribes to common channels, sets up component logging and provides
    various convinience methods

    :ivar bus: event bus
    :ivar config: component configuration
    """

    #: Component priority in the event bus. Default is 50
    priority = 50

    @classmethod
    def name(cls) -> str:
        """Component name.

        Defaults to class name

        :return: component name
        :rtype: str
        """
        return cls.__name__

    def __init__(self, bus: event_bus.EventBus, config: Dict[str, Any]):
        self.bus = bus
        self.config = config
        self._logger = logging.getLogger(self.name())

        self.subscribe(event_bus.DefaultChannels.ON_START, self.on_start)
        self.subscribe(event_bus.DefaultChannels.ON_STARTED, self.on_started)
        self.subscribe(event_bus.DefaultChannels.ON_EXIT, self.on_exit)

    @classmethod
    def interactive(cls) -> questions.Questionnaire:
        """Returns interatctive questionnaire for component configuration

        :return: component configuration questionnaire
        :rtype: questions.Questionnaire
        """
        return questions.Questionnaire([])

    def on_start(self):
        """Handles 'on-start' event."""
        self.log_info(f'Component {self.name()} starting...')

    def on_started(self):
        """Handles 'on-started' event."""
        self.log_info(f'Component {self.name()} started')

    def on_exit(self):
        """Handles 'on-exit' event."""
        self.log_info(f'Component {self.name()} exiting...')

    def subscribe(self, channel: Hashable, callback, priority: int = None):
        """Subscribes to an event channel

        :param channel: event name
        :type channel: Hashable
        :param callback: event handler
        :type callback: function
        :param priority: subscription priority, defaults to None
        :type priority: int, optional
        """
        if priority is None:
            priority = self.priority
        self.bus.subscribe(channel, callback, priority)

    def broadcast(self, channel: Hashable, *args, **kwargs) -> List[Any]:
        """Broadcasts the event to all listeners

        :param channel: event name
        :type channel: Hashable
        :return: list of results from all listeners
        :rtype: List[Any]
        """
        return self.bus.broadcast(channel, *args, **kwargs)

    def broadcast_nothrow(self, channel: Hashable, *args, **kwargs) -> List[Tuple[Any, bool]]:
        """Broadcast the event to all listeners on the channel. Method does not raise an exception

        :param channel: event name
        :type channel: Hashable
        :return: list of results from all listeners
        :rtype: List[Tuple[Any, bool]]
        """
        return self.bus.broadcast_nothrow(channel, *args, **kwargs)

    def send_one(self, channel: Hashable, *args, **kwargs) -> Any:
        """Sends event to one listiner with highest priority

        :param channel: event name
        :type channel: Hashable
        :return: result from a listiner with highest priority
        :rtype: Any
        """
        return self.bus.send_one(channel, *args, **kwargs)

    def send_any(self, channel: Hashable, *args, **kwargs) -> Any:
        """Broadcast the specfied event and returns firts none `None` result

        :param channel: event name
        :type channel: Hashable
        :return: first none `None` result or `None`
        :rtype: Any
        """
        return self.bus.send_any(channel, *args, **kwargs)

    def log(self, level: int, msg, *args, **kwargs):
        """Logger wrapper

        :param level: logging leve
        :type level: int
        :param msg: logging message
        """
        self._logger.log(level, msg, *args, **kwargs)

    def log_debug(self, msg, *args, **kwargs):
        """Log debug

        :param msg: logging message
        """
        self._logger.debug(msg, *args, **kwargs)

    def log_info(self, msg, *args, **kwargs):
        """Log info

        :param msg: logging message
        """
        self._logger.info(msg, *args, **kwargs)

    def log_warning(self, msg, *args, **kwargs):
        """Log warning

        :param msg: logging message
        """
        self._logger.warning(msg, *args, **kwargs)

    def log_error(self, msg, *args, **kwargs):
        """Log error

        :param msg: logging message
        """
        self._logger.error(msg, *args, **kwargs)

    def log_critical(self, msg, *args, **kwargs):
        """Log critical

        :param msg: logging message
        """
        self._logger.critical(msg, *args, **kwargs)

    def log_exception(self, msg, *args, **kwargs):
        """Log exception

        :param msg: logging message
        """
        self._logger.exception(msg, *args, **kwargs)
