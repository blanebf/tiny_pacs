# -*- coding: utf-8 -*-
import logging

from . import event_bus


class Component:
    priority = 50

    @classmethod
    def name(cls):
        return cls.__name__

    def __init__(self, bus: event_bus.EventBus, config: dict):
        self.bus = bus
        self.config = config
        self._logger = logging.getLogger(self.name())

        self.subscribe(event_bus.DefaultChannels.ON_START, self.on_start)
        self.subscribe(event_bus.DefaultChannels.ON_STARTED, self.on_started)
        self.subscribe(event_bus.DefaultChannels.ON_EXIT, self.on_exit)

    def on_start(self):
        self.log_info(f'Component {self.name()} starting...')

    def on_started(self):
        self.log_info(f'Component {self.name()} started')

    def on_exit(self):
        self.log_info(f'Component {self.name()} exiting...')

    def subscribe(self, channel: str, callback, priority=None):
        if priority is None:
            priority = self.priority
        self.bus.subscribe(channel, callback, priority)

    def broadcast(self, channel: str, *args, **kwargs):
        return self.bus.broadcast(channel, *args, **kwargs)

    def broadcast_nothrow(self, channel: str, *args, **kwargs):
        return self.bus.broadcast_nothrow(channel, *args, **kwargs)

    def send_one(self, channel: str, *args, **kwargs):
        return self.bus.send_one(channel, *args, **kwargs)

    def send_any(self, channel: str, *args, **kwargs):
        return self.bus.send_any(channel, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        self._logger.log(level, msg, *args, **kwargs)

    def log_debug(self, msg, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs)

    def log_info(self, msg, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)

    def log_warning(self, msg, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)

    def log_error(self, msg, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)

    def log_critical(self, msg, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)

    def log_exception(self, msg, *args, **kwargs):
        self._logger.exception(msg, *args, **kwargs)
