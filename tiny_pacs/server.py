"""Provides main server implementation.

Initializes all components and starts listening for incoming connections
"""
import logging
import logging.config
import time
from collections.abc import Iterator
from typing import Any

import trolleybus

from . import ae, component, config


class Server:
    """Server class itself.

    Sets up event bus. Initializes all components and passes config to them.

    :ivar config: server config
    :ivar bus: event bus
    :ivar ae: AE instance
    :ivar components: all available components
    """

    def __init__(self, _config: config.Config):
        """Initializes server

        :param _config: server configuration
        :type _config: config.Config
        """
        self.config = _config
        logging.config.dictConfig(self.config.log)
        self.bus = trolleybus.EventBus()
        self.ae: ae.AE | None = None
        self.components = list(self.initialize_components())

    def start(self) -> None:
        """Starts the server.

        Emits `OnStart` and `OnStarted` events via
        :meth:`trolleybus.EventBus.start` and starts the AE serving thread.
        """
        self.bus.start()
        self.ae = ae.AE(self.bus, self.config.ae)
        # AE binds its port at construction time; entering the context
        # manager starts the serving thread.
        self.ae.__enter__()

    def start_with_block(self) -> None:
        """Starts the server and blocks current thread."""
        self.start()
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            logging.info('Server exiting due to keyboard interupt')
        except SystemExit:
            logging.info('Server exiting due to SystemExit')
        finally:
            self.exit()

    def exit(self) -> None:
        """Handles server exit.

        Emits `OnExit` event via :meth:`trolleybus.EventBus.stop` (listener
        exceptions are suppressed and returned as
        :class:`trolleybus.ListenerResult` objects) and stops the AE.
        """
        self.bus.stop()
        if self.ae is not None:
            self.ae.quit()

    def initialize_components(self) -> Iterator[component.Component[Any]]:
        """Component initialization

        :yield: initializes components
        :rtype: component.Component
        """
        for _component, _config in self.config.components.items():
            # Component configurations are validated against the config model
            # each component provides (see ``Component.config_model``) when
            # the configuration is loaded, so by this point every entry is a
            # validated model instance.
            if not _config.on:
                # Component is disabled
                continue

            factory = config.COMPONENT_REGISTRY.get(_component)
            if factory is None:
                # TODO: add dynamic component loading
                logging.error('Unknown component %s, skipping', _component)
                continue

            yield factory(self.bus, _config)
