# -*- coding: utf-8 -*-
import logging
import logging.config
import time
import threading

from pynetdicom2 import uids
from . import ae
from . import config
from . import event_bus


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
        self.bus = event_bus.EventBus()
        self.ae = None
        self.components = list(self.initalize_components())

    def start(self):
        """Starts the server.

        Broadcasts `ON_START` and `ON_STARTED` events.
        """
        self.bus.broadcast(event_bus.DefaultChannels.ON_START)
        self.ae = ae.AE(self.bus, self.config.ae)
        threading.Thread(target=self.ae.serve_forever).start()
        # TODO: Wait for actual AE to start
        self.bus.broadcast(event_bus.DefaultChannels.ON_STARTED)

    def start_with_block(self):
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

    def exit(self):
        """Handles server exit.

        Broadcasts `ON_EXIT` event.
        """
        self.bus.broadcast_nothrow(event_bus.DefaultChannels.ON_EXIT)
        self.ae.quit()

    def initalize_components(self):
        """Component initialization

        :yield: initializes components
        :rtype: component.Component
        """
        for component, _config in self.config.components.items():
            is_on = _config.get('on', False)
            if not is_on:
                # Component is disabled
                continue

            factory = config.COMPONENT_REGISTRY.get(component)
            if factory is None:
                # TODO: add dynamic component loading
                pass

            component = factory(self.bus, _config)
            yield component
