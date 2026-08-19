"""Base component implementation"""
import logging
from typing import Any, Callable, TypeVar

import trolleybus

from . import questions

TP = TypeVar('TP')
TR = TypeVar('TR')


class Component(trolleybus.EmitterMixin):
    """Base component class

    Subscribes to common lifecycle events, sets up component logging and
    provides various convenience methods.

    Emission shortcuts (``broadcast``, ``broadcast_nothrow``, ``send_one``,
    ``send_any``) are provided by :class:`trolleybus.EmitterMixin`.

    Unlike :class:`trolleybus.Subscriber`, component handlers are attached
    immediately on construction instead of on :meth:`trolleybus.EventBus.start`.
    This preserves two semantics tiny_pacs relies on:

        * components override each other's handler methods freely (a deferred
          attach scheme would require re-decorating every override);
        * handlers are available during the very first ``OnStart`` broadcast,
          which the :class:`~tiny_pacs.db.Database` component needs in order
          to collect tables from other components.

    :ivar bus: event bus
    :ivar config: component configuration
    """

    #: Component priority in the event bus. Higher priority runs first.
    priority = trolleybus.DEFAULT_PRIORITY

    @classmethod
    def name(cls) -> str:
        """Component name.

        Defaults to class name

        :return: component name
        :rtype: str
        """
        return cls.__name__

    def __init__(self, bus: trolleybus.EventBus, config: dict[str, Any]):
        super().__init__(bus)
        self.config = config
        self._logger = logging.getLogger(self.name())

        self.subscribe(trolleybus.OnStart, self._handle_start)
        self.subscribe(trolleybus.OnStarted, self._handle_started)
        self.subscribe(trolleybus.OnExit, self._handle_exit)

    @classmethod
    def interactive(cls) -> questions.Questionnaire:
        """Returns interactive questionnaire for component configuration

        :return: component configuration questionnaire
        :rtype: questions.Questionnaire
        """
        return questions.Questionnaire([])

    def _handle_start(self, _: None) -> None:
        self.on_start()

    def _handle_started(self, _: None) -> None:
        self.on_started()

    def _handle_exit(self, _: None) -> None:
        self.on_exit()

    def on_start(self):
        """Handles `OnStart` event."""
        self.log_info(f'Component {self.name()} starting...')

    def on_started(self):
        """Handles `OnStarted` event."""
        self.log_info(f'Component {self.name()} started')

    def on_exit(self):
        """Handles `OnExit` event."""
        self.log_info(f'Component {self.name()} exiting...')

    def subscribe(
            self,
            event: type[trolleybus.Event[TP, TR]],
            callback: Callable[[TP], TR],
            priority: int | None = None
    ) -> Callable[[TP], TR]:
        """Subscribes to an event

        :param event: event class
        :type event: type[trolleybus.Event]
        :param callback: event handler. Receives a single payload argument
        :type callback: function
        :param priority: subscription priority, defaults to component priority
        :type priority: int, optional
        """
        if priority is None:
            priority = self.priority
        return self.bus.subscribe(event, callback, priority)

    def log(self, level: int, msg, *args, **kwargs):
        """Logger wrapper

        :param level: logging level
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
