"""Base component implementation"""
import logging
from collections.abc import Callable
from typing import Any, ClassVar, Generic, TypeVar, cast

import pydantic
import trolleybus

from . import questions

TP = TypeVar('TP')
TR = TypeVar('TR')


class ComponentConfig(pydantic.BaseModel):
    """Base configuration model for all components.

    Every component declares its own configuration model by subclassing this
    model (see :attr:`Component.config_model`); the config loader validates
    the raw component configuration against that model at load time.

    :ivar on: whether the component is enabled. Components are skipped
              unless ``on`` is true, so the default is false: an entry
              that omits ``on`` does not enable the component. The
              built-in default components set ``on`` explicitly (see
              :data:`tiny_pacs.config.DEFAULT_COMPONENTS`).
    """

    model_config = pydantic.ConfigDict(extra='forbid')

    on: bool = False

    @pydantic.model_validator(mode='before')
    @classmethod
    def _normalize_yaml_keys(cls, data: Any) -> Any:
        # PyYAML follows YAML 1.1 and parses the bare ``on``/``off`` keys as
        # booleans, so ``on: true`` from a config file arrives as
        # ``{True: True}`` — rewrite the boolean keys back to the ``on``
        # flag. A bare ``off:`` key is parsed as ``False`` and means the
        # negation of ``on``.
        if (not isinstance(data, dict)
                or (True not in data and False not in data)):
            return data
        result: dict[Any, Any] = {}
        for key, value in data.items():
            if key is True:
                result['on'] = value
            elif key is False:
                if isinstance(value, bool):
                    result['on'] = not value
            else:
                result[key] = value
        return result


TConfig = TypeVar('TConfig', bound=ComponentConfig)


class Component(trolleybus.EmitterMixin, Generic[TConfig]):
    """Base component class

    Subscribes to common lifecycle events, sets up component logging and
    provides various convenience methods.

    Emission shortcuts (``broadcast``, ``broadcast_nothrow``, ``send_one``,
    ``send_any``) are provided by :class:`trolleybus.EmitterMixin`.

    Unlike :class:`trolleybus.Subscriber`, component handlers are attached
    immediately on construction instead of on
    :meth:`trolleybus.EventBus.start`. This preserves two semantics
    tiny_pacs relies on:

        * components override each other's handler methods freely (a deferred
          attach scheme would require re-decorating every override);
        * handlers are available during the very first ``OnStart`` broadcast,
          which the :class:`~tiny_pacs.db.Database` component needs in order
          to collect tables from other components.

    Configuration is provided by a :class:`ComponentConfig` subclass declared
    through :attr:`config_model`. Plain dicts passed to the constructor are
    validated against that model automatically.

    :ivar bus: event bus
    :ivar config: validated component configuration
    """

    #: Component priority in the event bus. Higher priority runs first.
    priority = trolleybus.DEFAULT_PRIORITY

    #: Component configuration model. Subclasses override this attribute with
    #: their own :class:`ComponentConfig` subclass; the config loader uses it
    #: to validate the component configuration at load time.
    config_model: ClassVar[type[ComponentConfig]] = ComponentConfig

    #: Schema name used to track the schema version of the component's
    #: tables (see :meth:`schema`). Defaults to the class name; components
    #: whose implementations share the same tables must set this attribute to
    #: a common name so they also share one schema version.
    schema_name: ClassVar[str | None] = None

    #: Validated component configuration
    config: TConfig

    @classmethod
    def name(cls) -> str:
        """Component name.

        Defaults to class name

        :return: component name
        :rtype: str
        """
        return cls.__name__

    @classmethod
    def schema(cls) -> str:
        """Schema name of the component.

        Used as the key of the schema version recorded by the DB component.
        Defaults to the class name, see :attr:`schema_name`.

        :return: schema name
        :rtype: str
        """
        return cls.schema_name or cls.name()

    def __init__(
            self, bus: trolleybus.EventBus, config: TConfig | dict[str, Any]
    ):
        """Initializes the component.

        Plain dict configurations are validated against
        :attr:`config_model`. Subscribes to the bus lifecycle events.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: TConfig or dict
        """
        super().__init__(bus)
        model_cls = type(self).config_model
        if isinstance(config, model_cls):
            self.config = config
        else:
            self.config = cast('TConfig', model_cls.model_validate(config))
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

    def on_start(self) -> None:
        """Handles `OnStart` event."""
        self.log_info(f'Component {self.name()} starting...')

    def on_started(self) -> None:
        """Handles `OnStarted` event."""
        self.log_info(f'Component {self.name()} started')

    def on_exit(self) -> None:
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

    def log(
            self, level: int, msg: object, *args: object, **kwargs: Any
    ) -> None:
        """Logger wrapper

        :param level: logging level
        :type level: int
        :param msg: logging message
        """
        self._logger.log(level, msg, *args, **kwargs)

    def log_debug(self, msg: object, *args: object, **kwargs: Any) -> None:
        """Log debug

        :param msg: logging message
        """
        self._logger.debug(msg, *args, **kwargs)

    def log_info(self, msg: object, *args: object, **kwargs: Any) -> None:
        """Log info

        :param msg: logging message
        """
        self._logger.info(msg, *args, **kwargs)

    def log_warning(self, msg: object, *args: object, **kwargs: Any) -> None:
        """Log warning

        :param msg: logging message
        """
        self._logger.warning(msg, *args, **kwargs)

    def log_error(self, msg: object, *args: object, **kwargs: Any) -> None:
        """Log error

        :param msg: logging message
        """
        self._logger.error(msg, *args, **kwargs)

    def log_critical(self, msg: object, *args: object, **kwargs: Any) -> None:
        """Log critical

        :param msg: logging message
        """
        self._logger.critical(msg, *args, **kwargs)

    def log_exception(self, msg: object, *args: object, **kwargs: Any) -> None:
        """Log exception

        :param msg: logging message
        """
        self._logger.exception(msg, *args, **kwargs)
