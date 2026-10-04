"""Configuration system.

Tiny PACS configuration is described with
`pydantic <https://docs.pydantic.dev>`_ models: the top-level
:class:`Config` holds the AE settings, the logging configuration and the
per-component configurations. Every component declares its own
configuration model via
:attr:`~tiny_pacs.component.Component.config_model`, and the config loader
validates the raw component configuration against that model at load time.

Additional components are discovered from the ``tiny_pacs.components``
entry point group (:func:`load_component_plugins`) on the first
:class:`Config` construction, so any installed extension package can
contribute components without core configuration.
"""
import copy
import json
import logging
import os
from importlib.metadata import entry_points
from typing import IO, Any, TypeAlias

import pydantic
import yaml  # type: ignore[import-untyped]
from pydicom import uid
from pynetdicom2 import uids

from . import component, config_comments, db, devices, http, pacs, storage

ConfigInput: TypeAlias = str | list[str] | IO[bytes] | dict[str, Any]


class TLSConfig(pydantic.BaseModel):
    """TLS settings for incoming DICOM connections.

    :ivar certificate: path to the server certificate PEM file
    :ivar key: path to the private key PEM file, if stored separately
    :ivar ca: path to a CA bundle used to verify client certificates
    """

    model_config = pydantic.ConfigDict(extra='forbid')

    certificate: str
    key: str | None = None
    ca: str | None = None


#: Transfer syntaxes the AE accepts by default
DEFAULT_SUPPORTED_TS: list[uid.UID] = [
    uid.ImplicitVRLittleEndian,
    uid.ExplicitVRLittleEndian,
    uids.DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN,
    uids.JPEG_BASELINE_PROCESS_1,
    uids.JPEG_EXTENDED_PROCESS_2_AND_4,
    uids.JPEG_LOSSLESS_NON_HIERARCHICAL_PROCESS_14,
    uids.JPEG_LOSSLESS_NON_HIERARCHICAL_FIRST_ORDER_PREDICTION_PROCESS_14_SELECTION_VALUE_1,  # noqa: E501
    uids.JPEG_LS_LOSSLESS_IMAGE_COMPRESSION,
    uids.JPEG_LS_LOSSY_NEAR_LOSSLESS_IMAGE_COMPRESSION,
    uids.JPEG_2000_IMAGE_COMPRESSION_LOSSLESS_ONLY,
    uids.JPEG_2000_IMAGE_COMPRESSION,
    uids.JPEG_2000_PART_2_MULTI_COMPONENT_IMAGE_COMPRESSION_LOSSLESS_ONLY,
    uids.JPEG_2000_PART_2_MULTI_COMPONENT_IMAGE_COMPRESSION,
    uids.JPIP_REFERENCED,
    uids.JPIP_REFERENCED_DEFLATE,
    uids.MPEG2_MAIN_PROFILE_MAIN_LEVEL,
    uids.MPEG2_MAIN_PROFILE_HIGH_LEVEL,
    uids.MPEG_4_AVC_H_264_HIGH_PROFILE_LEVEL_4_1,
    uids.MPEG_4_AVC_H_264_BD_COMPATIBLE_HIGH_PROFILE_LEVEL_4_1,
    uids.MPEG_4_AVC_H_264_HIGH_PROFILE_LEVEL_4_2_FOR_2D_VIDEO,
    uids.MPEG_4_AVC_H_264_HIGH_PROFILE_LEVEL_4_2_FOR_3D_VIDEO,
    uids.MPEG_4_AVC_H_264_STEREO_HIGH_PROFILE_LEVEL_4_2,
    uids.HEVC_H_265_MAIN_PROFILE_LEVEL_5_1,
    uids.HEVC_H_265_MAIN_10_PROFILE_LEVEL_5_1,
    uids.RLE_LOSSLESS,
    uids.RFC_2557_MIME_ENCAPSULATION,
    uids.XML_ENCODING,
]


class AEConfig(pydantic.BaseModel):
    """Application entity settings.

    :ivar ae_title: AE title (or a list of AE titles) the server answers to
    :ivar port: SCP TCP port
    :ivar max_pdu_length: maximum PDU length in bytes
    :ivar dump_ds: dump datasets and association PDUs to the log
    :ivar supported_ts: list of supported transfer syntax UIDs
    :ivar tls: TLS settings; all incoming connections are wrapped in TLS
        when given
    """

    model_config = pydantic.ConfigDict(extra='forbid')

    ae_title: str | list[str] = pydantic.Field(
        default_factory=lambda: ['TINY_PACS']
    )
    port: int = 11112
    max_pdu_length: int = 65536
    dump_ds: bool = True
    supported_ts: list[str] = pydantic.Field(
        default_factory=lambda: [str(ts) for ts in DEFAULT_SUPPORTED_TS]
    )
    tls: TLSConfig | None = None


COMPONENT_REGISTRY: dict[str, type[component.Component[Any]]] = {
    'Database': db.Database,
    'Devices': devices.Devices,
    'PACS': pacs.PACS,
    'HttpServer': http.HttpServer,
    'FileStorage': storage.FileStorage,
    'InMemoryStorage': storage.InMemoryStorage,
    'TempFileStorage': storage.TempFileStorage
}

#: Origin of every registered component: ``built-in``, ``registered``
#: (added programmatically via :func:`register_component`) or the name of
#: the distribution providing the component through a
#: :data:`COMPONENTS_GROUP` entry point
COMPONENT_ORIGINS: dict[str, str] = dict.fromkeys(COMPONENT_REGISTRY,
                                                  'built-in')

#: Entry point group advertising component classes. Every distribution
#: installed into the environment may declare ``name = module:Component``
#: entries here; the entry point name becomes the component's name in the
#: ``components`` configuration section.
COMPONENTS_GROUP = 'tiny_pacs.components'

_plugins_loaded = False


def register_component(
        name: str, factory: type[component.Component[Any]],
        origin: str = 'registered'
) -> None:
    """Registers a component class.

    Registered components become available in the ``components`` config
    section; their configuration is validated against the model declared via
    :attr:`~tiny_pacs.component.Component.config_model`. A component
    registered under an existing name replaces the previous one, so a
    programmatic registration always wins over built-ins and entry-point
    plugins alike.

    :param name: component name used in configuration files
    :type name: str
    :param factory: component class
    :type factory: type[component.Component]
    :param origin: where the component comes from; recorded in
                   :data:`COMPONENT_ORIGINS`, defaults to ``registered``
    :type origin: str
    """
    COMPONENT_REGISTRY[name] = factory
    COMPONENT_ORIGINS[name] = origin


def get_component_origin(name: str) -> str:
    """Returns the origin of a registered component.

    :param name: component name
    :type name: str
    :return: ``built-in``, ``registered`` or the name of the distribution
             providing the component through an entry point
    :rtype: str
    """
    return COMPONENT_ORIGINS.get(name, 'registered')


def load_component_plugins() -> None:
    """Registers components discovered from entry points. Runs once.

    Components are discovered from the :data:`COMPONENTS_GROUP` entry point
    group; any installed distribution — first-party or third-party — may
    contribute. Entry points are loaded lazily, so extension modules are
    only imported when discovery runs. Broken entry points and entries that
    are not :class:`~tiny_pacs.component.Component` subclasses are logged
    and skipped, they never break the configuration loader.

    Precedence: an entry point named like a built-in component replaces
    that built-in (logged at INFO); when two distributions advertise the
    same name the last-loaded one wins (logged at WARNING); a programmatic
    :func:`register_component` call always wins over installed plugins,
    regardless of when it runs.
    """
    global _plugins_loaded
    if _plugins_loaded:
        return
    _plugins_loaded = True
    logger = logging.getLogger('tiny_pacs.config')
    for ep in entry_points(group=COMPONENTS_GROUP):
        try:
            factory = ep.load()
        except KeyboardInterrupt:
            raise
        except BaseException:
            logger.exception(
                'Failed to load component entry point %r, skipping', ep.name
            )
            continue
        if not (isinstance(factory, type)
                and issubclass(factory, component.Component)):
            logger.error(
                'Entry point %r is not a Component subclass, skipping',
                ep.name
            )
            continue
        dist_name = ep.dist.name if ep.dist is not None else 'unknown'
        if COMPONENT_ORIGINS.get(ep.name) == 'registered':
            # Programmatic register_component() calls always win over
            # installed plugins, regardless of their timing
            logger.info(
                'Component %r was registered programmatically, the entry '
                'point provided by %s is ignored', ep.name, dist_name
            )
            continue
        if ep.name in COMPONENT_REGISTRY:
            if COMPONENT_ORIGINS.get(ep.name) == 'built-in':
                logger.info(
                    'Component %r: the built-in implementation is replaced '
                    'by %s', ep.name, dist_name
                )
            else:
                logger.warning(
                    'Component name %r is provided by both %s and %s; '
                    '%s wins', ep.name,
                    COMPONENT_ORIGINS.get(ep.name, 'registered'), dist_name,
                    dist_name
                )
        register_component(ep.name, factory, dist_name)


#: Components used when no components are configured at all
DEFAULT_COMPONENTS: dict[str, dict[str, Any]] = {
    'Database': {'on': True},
    'Devices': {'on': True},
    'PACS': {'on': True},
    'InMemoryStorage': {'on': True}
}

DEFAULT_LOG_CONF = {
    'version': 1,
    # Keep loggers created before ``dictConfig`` runs (e.g. the config
    # loader's own logger during plugin discovery) working
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': ('%(asctime)s - %(levelname)-8s - '
                       '%(name)-15s - %(message)s')
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }
}


def _default_components() -> dict[str, component.ComponentConfig]:
    return _validate_component_configs(DEFAULT_COMPONENTS, {})


def extension_component_defaults() -> dict[str, component.ComponentConfig]:
    """Default configurations of the available extension components.

    Every component provided by an extension package (an entry-point
    plugin or a programmatic :func:`register_component` call) is returned
    with the defaults of its own configuration model, which keep the
    component disabled (``on`` defaults to false). Built-in components are
    not included, whether or not they are part of
    :data:`DEFAULT_COMPONENTS`. An extension whose configuration model has
    required fields (no defaults) cannot be constructed from an empty
    configuration; such a component is logged and skipped.

    :return: component name to default configuration of every available
             extension component
    :rtype: dict[str, component.ComponentConfig]
    """
    load_component_plugins()
    logger = logging.getLogger('tiny_pacs.config')
    result: dict[str, component.ComponentConfig] = {}
    for name, factory in COMPONENT_REGISTRY.items():
        if get_component_origin(name) == 'built-in':
            continue
        try:
            result[name] = factory.config_model.model_validate({})
        except pydantic.ValidationError:
            logger.warning(
                'Component %r requires configuration fields without '
                'defaults, it cannot be pre-populated in a generated '
                'configuration', name
            )
    return result


def _validate_component_configs(
        value: Any,
        base: dict[str, component.ComponentConfig]
) -> dict[str, component.ComponentConfig]:
    """Validates raw component configurations.

    Each entry is validated with the configuration model the component
    provides via :attr:`~tiny_pacs.component.Component.config_model`. An
    entry in ``value`` overrides the corresponding entry in ``base``
    wholesale — fields omitted by a later configuration source fall back to
    the component model defaults instead of resurrecting values set by an
    earlier source. Components that are not mentioned in ``value`` keep
    their current configuration from ``base``.

    :param value: raw ``components`` section of a configuration
    :param base: currently configured components
    :return: validated component configurations
    :raises pydantic.ValidationError: raised when a component configuration
                                      does not match the model provided by
                                      that component
    """
    result = dict(base)
    if not value:
        return result
    if not isinstance(value, dict):
        raise ValueError(
            '"components" must be a mapping of component name to its '
            'configuration'
        )
    logger = logging.getLogger('tiny_pacs.config')
    for name, data in value.items():
        factory = COMPONENT_REGISTRY.get(name)
        if factory is None:
            logger.error('Unknown component %s, skipping', name)
            continue
        model_cls = factory.config_model
        if isinstance(data, model_cls):
            result[name] = data
            continue
        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise ValueError(
                f'Configuration for component {name} must be a mapping'
            )
        result[name] = model_cls.model_validate(data)
    return result


class Config(pydantic.BaseModel):
    """Config reader for Tiny PACS.

    Provides ``ae``, ``log`` and ``components`` sections. Component
    configurations are validated at load time against the configuration model
    each component provides via
    :attr:`~tiny_pacs.component.Component.config_model`.

    :ivar ae: application entity configuration
    :ivar log: logging configuration, passed to
        :func:`logging.config.dictConfig`
    :ivar components: effective component configurations. Components that are
                      never configured fall back to
                      :data:`DEFAULT_COMPONENTS`; a component that is
                      configured replaces its entry wholesale.
    """

    ae: AEConfig = pydantic.Field(default_factory=AEConfig)
    log: dict[str, Any] = pydantic.Field(
        default_factory=lambda: copy.deepcopy(DEFAULT_LOG_CONF)
    )
    components: dict[str, component.ComponentConfig] = pydantic.Field(
        default_factory=_default_components
    )

    @pydantic.model_validator(mode='before')
    @classmethod
    def _load_component_plugins(cls, data: Any) -> Any:
        # Plugin components must be registered before any ``components``
        # section is validated against the registry
        load_component_plugins()
        return data

    @pydantic.field_validator('components', mode='before')
    @classmethod
    def _validate_components(
            cls, value: Any
    ) -> dict[str, component.ComponentConfig]:
        return _validate_component_configs(value, _default_components())

    def update_config(self, _config: ConfigInput) -> None:
        """Reads a configuration source and updates this configuration.

        Configured component entries replace any previous entry for the same
        component wholesale; components that are not configured keep their
        current configuration.

        :param _config: configuration to read.
        :type _config: ConfigInput
        :raises pydantic.ValidationError: raised when the configuration does
                                          not match the configuration models
        """
        data: dict[str, Any] | None
        if isinstance(_config, list):
            for _conf in _config:
                self.update_config(_conf)
            return
        elif isinstance(_config, str):
            _, ext = os.path.splitext(_config)
            if ext == '.json':
                data = self._read_json(_config)
            else:
                data = self._read_yaml(_config)
        elif hasattr(_config, 'read'):
            try:
                data = yaml.safe_load(_config)
            except Exception:
                data = json.load(_config)
        elif isinstance(_config, dict):
            data = _config
        else:
            return

        if not data:
            return
        ae_conf = data.get('ae')
        if ae_conf:
            self.ae = AEConfig.model_validate(
                {**self.ae.model_dump(), **ae_conf}
            )
        log_conf = data.get('log')
        if log_conf:
            self.log = {**self.log, **log_conf}
        components_conf = data.get('components')
        if components_conf:
            self.components = _validate_component_configs(
                components_conf, dict(self.components)
            )

    @staticmethod
    def _read_yaml(file_name: str) -> Any:
        with open(file_name) as fp:
            return yaml.safe_load(fp)

    @staticmethod
    def _read_json(file_name: str) -> Any:
        with open(file_name) as fp:
            return json.load(fp)


def dump_yaml(conf: Config, comments: bool = True) -> str:
    """Serializes a configuration into a YAML document.

    The output contains the effective configuration with all the default
    values applied, so it can be saved to a file and loaded back either
    with :meth:`Config.update_config` or via the ``-c`` command-line
    option.

    With ``comments`` (the default) the document is annotated by
    :mod:`tiny_pacs.config_comments`: a banner, per-section and
    per-component comments derived from the component and configuration
    model docstrings, and machine-checked facts (enum values, numeric
    bounds, required fields). Comments are ignored by ``yaml.safe_load``,
    so the round-trip is unaffected; ``comments=False`` keeps the plain
    dump for machine consumers.

    :param conf: configuration to serialize
    :type conf: Config
    :param comments: generate explanatory comments, defaults to True
    :type comments: bool
    :return: YAML representation of the configuration
    :rtype: str
    """
    data = conf.model_dump()
    # ``components`` is declared as a mapping onto the ComponentConfig base,
    # so Config.model_dump() serializes every entry against the base schema
    # and silently drops subclass fields (e.g. FileStorage ``storage_dir``
    # or ``overwrite``); dump each component with its actual model instead.
    # JSON mode keeps enums (e.g. the database ``driver``) representable by
    # yaml.safe_dump and re-validates into the same values on load.
    data['components'] = {
        name: component_config.model_dump(mode='json')
        for name, component_config in conf.components.items()
    }
    text: str = yaml.safe_dump(
        data, sort_keys=False, default_flow_style=False
    )
    if not comments:
        return text
    return config_comments.render_commented_yaml(
        conf, text, COMPONENT_REGISTRY, COMPONENT_ORIGINS
    )


def write_yaml(conf: Config, file_name: str, comments: bool = True) -> None:
    """Writes a configuration to a file readable only by its owner.

    Configurations may contain credentials (e.g. the PostgreSQL password),
    so the file is created with mode ``0600``.

    :param conf: configuration to serialize
    :type conf: Config
    :param file_name: name of the file to write
    :type file_name: str
    :param comments: generate explanatory comments, defaults to True
    :type comments: bool
    """
    fd = os.open(file_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as fp:
        fp.write(dump_yaml(conf, comments=comments))
