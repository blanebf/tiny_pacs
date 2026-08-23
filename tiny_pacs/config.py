"""Configuration system.

Tiny PACS configuration is described with
`pydantic <https://docs.pydantic.dev>`_ models: the top-level
:class:`Config` holds the AE settings, the logging configuration and the
per-component configurations. Every component declares its own
configuration model via
:attr:`~tiny_pacs.component.Component.config_model`, and the config loader
validates the raw component configuration against that model at load time.
"""
import copy
import json
import logging
import os
from typing import IO, Any, TypeAlias

import pydantic
import yaml  # type: ignore[import-untyped]
from pydicom import uid
from pynetdicom2 import uids

from . import component, db, devices, pacs, storage

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
    'FileStorage': storage.FileStorage,
    'InMemoryStorage': storage.InMemoryStorage,
    'TempFileStorage': storage.TempFileStorage
}


def register_component(
        name: str, factory: type[component.Component[Any]]
) -> None:
    """Registers a component class.

    Registered components become available in the ``components`` config
    section; their configuration is validated against the model declared via
    :attr:`~tiny_pacs.component.Component.config_model`.

    :param name: component name used in configuration files
    :type name: str
    :param factory: component class
    :type factory: type[component.Component]
    """
    COMPONENT_REGISTRY[name] = factory


#: Components used when no components are configured at all
DEFAULT_COMPONENTS: dict[str, dict[str, Any]] = {
    'Database': {'on': True},
    'Devices': {'on': True},
    'PACS': {'on': True},
    'InMemoryStorage': {'on': True}
}

DEFAULT_LOG_CONF = {
    'version': 1,
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


def dump_yaml(conf: Config) -> str:
    """Serializes a configuration into a YAML document.

    The output contains the effective configuration with all the default
    values applied, so it can be saved to a file and loaded back either
    with :meth:`Config.update_config` or via the ``-c`` command-line
    option.

    :param conf: configuration to serialize
    :type conf: Config
    :return: YAML representation of the configuration
    :rtype: str
    """
    text: str = yaml.safe_dump(
        conf.model_dump(), sort_keys=False, default_flow_style=False
    )
    return text


def write_yaml(conf: Config, file_name: str) -> None:
    """Writes a configuration to a file readable only by its owner.

    Configurations may contain credentials (e.g. the PostgreSQL password),
    so the file is created with mode ``0600``.

    :param conf: configuration to serialize
    :type conf: Config
    :param file_name: name of the file to write
    :type file_name: str
    """
    fd = os.open(file_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as fp:
        fp.write(dump_yaml(conf))
