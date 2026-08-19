"""Configuration system."""
import json
import os
from typing import IO, Any, TypeAlias, cast

import yaml  # type: ignore[import-untyped]
from pydicom import uid
from pynetdicom2 import uids

from . import component, db, devices, pacs, storage

ConfigInput: TypeAlias = str | list[str] | IO[bytes] | dict[str, Any]


class Config(dict):
    """Config reader for Tiny PACS."""

    def __init__(self):
        super().__init__()
        self['components'] = {}
        self['ae'] = DEFAULT_AE_CONFIG.copy()
        self['log'] = DEFAULT_LOG_CONF.copy()

    def update_config(self, _config: ConfigInput):
        """Read configuration or

        :param _config: configuration to read.
        :type _config: ConfigInput
        """
        data: dict | None
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
        self.ae.update(data.get('ae', {}))
        self.log.update(data.get('log', {}))
        self.components.update(data.get('components', {}))

    @property
    def ae(self) -> dict:
        """AE configuration.

        :rtype: dict
        """
        return cast(dict, self['ae'])

    @property
    def log(self) -> dict:
        """Logging configuration.

        :rtype: dict
        """
        return cast(dict, self['log'])

    @property
    def components(self) -> dict:
        """Components configuration

        :rtype: dict
        """
        if not self['components']:
            return DEFAULT_COMPONENTS

        return cast(dict, self['components'])

    @staticmethod
    def _read_yaml(file_name: str):
        with open(file_name) as fp:
            return yaml.safe_load(fp)

    @staticmethod
    def _read_json(file_name: str):
        with open(file_name) as fp:
            return json.load(fp)


COMPONENT_REGISTRY: dict[str, type[component.Component]] = {
    'Database': db.Database,
    'Devices': devices.Devices,
    'PACS': pacs.PACS,
    'FileStorage': storage.FileStorage,
    'InMemoryStorage': storage.InMemoryStorage,
    'TempFileStorage': storage.TempFileStorage
}

DEFAULT_AE_CONFIG = {
    'ae_title': ['TINY_PACS'],
    'port': 11112,
    'max_pdu_length': 65536,
    'dump_ds': True,
    'supported_ts': [
        uid.ImplicitVRLittleEndian,
        uid.ExplicitVRLittleEndian,
        uids.DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN,
        uids.JPEG_BASELINE_PROCESS_1,
        uids.JPEG_EXTENDED_PROCESS_2_AND_4,
        uids.JPEG_LOSSLESS_NON_HIERARCHICAL_PROCESS_14,
        uids.JPEG_LOSSLESS_NON_HIERARCHICAL_FIRST_ORDER_PREDICTION_PROCESS_14_SELECTION_VALUE_1,
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
}

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
            'format': '%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s'
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
