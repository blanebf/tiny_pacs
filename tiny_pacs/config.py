# -*- coding: utf-8 -*-
"""Configuration system."""
import json
import os

from typing import Any, Dict, IO, List, Type, Union

from pydicom import uid
from pynetdicom2 import uids
import yaml

from . import component
from . import db
from . import devices
from . import pacs
from . import storage


ConfigInput = Union[str, List[str], IO[bytes], dict]


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
        if isinstance(_config, list):
            for _conf in _config:
                self.update_config(_conf)
            return
        elif isinstance(_config, str):
            _, ext = os.path.splitext(_config)
            if ext == '.json':
                _config = self._read_json(_config)
            else:
                _config = self._read_yaml(_config)
        elif hasattr(_config, 'read'):
            try:
                _config = yaml.load(_config)
            except Exception:   # pylint: disable=broad-except
                _config = json.load(_config)
        else:
            return

        self.ae.update(_config.get('ae', {}))
        self.log.update(_config.get('log', {}))
        self.components.update(_config.get('components', {}))

    @property
    def ae(self) -> dict:
        """AE configuration.

        :rtype: dict
        """
        return self['ae']

    @property
    def log(self) -> dict:
        """Logging configuration.

        :rtype: dict
        """
        return self['log']

    @property
    def components(self) -> dict:
        """Components configuration

        :rtype: dict
        """
        if not self['components']:
            return DEFAULT_COMPONENTS

        return self['components']

    @staticmethod
    def _read_yaml(file_name: str):
        with open(file_name) as fp:
            return yaml.load(fp)

    @staticmethod
    def _read_json(file_name: str):
        with open(file_name) as fp:
            return json.load(fp)


COMPONENT_REGISTRY: Dict[str, Type[component.Component]] = {
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

DEFAULT_COMPONENTS: Dict[str, Dict[str, Any]] = {
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
