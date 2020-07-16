# -*- coding: utf-8 -*-
import json
import os

from pydicom import uid
from pynetdicom2 import uids
import yaml

from . import ae
from . import db
from . import devices
from . import pacs
from . import storage


class Config(dict):
    def __init__(self):
        super().__init__()
        self['components'] = {}
        self['ae'] = DEFAULT_AE_CONFIG.copy()
        self['log'] = DEFAULT_LOG_CONF.copy()

    def update_config(self, _config):
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
                _config = self._read_yaml(_config)
            except Exception:
                _config = self._read_json(_config)
            else:
                _config = None

        self.ae.update(_config.get('ae', {}))
        self.log.update(_config.get('log', {}))
        self.components.update(_config.get('components', {}))

    @property
    def ae(self):
        return self['ae']

    @property
    def log(self):
        return self['log']

    @property
    def components(self):
        if not self['components']:
            return DEFAULT_COMPONENTS

        return self['components']

    def _read_yaml(self, file_name):
        with open(file_name) as fp:
            return yaml.load(fp)

    def _read_json(self, file_name):
        with open(file_name) as fp:
            return json.load(fp)


COMPONENT_REGISTRY = {
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

DEFAULT_COMPONENTS = {
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
