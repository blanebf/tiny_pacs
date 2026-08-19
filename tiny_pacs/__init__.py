# -*- coding: utf-8 -*-
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version

try:
    __version__ = version('tiny_pacs')
except PackageNotFoundError:
    __version__ = '0.0.0'
