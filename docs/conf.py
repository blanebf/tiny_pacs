"""Sphinx configuration for the tiny_pacs documentation."""
import os
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version

sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('../extensions/tiny_pacs_admin'))
sys.path.insert(0, os.path.abspath('../extensions/tiny_pacs_identity'))
sys.path.insert(0, os.path.abspath('../extensions/tiny_pacs_audit'))

project = 'tiny_pacs'
copyright = "2020, Pavel 'Blane' Tuchin"
author = "Pavel 'Blane' Tuchin"

try:
    release = package_version('tiny_pacs')
except PackageNotFoundError:
    release = '0.0.0'
version = release

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
]

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

autosummary_generate = True

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
    'member-order': 'bysource',
}
autoclass_content = 'both'

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pydicom': ('https://pydicom.github.io/pydicom/stable/', None),
    'pydantic': ('https://docs.pydantic.dev/latest/', None),
}

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
