from importlib.metadata import version

from tiny_pacs import __version__


def test_version():
    assert __version__ == version('tiny_pacs')
