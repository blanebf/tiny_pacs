"""Configuration validation of the DICOMWeb component."""
import pytest

from tiny_pacs_dicomweb.component import DICOMWebConfig


def test_defaults() -> None:
    config = DICOMWebConfig(on=True)
    assert config.prefix == '/dicomweb'
    assert config.auth == 'none'
    assert config.tokens == []
    assert config.max_part_size == 64 * 1024 * 1024


def test_prefix_is_normalized() -> None:
    assert DICOMWebConfig(prefix='/dicomweb/').prefix == '/dicomweb'
    assert DICOMWebConfig(prefix='  /wado-root  ').prefix == '/wado-root'


@pytest.mark.parametrize('prefix', ['/', 'dicomweb', '//', ''])
def test_unusable_prefixes_are_refused(prefix: str) -> None:
    with pytest.raises(ValueError, match='prefix'):
        DICOMWebConfig(prefix=prefix)


def test_unknown_keys_are_refused() -> None:
    with pytest.raises(ValueError):
        DICOMWebConfig(host='0.0.0.0')  # type: ignore[call-arg]


@pytest.mark.parametrize('auth', ['none', 'basic', 'token'])
def test_auth_literal(auth: str) -> None:
    tokens = ['a-token'] if auth == 'token' else []
    config = DICOMWebConfig(auth=auth,  # type: ignore[arg-type]
                            tokens=tokens)
    assert config.auth == auth


def test_unknown_auth_is_refused() -> None:
    with pytest.raises(ValueError):
        DICOMWebConfig(auth='digest')  # type: ignore[arg-type]


def test_token_mode_requires_tokens() -> None:
    with pytest.raises(ValueError, match='tokens'):
        DICOMWebConfig(auth='token', tokens=[])
    with pytest.raises(ValueError, match='tokens'):
        DICOMWebConfig(auth='token')
    with pytest.raises(ValueError, match='tokens'):
        DICOMWebConfig(auth='token', tokens=['ok', '  '])
    with pytest.raises(ValueError, match='tokens'):
        DICOMWebConfig(auth='token', tokens=['n\u00f6n-ascii'])


def test_non_token_modes_ignore_tokens() -> None:
    config = DICOMWebConfig(auth='none', tokens=['unused'])
    assert config.tokens == ['unused']


def test_part_size_bounds() -> None:
    assert DICOMWebConfig(max_part_size=1024).max_part_size == 1024
    with pytest.raises(ValueError):
        DICOMWebConfig(max_part_size=1023)
