"""Unit tests of the AdminWeb configuration validation matrix.

The HTTP transport settings (``host``/``port``/``allow_remote``/
``threads``) live on the core ``HttpServer`` component and are validated
there; the console configuration only carries application-level fields.
"""
from typing import Any

import pytest

from tiny_pacs_admin_web.component import AdminWebConfig


def test_defaults() -> None:
    config = AdminWebConfig()
    assert config.on is False
    assert config.secure_cookie is False
    assert config.session_ttl == 3600
    assert config.max_sessions == 100
    assert config.max_login_failures == 5
    assert config.expose_users_to_viewer is False


def test_yaml_on_key_normalization() -> None:
    # PyYAML parses the bare ``on`` key as a boolean; the core config
    # model normalizes it back
    config = AdminWebConfig.model_validate({True: True})
    assert config.on is True


@pytest.mark.parametrize('data', [
    {'session_ttl': 0},
    {'max_sessions': 0},
    {'max_login_failures': 0},
])
def test_bounds(data: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        AdminWebConfig(**data)


def test_boundaries_accept_edge_values() -> None:
    assert AdminWebConfig(session_ttl=1).session_ttl == 1
    assert AdminWebConfig(max_sessions=1).max_sessions == 1
    assert AdminWebConfig(max_login_failures=1).max_login_failures == 1


def test_unknown_keys_forbidden() -> None:
    with pytest.raises(ValueError):
        AdminWebConfig.model_validate({'nonexistent': True})


@pytest.mark.parametrize('data', [
    {'host': '127.0.0.1'},
    {'port': 11113},
    {'allow_remote': True},
    {'threads': 8},
])
def test_transport_settings_refused(data: dict[str, Any]) -> None:
    """The former ``AdminWeb`` transport keys are refused instead of
    silently ignored: they moved to the core ``HttpServer`` component."""
    with pytest.raises(ValueError):
        AdminWebConfig.model_validate(data)
