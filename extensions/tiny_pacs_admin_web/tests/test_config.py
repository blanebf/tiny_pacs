"""Unit tests of the AdminWeb configuration validation matrix."""
from typing import Any

import pytest

from tiny_pacs_admin_web.component import AdminWebConfig, is_loopback


def test_defaults() -> None:
    config = AdminWebConfig()
    assert config.on is False
    assert config.host == '127.0.0.1'
    assert config.port == 11113
    assert config.allow_remote is False
    assert config.secure_cookie is False
    assert config.session_ttl == 3600
    assert config.max_sessions == 100
    assert config.max_login_failures == 5
    assert config.expose_users_to_viewer is False
    assert config.threads >= 1


def test_yaml_on_key_normalization() -> None:
    # PyYAML parses the bare ``on`` key as a boolean; the core config
    # model normalizes it back
    config = AdminWebConfig.model_validate({True: True})
    assert config.on is True


@pytest.mark.parametrize('host', [
    '127.0.0.1', '::1', 'localhost'
])
def test_loopback_hosts_recognized(host: str) -> None:
    assert is_loopback(host)
    assert AdminWebConfig(host=host).host == host


@pytest.mark.parametrize('host', [
    '0.0.0.0', '192.168.1.5', 'pacs.example.org', ''
])
def test_remote_hosts_refused_without_allow_remote(host: str) -> None:
    assert not is_loopback(host)
    with pytest.raises(ValueError, match='allow_remote'):
        AdminWebConfig(host=host)


@pytest.mark.parametrize('host', ['0.0.0.0', '192.168.1.5'])
def test_remote_hosts_accepted_with_allow_remote(host: str) -> None:
    config = AdminWebConfig(host=host, allow_remote=True)
    assert config.host == host
    assert config.allow_remote is True


@pytest.mark.parametrize('data', [
    {'port': -1},
    {'port': 65536},
    {'session_ttl': 0},
    {'max_sessions': 0},
    {'max_login_failures': 0},
    {'threads': 0},
])
def test_bounds(data: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        AdminWebConfig(**data)


def test_boundaries_accept_edge_values() -> None:
    config = AdminWebConfig(port=0)
    assert config.port == 0, 'ephemeral port is allowed'
    assert AdminWebConfig(port=65535).port == 65535
    assert AdminWebConfig(session_ttl=1).session_ttl == 1
    assert AdminWebConfig(threads=1).threads == 1


def test_unknown_keys_forbidden() -> None:
    with pytest.raises(ValueError):
        AdminWebConfig.model_validate({'nonexistent': True})
