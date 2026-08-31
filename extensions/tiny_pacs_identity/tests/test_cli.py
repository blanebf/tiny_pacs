"""Tests of the CLI subcommands against temporary configs and databases."""
import getpass
import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from tiny_pacs import __main__ as cli


@pytest.fixture
def passwords(monkeypatch: pytest.MonkeyPatch
              ) -> Iterator[Callable[[list[str]], None]]:
    """Feeds scripted answers to the getpass prompts.

    :yield: setter taking the list of prompts' answers in order
    """
    answers: list[str] = []

    def fake_getpass(prompt: str = '', stream: Any = None) -> str:
        if not answers:
            raise AssertionError('unexpected getpass prompt')
        return answers.pop(0)

    monkeypatch.setattr(getpass, 'getpass', fake_getpass)

    def set_answers(scripted: list[str]) -> None:
        answers[:] = scripted

    yield set_answers


def _run(argv: list[str]) -> Any:
    """Parses and executes a ``tiny-pacs`` subcommand."""
    args = cli.parse_args(argv)
    handler = getattr(args, 'command_handler', None)
    if handler is not None:
        return handler(args)
    raise AssertionError(f'no command handler for {argv!r}')


def test_users_add_and_list(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])
    out = capsys.readouterr().out
    assert 'Added user alice' in out

    _run(['users', 'list', '-c', conf])
    out = capsys.readouterr().out
    assert 'alice' in out
    assert 'True' in out
    # Password values and hashes never appear in the output
    assert 'secret' not in out
    assert '$' not in out


def test_users_add_then_authenticate_against_db(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])

    from tiny_pacs_admin import runtime

    from tiny_pacs_identity import events as identity_events
    from tiny_pacs_identity.users import Users
    with runtime.admin_context(conf, [Users.name()]) as (bus, _):
        row = bus.send_one(identity_events.UserByName, 'alice')
    assert row is not None
    from tiny_pacs_identity import hashing
    assert hashing.verify_password('secret', row.password_hash)


def test_users_list_json(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])
    capsys.readouterr()

    _run(['users', 'list', '--format', 'json', '-c', conf])
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1
    assert data[0]['username'] == 'alice'
    assert data[0]['is_active'] is True
    assert 'password' not in json.dumps(data)


def test_users_add_password_mismatch(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords(['secret', 'other'])
    with pytest.raises(SystemExit):
        _run(['users', 'add', 'alice', '-c', conf])
    err = capsys.readouterr().err
    assert 'do not match' in err


def test_users_add_empty_password(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords([''])
    with pytest.raises(SystemExit):
        _run(['users', 'add', 'alice', '-c', conf])
    err = capsys.readouterr().err
    assert 'must not be empty' in err


def test_users_add_duplicate(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])
    passwords(['secret', 'secret'])
    with pytest.raises(SystemExit):
        _run(['users', 'add', 'alice', '-c', conf])


def test_users_passwd(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])
    capsys.readouterr()

    passwords(['newpass', 'newpass'])
    _run(['users', 'passwd', 'alice', '-c', conf])
    out = capsys.readouterr().out
    assert 'Changed password of user alice' in out

    from tiny_pacs_admin import runtime

    from tiny_pacs_identity import events as identity_events
    from tiny_pacs_identity import hashing
    from tiny_pacs_identity.users import Users
    with runtime.admin_context(conf, [Users.name()]) as (bus, _):
        row = bus.send_one(identity_events.UserByName, 'alice')
    assert row is not None
    assert hashing.verify_password('newpass', row.password_hash)
    assert not hashing.verify_password('secret', row.password_hash)


def test_users_passwd_unknown(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None]
) -> None:
    conf = config_file()
    passwords(['newpass', 'newpass'])
    with pytest.raises(SystemExit):
        _run(['users', 'passwd', 'ghost', '-c', conf])


def test_users_remove(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])
    capsys.readouterr()

    _run(['users', 'remove', 'alice', '-c', conf])
    out = capsys.readouterr().out
    assert 'Removed user alice' in out

    _run(['users', 'list', '--format', 'json', '-c', conf])
    assert json.loads(capsys.readouterr().out) == []


def test_users_remove_unknown(config_file: Callable[..., str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit):
        _run(['users', 'remove', 'ghost', '-c', conf])


def test_users_set_active(
        config_file: Callable[..., str],
        passwords: Callable[[list[str]], None],
        capsys: pytest.CaptureFixture[str]
) -> None:
    conf = config_file()
    passwords(['secret', 'secret'])
    _run(['users', 'add', 'alice', '-c', conf])
    capsys.readouterr()

    _run(['users', 'set-active', 'alice', '--inactive', '-c', conf])
    out = capsys.readouterr().out
    assert 'User alice is now inactive' in out

    _run(['users', 'set-active', 'alice', '--active', '-c', conf])
    out = capsys.readouterr().out
    assert 'User alice is now active' in out


def test_users_set_active_requires_flag(
        config_file: Callable[..., str]) -> None:
    conf = config_file()
    with pytest.raises(SystemExit):
        _run(['users', 'set-active', 'alice', '-c', conf])
