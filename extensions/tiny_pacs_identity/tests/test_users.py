"""Tests of the Users component."""
from typing import Any

import pytest
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events

from tiny_pacs_identity import hashing
from tiny_pacs_identity.models import UserModel


def _version(name: str) -> int | None:
    row = core_db.SchemaVersion.get_or_none(
        core_db.SchemaVersion.component == name
    )
    return row.version if row is not None else None


def test_baseline_migration_fresh_db(identity_bus: Any) -> None:
    identity_bus()
    assert _version('Users') == 1


def test_migration_restart_is_noop(identity_bus: Any) -> None:
    _, _, _, database, path = identity_bus()
    database.db.close()
    identity_bus(db_path=path)
    # The restart applies no migrations and keeps the recorded version
    assert _version('Users') == 1


def test_user_add_and_lookup(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    row = bus.send_one(core_events.UserAdd, {
        'username': 'alice', 'password': 'secret'
    })
    assert row.username == 'alice'
    assert row.is_active
    assert row.last_login is None
    # Passwords are stored hashed, never in plaintext
    assert row.password_hash != 'secret'
    assert hashing.verify_password('secret', row.password_hash)

    found = bus.send_one(core_events.UserByName, 'alice')
    assert found is not None
    assert found.username == 'alice'
    assert bus.send_one(core_events.UserByName, 'ghost') is None


def test_user_add_strips_username(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    row = bus.send_one(core_events.UserAdd, {
        'username': '  padded  ', 'password': 'secret'
    })
    assert row.username == 'padded'


def test_user_add_validates_payload(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    with pytest.raises(ValueError, match='username is required'):
        bus.send_one(core_events.UserAdd, {'password': 'secret'})
    with pytest.raises(ValueError, match='username must not be empty'):
        bus.send_one(core_events.UserAdd,
                     {'username': '   ', 'password': 'secret'})
    with pytest.raises(ValueError, match='password is required'):
        bus.send_one(core_events.UserAdd, {'username': 'alice'})
    with pytest.raises(ValueError, match='password is required'):
        bus.send_one(core_events.UserAdd,
                     {'username': 'alice', 'password': ''})
    with pytest.raises(ValueError, match='at most 64'):
        bus.send_one(core_events.UserAdd,
                     {'username': 'x' * 65, 'password': 'secret'})


def test_user_add_rejects_duplicates(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    with pytest.raises(ValueError, match='already exists'):
        bus.send_one(core_events.UserAdd,
                     {'username': 'alice', 'password': 'other'})


def test_user_list_ordered(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'bob', 'password': 'b'})
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'a'})
    rows = bus.send_one(core_events.UserList, None)
    assert [row.username for row in rows] == ['alice', 'bob']


def test_user_set_password(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'old'})
    bus.send_one(core_events.UserSetPassword,
                 {'username': 'alice', 'password': 'new'})
    row = UserModel.get(UserModel.username == 'alice')
    assert not hashing.verify_password('old', row.password_hash)
    assert hashing.verify_password('new', row.password_hash)


def test_user_set_password_unknown(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    with pytest.raises(ValueError, match='Unknown user'):
        bus.send_one(core_events.UserSetPassword,
                     {'username': 'ghost', 'password': 'new'})


def test_user_remove(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    assert bus.send_one(core_events.UserRemove, 'alice') is True
    assert bus.send_one(core_events.UserRemove, 'alice') is False
    assert UserModel.select().count() == 0


def test_user_set_active(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    row = bus.send_one(core_events.UserSetActive,
                       {'username': 'alice', 'is_active': False})
    assert row.is_active is False
    row = bus.send_one(core_events.UserSetActive,
                       {'username': 'alice', 'is_active': True})
    assert row.is_active is True


def test_user_set_active_validates_payload(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    with pytest.raises(ValueError, match='is_active is required'):
        bus.send_one(core_events.UserSetActive, {'username': 'alice'})
    with pytest.raises(ValueError, match='Unknown user'):
        bus.send_one(core_events.UserSetActive,
                     {'username': 'ghost', 'is_active': True})


def test_user_set_active_requires_boolean(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    bus.send_one(core_events.UserSetActive,
                 {'username': 'alice', 'is_active': False})
    # Truthy strings must not silently flip the flag
    with pytest.raises(ValueError, match='is_active must be a boolean'):
        bus.send_one(core_events.UserSetActive,
                     {'username': 'alice', 'is_active': 'false'})
    row = UserModel.get(UserModel.username == 'alice')
    assert row.is_active is False


def test_user_remove_normalizes_username(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': '  alice  ', 'password': 'secret'})
    # The same normalization as every other handler
    assert bus.send_one(core_events.UserRemove, ' alice ') is True
    assert UserModel.select().count() == 0
    with pytest.raises(ValueError, match='must not be empty'):
        bus.send_one(core_events.UserRemove, '   ')


def test_user_verify_password(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    row = bus.send_one(core_events.UserVerify,
                       {'username': 'alice', 'password': 'secret'})
    assert row is not None
    assert row.username == 'alice'
    # A successful verification records the login
    assert UserModel.get(UserModel.username == 'alice').last_login is not None
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'alice', 'password': 'wrong'}) is None
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'ghost', 'password': 'secret'}) is None


def test_user_verify_username_only(identity_bus: Any) -> None:
    """A None password is a username-only check (identity type 1)."""
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    row = bus.send_one(core_events.UserVerify,
                       {'username': 'alice', 'password': None})
    assert row is not None
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'ghost', 'password': None}) is None


def test_user_verify_inactive(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    bus.send_one(core_events.UserSetActive,
                 {'username': 'alice', 'is_active': False})
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'alice', 'password': 'secret'}) is None
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'alice', 'password': None}) is None


def test_user_verify_unknown_runs_dummy_proof(
        identity_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown users still run one password proof (timing equalized)."""
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    calls: list[str] = []
    original = hashing.verify_password

    def recording(password: str, stored: str) -> bool:
        calls.append(stored)
        return original(password, stored)

    monkeypatch.setattr(hashing, 'verify_password', recording)
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'ghost', 'password': 'secret'}) is None
    assert len(calls) == 1
    assert calls[0] != UserModel.get(UserModel.username == 'alice')\
        .password_hash


def test_user_verifies_payload(identity_bus: Any) -> None:
    bus, _, _, _, _ = identity_bus()
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    # Unusable usernames (peer-provided DICOM text) are failed
    # verifications, never exceptions escaping into the association flow
    assert bus.send_one(core_events.UserVerify,
                        {'username': None, 'password': 'secret'}) is None
    assert bus.send_one(core_events.UserVerify,
                        {'username': '', 'password': None}) is None
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'x' * 65,
                         'password': 'secret'}) is None
    with pytest.raises(ValueError, match='password must be a string'):
        bus.send_one(core_events.UserVerify,
                     {'username': 'alice', 'password': 42})


def test_user_verify_invalid_username_runs_dummy_proof(
        identity_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """The timing-equalized proof runs even for unusable usernames."""
    bus, _, _, _, _ = identity_bus()
    calls: list[str] = []

    def recording(password: str, stored: str) -> bool:
        calls.append(stored)
        return True

    monkeypatch.setattr(hashing, 'verify_password', recording)
    assert bus.send_one(core_events.UserVerify,
                        {'username': 'x' * 65, 'password': 'secret'}) is None
    assert len(calls) == 1


def test_mutations_emit_audit_records(identity_bus: Any) -> None:
    """Successful user mutations are broadcast for the audit trail."""
    bus, _, _, _, _ = identity_bus()
    records: list[Any] = []
    bus.subscribe(core_events.AuditRecord, records.append)
    bus.send_one(core_events.UserAdd,
                 {'username': 'alice', 'password': 'secret'})
    bus.send_one(core_events.UserSetPassword,
                 {'username': 'alice', 'password': 'new'})
    bus.send_one(core_events.UserSetActive,
                 {'username': 'alice', 'is_active': False})
    bus.send_one(core_events.UserRemove, 'alice')
    assert [(r.event, r.details.get('user')) for r in records] == [
        ('user-add', 'alice'),
        ('user-passwd', 'alice'),
        ('user-set-active', 'alice'),
        ('user-remove', 'alice')
    ]
    assert all(r.category == 'admin' and r.status == 'success'
               for r in records)
    assert records[2].details['is_active'] is False
    # Secrets never reach the audit details
    for record in records:
        assert 'secret' not in str(record.details)
        assert 'new' not in str(record.details)
