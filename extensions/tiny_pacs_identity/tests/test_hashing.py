"""Tests of the password hashing helpers."""
import hashlib
import hmac
from typing import Any

import pytest

from tiny_pacs_identity import hashing


def test_hash_roundtrip() -> None:
    stored = hashing.hash_password('correct horse')
    assert hashing.verify_password('correct horse', stored)


def test_hash_is_salted() -> None:
    first = hashing.hash_password('same-password')
    second = hashing.hash_password('same-password')
    # Different salts: identical passwords never produce identical hashes
    assert first != second
    assert hashing.verify_password('same-password', first)
    assert hashing.verify_password('same-password', second)


def test_wrong_password_rejected() -> None:
    stored = hashing.hash_password('correct horse')
    assert not hashing.verify_password('battery staple', stored)
    assert not hashing.verify_password('', stored)


def test_hash_format_versioning() -> None:
    stored = hashing.hash_password('secret')
    scheme = stored.split('$')[0]
    assert scheme in (hashing.SCRYPT, hashing.PBKDF2)
    fields = stored.split('$')
    if scheme == hashing.SCRYPT:
        assert len(fields) == 6
    else:
        assert len(fields) == 4


def test_unicode_password() -> None:
    stored = hashing.hash_password('pässwörd €')
    assert hashing.verify_password('pässwörd €', stored)
    assert not hashing.verify_password('pässwörd $', stored)


def test_verify_rejects_foreign_formats() -> None:
    assert not hashing.verify_password('secret', '')
    assert not hashing.verify_password('secret', 'unknown$1$2$3')
    assert not hashing.verify_password('secret', 'scrypt$abc$def')
    assert not hashing.verify_password('secret', 'scrypt$1$1$1$nothex$xx')
    assert not hashing.verify_password('secret', 'pbkdf2$100$zz$yy')


def test_verify_scrypt_format_directly() -> None:
    # A hand-built scrypt record must verify regardless of who produced it
    salt = b'0123456789abcdef'
    derived = hashlib.scrypt(
        b'hunter2', salt=salt, n=1 << 14, r=8, p=1,
        maxmem=64 * 1024 * 1024, dklen=32
    )
    stored = f'scrypt${1 << 14}$8$1${salt.hex()}${derived.hex()}'
    assert hashing.verify_password('hunter2', stored)
    assert not hashing.verify_password('hunter3', stored)


def test_verify_pbkdf2_format_directly() -> None:
    salt = b'fedcba9876543210'
    derived = hashlib.pbkdf2_hmac('sha256', b'hunter2', salt, 1000)
    stored = f'pbkdf2$1000${salt.hex()}${derived.hex()}'
    assert hashing.verify_password('hunter2', stored)
    assert not hashing.verify_password('hunter3', stored)


def test_scrypt_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_scrypt(password: bytes, **kwargs: Any) -> bytes:
        raise ValueError('scrypt is not supported')

    monkeypatch.setattr(hashlib, 'scrypt', no_scrypt, raising=False)
    stored = hashing.hash_password('fallback')
    assert stored.startswith(f'{hashing.PBKDF2}$')
    assert hashing.verify_password('fallback', stored)
    assert not hashing.verify_password('wrong', stored)


def test_empty_password_rejected() -> None:
    with pytest.raises(ValueError, match='must not be empty'):
        hashing.hash_password('')


def test_timing_safe_compare_used(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[bytes, bytes]] = []
    original = hmac.compare_digest

    def recording(a: bytes, b: bytes) -> bool:
        calls.append((a, b))
        return original(a, b)

    monkeypatch.setattr(hmac, 'compare_digest', recording)
    stored = hashing.hash_password('timing')
    assert hashing.verify_password('timing', stored)
    assert len(calls) == 1
