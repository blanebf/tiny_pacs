"""Password hashing.

Hashes and verifies user passwords with the standard library only:
:func:`hashlib.scrypt` with a :func:`hashlib.pbkdf2_hmac` fallback for
builds without scrypt support. Every password receives its own random
salt; the result is serialized as one self-describing string whose first
``$``-separated field names the scheme, so new schemes can be introduced
without invalidating stored hashes.

Serialized formats::

    scrypt$<n>$<r>$<p>$<salt hex>$<hash hex>
    pbkdf2$<iterations>$<salt hex>$<hash hex>

Verification compares in constant time and never reveals through its
return value whether the user or the password was wrong.
"""
import hashlib
import hmac
import secrets

#: Serialization field separator
_SEPARATOR = '$'

#: Scheme name of the scrypt serialization format
SCRYPT = 'scrypt'

#: Scheme name of the PBKDF2-HMAC-SHA256 serialization format
PBKDF2 = 'pbkdf2'

#: scrypt CPU/memory cost parameters: OWASP's equivalent-defense settings
#: for a 16 MiB memory budget (n=2**14, r=8, p=5), keeping one
#: verification cheap enough to run on the association acceptance path
#: while concurrent verifications stay within a 64 MiB memory budget
_SCRYPT_N = 1 << 14
_SCRYPT_R = 8
_SCRYPT_P = 5
_SCRYPT_MAXMEM = 64 * 1024 * 1024

#: PBKDF2 iteration count (fallback scheme)
_PBKDF2_ITERATIONS = 600_000

#: Salt and derived key sizes in bytes
_SALT_BYTES = 16
_KEY_BYTES = 32


def hash_password(password: str) -> str:
    """Hashes a password with a fresh random salt.

    :param password: password to hash
    :type password: str
    :return: serialized hash (scheme, parameters, salt and digest)
    :rtype: str
    :raises ValueError: raised for an empty password
    """
    if not password:
        raise ValueError('password must not be empty')
    salt = secrets.token_bytes(_SALT_BYTES)
    scrypt = getattr(hashlib, 'scrypt', None)
    if scrypt is not None:
        try:
            derived = scrypt(
                password.encode('utf-8'), salt=salt,
                n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P,
                maxmem=_SCRYPT_MAXMEM, dklen=_KEY_BYTES
            )
        except (ValueError, OSError):
            # Builds without usable scrypt fall back to PBKDF2
            pass
        else:
            return _SEPARATOR.join((
                SCRYPT, str(_SCRYPT_N), str(_SCRYPT_R), str(_SCRYPT_P),
                salt.hex(), derived.hex()
            ))
    derived = hashlib.pbkdf2_hmac(
        'sha256', password.encode('utf-8'), salt, _PBKDF2_ITERATIONS,
        dklen=_KEY_BYTES
    )
    return _SEPARATOR.join((
        PBKDF2, str(_PBKDF2_ITERATIONS), salt.hex(), derived.hex()
    ))


def verify_password(password: str, stored: str) -> bool:
    """Verifies a password against a serialized hash.

    Runs in constant time with respect to the stored hash; malformed or
    unknown formats simply fail verification.

    :param password: password to check
    :type password: str
    :param stored: serialized hash as returned by :func:`hash_password`
    :type stored: str
    :return: whether the password matches the stored hash
    :rtype: bool
    """
    fields = stored.split(_SEPARATOR)
    try:
        if fields[0] == SCRYPT:
            if len(fields) != 6:
                return False
            n, r, p = int(fields[1]), int(fields[2]), int(fields[3])
            salt = bytes.fromhex(fields[4])
            expected = bytes.fromhex(fields[5])
            scrypt = getattr(hashlib, 'scrypt', None)
            if scrypt is None:
                return False
            derived = scrypt(
                password.encode('utf-8'), salt=salt,
                n=n, r=r, p=p, maxmem=_SCRYPT_MAXMEM, dklen=len(expected)
            )
        elif fields[0] == PBKDF2:
            if len(fields) != 4:
                return False
            iterations = int(fields[1])
            salt = bytes.fromhex(fields[2])
            expected = bytes.fromhex(fields[3])
            derived = hashlib.pbkdf2_hmac(
                'sha256', password.encode('utf-8'), salt, iterations,
                dklen=len(expected)
            )
        else:
            return False
    except (ValueError, OverflowError):
        return False
    return hmac.compare_digest(derived, expected)
