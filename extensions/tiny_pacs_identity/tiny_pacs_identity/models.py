"""User model and schema migrations.

Provides the database-backed user record used by the
:class:`~tiny_pacs_identity.users.Users` component, together with its
baseline schema migration published through the
:class:`tiny_pacs.events.Migrations` mechanism.
"""
import datetime

import peewee
from tiny_pacs import schema


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class UserModel(peewee.Model):
    """Database record of a PACS user.

    :ivar username: login name presented in the DICOM user identity
                    sub-item
    :ivar password_hash: salted password hash (see
                         :mod:`tiny_pacs_identity.hashing`); plaintext
                         passwords are never stored
    :ivar is_active: inactive users are rejected by the authentication
    :ivar created: when the user was added
    :ivar last_login: when the user last authenticated successfully
    """

    #: Login name
    username = peewee.CharField(max_length=64, unique=True)

    #: Salted password hash
    password_hash = peewee.CharField(max_length=255)

    #: Inactive users are rejected by the authentication
    is_active = peewee.BooleanField(default=True)

    #: When the user was added
    created = peewee.DateTimeField(default=_utcnow)

    #: When the user last authenticated successfully
    last_login = peewee.DateTimeField(null=True)


#: Tables owned by the identity components
TABLES: list[type[peewee.Model]] = [UserModel]

#: Schema migrations of the identity tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create user tables')
]
