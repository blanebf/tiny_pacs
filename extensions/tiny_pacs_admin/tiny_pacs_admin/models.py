"""Device model and schema migrations.

Provides the database-backed device record used by the
:class:`~tiny_pacs_admin.store.DeviceStore` component, together with its
baseline schema migration published through the
:class:`tiny_pacs.events.Migrations` mechanism.
"""
import datetime
import enum

import peewee
from tiny_pacs import schema


class IdentityPolicy(str, enum.Enum):
    """Identity requirement of a device for incoming associations."""

    #: No user identity required
    NONE = 'none'

    #: User identity required; username must exist, password is optional
    USERNAME = 'username'

    #: Username/password identity (type 2) with a valid password required
    PASSWORD = 'password'


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class DeviceModel(peewee.Model):
    """Database record of a remote DICOM device.

    :ivar aet: remote AE title
    :ivar address: remote IP address or host name
    :ivar port: remote SCP TCP port
    :ivar identity: per-device identity policy, an
                    :class:`IdentityPolicy` value
    :ivar username: outgoing user identity negotiation username
    :ivar password: outgoing user identity negotiation password
    :ivar extra: additional device fields serialized as JSON
    :ivar created: when the device was added
    :ivar updated: when the device was last modified
    """

    #: Remote AE title
    aet = peewee.CharField(max_length=16, unique=True)

    #: Remote IP address or host name
    address = peewee.CharField(max_length=255)

    #: Remote SCP TCP port
    port = peewee.IntegerField()

    #: Per-device identity policy (see :class:`IdentityPolicy`)
    identity = peewee.CharField(
        max_length=8, default=IdentityPolicy.NONE.value
    )

    #: Outgoing identity username
    username = peewee.CharField(max_length=64, null=True)

    #: Outgoing identity password
    password = peewee.CharField(max_length=255, null=True)

    #: Additional device fields, serialized as JSON
    extra = peewee.TextField(default='{}')

    #: When the device was added
    created = peewee.DateTimeField(default=_utcnow)

    #: When the device was last modified
    updated = peewee.DateTimeField(default=_utcnow)


#: Tables owned by the admin components
TABLES: list[type[peewee.Model]] = [DeviceModel]

#: Schema migrations of the admin tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create device tables')
]
