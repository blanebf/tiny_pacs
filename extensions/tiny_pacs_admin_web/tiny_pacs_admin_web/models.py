"""Web grant model and schema migrations.

Provides the single table owned by the web administration extension:
:class:`WebGrantModel` records which PACS users may log into the console
and with which role. A user authenticates with the regular PACS
credentials (verified through :class:`tiny_pacs.events.UserVerify`) *and*
needs a row here — without a grant the login is rejected exactly like a
wrong password.

The table ships through the :class:`tiny_pacs.events.Migrations` mechanism
like every other extension table.
"""
import datetime

import peewee
from tiny_pacs import schema


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


#: The console role an operator may hold: every page and every mutation
ROLE_ADMIN = 'admin'

#: The read-only console role: GET pages only, mutations are rejected
ROLE_VIEWER = 'viewer'

#: Every role a :class:`WebGrantModel` row may carry
ROLES: tuple[str, ...] = (ROLE_ADMIN, ROLE_VIEWER)


class WebGrantModel(peewee.Model):
    """Database record granting console access to a PACS user.

    :ivar username: login name of the PACS user (verified through
                    :class:`tiny_pacs.events.UserVerify` at login)
    :ivar role: ``admin`` (every page and mutation) or ``viewer``
                (read-only pages, mutations rejected)
    :ivar created: when the grant was created
    """

    #: Login name of the PACS user
    username = peewee.CharField(max_length=64, unique=True)

    #: Console role, see :data:`ROLES`
    role = peewee.CharField(max_length=8, default=ROLE_VIEWER)

    #: When the grant was created
    created = peewee.DateTimeField(default=_utcnow)


#: Tables owned by the web administration component
TABLES: list[type[peewee.Model]] = [WebGrantModel]

#: Schema migrations of the web administration tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create web grant tables')
]
