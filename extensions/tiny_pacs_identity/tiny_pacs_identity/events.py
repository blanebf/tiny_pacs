"""User management events.

The events are handled by the :class:`~tiny_pacs_identity.users.Users`
component. Passwords travel as plaintext payloads only within the process
(and are prompted with ``getpass`` in the CLI, never passed as command
line arguments); handlers hash them before they reach the database.
"""
from typing import Any

import trolleybus

from . import models


class UserByName(trolleybus.Event[str, models.UserModel | None]):
    """Request a user by login name.

    The result is None for unknown usernames.
    """


class UserList(trolleybus.Event[None, list[models.UserModel]]):
    """Request every user, ordered by username.

    Password hashes are never included in CLI output; event consumers
    receive the records as stored.
    """


class UserAdd(trolleybus.Event[dict[str, Any], models.UserModel]):
    """Register a new user.

    Payload is a mapping with ``username`` and ``password``; the password
    is hashed before storage.
    """


class UserSetPassword(trolleybus.Event[dict[str, Any], models.UserModel]):
    """Change the password of an existing user.

    Payload is a mapping with ``username`` and ``password``.
    """


class UserRemove(trolleybus.Event[str, bool]):
    """Remove a user by login name.

    The result is whether the user existed (and was removed).
    """


class UserSetActive(trolleybus.Event[dict[str, Any], models.UserModel]):
    """Activate or deactivate a user.

    Payload is a mapping with ``username`` and ``is_active``.
    """
