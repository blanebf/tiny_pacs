"""Database-backed user registry component.

Keeps the users that incoming associations authenticate as in the
database. Passwords are stored salted and hashed (see
:mod:`tiny_pacs_identity.hashing`), never in plaintext; the
:class:`~tiny_pacs_identity.auth.UserIdentityAuth` component consumes the
records through the core :class:`~tiny_pacs.events.UserVerify` event when
enforcing a device's identity policy.

User mutations are broadcast as :class:`~tiny_pacs.events.AuditRecord`
so an installed audit component records them.

Create and maintain users with the ``users`` CLI subcommands against the
offline database before starting a server that requires identity.
"""
import secrets
from collections.abc import Mapping
from typing import Any

import trolleybus
from tiny_pacs import component, events, schema

from . import hashing, models
from .models import UserModel

#: Maximum length of a login name (size of the database column)
MAX_USERNAME_LENGTH = 64


class UsersConfig(component.ComponentConfig):
    """Configuration of the :class:`Users` component.

    The component has no settings of its own; this model exists so the
    component provides its own config type to the loader like every other
    component.
    """


class Users(component.Component[UsersConfig]):
    """Component that keeps user accounts in the database.

    Handles the following events:

        * :class:`~tiny_pacs.events.Migrations`
        * :class:`~tiny_pacs.events.UserByName`
        * :class:`~tiny_pacs.events.UserVerify`
        * :class:`~tiny_pacs.events.UserList`
        * :class:`~tiny_pacs.events.UserAdd`
        * :class:`~tiny_pacs.events.UserSetPassword`
        * :class:`~tiny_pacs.events.UserRemove`
        * :class:`~tiny_pacs.events.UserSetActive`
    """

    config_model = UsersConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: UsersConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: UsersConfig or dict
        """
        super().__init__(bus, config)
        # Pre-computed hash verified against for unknown or inactive
        # users, so every password attempt runs exactly one proof and
        # rejection timing never reveals whether a username exists
        self._dummy_password_hash = hashing.hash_password(
            secrets.token_urlsafe(32)
        )
        self.subscribe(events.Migrations, self.migrations)
        self.subscribe(events.UserByName, self.user_by_name)
        self.subscribe(events.UserVerify, self.user_verify)
        self.subscribe(events.UserList, self.user_list)
        self.subscribe(events.UserAdd, self.user_add)
        self.subscribe(events.UserSetPassword, self.user_set_password)
        self.subscribe(events.UserRemove, self.user_remove)
        self.subscribe(events.UserSetActive, self.user_set_active)

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), models.TABLES, models.MIGRATIONS
        )

    def user_by_name(self, username: str) -> UserModel | None:
        """Handles `UserByName` event

        :param username: login name of the user
        :type username: str
        :return: user record or None for unknown usernames
        :rtype: UserModel or None
        """
        return UserModel.get_or_none(UserModel.username == username)

    def user_verify(self, payload: Mapping[str, Any]) -> UserModel | None:
        """Handles `UserVerify` event

        Verifies the presented credentials: exactly one password proof
        runs — against a dummy hash for unknown or inactive users — so
        the timing never reveals whether the username exists. A ``None``
        password is a username-only check (DICOM user identity type 1).
        A successful verification records the login (``last_login``).

        A missing, empty or oversized username is treated like an unknown
        user (failed verification, never an exception): DICOM identity
        items carry arbitrary peer-provided text, and a raised error
        would kill the association without an A-ASSOCIATE-RJ instead of
        rejecting it cleanly.

        :param payload: ``username`` and ``password`` mapping; the
                        password may be None for username-only checks
        :type payload: Mapping
        :return: the authenticated user record or None
        :rtype: UserModel or None
        :raises ValueError: raised when the password is neither a string
                            nor None (a programming error, not peer input)
        """
        password = payload.get('password')
        if password is not None and not isinstance(password, str):
            raise ValueError('password must be a string or None')
        try:
            username = self._clean_username(payload.get('username'))
        except ValueError:
            username = None
        if username is None:
            if password is not None:
                # Keep the timing equal with the regular verification
                hashing.verify_password(password,
                                        self._dummy_password_hash)
            return None
        row = UserModel.get_or_none(UserModel.username == username)
        active = row is not None and row.is_active
        if password is None:
            if not active:
                return None
        else:
            if active:
                assert row is not None
                valid = hashing.verify_password(password, row.password_hash)
            else:
                hashing.verify_password(password, self._dummy_password_hash)
                valid = False
            if not valid:
                return None
        # Both success paths imply an existing, active user
        assert row is not None
        self._record_login(row)
        return row

    def _record_login(self, row: UserModel) -> None:
        """Records a successful authentication of a user."""
        with self.atomic():
            row.last_login = models._utcnow()
            row.save()

    def user_list(self, _: None = None) -> list[UserModel]:
        """Handles `UserList` event

        :return: every user, ordered by username
        :rtype: list[UserModel]
        """
        return list(UserModel.select().order_by(UserModel.username))

    def user_add(self, payload: Mapping[str, Any]) -> UserModel:
        """Handles `UserAdd` event

        :param payload: ``username`` and ``password`` mapping
        :type payload: Mapping
        :return: the created user record
        :rtype: UserModel
        :raises ValueError: raised for missing, invalid or duplicate
                            usernames and for empty passwords
        """
        username = self._username(payload)
        password = self._password(payload)
        with self.atomic():
            if UserModel.get_or_none(
                    UserModel.username == username) is not None:
                raise ValueError(f'User {username} already exists')
            row = UserModel.create(
                username=username,
                password_hash=hashing.hash_password(password),
                is_active=True,
                created=models._utcnow()
            )
        self.log_info('Added user %s', username)
        self._audit('user-add', username)
        return row

    def user_set_password(self, payload: Mapping[str, Any]) -> UserModel:
        """Handles `UserSetPassword` event

        :param payload: ``username`` and ``password`` mapping
        :type payload: Mapping
        :return: the updated user record
        :rtype: UserModel
        :raises ValueError: raised for a missing or unknown username and
                            for an empty password
        """
        username = self._username(payload)
        password = self._password(payload)
        with self.atomic():
            row = UserModel.get_or_none(UserModel.username == username)
            if row is None:
                raise ValueError(f'Unknown user {username}')
            row.password_hash = hashing.hash_password(password)
            row.save()
        self.log_info('Changed password of user %s', username)
        # The record notes the fact of the change, never the value
        self._audit('user-passwd', username)
        return row

    def user_remove(self, username: str) -> bool:
        """Handles `UserRemove` event

        :param username: login name of the user to remove; normalized
                         like in every other handler (stripped, validated)
        :type username: str
        :return: whether the user existed and was removed
        :rtype: bool
        :raises ValueError: raised for a missing, empty or oversized
                            username
        """
        username = self._clean_username(username)
        deleted = bool(
            UserModel.delete().where(UserModel.username == username)
            .execute()
        )
        if deleted:
            self.log_info('Removed user %s', username)
            self._audit('user-remove', username)
        return deleted

    def user_set_active(self, payload: Mapping[str, Any]) -> UserModel:
        """Handles `UserSetActive` event

        :param payload: ``username`` and ``is_active`` mapping
        :type payload: Mapping
        :return: the updated user record
        :rtype: UserModel
        :raises ValueError: raised for a missing or unknown username and
                            for a missing ``is_active`` flag
        """
        username = self._username(payload)
        if 'is_active' not in payload:
            raise ValueError('is_active is required')
        is_active = payload['is_active']
        if not isinstance(is_active, bool):
            raise ValueError('is_active must be a boolean')
        with self.atomic():
            row = UserModel.get_or_none(UserModel.username == username)
            if row is None:
                raise ValueError(f'Unknown user {username}')
            row.is_active = is_active
            row.save()
        self.log_info('User %s is now %s',
                      username, 'active' if is_active else 'inactive')
        self._audit('user-set-active', username, is_active=is_active)
        return row

    @staticmethod
    def _clean_username(value: Any) -> str:
        """Normalizes and validates a login name.

        Every handler applies the same normalization, so a username is
        addressed identically by all user events.
        """
        if not isinstance(value, str):
            raise ValueError('username is required')
        username = value.strip()
        if not username:
            raise ValueError('username must not be empty')
        if len(username) > MAX_USERNAME_LENGTH:
            raise ValueError(
                f'username must be at most {MAX_USERNAME_LENGTH} characters'
            )
        return username

    @staticmethod
    def _username(payload: Mapping[str, Any]) -> str:
        """Extracts and validates the ``username`` payload field."""
        return Users._clean_username(payload.get('username'))

    @staticmethod
    def _password(payload: Mapping[str, Any]) -> str:
        """Extracts and validates the ``password`` payload field."""
        password = payload.get('password')
        if not isinstance(password, str) or not password:
            raise ValueError('password is required')
        return password

    def atomic(self) -> Any:
        """Context manager for handling simple transactions

        :return: atomic transaction
        """
        return self.send_one(events.Atomic, None)

    def _audit(self, event: str, username: str, **details: Any) -> None:
        """Broadcasts an audit record of a successful user mutation.

        Fire-and-forget: listener failures never affect the mutation, and
        no listener is required. Details never contain secrets.

        :param event: audit event name, e.g. ``user-add``
        :type event: str
        :param username: login name of the affected user
        :type username: str
        """
        self.broadcast_nothrow(
            events.AuditRecord,
            events.AuditRecordPayload(
                category='admin', event=event,
                details={'user': username, **details}
            )
        )
