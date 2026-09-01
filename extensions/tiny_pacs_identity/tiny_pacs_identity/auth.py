"""Association authentication by the DICOM User Identity sub-item.

The :class:`UserIdentityAuth` component subscribes to
:class:`~tiny_pacs.events.Assoc` with a priority above the device
registry components: a request that fails authentication is rejected
before any listener sees it, so a rejected association is never
auto-added as a device either.

Every association is checked against the calling device's identity
policy; the policy resolution order is the device policy itself, then
:attr:`UserIdentityAuthConfig.default_policy` for known devices without
one, then :attr:`UserIdentityAuthConfig.unknown_device_policy` for
unknown devices.

Security note: User Identity negotiation transmits credentials inside
A-ASSOCIATE. Enable TLS (``ae.tls``) whenever non-``none`` identity
policies are used.
"""
import enum
import secrets
from typing import Any, NoReturn

import trolleybus
from pynetdicom2 import exceptions, pdu
from pynetdicom2.userdataitems import (
    UserIdentityNegotiationSubItem,
    UserIdentityNegotiationSubItemAc,
)
from tiny_pacs import component, events
from tiny_pacs import identity as core_identity
from tiny_pacs_admin import events as admin_events
from tiny_pacs_admin.models import IdentityPolicy

from . import events as identity_events
from . import hashing, models
from .models import UserModel

#: Username-only identity (PS3.7 D.3.3.7.1, type 1)
_IDENTITY_TYPE_USERNAME = 1

#: Username/password identity (PS3.7 D.3.3.7.1, type 2)
_IDENTITY_TYPE_PASSWORD = 2

_NONE = IdentityPolicy.NONE.value
_PASSWORD = IdentityPolicy.PASSWORD.value
_REJECT = 'reject'


class UnknownDevicePolicy(str, enum.Enum):
    """Identity policy applied to associations from unknown devices."""

    #: No user identity required
    NONE = 'none'

    #: User identity required; username must exist, password is optional
    USERNAME = 'username'

    #: Username/password identity (type 2) with a valid password required
    PASSWORD = 'password'

    #: Reject associations from unknown devices
    REJECT = 'reject'


class UserIdentityAuthConfig(component.ComponentConfig):
    """Configuration of the :class:`UserIdentityAuth` component.

    :ivar default_policy: policy for known devices without an explicit
                          per-device policy
    :ivar unknown_device_policy: policy for devices no registry knows;
                          ``reject`` refuses such associations outright
    """

    default_policy: IdentityPolicy = IdentityPolicy.NONE
    unknown_device_policy: UnknownDevicePolicy = UnknownDevicePolicy.NONE


class UserIdentityAuth(component.Component[UserIdentityAuthConfig]):
    """Component that authenticates incoming associations.

    Handles the following events:

        * :class:`~tiny_pacs.events.Assoc`
    """

    config_model = UserIdentityAuthConfig

    #: Higher priority than the device registry components: a rejected
    #: association aborts the remaining ``Assoc`` listeners, so it is
    #: never auto-added as a device.
    priority = trolleybus.DEFAULT_PRIORITY + 20

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: UserIdentityAuthConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: UserIdentityAuthConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.Assoc, self.on_assoc)
        # Pre-computed hash verified against for unknown or inactive
        # users, so every password attempt runs exactly one proof and
        # rejection timing never reveals whether a username exists
        self._dummy_password_hash = hashing.hash_password(
            secrets.token_urlsafe(32)
        )

    def on_started(self) -> None:
        """Handles `OnStarted` event.

        Warns when a DB-backed device registry auto-adds unknown devices
        with an identity policy that differs from the configured
        ``unknown_device_policy``: the registry's default governs the
        device's next association, so the two defaults should be
        configured consistently.
        """
        super().on_started()
        if not self.bus.has_listeners(identity_events.UserByName):
            self.log_error(
                'UserIdentityAuth is enabled but no Users component '
                'provides user accounts; every presented identity is '
                'treated as an unknown user'
            )
        auto_add_identity = self.send_any(admin_events.AutoAddIdentity, None)
        if auto_add_identity is None:
            return
        if auto_add_identity.value != self.config.unknown_device_policy.value:
            self.log_warning(
                'DeviceStore auto-adds unknown devices with identity '
                'policy %r but the unknown device policy is %r; configure '
                'the two defaults consistently',
                auto_add_identity.value,
                self.config.unknown_device_policy.value
            )

    def on_assoc(self, payload: events.AssocPayload) -> None:
        """Handles `Assoc` event: authenticates the association.

        Decodes the User Identity sub-item, resolves the calling
        device's identity policy and verifies the presented credentials.
        A failed verification rejects the association; a successful one
        records the login and answers a positive identity response when
        the request asked for one.

        :param payload: association acceptor and request parameters
        :type payload: events.AssocPayload
        :raises pynetdicom2.exceptions.AssociationRejectedError: raised
                when the association does not satisfy the device's
                identity policy
        """
        assoc = payload.assoc
        calling_aet = assoc.calling_ae_title.strip()
        identity_item = core_identity.get_user_identity(assoc)
        policy = self._resolve_policy(calling_aet)
        user = self._verify(policy, identity_item, calling_aet)
        if user is not None:
            self.log_info(
                'Association from %s authenticated as %s',
                calling_aet, user.username
            )
            self._update_last_login(user)
        self._answer_identity(assoc, identity_item)

    def _resolve_policy(self, calling_aet: str) -> str:
        """Resolves the identity policy of the calling device.

        Resolution order: the per-device policy of a known device, then
        :attr:`UserIdentityAuthConfig.default_policy` for known devices
        without one, then
        :attr:`UserIdentityAuthConfig.unknown_device_policy`.

        :param calling_aet: AE title of the calling device
        :type calling_aet: str
        :return: effective policy value
        :rtype: str
        """
        device = self.send_any(events.DeviceByAE, calling_aet)
        if device is None:
            return self.config.unknown_device_policy.value
        extra = device.model_extra or {}
        raw = extra.get('identity')
        if raw is None:
            return self.config.default_policy.value
        value = raw.value if isinstance(raw, enum.Enum) else raw
        try:
            return IdentityPolicy(value).value
        except (TypeError, ValueError):
            self.log_warning(
                'Device %s carries an invalid identity policy %r; '
                'falling back to %s',
                calling_aet, raw, self.config.default_policy.value
            )
            return self.config.default_policy.value

    def _verify(
            self,
            policy: str,
            identity_item: UserIdentityNegotiationSubItem | None,
            calling_aet: str
    ) -> UserModel | None:
        """Verifies the presented identity against the policy.

        :param policy: effective identity policy value
        :param identity_item: decoded User Identity sub-item, or None
                              when the request carries none
        :param calling_aet: AE title of the calling device (for logging)
        :return: the authenticated user, or None when the policy accepts
                 without one (or only advisory verification applies)
        :rtype: UserModel or None
        :raises pynetdicom2.exceptions.AssociationRejectedError: raised
                when the identity does not satisfy the policy
        """
        if policy == _REJECT:
            self._reject(
                calling_aet,
                'associations from unknown devices are rejected'
            )
        if identity_item is None:
            if policy == _NONE:
                return None
            self._reject(calling_aet, 'user identity is required')
        identity_type = identity_item.user_identity_type
        if identity_type == _IDENTITY_TYPE_USERNAME:
            return self._verify_username(policy, identity_item, calling_aet)
        if identity_type == _IDENTITY_TYPE_PASSWORD:
            return self._verify_password(policy, identity_item, calling_aet)
        # Kerberos, SAML and JWT identities (types 3-5) are not supported
        self._reject(
            calling_aet,
            f'unsupported user identity type {identity_type}'
        )

    def _verify_username(
            self,
            policy: str,
            identity_item: UserIdentityNegotiationSubItem,
            calling_aet: str
    ) -> UserModel | None:
        """Verifies a username-only identity (type 1)."""
        username = identity_item.primary_field
        if not isinstance(username, str):
            self._reject(calling_aet, 'username is not valid text')
        if policy == _PASSWORD:
            self._reject(
                calling_aet, 'username/password identity is required'
            )
        user = self._known_user(username)
        if policy == _NONE:
            # Advisory verification: log the failure, accept anyway
            if user is None:
                self.log_warning(
                    'Association from %s presents unknown or inactive '
                    'user %s; accepting due to the none identity policy',
                    calling_aet, username
                )
                return None
            return user
        if user is None:
            self._reject(calling_aet, 'unknown or inactive user')
        return user

    def _verify_password(
            self,
            policy: str,
            identity_item: UserIdentityNegotiationSubItem,
            calling_aet: str
    ) -> UserModel | None:
        """Verifies a username/password identity (type 2)."""
        username = identity_item.primary_field
        password = identity_item.secondary_field
        if not isinstance(username, str) or not isinstance(password, str):
            self._reject(calling_aet, 'identity fields are not valid text')
        user = self._known_user(username)
        if user is not None:
            valid = hashing.verify_password(password, user.password_hash)
        else:
            # Equalize timing: run one password proof even for unknown
            # or inactive users, so a rejection never reveals whether
            # the username exists
            hashing.verify_password(password, self._dummy_password_hash)
            valid = False
        if policy == _NONE:
            # Advisory verification: log the failure, accept anyway
            if not valid:
                self.log_warning(
                    'Association from %s presents invalid credentials '
                    'for user %s; accepting due to the none identity '
                    'policy', calling_aet, username
                )
                return None
            return user
        if not valid:
            self._reject(calling_aet, 'invalid credentials')
        return user

    def _known_user(self, username: str) -> UserModel | None:
        """Looks up an active user by login name.

        Returns None when no user registry is listening (the ``Users``
        component is disabled): the authentication then fails closed
        instead of aborting the association with an event bus error.
        """
        if not self.bus.has_listeners(identity_events.UserByName):
            return None
        user = self.send_one(identity_events.UserByName, username)
        if user is None or not user.is_active:
            return None
        return user

    @staticmethod
    def _update_last_login(user: UserModel) -> None:
        """Records the successful authentication of a user."""
        user.last_login = models._utcnow()
        user.save()

    @staticmethod
    def _answer_identity(
            assoc: pdu.AAssociateRqPDU,
            identity_item: UserIdentityNegotiationSubItem | None
    ) -> None:
        """Prepares the User Identity response of the accepted request.

        The association response reuses the request's User Information
        item, so the request sub-item (0x58) is removed from it and —
        when a positive response was requested (PS3.7 D.3.3.7.3) —
        replaced with the empty response sub-item (0x59) before the
        acceptor serializes the A-ASSOCIATE-AC.
        """
        if identity_item is None or not assoc.variable_items:
            return
        user_info = assoc.variable_items[-1]
        if not isinstance(user_info, pdu.UserInformationItem):
            return
        user_info.user_data = [
            sub_item for sub_item in user_info.user_data
            if not isinstance(sub_item, UserIdentityNegotiationSubItem)
        ]
        if identity_item.positive_response_req == 1:
            user_info.user_data.append(UserIdentityNegotiationSubItemAc(b''))

    def _reject(self, calling_aet: str, reason: str) -> NoReturn:
        """Rejects the association being authenticated.

        The rejection aborts the remaining ``Assoc`` listeners, so lower
        priority components (device auto-add) never see the request.

        :raises pynetdicom2.exceptions.AssociationRejectedError: always
        """
        self.log_warning(
            'Rejecting association from %s: %s', calling_aet, reason
        )
        raise exceptions.AssociationRejectedError(1, 1, 7)
