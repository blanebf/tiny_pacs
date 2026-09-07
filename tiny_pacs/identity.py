"""DICOM User Identity Negotiation helpers.

An association requestor may present a User Identity sub-item (PS3.7
D.3.3.7.1) in its A-ASSOCIATE-RQ. ``pynetdicom2`` already decodes the
user-data sub-items, this module only provides the lookup so components
implementing authentication do not reimplement it.

The module also owns the shared :class:`IdentityPolicy` vocabulary: the
per-device identity requirement consumed by device registries and
authentication components alike.
"""
import enum

from pynetdicom2 import pdu
from pynetdicom2.userdataitems import UserIdentityNegotiationSubItem


class IdentityPolicy(str, enum.Enum):
    """Identity requirement of a device for incoming associations."""

    #: No user identity required
    NONE = 'none'

    #: User identity required; username must exist, password is optional
    USERNAME = 'username'

    #: Username/password identity (type 2) with a valid password required
    PASSWORD = 'password'


def get_user_identity(
        assoc: pdu.AAssociateRqPDU
) -> UserIdentityNegotiationSubItem | None:
    """Returns the User Identity sub-item of an incoming association request.

    :param assoc: association request parameters
    :type assoc: pdu.AAssociateRqPDU
    :return: the User Identity sub-item, or None when the request carries
             none
    :rtype: UserIdentityNegotiationSubItem or None
    """
    for item in assoc.variable_items:
        if not isinstance(item, pdu.UserInformationItem):
            continue
        for sub_item in item.user_data:
            if isinstance(sub_item, UserIdentityNegotiationSubItem):
                return sub_item
    return None
