"""tiny-pacs-identity: users and association authentication for tiny_pacs.

The extension plugs into tiny_pacs through entry points: the
:class:`~tiny_pacs_identity.users.Users` component keeps user accounts in
the database and the :class:`~tiny_pacs_identity.auth.UserIdentityAuth`
component authenticates incoming associations according to the calling
device's identity policy (DICOM User Identity negotiation, PS3.7
D.3.3.7). The ``users`` CLI subcommand manages the accounts offline.
"""
__version__ = '0.1.0'
