"""tiny-pacs-admin: admin CLI and DB-backed device registry for tiny_pacs.

The extension plugs into tiny_pacs through entry points: the
:class:`~tiny_pacs_admin.store.DeviceStore` component keeps remote DICOM
devices in the database (per-device identity policy, configurable
auto-add defaults) and the CLI subcommands ``devices``, ``components``
and ``db`` administer the server offline.
"""
__version__ = '0.1.0'
