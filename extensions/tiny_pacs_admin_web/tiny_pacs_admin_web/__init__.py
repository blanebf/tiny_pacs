"""tiny-pacs-admin-web: browser-based administration console.

The extension plugs into tiny_pacs through entry points: the
:class:`~tiny_pacs_admin_web.component.AdminWeb` component contributes a
small web administration console (login, dashboard, device and user
management, read-only archive browsing) to the core shared HTTP server
(:class:`tiny_pacs.http.HttpServer`) as a plain WSGI application — it
runs no server of its own — and the ``web-admin`` CLI subcommand manages
the console's own grant table offline.

It imports the core only: every administration action is a core event on
the bus (:class:`~tiny_pacs.events.UserVerify`, the device/user CRUD
events, the archive query events, ...), so whichever extension provides
the listeners — the first-party admin/identity packages or any future
replacement — is an installation concern, never an import concern.
"""
__version__ = '0.1.0'
