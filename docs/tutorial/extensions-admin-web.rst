Web administration: ``tiny-pacs-admin-web``
===========================================

``tiny-pacs-admin-web`` serves a small browser-based administration
console from inside the running server process: a dashboard, device
management (including a C-ECHO test) and user management — without shell
access or YAML editing. It is the GUI counterpart of the ``devices`` and
``users`` CLI subcommands, and every action it performs travels over the
event bus as a core event, so the console works with *whichever*
extension provides the listeners and mutations are audited exactly like
CLI-driven ones.

The extension contributes:

* ``AdminWeb`` — the component binding an HTTP port (bottle application
  served by waitress in a daemon thread) and owning the single
  ``WebGrant`` table of console grants;
* the ``web-admin`` CLI subcommand (``grant``/``revoke``/``list``)
  managing that table offline.

It imports the core only: login verifies credentials through
:class:`~tiny_pacs.events.UserVerify` (served, for example, by the
``Users`` component of ``tiny-pacs-identity``), device and user pages
consume the core CRUD events (served, for example, by ``DeviceStore``
from ``tiny-pacs-admin``), and the dashboard reads
:class:`~tiny_pacs.events.SchemaVersions`,
:class:`~tiny_pacs.events.TableCounts` and
:class:`~tiny_pacs.events.StorageStatsQuery`. A feature whose listeners
are missing degrades to a "not available" page — never to a stack trace.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-admin-web

Dependencies are the core plus ``bottle`` and ``waitress`` (both small,
pure-Python, no build step, no CDNs — the UI's CSS and JavaScript ship
inside the package). Installing the extension never changes server
behaviour: the component stays disabled until enabled in the
configuration.

Bootstrap: users and grants
---------------------------

The console needs two things per operator:

1. a PACS user with a password — the regular user registry, e.g.
   ``tiny-pacs users add alice``;
2. a *console grant* — without it even valid credentials are refused:

.. code-block:: bash

    tiny-pacs web-admin grant alice --role admin
    tiny-pacs web-admin grant bob --role viewer
    tiny-pacs web-admin list
    tiny-pacs web-admin revoke bob

The commands run offline through the headless admin runtime against the
configured database, so SQLite deployments need a file-based database
(``db_name`` plus ``mode: rwc``), like every other admin command. They
instantiate the ``Database`` component only — the console never binds
its HTTP port during CLI runs (the ``TINY_PACS_HEADLESS`` environment
guard additionally suppresses binding for any headless run that does
construct the component).

Two roles exist:

.. list-table::
   :header-rows: 1

   * - Role
     - Rights
   * - ``admin``
     - every page and every mutation
   * - ``viewer``
     - read-only pages; every POST is rejected with 403 and mutation
       forms are not rendered

Grants can be changed while the server runs: the role is re-read from
the table on every request, and a revoked grant destroys the affected
session on its next click.

Configuration
-------------

.. code-block:: yaml

    components:
      Database:
        on: true
        db_name: pacs.db        # file-based; required for the CLI
        mode: rwc
      Users:
        on: true                # answers UserVerify (tiny-pacs-identity)
      DeviceStore:
        on: true                # answers the device CRUD events
      AdminWeb:
        on: true
        host: 127.0.0.1         # loopback behind a TLS proxy (default)
        port: 11113
        allow_remote: false     # refuses non-loopback binds unless true
        secure_cookie: false    # true behind the TLS-terminating proxy
        session_ttl: 3600       # idle timeout of a session, seconds
        max_sessions: 100       # LRU eviction beyond this
        max_login_failures: 5   # before the exponential backoff starts
        expose_users_to_viewer: false
        threads: 8              # waitress worker threads

Behaviour worth knowing:

* ``host`` must be a loopback address (``127.0.0.1``, ``::1``,
  ``localhost``) unless ``allow_remote: true`` — the refusal happens at
  configuration validation. With ``allow_remote`` the startup logs a
  loud WARNING: waitress has **no TLS support**, so a non-loopback bind
  without a proxy in front exposes login passwords in clear text.
* ``port: 0`` binds an ephemeral port; the actually bound port appears
  in the startup log (useful for tests).
* No component answering ``UserVerify`` → login would be impossible, so
  the console warns and stays disabled; the DICOM AE is never affected.
  The same holds for an HTTP port that cannot be bound (logged at
  CRITICAL): the AE keeps running.
* ``expose_users_to_viewer: false`` (the default) hides the users page
  from viewers entirely — usernames can be sensitive in hospital
  settings.
* ``secure_cookie: true`` adds the ``Secure`` flag to the session
  cookie. Enable it whenever the console is reached through HTTPS (i.e.
  behind the reverse proxy below); leave it off for plain-HTTP loopback
  experiments.

Sessions and security primitives
--------------------------------

* Login succeeds only for valid credentials **and** an existing grant;
  unknown user, wrong password, inactive user, missing grant and
  throttle lockout all render the *identical* generic error page.
* Sessions live in an in-memory, lock-guarded store — by design a
  single-process console: sessions are lost on restart and never shared
  between nodes. Tokens are ``secrets.token_urlsafe(32)``.
* The session cookie is ``HttpOnly``, ``SameSite=Strict``, ``Path=/``,
  ``Max-Age``-bounded and (with ``secure_cookie``) ``Secure``.
* Every state-changing request carries a per-session CSRF token — a
  hidden ``csrf_token`` form field for pages, the ``X-CSRF-Token``
  header for the JSON API — compared in constant time.
* Failed logins are counted per (username, peer); after
  ``max_login_failures`` an exponential backoff window (5 s, doubling,
  capped at 1 h) refuses attempts *before* any password proof runs.
* Changing a user's password through the console invalidates that
  user's other sessions in the process.
* Device outgoing passwords are never displayed: the list shows
  ``********`` and editing uses a dedicated "set new password" field, so
  the stored value never round-trips through the browser.
* Responses carry a ``self``-only Content-Security-Policy without inline
  scripts or styles, ``X-Content-Type-Options: nosniff``,
  ``Referrer-Policy: no-referrer``, ``X-Frame-Options: DENY`` and
  ``Cache-Control: no-store``. Error pages are generic; bottle's debug
  mode is forced off.
* Request logging records method, path, status and duration only —
  never query strings or form bodies.

The pages
---------

.. list-table::
   :header-rows: 1

   * - URL
     - Purpose
   * - ``/login``, ``POST /logout``
     - session handling (logout is a CSRF-protected form)
   * - ``/``
     - dashboard: component registry with origins, schema versions, DB
       row counts, storage headlines, feature availability
   * - ``/devices``
     - device list (identity policy, masked outgoing password)
   * - ``/devices/add``, ``/devices/{aet}/edit``
     - add/edit forms (address, port, identity policy, outgoing
       credentials)
   * - ``/devices/{aet}``
     - detail view with the delete confirmation and the C-ECHO button
       (``POST /api/echo/{aet}``, run in a worker thread with a bounded
       timeout)
   * - ``/users``
     - user list with console roles; add, password and active-state
       forms (admin only)

Deployment: TLS-terminating reverse proxy
-----------------------------------------

waitress serves plain HTTP only. The supported production deployment is
the loopback bind behind a TLS-terminating reverse proxy. An nginx
recipe:

.. code-block:: nginx

    server {
        listen 443 ssl;
        listen [::]:443 ssl;
        server_name pacs-admin.example.org;

        ssl_certificate     /etc/ssl/private/tiny_pacs.crt;
        ssl_certificate_key /etc/ssl/private/tiny_pacs.key;

        # Optional: restrict to the hospital VPN/admin subnet
        # allow 10.0.0.0/8; deny all;

        location / {
            proxy_pass http://127.0.0.1:11113;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_read_timeout 60s;
        }
    }

with the console configured as:

.. code-block:: yaml

    components:
      AdminWeb:
        on: true
        host: 127.0.0.1     # keep the loopback bind
        port: 11113
        secure_cookie: true # cookies only travel over the HTTPS hop

Notes for proxy operators:

* Login throttling and request logs see the proxy's address as the peer
  unless the deployment terminates TLS on the same host (the loopback
  case above). Per-client tuning behind multi-stage proxies is tracked
  for a later console release.
* The proxy should not buffer or rewrite the ``Set-Cookie`` and
  ``Content-Security-Policy`` headers; nothing else about the console is
  proxy-sensitive.

Limitations (v0.1)
------------------

* Single-process sessions (see above); restarts sign everybody out.
* The viewers' user access is page-level (``expose_users_to_viewer``);
  finer semantics are being confirmed for a later release.
* YAML configuration is not editable through the UI by design — the
  effective-configuration viewer lands in a later release together with
  the storage and archive pages (v0.2/v0.3 of the console roadmap).

See also
--------

* :doc:`extensions-admin` — the device registry and CLI the devices page
  builds on;
* :doc:`extensions-identity` — the user registry answering ``UserVerify``
  and serving the users page;
* :doc:`extensions-audit` — records the web-driven mutations through the
  same ``AuditRecord`` events as the CLI.
