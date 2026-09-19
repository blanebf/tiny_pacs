Web administration: ``tiny-pacs-admin-web``
===========================================

``tiny-pacs-admin-web`` serves a small browser-based administration
console from inside the running server process: a dashboard, device
management (including a C-ECHO test), user management and a read-only
archive browser — without shell access or YAML editing. It is the GUI
counterpart of the ``devices`` and ``users`` CLI subcommands, and every
action it performs travels over the event bus as a core event, so the
console works with *whichever* extension provides the listeners and
mutations are audited exactly like CLI-driven ones.

The extension contributes:

* ``AdminWeb`` — the component providing the console as a plain WSGI
  application (bottle) to the **core shared ``HttpServer`` component**
  through :class:`~tiny_pacs.events.HttpAppsRegistry`, mounted on the
  root prefix ``/``. It runs no server of its own and owns the single
  ``WebGrant`` table of console grants;
* the ``web-admin`` CLI subcommand (``grant``/``revoke``/``list``)
  managing that table offline.

It imports the core only: login verifies credentials through
:class:`~tiny_pacs.events.UserVerify` (served, for example, by the
``Users`` component of ``tiny-pacs-identity``), device and user pages
consume the core CRUD events (served, for example, by ``DeviceStore``
from ``tiny-pacs-admin``), the archive browser consumes the core archive
query events (answered by the built-in ``PACS`` component), and the
dashboard reads :class:`~tiny_pacs.events.SchemaVersions`,
:class:`~tiny_pacs.events.TableCounts`,
:class:`~tiny_pacs.events.HttpMountsQuery` and
:class:`~tiny_pacs.events.StorageStatsQuery`. A feature whose listeners
are missing degrades to a "not available" page — never to a stack trace.

One HTTP server for everything
------------------------------

The process runs a **single** HTTP server: the core ``HttpServer``
component binds one port, runs one waitress worker pool and dispatches
requests to every contributed WSGI application by longest URL prefix
(the console is the root application on ``/``; other front-ends such as
the planned DICOMweb mount on their own prefixes). All bind-level policy
lives there: the loopback default, the ``allow_remote`` validation, the
loud WARNING for remote binds, port-conflict degradation and the
``TINY_PACS_HEADLESS`` guard. The dashboard's *HTTP mounts* row (and
:class:`~tiny_pacs.events.HttpMountsQuery`) show what is mounted.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-admin-web

Dependencies are the core plus ``bottle`` (single-module pure-Python, no
build step, no CDNs — the UI's CSS and JavaScript ship inside the
package). The WSGI server itself (waitress) belongs to the core HTTP
extra: ``pip install tiny_pacs[web-admin]`` pulls the extension *and*
``tiny_pacs[http]`` in one go. Installing the extension never changes
server behaviour: the component stays disabled until enabled in the
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
instantiate the ``Database`` component only — no HTTP port is ever bound
during CLI runs (the ``TINY_PACS_HEADLESS`` environment guard
additionally suppresses serving for any headless run that does construct
the components: ``HttpServer`` binds nothing and ``AdminWeb`` registers
no app while it is set).

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

The HTTP transport is configured once on the core ``HttpServer``;
``AdminWeb`` keeps only application-level settings:

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
      HttpServer:               # core: shared by every HTTP front-end
        on: true
        host: 127.0.0.1         # loopback behind a TLS proxy (default)
        port: 11113
        allow_remote: false     # refuses non-loopback binds unless true
        threads: 16             # the single waitress worker pool
      AdminWeb:
        on: true
        secure_cookie: false    # true behind the TLS-terminating proxy
        session_ttl: 3600       # idle timeout of a session, seconds
        max_sessions: 100       # LRU eviction beyond this
        max_login_failures: 5   # before the exponential backoff starts
        expose_users_to_viewer: false

Behaviour worth knowing:

* ``HttpServer.host`` must be a loopback address (``127.0.0.1``, ``::1``,
  ``localhost``) unless ``allow_remote: true`` — the refusal happens at
  configuration validation. With ``allow_remote`` the startup logs a
  loud WARNING: waitress has **no TLS support**, so a non-loopback bind
  without a proxy in front exposes login passwords in clear text.
* ``HttpServer.port: 0`` binds an ephemeral port; the actually bound
  port appears in the startup log (useful for tests). Without
  ``waitress`` installed the server logs a WARNING and stays disabled —
  install ``tiny_pacs[http]``.
* An ``HttpServer`` with **zero** contributed applications binds nothing
  and stays dormant, so enabling it is harmless on installs without any
  web front-end.
* No component answering ``UserVerify`` → login would be impossible, so
  the console warns and registers no app on the shared server; the
  shared server (and every other mount) and the DICOM AE are never
  affected. The same holds for an HTTP port that cannot be bound
  (logged as CRITICAL by ``HttpServer``): the AE keeps running.
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
* Request logging records method, path, status and duration only — never
  query strings or form bodies.

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
       row counts, storage headlines, HTTP mounts, feature availability
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
   * - ``/archive``
     - patients table with a search form mapped 1:1 onto the core
       ``ArchiveFilter`` (patient ID/name/birth date, accession number,
       study date range, modality and the UID fields) plus pagination
   * - ``/archive/studies/{patient_id}``, ``/archive/series/{study_uid}``,
       ``/archive/instances/{series_uid}``
     - drill-down levels with the UID filters pre-filled from the path

The archive browser
-------------------

The archive browser is **read-only** for both roles (there is no POST
endpoint in the section; DICOM-level deletion lands together with the
access-control work). It renders the rows of the core archive query
events answered by the ``PACS`` component — no file I/O and no worker
threads involved:

* the search form submits with GET; its fields map 1:1 onto
  ``ArchiveFilter``. Dates accept ``YYYY-MM-DD`` (what the date inputs
  submit) and ``YYYYMMDD``; an invalid date re-renders the form with an
  error instead of querying. Filters naming a lower level also work on
  upper levels (e.g. a study date range narrows the *patients* table
  through the studies they own);
* every table paginates with a fixed page size of 50 rows via
  ``limit``/``offset`` against the total match count; the previous/next
  links carry the current search context;
* rows link down patients → studies → series → instances; the pinned UID
  of a drill-down level travels in the URL path and renders as a
  read-only form field;
* values render through the auto-escaping templates like everywhere
  else, and identifiers are URL-quoted.

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

with the server configured as:

.. code-block:: yaml

    components:
      HttpServer:
        on: true
        host: 127.0.0.1     # keep the loopback bind
        port: 11113
      AdminWeb:
        on: true
        secure_cookie: true # cookies only travel over the HTTPS hop

One upstream serves every mounted front-end; additional HTTP front-ends
(e.g. DICOMweb at ``/dicomweb``) need no extra proxy server block unless
they should be exposed under their own hostname or path rules.

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
* The archive browser shows the database rows only: per-instance DICOM
  tag dumps, downloads and the storage maintenance page land in v0.2 of
  the console roadmap; the audit trail and the effective-configuration
  viewer in v0.3.
* YAML configuration is not editable through the UI by design.

See also
--------

* :doc:`extensions-admin` — the device registry and CLI the devices page
  builds on;
* :doc:`extensions-identity` — the user registry answering ``UserVerify``
  and serving the users page;
* :doc:`extensions-audit` — records the web-driven mutations through the
  same ``AuditRecord`` events as the CLI.
