tiny_pacs_admin_web
===================

Browser-based administration console for `tiny_pacs
<https://github.com/blanebf/tiny_pacs>`_: a small web UI served from
inside the server process for administering a live PACS — login,
dashboard, device and user management and read-only archive browsing —
without shell access or YAML editing.

What it provides
----------------

* The ``AdminWeb`` component — a `bottle <https://bottlepy.org/>`_
  application contributed to the core shared ``HttpServer`` component
  (answered through the ``HttpAppsRegistry`` event, mounted on the root
  prefix ``/``). It runs no server of its own: binding, the waitress
  worker pool and every bind-level policy live in the core server, and
  the documented deployment is a TLS-terminating reverse proxy in front
  of it.
* The ``tiny-pacs web-admin grant|revoke|list`` subcommands managing the
  console's own grant table offline through the core headless admin
  runtime.

Event-based only: every administration action is a core event on the bus
(``UserVerify``, the device/user CRUD events, the archive query events,
``GetClient``, the database and storage inspection events). The runtime
package imports the core only — whichever extension provides the
listeners (the first-party admin/identity packages or any future
replacement) is an installation concern, never an import concern.
Missing listeners degrade pages to "not available"; they never produce
stack traces.

Access control
--------------

A user may log in only with valid PACS credentials (verified through
``UserVerify``) **and** a console grant:

.. code-block:: bash

    tiny-pacs web-admin grant alice --role admin
    tiny-pacs web-admin grant bob --role viewer
    tiny-pacs web-admin list
    tiny-pacs web-admin revoke bob

``admin`` may use every page and mutation; ``viewer`` gets read-only
pages (every POST is rejected with 403; the archive browser is read-only
for both roles). Sessions live in memory (single-process by design) and
are protected by HttpOnly/SameSite=Strict cookies, per-session CSRF
tokens, login throttling with exponential backoff and a ``self``-only
Content-Security-Policy.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-admin-web

Requires ``tiny_pacs >=0.4,<0.5`` and ``bottle``. HTTP serving comes from
the core ``HttpServer`` component and its ``tiny_pacs[http]`` extra
(waitress), which ``tiny_pacs[web-admin]`` implies. Installing the
extension never changes server behaviour: the component stays disabled
until enabled in the configuration.

Configuration
-------------

.. code-block:: yaml

    components:
      HttpServer:              # core: shared by every HTTP front-end
        on: true
        host: 127.0.0.1        # reverse-proxy pattern; waitress has no TLS
        port: 11113
        allow_remote: false    # refuses non-loopback binds unless true
        threads: 16            # the single waitress worker pool
      AdminWeb:
        on: true
        secure_cookie: true    # mark cookies Secure behind the TLS proxy
        session_ttl: 3600      # session idle timeout, seconds
        max_sessions: 100
        max_login_failures: 5  # before the exponential backoff starts
        expose_users_to_viewer: false

The console needs a user registry answering ``UserVerify`` (e.g. the
``Users`` component of ``tiny-pacs-identity``); without one the component
warns and mounts no console app — the shared server and the DICOM AE keep
running. A port that cannot be bound is logged as CRITICAL by the core
``HttpServer`` and disables only the HTTP server, never the AE.

Documentation
-------------

See the `Web administration tutorial
<https://tiny-pacs.readthedocs.io/en/latest/tutorial/extensions-admin-web.html>`_
in the tiny_pacs documentation, including the TLS-terminating
reverse-proxy recipe.
