tiny_pacs_identity
==================

User management and association authentication for `tiny_pacs
<https://github.com/blanebf/tiny_pacs>`_: user accounts live in the
database and incoming associations are authenticated according to the
calling device's identity policy (DICOM User Identity negotiation, PS3.7
D.3.3.7).

What it provides
----------------

* The ``Users`` component — keeps user accounts (salted password hashes,
  an active flag, last-login bookkeeping) in the database.
* The ``UserIdentityAuth`` component — authenticates incoming
  associations against the calling device's per-device identity policy
  (``none``, ``username``, ``password``); known devices without a policy
  fall back to ``default_policy``, unknown devices to
  ``unknown_device_policy`` (``none``, ``username``, ``password`` or
  ``reject``).
* The ``tiny-pacs users list|add|passwd|remove|set-active`` subcommands
  for offline user management. Passwords are prompted and never accepted
  as command line arguments.

It depends on the core only: the per-device identity policy vocabulary
(``tiny_pacs.identity.IdentityPolicy``) and the user management
events live in ``tiny_pacs``, and device registries are reached through
the core device events.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-identity

or with the convenience extra:

.. code-block:: bash

    pip install tiny_pacs[identity]

Installing the extension never changes server behaviour: both components
stay disabled until enabled in the configuration.

Configuration
-------------

.. code-block:: yaml

    components:
      DeviceStore:
        on: true
        default_identity: none    # policy for auto-added devices
      Users:
        on: true
      UserIdentityAuth:
        on: true
        default_policy: none           # known devices without a policy
        unknown_device_policy: none    # none | username | password | reject

Set a per-device policy in YAML (``identity: password`` next to the
device) or with ``tiny-pacs devices add/update --identity``. A
"require identity everywhere" deployment is ``default_policy: password``
plus ``unknown_device_policy: reject``.

Create users with the CLI against the offline database before starting
the server:

.. code-block:: bash

    tiny-pacs users add alice
    tiny-pacs users list

Security note: User Identity negotiation transmits credentials inside
A-ASSOCIATE. Enable TLS (``ae.tls``) whenever non-``none`` identity
policies are used.

Documentation
-------------

See the `User identity tutorial
<https://tiny-pacs.readthedocs.io/en/latest/tutorial/extensions-identity.html>`_
in the tiny_pacs documentation.
