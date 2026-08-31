User identity: ``tiny-pacs-identity``
=====================================

``tiny-pacs-identity`` authenticates incoming associations. User accounts
live in the database; when an association arrives, the request is checked
against the calling device's identity policy using the DICOM User Identity
negotiation sub-item (PS3.7 D.3.3.7).

The extension contributes two components:

* ``Users`` — keeps user accounts (salted password hashes, an active flag
  and last-login bookkeeping) in the database;
* ``UserIdentityAuth`` — enforces the identity policies on incoming
  associations.

It depends on ``tiny-pacs-admin``: the identity policy is part of the
device record (see :doc:`extensions-admin`), so installing
``tiny-pacs-identity`` pulls the device registry in automatically.

Installation
------------

.. code-block:: bash

    pip install tiny-pacs-identity

or with the convenience extra:

.. code-block:: bash

    pip install tiny_pacs[identity]

The effect is identical; the extras are wired to the published extension
distributions, so until ``tiny-pacs-identity`` is published on PyPI,
install the extension directly from the repository.

Installing the extension never changes server behaviour: both components
stay disabled until enabled in the configuration.

Bootstrap: create users before enforcing identity
-------------------------------------------------

Users are managed offline with the ``users`` CLI subcommands against the
configured database — there is no implicit "first user" magic. Create the
accounts **before** starting a server configuration that requires
identity, otherwise every association needing that user is rejected:

.. code-block:: bash

    tiny-pacs users add alice
    Password:
    Repeat password:
    Added user alice

Passwords are prompted with ``getpass`` and are never accepted as command
line arguments. The other commands:

.. code-block:: bash

    tiny-pacs users list
    tiny-pacs users passwd alice
    tiny-pacs users remove alice
    tiny-pacs users set-active alice --inactive
    tiny-pacs users set-active alice --active

Deactivating a user locks the account out immediately without deleting
it; ``users list`` shows the accounts (never password hashes). Every
subcommand accepts the shared ``-c/--config`` flags and needs the same
persistent database configuration as the other admin commands — for
SQLite set ``db_name`` and ``mode: rwc`` on the ``Database`` component.

Configuration
-------------

Enable both components and tune the fallback policies:

.. code-block:: yaml

    components:
      Users:
        on: true
      UserIdentityAuth:
        on: true
        default_policy: none           # known devices without a policy
        unknown_device_policy: none    # none | username | password | reject

Policy resolution order
^^^^^^^^^^^^^^^^^^^^^^^

When an association arrives, the effective policy is resolved in this
order:

1. the calling device's own ``identity`` policy, when the device is known
   and carries one (set per device, see below);
2. ``default_policy``, when the device is known but has no policy of its
   own (e.g. a YAML device of the built-in ``Devices`` component that
   omits the field);
3. ``unknown_device_policy``, when no device registry knows the calling
   AE title — or ``reject`` refuses such associations outright.

A "require identity everywhere" deployment is expressed without any
extra switch: ``default_policy: password`` plus
``unknown_device_policy: reject``.

Per-device policy
^^^^^^^^^^^^^^^^^

The policy is part of the device record, configured either in YAML:

.. code-block:: yaml

    components:
      Devices:
        on: true
        devices:
          SOME_MODALITY:
            aet: SOME_MODALITY
            address: 10.0.0.5
            port: 104
            identity: password

or in the database through the admin CLI:

.. code-block:: bash

    tiny-pacs devices add MRI_01 --address 10.0.0.20 --identity password
    tiny-pacs devices update MRI_01 --identity username

Enforcement matrix
^^^^^^^^^^^^^^^^^^

The effective policy crossed with what the association presents:

.. list-table::
   :header-rows: 1

   * - Policy
     - No identity
     - Type 1 (username)
     - Type 2 (user + password)
     - Types 3–5 (Kerberos, SAML, JWT)
   * - ``none``
     - accept
     - accepted if the user exists and is active [1]_
     - accepted if the credentials are valid [1]_
     - rejected
   * - ``username``
     - reject
     - accepted if the user exists and is active
     - accepted if the password is valid
     - rejected
   * - ``password``
     - reject
     - reject
     - accepted if the password is valid
     - rejected
   * - ``reject``
     - reject
     - reject
     - reject
     - rejected

.. [1] Advisory verification in ``none`` mode: invalid credentials are
       logged but the association still succeeds.

A rejected association receives an A-ASSOCIATE-RJ and — because the
authentication runs before the device registries — is never auto-added as
a device either. Kerberos, SAML and JWT identities (types 3–5) are not
supported in this version and are rejected under every policy.

Auto-add defaults
^^^^^^^^^^^^^^^^^

When ``DeviceStore`` auto-add is enabled, unknown devices that pass
authentication are registered with its ``default_identity`` policy, which
then governs their next association. That default and the auth
component's ``unknown_device_policy`` therefore describe the same
devices and should be configured consistently; ``UserIdentityAuth`` logs
a warning at startup when they disagree. With auto-add disabled,
``unknown_device_policy`` alone governs.

.. code-block:: yaml

    components:
      DeviceStore:
        on: true
        default_identity: username     # policy of auto-added devices
      Devices:
        on: true
        auto_add: false
      UserIdentityAuth:
        on: true
        unknown_device_policy: username

Outgoing identity
^^^^^^^^^^^^^^^^^

tiny_pacs can also act as a requestor presenting identity: a device's
``username`` and ``password`` fields are forwarded to the association
layer for outgoing connections (C-MOVE sub-operations, Storage
Commitment, or a plain :class:`~tiny_pacs.client.DICOMClient`).

Security
--------

.. warning::

   User Identity negotiation transmits credentials inside A-ASSOCIATE.
   Enable the existing TLS support (``ae.tls``) whenever non-``none``
   identity policies are used:

   .. code-block:: yaml

       ae:
         tls:
           certificate: /etc/tiny_pacs/server.pem
           key: /etc/tiny_pacs/server.key

Additional guarantees:

* user passwords are stored salted and hashed (``scrypt``, with a
  ``PBKDF2`` fallback); verification compares in constant time;
* rejected associations never reveal through the response whether the
  user or the password was wrong;
* the CLI prompts for passwords and never logs or accepts them as
  arguments.

Reference implementation
------------------------

Like :doc:`extensions-admin`, ``tiny-pacs-identity`` doubles as a
reference implementation of the extension contract described in
:doc:`extensions`: components published through ``tiny_pacs.components``
(with their own tables and :class:`~tiny_pacs.events.Migrations`) and a
CLI subcommand published through ``tiny_pacs.cli``.
