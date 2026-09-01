Installation
============

``tiny_pacs`` requires Python ``>= 3.10`` and works on any platform.

Installing from PyPI
--------------------

The core and the first-party extensions are published as separate
distributions (see :doc:`extensions`). Install the core alone or with the
convenience extras:

.. code-block:: bash

    pip install tiny_pacs
    pip install tiny_pacs[admin]          # admin CLI + DB device registry
    pip install tiny_pacs[identity]       # user management + association auth
    pip install tiny_pacs[admin,identity]

This installs the ``tiny_pacs`` package together with the ``tiny-pacs``
command-line script.

Installing from git
-------------------

To follow the development state, install straight from the git
repository:

.. code-block:: bash

    pip install git+https://github.com/blanebf/tiny_pacs.git

The extensions live in the same repository and are installed by
subdirectory; install the core first, so their ``tiny_pacs`` requirement
is already satisfied:

.. code-block:: bash

    pip install "git+https://github.com/blanebf/tiny_pacs.git#subdirectory=extensions/tiny_pacs_admin"
    pip install "git+https://github.com/blanebf/tiny_pacs.git#subdirectory=extensions/tiny_pacs_identity"

Optional extensions
-------------------

Extensions are optional, independently versioned packages that plug into
``tiny_pacs`` through entry points (see :doc:`extensions`). The core
provides convenience extras that pull them in:

.. code-block:: bash

    pip install tiny_pacs[admin]        # admin CLI + DB device registry
    pip install tiny_pacs[identity]     # user management + association auth
    pip install tiny_pacs[admin,identity]

An extension can also be installed directly (``pip install
tiny-pacs-admin``); the effect is identical. ``tiny-pacs-identity`` builds
on the device registry, so installing it pulls ``tiny-pacs-admin`` in
automatically. Installing an extension alone never changes server
behaviour — its components stay disabled until enabled in the
configuration, and its CLI subcommands are additive.

Verifying the installation
--------------------------

Both CLI commands should respond:

.. code-block:: bash

    tiny-pacs run --help
    tiny-pacs config --help

Optional: PostgreSQL driver
---------------------------

By default ``tiny_pacs`` uses SQLite and needs nothing extra. For the
PostgreSQL backend install a driver separately, for example:

.. code-block:: bash

    pip install psycopg2-binary

See :doc:`configuration` for the corresponding ``Database`` component
settings.

Development setup
-----------------

For development, use `Poetry <https://python-poetry.org/>`_:

.. code-block:: bash

    git clone git@github.com/blanebf/tiny_pacs.git
    cd tiny_pacs
    poetry install

``poetry install`` sets up a virtual environment with the package, its
dependencies and the development tools (``pytest``, ``ruff``, ``flake8``,
``mypy``). Run commands through ``poetry run``:

.. code-block:: bash

    poetry run tiny-pacs run
    poetry run pytest

Next steps
----------

Continue with :doc:`configuration` to learn how the server is configured,
or jump straight to :doc:`running` to start a server with the built-in
defaults.
