Installation
============

``tiny_pacs`` requires Python ``>= 3.10`` and works on any platform.

Installing from git
-------------------

The package is not on PyPI yet, so install it straight from the git
repository:

.. code-block:: bash

    pip install git+https://github.com/blanebf/tiny_pacs.git

This installs the ``tiny_pacs`` package together with the ``tiny-pacs``
command-line script.

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
