Writing a third-party extension
===============================

``tiny_pacs`` aims for an ecosystem of third-party extensions. Extensions
are discovered through Python entry points; the core treats a third-party
package exactly like its own first-party extensions. This guide covers the
extension contract: the entry-point groups, the compatibility promise, and
the rules an extension must follow.

Compatibility promise
---------------------

Within a core minor series (e.g. ``0.3.x``) the following never break:

* the entry-point group names and value formats;
* the extension-facing surface: :class:`tiny_pacs.component.Component`,
  :class:`tiny_pacs.component.ComponentConfig`,
  :func:`tiny_pacs.config.register_component`;
* everything documented in :mod:`tiny_pacs.events`,
  :mod:`tiny_pacs.schema` and the API reference;
* the CLI helpers :func:`tiny_pacs.__main__.add_common_arguments` and the
  headless admin runtime :func:`tiny_pacs.admin.admin_context`.

Breaking changes land in a new minor series and are announced in the
changelog together with a migration note. Extensions pin the core minor
series they were built and tested against:

.. code-block:: toml

    [tool.poetry.dependencies]
    tiny_pacs = ">=0.4,<0.5"

Entry point groups
------------------

Two groups are defined. The entry point **name** is user-visible in both
cases: for components it is the name used in the ``components`` section of
YAML configs, for the CLI it is the subcommand name.

.. list-table::
   :header-rows: 1

   * - Group
     - Entry point value
     - Used for
   * - ``tiny_pacs.components``
     - ``Name = module:ComponentClass``
     - auto-register components
   * - ``tiny_pacs.cli``
     - ``name = module:register``
     - add ``tiny-pacs`` subcommands

Any distribution installed into the same environment — first-party or
third-party — participates; no core configuration, code change or "plugin
list" is needed. Entry points are loaded lazily: an extension module is
only imported when the core runs discovery.

Entry-point syntax
------------------

The entry-point table key depends on the package-metadata style of the
declaring ``pyproject.toml``; a file must use the one matching its own
metadata table and never mix the two. This repository uses the legacy
Poetry style; PEP 621 projects declare the identical entry points under
``[project.entry-points."group"]`` — the installed metadata is the same, so
discovery sees no difference.

.. list-table::
   :header-rows: 1

   * - Metadata style in ``pyproject.toml``
     - Entry-point table
   * - Legacy Poetry ``[tool.poetry]`` (used by this repository)
     - ``[tool.poetry.plugins."group"]``
   * - PEP 621 ``[project]`` (Poetry ≥2.0, setuptools, hatch)
     - ``[project.entry-points."group"]``

Legacy Poetry example:

.. code-block:: toml

    # pacs_audit/pyproject.toml
    [tool.poetry.plugins."tiny_pacs.components"]
    StoreAudit = "pacs_audit.components:StoreAudit"

    [tool.poetry.plugins."tiny_pacs.cli"]
    audit = "pacs_audit.cli:register"

The same declarations from a PEP 621 package read:

.. code-block:: toml

    # pacs_audit/pyproject.toml using [project] metadata
    [project.entry-points."tiny_pacs.components"]
    StoreAudit = "pacs_audit.components:StoreAudit"

    [project.entry-points."tiny_pacs.cli"]
    audit = "pacs_audit.cli:register"

Component entry points
----------------------

The value is the import path of a
:class:`~tiny_pacs.component.Component` subclass. The component is
registered under the entry point name on the first ``Config``
construction, before any ``components`` section is validated — install the
package, add a matching YAML entry and it works (``tiny-pacs config``
generates the entry for you: generated configurations list every
extension component with its defaults, disabled):

.. code-block:: yaml

    components:
      StoreAudit:
        on: true
        audit_log: /var/log/tiny_pacs/audit.log

Precedence rules:

* an entry point named like a built-in (``Database``, ``Devices``,
  ``PACS``, ...) **replaces** that built-in — the documented override
  mechanism, logged at INFO;
* two distributions advertising the same name: the last-loaded wins, logged
  at WARNING. Pick unique names; prefix them with your project name when
  in doubt;
* a programmatic :func:`~tiny_pacs.config.register_component` call always
  wins over installed plugins, regardless of when it runs (embedder use
  case).

CLI entry points
----------------

Each value is a callable receiving a guarded facade over argparse's
subparsers action; it builds its own command tree and returns nothing.
Subcommands provide their execution function via
``parser.set_defaults(command_handler=...)``; ``tiny-pacs`` dispatches to
it after parsing.

Use :func:`tiny_pacs.__main__.add_common_arguments` for the shared
``-c/--config`` flags so third-party commands stay consistent with the
built-ins. Additional shared helpers keep subcommand output uniform:
:func:`tiny_pacs.__main__.add_action_parser` adds an action subparser with
the config flags and registers its handler,
:func:`~tiny_pacs.__main__.format_table` renders plain-text tables and
:func:`~tiny_pacs.__main__.fail` reports an error on stderr and exits with
status 1 (the shared error contract of every subcommand).

Registrations are isolated from the parser. Reserved subcommand names are
``run``, ``config`` (and argparse's ``help``); a plugin that tries to add
one — whether as its entry point name or inside ``register()`` — is logged
and the registration ignored. The same holds for a name another plugin (or
a built-in) already uses: the existing command wins and the duplicate is
logged with a WARNING. A broken ``register()`` is logged and reverted, so
it never leaves a half-registered subcommand behind. A plugin can never
break the ``tiny-pacs`` binary.

Discovery lifecycle
-------------------

Per-process order:

1. First ``Config()`` construction (CLI or embedder):
   ``tiny_pacs.components`` entry points are loaded once and registered.
2. Configuration sources are read; ``components`` sections are validated
   against the registry — third-party components are visible here.
3. The ``tiny-pacs`` CLI builds its parser. ``tiny_pacs.cli`` entry points
   are loaded and their ``register()`` called, except for invocations of
   the built-in ``run`` and ``config`` commands, which skip plugin
   discovery entirely and never import plugin modules.
4. ``Server`` instantiates enabled components in configuration order and
   starts the event bus.

Installing an extension never changes server behaviour by itself: a
component stays disabled until its YAML entry sets ``on: true``
(:attr:`~tiny_pacs.component.ComponentConfig.on` defaults to ``False``),
and CLI subcommands are additive. Installing a third-party plugin into a
running deployment is a no-op until the operator opts in.

Checklist for extension authors
-------------------------------

- Depend on ``tiny_pacs >=0.4,<0.5`` — the minor series built and tested
  against.
- Declare entry points in the groups you implement (table key per your
  metadata style — ``[tool.poetry.plugins."group"]`` for legacy Poetry
  ``[tool.poetry]`` metadata, ``[project.entry-points."group"]`` for PEP
  621 ``[project]`` metadata; never both in one file); component names are
  YAML keys visible to operators — keep them unique, prefix with your
  project name when in doubt; registering under a built-in name is an
  intentional override, nothing else.
- Import only documented ``tiny_pacs`` modules. ``pynetdicom2``/
  ``trolleybus`` and the core's private helpers are not part of the
  extension API.
- Components must construct cleanly against a valid config and fail
  gracefully otherwise: configuration errors surface through the pydantic
  config model; keep runtime failures inside lifecycle hooks, logged.
- Ship DB tables via the :class:`~tiny_pacs.events.Migrations` mechanism
  (see :doc:`migrations`) so upgrades of the plugin can migrate data.
- Test against Python 3.10–3.14 and both SQLite and PostgreSQL when you
  provide tables; mirror the core's pytest style.
- Document a ``components:`` config example; state TLS implications if the
  extension handles credentials.

The first-party ``tiny-pacs-admin`` and ``tiny-pacs-identity`` extensions
are the reference implementations of the contract.
