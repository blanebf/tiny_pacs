# Changelog

All notable changes to `tiny_pacs` are documented here. The project
follows semantic versioning.

**Compatibility promise (extension API):** within a minor series (e.g.
`0.3.x`) the stable extension surface never breaks: the entry-point group
names and value formats (`tiny_pacs.components`, `tiny_pacs.cli`),
`component.Component` / `ComponentConfig` / `config.register_component`,
everything in `tiny_pacs.events`, `tiny_pacs.schema` and the documented API
reference, and the CLI helpers `add_common_arguments` / `admin_context`.
Breaking changes land in a new minor series and are announced here together
with a migration note. Extensions pin the series they target, e.g.
`tiny_pacs >=0.3,<0.4`.

## 0.3.0

New plugin APIs: extensions are optional distributions discovered through
Python entry points; see the "Writing a third-party extension" tutorial page.

- Component discovery: `tiny_pacs.components` entry points are loaded
  lazily on the first `Config` construction (`config.load_component_plugins`).
  Broken entry points (including `SystemExit` during import) and entries
  that are not `Component` subclasses are logged and skipped. An entry
  point named like a built-in component replaces that built-in (INFO);
  duplicate names across distributions resolve to the last-loaded entry
  (WARNING); a programmatic `register_component` always wins over installed
  plugins, regardless of when it runs.
- Registry origin metadata: `config.COMPONENT_ORIGINS` /
  `config.get_component_origin` record whether each component is built-in,
  registered programmatically or provided by an entry point, including the
  providing distribution name.
- CLI discovery: `tiny_pacs.cli` entry points add `tiny-pacs` subcommands;
  `__main__.main` dispatches to the
  `parser.set_defaults(command_handler=...)` callable (a subcommand without
  one exits with an error instead of falling through to `run`). Plugins
  receive a guarded subparsers facade: reserved names (`run`, `config`,
  `help`) and names already taken by another plugin or a built-in cannot be
  added or replaced and are ignored with a WARNING; a broken `register()`
  is reverted so no partial subcommands remain. Invocations of the built-in
  `run`/`config` commands (and the legacy invocation) never import plugin
  modules; `run`/`config` behaviour is unchanged.
- `__main__.add_common_arguments(parser)` factored out of
  `add_run_arguments` for plugin CLI reuse (shared `-c/--config` flags).
- `identity.get_user_identity(assoc)` helper returning the DICOM User
  Identity sub-item (PS3.7 D.3.3.7.1) of an incoming association request,
  for extensions implementing association authentication.
- Fix: the default logging configuration sets `disable_existing_loggers:
  False`, so loggers created before the server applies `dictConfig` (config
  loader, plugin discovery) keep emitting.
- `events.DeviceConfigs`: new event answered by device registry components
  with their configured devices (`Devices` returns its in-memory
  configuration); lets database-backed registries import the
  YAML-configured devices.
- `tiny-pacs config show` dumps the effective configuration (defaults
  merged with the `-c/--config` sources); `tiny-pacs config` without the
  action keeps generating a fresh configuration.
- New first-party extension `tiny-pacs-admin` (0.1.0): a database-backed
  device registry (`DeviceStore` component, per-device identity policy,
  configurable auto-add defaults) and the `devices`, `components` and
  `db` CLI subcommands for offline administration. See the
  "Administration: tiny-pacs-admin" tutorial page.
- New first-party extension `tiny-pacs-identity` (0.1.0, depends on
  `tiny-pacs-admin`): user management and association authentication.
  The `Users` component keeps accounts in the database (salted scrypt
  password hashes, with a PBKDF2 fallback); the `UserIdentityAuth`
  component authenticates incoming associations via the DICOM User
  Identity sub-item according to the calling device's identity policy
  (`none`/`username`/`password`), with `default_policy` and
  `unknown_device_policy` (plus `reject`) fallbacks, advisory
  verification in `none` mode, an answered positive-response sub-item
  (PS3.7 D.3.3.7.3) and rejection above the device registries, so
  rejected associations are never auto-added. The `users` CLI subcommand
  (`list`/`add`/`passwd`/`remove`/`set-active`) manages accounts offline
  with `getpass` prompts. New `DeviceStore` event `AutoAddIdentity`
  reports the auto-add policy so `UserIdentityAuth` can warn about
  conflicting defaults at startup. See the "User identity:
  tiny-pacs-identity" tutorial page.
- `UserIdentityAuth` hardening: password verification timing is equalized
  for unknown and inactive users (one proof per attempt, so rejections do
  not reveal whether a username exists); when no `Users` component is
  enabled the authentication fails closed with a startup error instead of
  aborting associations with an event bus error. Password hashing follows
  the OWASP cost guidance (scrypt `n=2**14, r=8, p=5`, PBKDF2 fallback at
  600,000 iterations); `UserSetActive` requires a boolean `is_active` and
  every user event normalizes the username identically.
- Shared CLI helpers for subcommand extensions:
  `__main__.add_action_parser`, `__main__.format_table`, `__main__.fail`
  and the `SubParsers`/`CommandHandler` types — a single canonical error
  contract and table output for every `tiny-pacs` subcommand.
- Declared `[tool.poetry.extras]` `admin` and `identity`; the extension
  packages are wired into the extras once published.
