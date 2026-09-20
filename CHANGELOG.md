# Changelog

All notable changes to `tiny_pacs` are documented here. The project
follows semantic versioning.

**Compatibility promise (extension API):** within a minor series (e.g.
`0.4.x`) the stable extension surface never breaks: the entry-point group
names and value formats (`tiny_pacs.components`, `tiny_pacs.cli`),
`component.Component` / `ComponentConfig` / `config.register_component`,
everything in `tiny_pacs.events`, `tiny_pacs.schema` and the documented API
reference, and the CLI helpers `add_common_arguments` / `admin_context`.
Breaking changes land in a new minor series and are announced here together
with a migration note. Extensions pin the series they target, e.g.
`tiny_pacs >=0.4,<0.5`.

## 0.4.0

*Version note: the `0.3.0` string was consumed on PyPI on 2026-09-01 by an
early snapshot that predates most of the entries below (its metadata ships
only the `admin`/`identity` extras and it has no `tiny_pacs.admin`,
archive-query events or HTTP server). Nothing from this section ever
reached PyPI as 0.3.0; all of it ships as 0.4.0.*

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
  configurable auto-add defaults) and the `devices`, `components`, `db`
  and `storage` CLI subcommands for offline administration. See the
  "Administration: tiny-pacs-admin" tutorial page.
- New first-party extension `tiny-pacs-identity` (0.1.0): user
  management and association authentication.
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
- Convenience extras: `pip install tiny_pacs[admin]` installs
  `tiny-pacs-admin`, `pip install tiny_pacs[identity]` installs
  `tiny-pacs-identity`, and `tiny_pacs[admin,identity]` installs both.
  When installing the core from the repository itself, the extras
  resolve against the bundled extension packages.
- Release automation: CI runs linting, type checks and tests per package
  (core and each extension across Python 3.10–3.14) plus an integration
  job that installs all three distributions from the repository and
  verifies the entry-point discovery end to end. The publish workflow
  builds and publishes the distribution matching the release tag
  (`<distribution-name>-<version>`, or a chosen set on demand) in
  dependency order via PyPI trusted publishing.
- Association context registry: new `tiny_pacs.assoc_context` module
  (`AssocContext` with a stable `correlation_id`, calling/called AE
  titles, peer address and the authenticated `username`). The AE opens
  the context on the accepting thread *before* broadcasting
  `events.Assoc` and closes it at association teardown;
  `assoc_context.current()` attributes work inside every listener of
  that association. The `Store`/`Find`/`Move`/`Get` payloads gain a
  `session` field carrying the context (default `None`, existing
  constructors unchanged).
- Association lifecycle events: `events.AssocRejected` (broadcast by the
  AE for an invalid called AE title and when an `Assoc` listener raises
  `AssociationRejectedError` — the rejection reason travels on the
  exception) and `events.AssocReleased` (broadcast through a new
  request-handler teardown hook on every association exit path:
  release, abort, timeout or error).
- `events.AuditRecord` — the cross-extension administrative audit event
  (category/event/device/username/status/details payload) emitted by
  mutating components: `DeviceStore` after device add/update/remove,
  `Users` after user add/passwd/remove/set-active and the storage
  components after applied cleanups. With no audit component installed
  the broadcast is a cheap no-op. Query vocabulary for a future audit
  component and the web admin app: `events.AuditFilter` plus the
  `AuditQuery`/`AuditCount`/`AuditStats` events.
- Shared CRUD vocabulary moved into the core (`tiny_pacs.events`): the
  device events `DeviceList`/`DeviceAdd`/`DeviceUpdate`/`DeviceRemove`
  and `AutoAddIdentity` (from `tiny_pacs_admin.events`) and the user
  events `UserByName`/`UserList`/`UserAdd`/`UserSetPassword`/
  `UserRemove`/`UserSetActive` (from `tiny_pacs_identity.events`), plus
  the new `events.UserVerify` (username+password — or username-only for
  DICOM identity type 1 — to the user record or None; the handler runs
  exactly one timing-equalized password proof and records `last_login`).
  The extension packages re-export nothing; `tiny-pacs-identity` no
  longer depends on `tiny-pacs-admin` and reads users exclusively
  through events.
- `identity.IdentityPolicy` moved from `tiny_pacs_admin.models` into the
  core (`tiny_pacs.identity`), where the admin and identity extensions
  and future identity-type validators import it from.
- Storage maintenance events answered by the storage components:
  `events.StorageStatsQuery` (`StorageStatsReport`: record totals by
  `is_stored`, oldest/newest `added`, per-SOP-class counts, and for
  file backends the directory, file count, total bytes and per-day
  folder sizes), `events.StorageVerifyQuery` (`StorageVerifyReport`:
  records with missing files, orphan files, stuck in-progress records)
  and `events.StorageCleanupCommand` (`StorageCleanupOptions` →
  `StorageCleanupReport`; dry run by default, `apply` for real
  deletion, `failed_older_than_days: 0` refused, every deletion
  containment-checked against the storage directory, orphan deletion
  gated by a grace window plus a record re-check so an in-flight
  C-STORE on a live server is never removed, applied cleanups
  broadcast an `AuditRecord`). Non-file backends answer gracefully with
  the DB-side sections only.
- `tiny-pacs-admin` gains the `storage` CLI subcommand group
  (`stats`/`verify`/`cleanup`): thin clients of the maintenance events
  run headless through `tiny_pacs.admin.admin_context` with the
  `Database` and the enabled storage components only (other components
  are never started against a possibly-live database). `stats` renders
  table/json/yaml and exits with status 2 when `--quota GB` is
  exceeded (monitoring-script friendly; non-positive or non-finite
  quotas are refused); `verify` reports missing/orphan/stuck items and
  maps `--delete-missing-records`/`--delete-orphans`/`--apply` onto
  `StorageCleanupOptions` (dry run by default; the destructive flags
  are refused without a file backend); `cleanup --older-than DAYS`
  refuses `0`, warns about recent store activity before an `--apply`
  and defaults to `--dry-run`. A configuration without any storage
  component fails with a friendly error and a non-zero exit status.
- Archive query events answered by the `PACS` component:
  `events.ArchiveFilter` plus `ArchivePatientQuery`/`ArchiveStudyQuery`/
  `ArchiveSeriesQuery`/`ArchiveInstanceQuery` returning
  `events.ArchiveItem` lists — identifiers, DB-level fields and a DICOM
  view (`tag → (VR, value)`) built from the PACS model mappings, with
  the total match count for pagination. Intended consumers: the web
  administration app and DICOMweb QIDO-RS.
- `events.StoreDataset`: the store pipeline now runs on decoded
  datasets. The AE broadcasts `Store` (session-attributed, for
  observers and future access control) and then — after decoding —
  `StoreDataset(ds, transfer_syntax, origin)`; the `PACS` component
  records the DB rows and broadcasts `StoreDone`/`StoreFailure`.
  Non-DIMSE sources (e.g. DICOMweb STOW-RS) broadcast `StoreDataset`
  directly with `origin='stow'`. `GetFilePayload` gains an optional
  `transfer_syntax` field for sources without a real presentation
  context; the storage components honour it.
- Pluggable AE services: `events.ServicesRegistry`/`events.ServiceHook`
  let extensions contribute SOP classes to the AE. The AE broadcasts
  `ServicesRegistry` after registering its built-in SCPs, adds the
  hooks' abstract syntaxes to its presentation contexts and routes
  C-FIND requests on them to the event class the hook declared
  (`on_find`) instead of the built-in `events.Find`. Hooks may only add
  services, never replace built-ins.
- Shared HTTP server: the new built-in `HttpServer` component
  (`tiny_pacs.http`) hosts every HTTP front-end of the process with one
  waitress server. It broadcasts the new
  `events.HttpAppsRegistry` after the bus has started, collects the
  contributed `events.HttpAppHook` applications and serves them through
  a dependency-free stdlib WSGI prefix dispatcher (longest-prefix match
  on path segments, `SCRIPT_NAME`/`PATH_INFO` rewrite, `/` as the
  catch-all; duplicate prefixes mount the first and log a WARNING). The
  new `events.HttpMountsQuery` introspection event reports the mounted
  `(name, prefix)` pairs (used for cross-front-end links). All
  bind-level policy lives in the component: loopback default with
  `allow_remote` validation at config load, a loud WARNING for allowed
  remote binds, port-conflict degradation (CRITICAL log, server
  disabled, DICOM AE keeps running), the `TINY_PACS_HEADLESS` guard,
  ephemeral `port: 0` binds and dormant behaviour when nothing is
  contributed. waitress is imported lazily and ships as the core extra
  `tiny_pacs[http]` (implied by `tiny_pacs[web-admin]`); without it the
  component warns and stays disabled. The dispatcher logs
  WARNING/CRITICAL only (unmatched paths are logged through `%r`, so
  percent-decoded control characters cannot forge log lines) —
  providers keep their own request logs. Hardening: the
  `TINY_PACS_HEADLESS` guard counts mere presence (fail-closed for the
  empty values orchestration tools produce), a loopback-accepted *name*
  is re-checked against its resolved addresses before binding (a
  `localhost` resolving non-loopback is refused with a CRITICAL line)
  and malformed provider answers (non-list, unusable prefix, missing
  app) are logged and skipped instead of breaking startup.
- Archive query filter semantics are uniform across the levels: filters
  naming a level below the queried one now restrict the upper levels
  too (e.g. a study date range, an accession number or a series/instance
  UID narrows a PATIENT-level query through the studies the patients
  own, exactly like the modality filter always did). Previously such
  filters were silently ignored on the levels above theirs.
- `admin_context` sets (and restores) the `TINY_PACS_HEADLESS` guard
  around the whole headless administration session, so even an unscoped
  CLI run that instantiates every enabled component (e.g. `tiny-pacs
  db info` against a config with `HttpServer: on`) never binds an HTTP
  port. Previously each CLI had to set the guard itself.
- Versioning: the core series is bumped to `0.4.0` (see the version
  note above) and the bundled extensions pin `tiny_pacs >=0.4,<0.5`.
  The already-published `tiny-pacs-admin`/`tiny-pacs-identity` `0.1.0`
  distributions pin `<0.4`, so they are re-released as `0.1.1` with the
  widened pin in the dependency-ordered release run.
- `tiny_pacs.admin`: the headless `admin_context` helper (and its
  `AdminError`) moved from `tiny_pacs_admin.runtime` into the core,
  where the extension contract already promised it; the admin and
  identity CLIs use the core helper.
- Database inspection events: `events.SchemaVersions` and
  `events.TableCounts`, answered by the `Database` component — the
  admin `db info` command no longer queries core tables directly.
- Storage `overwrite` option (shared by `FileStorage`,
  `InMemoryStorage` and `TempFileStorage` via `StorageConfig`,
  default `false`): a C-STORE for an instance that is already stored is
  refused with a failure C-STORE-RSP (`0xA700`) instead of overwriting
  it, and can be opted into replacing the stored instance. An
  overwrite is only destructive once it succeeded: the record is
  swapped onto the incoming copy while the stored one is kept, the
  replaced copy is removed on `StoreDone` and a failed replacement
  rolls the record back to it. Fix: such a duplicate store no longer
  aborts the association — the unique `sop_instance_uid` violation
  used to escape `get_file` on the DUL thread, where pynetdicom2 turns
  any exception into an A-ABORT. The storage components now resolve
  the duplicate inside `GetFile` (refuse through a discard buffer
  surfaced by the `Store` handler, or swap the record when `overwrite`
  is on), and a lost creation race against a parallel store of the
  same instance is likewise answered with the refusal status instead
  of raising.
- Fix: `config.dump_yaml` serialized the component entries against the
  `ComponentConfig` base schema, silently dropping every
  component-specific field (`FileStorage.storage_dir`, the new
  `overwrite`, `Database.driver`, …) from generated and saved
  configurations; each component is now dumped with its actual model.
- Fix: pooled DB connections no longer leak from worker threads
  (`playhouse.pool.MaxConnectionsExceeded` after a handful of incoming
  associations and web requests). peewee tracks connections per thread
  and the pool reclaims them only on an explicit close in that thread,
  but every DICOM association runs on fresh threads (the connection
  thread and pynetdicom2's DUL provider thread) and the waitress
  workers keep theirs for their lifetime — nothing ever released them,
  so the pool (default `max_conn: 20`) filled up permanently. The new
  `events.CloseConnection`, answered by the `Database` component,
  closes the calling thread's connection (a pooled one returns to the
  pool for reuse) and is sent at every worker boundary: association
  teardown on the connection thread, after the per-store `GetFile`
  call on the DUL thread and after each served HTTP request (a small
  WSGI wrapper around the dispatcher, releasing even on failed or
  aborted responses). Pooled SQLite connections are now opened with
  `check_same_thread=False` so a released connection can be reused by
  the next thread: the pool hands a connection to exactly one thread
  at a time, which makes the cross-thread reuse safe.
