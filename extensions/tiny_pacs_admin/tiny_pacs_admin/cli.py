"""Command line subcommands.

Registers the ``devices``, ``components``, ``db`` and ``storage``
subcommands of the ``tiny-pacs`` CLI through the ``tiny_pacs.cli`` entry
point group. Every command takes the shared ``-c/--config`` flags and
works offline against the configured database (see
:func:`tiny_pacs.admin.admin_context`).
"""
import argparse
import datetime
import functools
import json
import math
import sys
from dataclasses import asdict
from typing import Any

import yaml  # type: ignore[import-untyped]
from tiny_pacs import client as core_client
from tiny_pacs import config as core_config
from tiny_pacs import events as core_events
from tiny_pacs import storage as core_storage
from tiny_pacs.__main__ import (
    CommandHandler,
    SubParsers,
    add_action_parser,
    fail,
    format_table,
)
from tiny_pacs.admin import AdminError, admin_context
from tiny_pacs.identity import IdentityPolicy

from .models import DeviceModel
from .store import DeviceStore


def _command(handler: CommandHandler) -> CommandHandler:
    """Wraps a command handler with uniform error reporting."""
    @functools.wraps(handler)
    def wrapper(args: argparse.Namespace) -> None:
        try:
            handler(args)
        except (AdminError, ValueError) as error:
            fail(str(error))
    return wrapper


def register_devices(subparsers: SubParsers) -> None:
    """Registers the ``devices`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'devices', help='manage remote DICOM devices'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')

    list_parser = add_action_parser(
        actions, 'list', 'list the registered devices',
        devices_list_command
    )
    list_parser.add_argument(
        '--format', choices=('table', 'json', 'yaml'), default='table',
        help='output format'
    )

    add_parser = add_action_parser(
        actions, 'add', 'register a new device', devices_add_command
    )
    add_parser.add_argument('aet', help='remote AE title')
    add_parser.add_argument('--address', required=True,
                            help='remote IP address or host name')
    add_parser.add_argument('--port', type=int, default=None,
                            help='remote SCP TCP port (default: the '
                                 'DeviceStore default_port setting)')
    add_parser.add_argument(
        '--identity', default=None,
        choices=[policy.value for policy in IdentityPolicy],
        help='identity policy of the device (default: the DeviceStore '
             'default_identity setting)'
    )
    add_parser.add_argument('--user', default=None,
                            help='outgoing identity username')
    add_parser.add_argument('--password', default=None,
                            help='outgoing identity password')

    update_parser = add_action_parser(
        actions, 'update', 'update an existing device',
        devices_update_command
    )
    update_parser.add_argument('aet', help='remote AE title')
    update_parser.add_argument('--address', default=None,
                               help='remote IP address or host name')
    update_parser.add_argument('--port', type=int, default=None,
                               help='remote SCP TCP port')
    update_parser.add_argument(
        '--identity', default=None,
        choices=[policy.value for policy in IdentityPolicy],
        help='identity policy of the device'
    )
    update_parser.add_argument('--user', default=None,
                               help='outgoing identity username')
    update_parser.add_argument('--password', default=None,
                               help='outgoing identity password')

    remove_parser = add_action_parser(
        actions, 'remove', 'remove a device', devices_remove_command
    )
    remove_parser.add_argument('aet', help='remote AE title')

    echo_parser = add_action_parser(
        actions, 'echo', 'check device connectivity with C-ECHO',
        devices_echo_command
    )
    echo_parser.add_argument('aet', help='remote AE title')


def register_components(subparsers: SubParsers) -> None:
    """Registers the ``components`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'components', help='inspect the component registry'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')
    add_action_parser(
        actions, 'list',
        'list registered components with their origin and state',
        components_list_command
    )


def register_db(subparsers: SubParsers) -> None:
    """Registers the ``db`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'db', help='inspect the database'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')
    add_action_parser(actions, 'info',
                      'show schema versions and table row counts',
                      db_info_command)


def register_storage(subparsers: SubParsers) -> None:
    """Registers the ``storage`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'storage', help='storage usage, consistency and cleanup'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')

    stats_parser = add_action_parser(
        actions, 'stats', 'show storage usage statistics',
        storage_stats_command
    )
    stats_parser.add_argument(
        '--format', choices=('table', 'json', 'yaml'), default='table',
        help='output format'
    )
    stats_parser.add_argument(
        '--quota', type=float, default=None, metavar='GB',
        help='exit with status 2 when disk usage exceeds this quota '
             '(needs a file-backed storage component)'
    )

    verify_parser = add_action_parser(
        actions, 'verify',
        'check storage records against the files on disk',
        storage_verify_command
    )
    verify_parser.add_argument(
        '--format', choices=('table', 'json', 'yaml'), default='table',
        help='output format'
    )
    verify_parser.add_argument(
        '--delete-missing-records', action='store_true',
        help='delete records whose file is gone (needs --apply)'
    )
    verify_parser.add_argument(
        '--delete-orphans', action='store_true',
        help='delete files no record references (needs --apply)'
    )
    verify_parser.add_argument(
        '--apply', action='store_true',
        help='really delete; the default is a dry run'
    )

    cleanup_parser = add_action_parser(
        actions, 'cleanup',
        'remove stuck in-progress records and their files',
        storage_cleanup_command
    )
    cleanup_parser.add_argument(
        '--older-than', type=int, required=True, metavar='DAYS',
        help='age threshold in days (minimum 1)'
    )
    mode = cleanup_parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', dest='apply', action='store_false',
                      help='report what would be removed (default)')
    mode.add_argument('--apply', dest='apply', action='store_true',
                      help='really delete')
    cleanup_parser.set_defaults(apply=False)


def _device_data(row: DeviceModel) -> dict[str, Any]:
    """Serializes a device record for output.

    Outgoing credentials are never included in the output.
    """
    data: dict[str, Any] = {
        'aet': row.aet,
        'address': row.address,
        'port': row.port,
        'identity': row.identity,
        'username': row.username,
        'created': row.created,
        'updated': row.updated
    }
    data.update(dict(json.loads(row.extra or '{}')))
    return data


def _device_payload(args: argparse.Namespace) -> dict[str, Any]:
    """Builds a device event payload from parsed arguments."""
    payload: dict[str, Any] = {'aet': args.aet}
    if getattr(args, 'address', None) is not None:
        payload['address'] = args.address
    if getattr(args, 'port', None) is not None:
        payload['port'] = args.port
    if getattr(args, 'identity', None) is not None:
        payload['identity'] = args.identity
    if getattr(args, 'user', None) is not None:
        payload['username'] = args.user
    if getattr(args, 'password', None) is not None:
        payload['password'] = args.password
    return payload


@_command
def devices_list_command(args: argparse.Namespace) -> None:
    """Lists the registered devices."""
    with admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        rows = bus.send_one(core_events.DeviceList, None)
    data = [_device_data(row) for row in rows]
    if args.format == 'json':
        print(json.dumps(data, indent=2, default=str))
    elif args.format == 'yaml':
        sys.stdout.write(yaml.safe_dump(data, sort_keys=False))
    else:
        print(format_table(
            ('AET', 'ADDRESS', 'PORT', 'IDENTITY'),
            [(device['aet'], device['address'], device['port'],
              device['identity']) for device in data]
        ))


@_command
def devices_add_command(args: argparse.Namespace) -> None:
    """Registers a new device."""
    payload = _device_payload(args)
    payload['address'] = args.address
    with admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        row = bus.send_one(core_events.DeviceAdd, payload)
    print(f'Added device {row.aet} ({row.address}:{row.port}, '
          f'identity: {row.identity})')


@_command
def devices_update_command(args: argparse.Namespace) -> None:
    """Updates an existing device."""
    payload = _device_payload(args)
    with admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        row = bus.send_one(core_events.DeviceUpdate, payload)
    print(f'Updated device {row.aet} ({row.address}:{row.port}, '
          f'identity: {row.identity})')


@_command
def devices_remove_command(args: argparse.Namespace) -> None:
    """Removes a device."""
    with admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        removed = bus.send_one(core_events.DeviceRemove, args.aet)
    if not removed:
        fail(f'Unknown device {args.aet}')
    print(f'Removed device {args.aet}')


@_command
def devices_echo_command(args: argparse.Namespace) -> None:
    """Checks device connectivity with C-ECHO."""
    conf = core_config.Config()
    conf.update_config(args.config)
    ae_title = conf.ae.ae_title
    local_aet = ae_title[0] if isinstance(ae_title, list) else ae_title
    # Resolve the device like the server does: the DB-backed registry
    # first, the in-memory registry as a fallback.
    with admin_context(args.config,
                       ['Devices', DeviceStore.name()]) as (bus, _):
        device = bus.send_any(core_events.DeviceByAE, args.aet)
    if device is None:
        fail(f'Unknown device {args.aet}')
        return
    client = core_client.DICOMClient(local_aet, device)
    try:
        client.echo()
    except Exception as error:
        detail = str(error) or error.__class__.__name__
        fail(f'C-ECHO to {args.aet} failed: {detail}')
        return
    print(f'C-ECHO to {args.aet} succeeded')


@_command
def components_list_command(args: argparse.Namespace) -> None:
    """Lists the component registry with origin and state."""
    conf = core_config.Config()
    conf.update_config(args.config)
    rows: list[tuple[str, str, str]] = []
    for name in sorted(core_config.COMPONENT_REGISTRY):
        component_config = conf.components.get(name)
        state = ('enabled'
                 if component_config is not None and component_config.on
                 else 'disabled')
        rows.append((name, core_config.get_component_origin(name), state))
    print(format_table(('NAME', 'ORIGIN', 'STATE'), rows))


@_command
def db_info_command(args: argparse.Namespace) -> None:
    """Shows schema versions and table row counts."""
    with admin_context(args.config) as (bus, _):
        versions = bus.send_one(core_events.SchemaVersions, None)
        counts = bus.send_one(core_events.TableCounts, None)
    print('Schema versions:')
    rows = sorted(versions.items())
    if rows:
        print(format_table(('COMPONENT', 'VERSION'), rows))
    else:
        print('  (no schema versions recorded)')
    print()
    print('Row counts:')
    print(format_table(('TABLE', 'ROWS'), sorted(counts.items())))


#: Friendly error for configurations without any storage component
_NO_STORAGE = (
    'No storage component answers the maintenance events; enable a '
    'storage component (FileStorage, InMemoryStorage or TempFileStorage) '
    'in the configuration.'
)


def _require_storage(bus: Any, event: Any) -> None:
    """Fails with a friendly error when no storage component listens."""
    if not bus.has_listeners(event):
        fail(_NO_STORAGE)


def _storage_components(config_files: Any) -> list[str]:
    """Returns the enabled storage components of the configuration.

    The storage commands run the headless administration bus with the
    storage components only: other enabled components (device registries,
    user stores, ...) must not perform their startup work — importing
    YAML devices, registering services — against a database a live
    server may be using.

    :param config_files: configuration source(s) as accepted by
                         :func:`tiny_pacs.admin.admin_context`
    :return: names of the enabled ``StorageBase`` components
    :rtype: list[str]
    """
    conf = core_config.Config()
    conf.update_config(config_files)
    names = []
    for name, component_config in conf.components.items():
        if not component_config.on:
            continue
        factory = core_config.COMPONENT_REGISTRY.get(name)
        if factory is not None and issubclass(factory,
                                              core_storage.StorageBase):
            names.append(name)
    return names


def _storage_context(config_files: Any) -> Any:
    """Opens the headless admin context restricted to storage components."""
    return admin_context(config_files, _storage_components(config_files))


def _warn_recent_activity(
        bus: Any, older_than: int) -> None:
    """Warns when storage records hint at a running server.

    Best-effort live-server hint for destructive cleanups: records added
    within the ``--older-than`` window suggest the database is in active
    use, so the deletion may race a live server.

    :param bus: running administration event bus
    :param older_than: cleanup age threshold in days
    """
    if not bus.has_listeners(core_events.StorageStatsQuery):
        return
    stats = bus.send_one(core_events.StorageStatsQuery, None)
    newest = stats.newest
    if newest is None:
        return
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    if now - newest < datetime.timedelta(days=older_than):
        print(
            'warning: recent store activity detected (newest record '
            f'{newest.isoformat()}); the server may be live. Destructive '
            'commands are safest with the server stopped.',
            file=sys.stderr
        )


def _format_bytes(size: int) -> str:
    """Renders a byte count with a human-readable unit."""
    value = float(size)
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if value < 1024 or unit == 'GiB':
            if unit == 'B':
                return f'{int(value)} {unit}'
            return f'{value:.1f} {unit}'
        value /= 1024
    return f'{value:.1f} GiB'


def _render(data: Any, fmt: str) -> None:
    """Prints structured data as a JSON or YAML document.

    Values are normalized through JSON first so both formats render the
    same document (datetimes as ISO strings, not YAML timestamps).
    """
    data = json.loads(json.dumps(data, default=str))
    if fmt == 'json':
        print(json.dumps(data, indent=2))
    else:
        sys.stdout.write(yaml.safe_dump(data, sort_keys=False))


@_command
def storage_stats_command(args: argparse.Namespace) -> None:
    """Shows storage usage statistics."""
    if args.quota is not None and not (
            math.isfinite(args.quota) and args.quota > 0):
        fail('--quota must be a positive, finite number of gigabytes')
    with _storage_context(args.config) as (bus, _):
        _require_storage(bus, core_events.StorageStatsQuery)
        report = bus.send_one(core_events.StorageStatsQuery, None)
        if args.quota is not None and report.file_bytes is None:
            fail('--quota needs a file-backed storage component (e.g. '
                 'FileStorage); the configured backend has no storage '
                 'directory')

    if args.format in ('json', 'yaml'):
        _render(asdict(report), args.format)
    else:
        print('Storage statistics:')
        print(format_table(
            ('FIELD', 'VALUE'),
            [
                ('records', report.records_total),
                ('stored', report.records_stored),
                ('failed', report.records_failed),
                ('oldest', report.oldest or '-'),
                ('newest', report.newest or '-')
            ]
        ))
        print()
        if report.storage_dir is None:
            print('Backend has no storage directory (non-file storage).')
        else:
            print(f'Storage directory: {report.storage_dir}')
            print(format_table(
                ('FIELD', 'VALUE'),
                [
                    ('files', report.file_count),
                    ('bytes', f'{report.file_bytes} '
                              f'({_format_bytes(report.file_bytes or 0)})')
                ]
            ))
            if report.per_sop_class:
                print()
                print('Records per SOP Class:')
                print(format_table(
                    ('SOP CLASS', 'RECORDS'),
                    sorted(report.per_sop_class.items())
                ))
            if report.per_day_bytes:
                print()
                print('Bytes per day folder:')
                print(format_table(
                    ('DAY', 'BYTES'),
                    sorted(report.per_day_bytes.items())
                ))

    if args.quota is not None and report.file_bytes is not None:
        quota_bytes = args.quota * 1024 ** 3
        if report.file_bytes > quota_bytes:
            print(
                f'error: storage usage {_format_bytes(report.file_bytes)} '
                f'exceeds the quota of {args.quota} GB',
                file=sys.stderr
            )
            raise SystemExit(2)


@_command
def storage_verify_command(args: argparse.Namespace) -> None:
    """Checks storage records against the files on disk."""
    destructive = args.delete_missing_records or args.delete_orphans
    with _storage_context(args.config) as (bus, _):
        _require_storage(bus, core_events.StorageVerifyQuery)
        report = bus.send_one(core_events.StorageVerifyQuery, None)
        if destructive and not report.file_backend:
            fail('--delete-missing-records/--delete-orphans need a '
                 'file-backed storage component (e.g. FileStorage); the '
                 'configured backend has no storage directory')
        cleanup = None
        if destructive:
            cleanup = bus.send_one(
                core_events.StorageCleanupCommand,
                core_events.StorageCleanupOptions(
                    delete_missing_records=args.delete_missing_records,
                    delete_orphans=args.delete_orphans,
                    apply=args.apply
                )
            )

    if args.format in ('json', 'yaml'):
        data = asdict(report)
        if cleanup is not None:
            data['cleanup'] = asdict(cleanup)
        _render(data, args.format)
    else:
        print('Storage verification:')
        print(f'  missing files:    {len(report.missing_files)}')
        print(f'  orphan files:     {len(report.orphan_files)}')
        print(f'  stuck records:    {len(report.stuck_records)}')
        if not report.file_backend:
            print('  (non-file backend: missing/orphan checks skipped)')
        for title, items in (
                ('Missing files', report.missing_files),
                ('Orphan files', report.orphan_files),
                ('Stuck records', report.stuck_records)):
            if items:
                print()
                print(f'{title}:')
                for item in items:
                    print(f'  {item}')
        if cleanup is not None:
            print()
            if args.apply:
                print(f'Removed {cleanup.records_removed} record(s) and '
                      f'{cleanup.files_removed} file(s)')
            else:
                print(f'Dry run: would remove {cleanup.would_remove_records}'
                      f' record(s) and {cleanup.would_remove_files} '
                      f'file(s); use --apply to really delete')
    if cleanup is not None and cleanup.errors:
        for error in cleanup.errors:
            print(f'error: {error}', file=sys.stderr)
        raise SystemExit(1)


@_command
def storage_cleanup_command(args: argparse.Namespace) -> None:
    """Removes stuck in-progress records and their files."""
    if args.older_than < 1:
        fail('--older-than must be at least 1 day: a running server may '
             'legitimately have in-progress stores (recommended minimum '
             'is 1 day)')
    with _storage_context(args.config) as (bus, _):
        _require_storage(bus, core_events.StorageCleanupCommand)
        if args.apply:
            _warn_recent_activity(bus, args.older_than)
        report = bus.send_one(
            core_events.StorageCleanupCommand,
            core_events.StorageCleanupOptions(
                failed_older_than_days=args.older_than,
                apply=args.apply
            )
        )

    if args.apply:
        print(f'Removed {report.records_removed} record(s) and '
              f'{report.files_removed} file(s)')
    else:
        print(f'Dry run: would remove {report.would_remove_records} '
              f'record(s) and {report.would_remove_files} file(s); '
              f'use --apply to really delete')
    if report.errors:
        for error in report.errors:
            print(f'error: {error}', file=sys.stderr)
        raise SystemExit(1)

