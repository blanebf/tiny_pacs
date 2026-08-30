"""Command line subcommands.

Registers the ``devices``, ``components`` and ``db`` subcommands of the
``tiny-pacs`` CLI through the ``tiny_pacs.cli`` entry point group. Every
command takes the shared ``-c/--config`` flags and works offline against
the configured database (see :mod:`tiny_pacs_admin.runtime`).
"""
import argparse
import functools
import json
import sys
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Protocol

import yaml  # type: ignore[import-untyped]
from tiny_pacs import client as core_client
from tiny_pacs import config as core_config
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events
from tiny_pacs.__main__ import add_common_arguments

from . import events as admin_events
from . import runtime
from .models import DeviceModel, IdentityPolicy
from .store import DeviceStore


class SubParsers(Protocol):
    """The subparsers facade passed to the registration callables."""

    def add_parser(self, name: str,
                   **kwargs: Any) -> argparse.ArgumentParser:
        """Adds a subcommand parser."""
        ...


CommandHandler = Callable[[argparse.Namespace], None]


def _fail(message: str) -> None:
    """Reports a command error and exits."""
    print(f'error: {message}', file=sys.stderr)
    raise SystemExit(1)


def _command(handler: CommandHandler) -> CommandHandler:
    """Wraps a command handler with uniform error reporting."""
    @functools.wraps(handler)
    def wrapper(args: argparse.Namespace) -> None:
        try:
            handler(args)
        except (runtime.AdminError, ValueError) as error:
            _fail(str(error))
    return wrapper


def _add_action_parser(
        actions: SubParsers, name: str, help_text: str,
        handler: CommandHandler
) -> argparse.ArgumentParser:
    """Adds an action subparser with the shared config flags."""
    action = actions.add_parser(name, help=help_text)
    add_common_arguments(action)
    action.set_defaults(command_handler=handler)
    return action


def _format_table(
        headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    """Renders rows as a plain-text table."""
    data = [[str(cell) for cell in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in data:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def line(cells: Sequence[str]) -> str:
        return '  '.join(
            cell.ljust(widths[index]) for index, cell in enumerate(cells)
        ).rstrip()

    lines = [line(headers), line(['-' * width for width in widths])]
    lines.extend(line(row) for row in data)
    return '\n'.join(lines)


def register_devices(subparsers: SubParsers) -> None:
    """Registers the ``devices`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'devices', help='manage remote DICOM devices'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')

    list_parser = _add_action_parser(
        actions, 'list', 'list the registered devices',
        devices_list_command
    )
    list_parser.add_argument(
        '--format', choices=('table', 'json', 'yaml'), default='table',
        help='output format'
    )

    add_parser = _add_action_parser(
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

    update_parser = _add_action_parser(
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

    remove_parser = _add_action_parser(
        actions, 'remove', 'remove a device', devices_remove_command
    )
    remove_parser.add_argument('aet', help='remote AE title')

    echo_parser = _add_action_parser(
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
    _add_action_parser(
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
    _add_action_parser(actions, 'info',
                       'show schema versions and table row counts',
                       db_info_command)


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
    with runtime.admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        rows = bus.send_one(admin_events.DeviceList, None)
    data = [_device_data(row) for row in rows]
    if args.format == 'json':
        print(json.dumps(data, indent=2, default=str))
    elif args.format == 'yaml':
        sys.stdout.write(yaml.safe_dump(data, sort_keys=False))
    else:
        print(_format_table(
            ('AET', 'ADDRESS', 'PORT', 'IDENTITY'),
            [(device['aet'], device['address'], device['port'],
              device['identity']) for device in data]
        ))


@_command
def devices_add_command(args: argparse.Namespace) -> None:
    """Registers a new device."""
    payload = _device_payload(args)
    payload['address'] = args.address
    with runtime.admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        row = bus.send_one(admin_events.DeviceAdd, payload)
    print(f'Added device {row.aet} ({row.address}:{row.port}, '
          f'identity: {row.identity})')


@_command
def devices_update_command(args: argparse.Namespace) -> None:
    """Updates an existing device."""
    payload = _device_payload(args)
    with runtime.admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        row = bus.send_one(admin_events.DeviceUpdate, payload)
    print(f'Updated device {row.aet} ({row.address}:{row.port}, '
          f'identity: {row.identity})')


@_command
def devices_remove_command(args: argparse.Namespace) -> None:
    """Removes a device."""
    with runtime.admin_context(args.config, [DeviceStore.name()]) as (
            bus, _):
        removed = bus.send_one(admin_events.DeviceRemove, args.aet)
    if not removed:
        _fail(f'Unknown device {args.aet}')
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
    with runtime.admin_context(args.config,
                               ['Devices', DeviceStore.name()]) as (bus, _):
        device = bus.send_any(core_events.DeviceByAE, args.aet)
    if device is None:
        _fail(f'Unknown device {args.aet}')
        return
    client = core_client.DICOMClient(local_aet, device)
    try:
        client.echo()
    except Exception as error:
        detail = str(error) or error.__class__.__name__
        _fail(f'C-ECHO to {args.aet} failed: {detail}')
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
    print(_format_table(('NAME', 'ORIGIN', 'STATE'), rows))


@_command
def db_info_command(args: argparse.Namespace) -> None:
    """Shows schema versions and table row counts."""
    with runtime.admin_context(args.config) as (_, database):
        versions = core_db.SchemaVersion.select().order_by(
            core_db.SchemaVersion.component
        )
        db_obj = database.db
        assert db_obj is not None
        counts: list[tuple[str, int]] = []
        for table in sorted(db_obj.get_tables()):
            cursor = db_obj.execute_sql(
                f'SELECT COUNT(*) FROM "{table}"'
            )
            row = cursor.fetchone()
            assert row is not None
            counts.append((table, int(row[0])))
    print('Schema versions:')
    rows = [(version.component, version.version) for version in versions]
    if rows:
        print(_format_table(('COMPONENT', 'VERSION'), rows))
    else:
        print('  (no schema versions recorded)')
    print()
    print('Row counts:')
    print(_format_table(('TABLE', 'ROWS'), counts))
