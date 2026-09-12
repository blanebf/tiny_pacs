"""Command line subcommands.

Registers the ``audit`` subcommand of the ``tiny-pacs`` CLI through the
``tiny_pacs.cli`` entry point group. Every command takes the shared
``-c/--config`` flags and works offline against the configured database
through the core :func:`tiny_pacs.admin.admin_context` helper (which
enforces the SQLite file-mode restriction with a friendly error).
"""
import argparse
import datetime
import functools
import json
from typing import Any

from tiny_pacs import events as core_events
from tiny_pacs.__main__ import (
    CommandHandler,
    SubParsers,
    add_action_parser,
    fail,
    format_table,
)
from tiny_pacs.admin import AdminError, admin_context

from .audit import AuditCleanup, AuditCleanupOptions, AuditLog
from .models import AuditEventModel

#: Offline administration must never run the retention purge as a side
#: effect (a read-only ``query``/``stats`` should not delete data, and
#: ``cleanup`` deletes by its explicit ``--older-than`` threshold instead).
#: Appended as the last configuration source, this overrides the component
#: entry wholesale (see ``Config.update_config``) with retention disabled;
#: the command re-enables it with ``on: True`` via ``admin_context``.
_ADMIN_OVERRIDE: dict[str, Any] = {
    'components': {AuditLog.name(): {'retention_days': 0}}
}


def _admin_sources(config: list[str]) -> list[Any]:
    """Returns the admin configuration sources with retention disabled."""
    return [*config, _ADMIN_OVERRIDE]


def _command(handler: CommandHandler) -> CommandHandler:
    """Wraps a command handler with uniform error reporting."""
    @functools.wraps(handler)
    def wrapper(args: argparse.Namespace) -> None:
        try:
            handler(args)
        except (AdminError, ValueError) as error:
            fail(str(error))
    return wrapper


def _parse_timestamp(value: str) -> datetime.datetime:
    """Parses an ISO-8601 timestamp into an aware UTC datetime.

    A trailing ``Z`` is accepted; a naive timestamp is assumed to be UTC so
    it compares consistently with the stored (UTC) records.

    :param value: timestamp as entered on the command line
    :type value: str
    :return: timezone-aware timestamp
    :rtype: datetime.datetime
    :raises ValueError: raised for an unparsable timestamp
    """
    text = value.strip()
    if text.endswith(('z', 'Z')):
        text = text[:-1] + '+00:00'
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(
            f'invalid timestamp {value!r}; use ISO-8601, e.g. '
            f'2024-01-31T12:00:00'
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def register_audit(subparsers: SubParsers) -> None:
    """Registers the ``audit`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'audit', help='query and maintain the audit trail'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')

    query_parser = add_action_parser(
        actions, 'query', 'list audit records', audit_query_command
    )
    query_parser.add_argument(
        '--category', action='append', default=None, metavar='C',
        help='restrict to a record category (repeatable)'
    )
    query_parser.add_argument(
        '--event', action='append', default=None, metavar='E',
        help='restrict to a record event (repeatable)'
    )
    query_parser.add_argument(
        '--aet', default=None, metavar='AET',
        help='restrict to a device AE title'
    )
    query_parser.add_argument(
        '--user', default=None, metavar='U', help='restrict to a username'
    )
    query_parser.add_argument(
        '--since', default=None, metavar='TS',
        help='earliest record timestamp (ISO-8601), inclusive'
    )
    query_parser.add_argument(
        '--until', default=None, metavar='TS',
        help='latest record timestamp (ISO-8601), inclusive'
    )
    query_parser.add_argument(
        '--limit', type=int, default=100,
        help='maximum number of records (default: 100)'
    )
    query_parser.add_argument(
        '--offset', type=int, default=0,
        help='number of matching records to skip (for paging)'
    )
    query_parser.add_argument(
        '--format', choices=('table', 'json'), default='table',
        help='output format'
    )

    stats_parser = add_action_parser(
        actions, 'stats', 'show audit trail statistics', audit_stats_command
    )
    stats_parser.add_argument(
        '--format', choices=('table', 'json'), default='table',
        help='output format'
    )

    cleanup_parser = add_action_parser(
        actions, 'cleanup', 'delete old audit records', audit_cleanup_command
    )
    cleanup_parser.add_argument(
        '--older-than', type=int, required=True, metavar='DAYS',
        help='age threshold in days (minimum 1)'
    )


def _row_data(row: AuditEventModel) -> dict[str, Any]:
    """Serializes an audit record for output, parsing the details blob."""
    try:
        details = json.loads(row.details) if row.details else {}
    except ValueError:
        details = {}
    return {
        'timestamp': row.timestamp,
        'category': row.category,
        'event': row.event,
        'device_aet': row.device_aet,
        'username': row.username,
        'peer': row.peer,
        'status': row.status,
        'details': details
    }


@_command
def audit_query_command(args: argparse.Namespace) -> None:
    """Lists the audit records matching the given filters."""
    payload = core_events.AuditFilter(
        categories=args.category or None,
        events=args.event or None,
        device_aet=args.aet,
        username=args.user,
        since=_parse_timestamp(args.since) if args.since else None,
        until=_parse_timestamp(args.until) if args.until else None,
        limit=args.limit,
        offset=args.offset
    )
    with admin_context(_admin_sources(args.config),
                       [AuditLog.name()]) as (bus, _):
        rows = bus.send_one(core_events.AuditQuery, payload)
    data = [_row_data(row) for row in rows]
    if args.format == 'json':
        print(json.dumps(data, indent=2, default=str))
    else:
        print(format_table(
            ('TIMESTAMP', 'CATEGORY', 'EVENT', 'STATUS', 'AET', 'USER'),
            [(row['timestamp'], row['category'], row['event'], row['status'],
              row['device_aet'] or '-', row['username'] or '-')
             for row in data]
        ))


@_command
def audit_stats_command(args: argparse.Namespace) -> None:
    """Shows aggregate audit trail statistics."""
    with admin_context(_admin_sources(args.config),
                       [AuditLog.name()]) as (bus, _):
        stats = bus.send_one(core_events.AuditStats, None)
    if args.format == 'json':
        print(json.dumps(stats, indent=2, default=str))
        return
    print('Audit statistics:')
    print(format_table(
        ('FIELD', 'VALUE'),
        [
            ('records', stats.get('total', 0)),
            ('oldest', stats.get('oldest') or '-'),
            ('newest', stats.get('newest') or '-')
        ]
    ))
    for title, key in (('Per category', 'by_category'),
                       ('Per event', 'by_event'),
                       ('Per status', 'by_status')):
        group = stats.get(key) or {}
        if group:
            print()
            print(f'{title}:')
            print(format_table(('NAME', 'RECORDS'), sorted(group.items())))


@_command
def audit_cleanup_command(args: argparse.Namespace) -> None:
    """Deletes audit records older than the given age."""
    if args.older_than < 1:
        fail('--older-than must be at least 1 day')
    with admin_context(_admin_sources(args.config),
                       [AuditLog.name()]) as (bus, _):
        removed = bus.send_one(
            AuditCleanup, AuditCleanupOptions(older_than_days=args.older_than)
        )
    print(f'Removed {removed} audit record(s) older than '
          f'{args.older_than} day(s)')
