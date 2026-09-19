"""Command line subcommands.

Registers the ``web-admin`` subcommand of the ``tiny-pacs`` CLI through
the ``tiny_pacs.cli`` entry point group. The commands manage the console's
grant table — who may log into the web administration console and with
which role — and work offline against the configured database through the
core :func:`tiny_pacs.admin.admin_context` helper with the ``Database``
and ``AdminWeb`` components: the grant table is created, bound and its
schema version recorded through the regular ``Migrations`` mechanism,
exactly like during a server start. ``AdminWeb`` never contributes its
console app in a headless run because the ``TINY_PACS_HEADLESS``
environment guard is set for the duration of every command.

Granting works independently of the user registry: a grant only takes
effect for a username that also passes ``UserVerify`` at login (e.g. a
user of the ``tiny-pacs-identity`` extension).
"""
import argparse
import functools
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import trolleybus
from tiny_pacs.__main__ import (
    CommandHandler,
    SubParsers,
    add_action_parser,
    fail,
    format_table,
)
from tiny_pacs.admin import AdminError, admin_context
from tiny_pacs.db import Database
from tiny_pacs.http import HEADLESS_ENV

from .component import AdminWeb
from .models import ROLE_VIEWER, ROLES, WebGrantModel, _utcnow


def _command(handler: CommandHandler) -> CommandHandler:
    """Wraps a command handler with uniform error reporting."""
    @functools.wraps(handler)
    def wrapper(args: argparse.Namespace) -> None:
        try:
            handler(args)
        except (AdminError, ValueError) as error:
            fail(str(error))
    return wrapper


@contextmanager
def _grant_context(config: list[str]
                   ) -> Iterator[tuple[trolleybus.EventBus, Database]]:
    """Opens the headless admin context for grant-table maintenance.

    Runs with the ``Database`` and ``AdminWeb`` components, so the grant
    table is created, bound and its schema version recorded through the
    regular ``Migrations`` mechanism instead of a manual
    ``create_table`` that would leave a database the later server start
    detects as pre-existing tables without a recorded schema version.
    The ``TINY_PACS_HEADLESS`` guard is set for the duration of the
    command, so the ``AdminWeb`` component never contributes its console
    app during the headless run.
    """
    previous = os.environ.get(HEADLESS_ENV)
    os.environ[HEADLESS_ENV] = '1'
    try:
        with admin_context(config, [AdminWeb.name()]) as (bus, database):
            yield bus, database
    finally:
        if previous is None:
            os.environ.pop(HEADLESS_ENV, None)
        else:
            os.environ[HEADLESS_ENV] = previous


def register_web_admin(subparsers: SubParsers) -> None:
    """Registers the ``web-admin`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser(
        'web-admin', help='manage web administration console grants'
    )
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')

    grant_parser = add_action_parser(
        actions, 'grant',
        'grant console access to a user', grant_command
    )
    grant_parser.add_argument('username', help='login name of the user')
    grant_parser.add_argument(
        '--role', choices=ROLES, default=ROLE_VIEWER,
        help=f'console role (default: {ROLE_VIEWER})'
    )

    revoke_parser = add_action_parser(
        actions, 'revoke', 'revoke console access', revoke_command
    )
    revoke_parser.add_argument('username', help='login name of the user')

    list_parser = add_action_parser(
        actions, 'list', 'list the console grants', list_command
    )
    list_parser.add_argument(
        '--format', choices=('table', 'json'), default='table',
        help='output format'
    )


def _grant_data(row: WebGrantModel) -> dict[str, Any]:
    """Serializes a grant record for output."""
    return {
        'username': row.username,
        'role': row.role,
        'created': row.created
    }


@_command
def grant_command(args: argparse.Namespace) -> None:
    """Grants console access, creating or updating the grant."""
    username = (args.username or '').strip()
    if not username:
        fail('username must not be empty')
    if args.role not in ROLES:
        fail(f'role must be one of: {", ".join(ROLES)}')
    with _grant_context(args.config):
        row = WebGrantModel.get_or_none(WebGrantModel.username == username)
        if row is None:
            row = WebGrantModel.create(
                username=username, role=args.role, created=_utcnow()
            )
            print(f'Granted {args.role} console access to {username}')
        elif row.role != args.role:
            row.role = args.role
            row.save()
            print(f'Updated the console role of {username} to {args.role}')
        else:
            print(f'{username} already has {args.role} console access')


@_command
def revoke_command(args: argparse.Namespace) -> None:
    """Revokes console access."""
    with _grant_context(args.config):
        deleted = bool(
            WebGrantModel.delete()
            .where(WebGrantModel.username == args.username)
            .execute()
        )
    if not deleted:
        fail(f'Unknown grant {args.username}')
    print(f'Revoked web console access of {args.username}. Live sessions '
          f'of the user are dropped on their next request.')


@_command
def list_command(args: argparse.Namespace) -> None:
    """Lists the console grants."""
    with _grant_context(args.config):
        rows = list(WebGrantModel.select().order_by(WebGrantModel.username))
    data = [_grant_data(row) for row in rows]
    if args.format == 'json':
        print(json.dumps(data, indent=2, default=str))
    else:
        print(format_table(
            ('USERNAME', 'ROLE', 'CREATED'),
            [(grant['username'], grant['role'], grant['created'])
             for grant in data]
        ))
