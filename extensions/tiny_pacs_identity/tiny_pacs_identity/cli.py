"""Command line subcommands.

Registers the ``users`` subcommand of the ``tiny-pacs`` CLI through the
``tiny_pacs.cli`` entry point group. Every command takes the shared
``-c/--config`` flags and works offline against the configured database.
Passwords are prompted with :mod:`getpass` and are never accepted as
command line arguments.
"""
import argparse
import functools
import getpass
import json
from typing import Any

from tiny_pacs.__main__ import (
    CommandHandler,
    SubParsers,
    add_action_parser,
    fail,
    format_table,
)
from tiny_pacs_admin import runtime

from . import events as identity_events
from .models import UserModel
from .users import Users


def _command(handler: CommandHandler) -> CommandHandler:
    """Wraps a command handler with uniform error reporting."""
    @functools.wraps(handler)
    def wrapper(args: argparse.Namespace) -> None:
        try:
            handler(args)
        except (runtime.AdminError, ValueError) as error:
            fail(str(error))
    return wrapper


def _prompt_password(confirm: bool = True) -> str:
    """Prompts for a password without echoing it.

    :param confirm: ask for the password twice and require a match,
                    defaults to True
    :type confirm: bool, optional
    :return: the entered password
    :rtype: str
    :raises ValueError: raised for an empty password or mismatched
                        confirmation
    """
    password = getpass.getpass('Password: ')
    if not password:
        raise ValueError('password must not be empty')
    if confirm and getpass.getpass('Repeat password: ') != password:
        raise ValueError('passwords do not match')
    return password


def register_users(subparsers: SubParsers) -> None:
    """Registers the ``users`` subcommand tree.

    :param subparsers: subparsers facade provided by the CLI
    """
    parser = subparsers.add_parser('users', help='manage PACS users')
    actions = parser.add_subparsers(dest='action', required=True,
                                    metavar='ACTION')

    list_parser = add_action_parser(
        actions, 'list', 'list the registered users', users_list_command
    )
    list_parser.add_argument(
        '--format', choices=('table', 'json'), default='table',
        help='output format'
    )

    add_parser = add_action_parser(
        actions, 'add', 'register a new user', users_add_command
    )
    add_parser.add_argument('username', help='login name of the user')

    passwd_parser = add_action_parser(
        actions, 'passwd', 'change the password of a user',
        users_passwd_command
    )
    passwd_parser.add_argument('username', help='login name of the user')

    remove_parser = add_action_parser(
        actions, 'remove', 'remove a user', users_remove_command
    )
    remove_parser.add_argument('username', help='login name of the user')

    active_parser = add_action_parser(
        actions, 'set-active', 'activate or deactivate a user',
        users_set_active_command
    )
    active_parser.add_argument('username', help='login name of the user')
    state = active_parser.add_mutually_exclusive_group(required=True)
    state.add_argument('--active', dest='active', action='store_true',
                       help='activate the user')
    state.add_argument('--inactive', dest='active', action='store_false',
                       help='deactivate the user')


def _user_data(row: UserModel) -> dict[str, Any]:
    """Serializes a user record for output.

    The password hash is never included in the output.
    """
    return {
        'username': row.username,
        'is_active': row.is_active,
        'created': row.created,
        'last_login': row.last_login
    }


@_command
def users_list_command(args: argparse.Namespace) -> None:
    """Lists the registered users."""
    with runtime.admin_context(args.config, [Users.name()]) as (bus, _):
        rows = bus.send_one(identity_events.UserList, None)
    data = [_user_data(row) for row in rows]
    if args.format == 'json':
        print(json.dumps(data, indent=2, default=str))
    else:
        print(format_table(
            ('USERNAME', 'ACTIVE', 'CREATED', 'LAST LOGIN'),
            [(user['username'], user['is_active'], user['created'],
              user['last_login']) for user in data]
        ))


@_command
def users_add_command(args: argparse.Namespace) -> None:
    """Registers a new user; the password is prompted."""
    password = _prompt_password()
    with runtime.admin_context(args.config, [Users.name()]) as (bus, _):
        row = bus.send_one(identity_events.UserAdd, {
            'username': args.username, 'password': password
        })
    print(f'Added user {row.username}')


@_command
def users_passwd_command(args: argparse.Namespace) -> None:
    """Changes the password of a user; the password is prompted."""
    password = _prompt_password()
    with runtime.admin_context(args.config, [Users.name()]) as (bus, _):
        row = bus.send_one(identity_events.UserSetPassword, {
            'username': args.username, 'password': password
        })
    print(f'Changed password of user {row.username}')


@_command
def users_remove_command(args: argparse.Namespace) -> None:
    """Removes a user."""
    with runtime.admin_context(args.config, [Users.name()]) as (bus, _):
        removed = bus.send_one(identity_events.UserRemove, args.username)
    if not removed:
        fail(f'Unknown user {args.username}')
    print(f'Removed user {args.username}')


@_command
def users_set_active_command(args: argparse.Namespace) -> None:
    """Activates or deactivates a user."""
    with runtime.admin_context(args.config, [Users.name()]) as (bus, _):
        row = bus.send_one(identity_events.UserSetActive, {
            'username': args.username, 'is_active': args.active
        })
    state = 'active' if row.is_active else 'inactive'
    print(f'User {row.username} is now {state}')
