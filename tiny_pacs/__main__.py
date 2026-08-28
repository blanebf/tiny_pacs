"""Command line interface of Tiny PACS."""
import argparse
import logging
import sys
from importlib.metadata import entry_points
from typing import Any

from . import config, interactive, server

#: Entry point group advertising CLI subcommands. Every distribution
#: installed into the environment may declare ``name = module:register``
#: entries here; the entry point name becomes the ``tiny-pacs`` subcommand
#: name and the loaded callable receives the subparsers action.
CLI_GROUP = 'tiny_pacs.cli'

#: Subcommand names that plugin registrations must not use
RESERVED_COMMAND_NAMES = frozenset(('run', 'config', 'help'))


def main() -> None:
    """Entry point of the ``tiny-pacs`` command.

    Subcommands registered through the :data:`CLI_GROUP` entry point group
    provide their execution function via
    ``parser.set_defaults(command_handler=...)``; when present, that
    handler receives the parsed arguments.
    """
    args = parse_args()
    handler = getattr(args, 'command_handler', None)
    if handler is not None:
        handler(args)
    elif args.command == 'config':
        config_command(args)
    elif args.command in (None, 'run'):
        run_command(args)
    else:
        # A plugin subcommand without a command_handler must never fall
        # through to the built-in commands
        sys.exit(f'tiny-pacs: subcommand {args.command!r} registered no '
                 'command_handler')


def run_command(args: argparse.Namespace) -> None:
    """Runs the Tiny PACS server.

    :param args: parsed command line arguments
    :type args: argparse.Namespace
    """
    pacs_conf = config.Config()
    pacs_conf.update_config(args.config)
    if args.aet:
        pacs_conf.ae.ae_title = [args.aet]
    if args.port:
        pacs_conf.ae.port = args.port
    if args.interactive:
        front = interactive.TerminalFront()
        _config, run_server = front.run()
        pacs_conf.update_config(_config)
        if not run_server:
            return
    srv = server.Server(pacs_conf)
    srv.start_with_block()


def config_command(args: argparse.Namespace) -> None:
    """Generates a YAML configuration file.

    Without ``--interactive`` the effective default configuration is
    written; with ``--interactive`` every configuration value is asked
    on the terminal first.

    :param args: parsed ``config`` command arguments
    """
    conf = config.Config()
    if args.interactive:
        front = interactive.TerminalFront()
        conf.update_config(front.run_questionnairies())
    if args.output:
        config.write_yaml(conf, args.output)
        print(f'Configuration saved to {args.output}')
    else:
        sys.stdout.write(config.dump_yaml(conf))


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the ``-c/--config`` arguments shared by all commands.

    Subcommands registered through the :data:`CLI_GROUP` entry point group
    should call this helper for their parsers so configuration handling
    stays consistent with the built-in commands.

    :param parser: parser the arguments are added to
    :type parser: argparse.ArgumentParser
    """
    parser.add_argument('-c', '--config', default=[], nargs='*',
                        help='Tiny PACS configuration')


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds arguments of the ``run`` command to the parser.

    :param parser: parser the arguments are added to
    :type parser: argparse.ArgumentParser
    """
    add_common_arguments(parser)
    parser.add_argument('-a', '--aet', default=None,
                        help='Override Tiny PACS AE Title configuration')
    parser.add_argument('-p', '--port', default=None, type=int,
                        help='Override Tiny PACS port configuration')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Provide configuration values interactively')


def build_parser(load_plugins: bool = True) -> argparse.ArgumentParser:
    """Builds the command line parser with all subcommands.

    Built-in subcommands are added first, then additional subcommands are
    discovered from the :data:`CLI_GROUP` entry point group.

    :param load_plugins: discover plugin subcommands, defaults to True
    :type load_plugins: bool, optional
    :return: command line parser
    :rtype: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog='tiny-pacs',
        epilog='Running tiny-pacs without a command is equivalent to '
               '"tiny-pacs run".'
    )
    subparsers = parser.add_subparsers(dest='command', metavar='COMMAND')

    run_parser = subparsers.add_parser(
        'run', help='run the Tiny PACS server',
        description='Run the Tiny PACS server.'
    )
    add_run_arguments(run_parser)

    config_parser = subparsers.add_parser(
        'config', help='generate a configuration file',
        description='Generate a YAML configuration file. The default values '
                    'are written when not in interactive mode.'
    )
    config_parser.add_argument('-o', '--output', default=None,
                               help='Write the configuration to a file '
                                    'instead of printing it to stdout')
    config_parser.add_argument('-i', '--interactive', action='store_true',
                               help='Provide configuration values '
                                    'interactively')

    if load_plugins:
        load_cli_plugins(subparsers)

    return parser


class _PluginSubParsers:
    """Subparsers facade passed to plugin ``register`` callables.

    Guards the real subparsers action: registrations under reserved names
    (see :data:`RESERVED_COMMAND_NAMES`) or already taken names are logged
    and ignored instead of replacing existing subcommands. Any other
    attribute access is forwarded to the wrapped action.
    """

    def __init__(self, subparsers: argparse.Action,
                 origins: dict[str, str], entry_point: str,
                 logger: logging.Logger):
        self._subparsers = subparsers
        self._origins = origins
        self._entry_point = entry_point
        self._logger = logger

    def __getattr__(self, name: str) -> Any:
        return getattr(self._subparsers, name)

    def add_parser(self, name: str, **kwargs: Any
                   ) -> argparse.ArgumentParser:
        """Adds a subcommand unless the name is reserved or already taken.

        Rejected registrations receive a detached parser: the plugin can
        keep configuring it, but the subcommand stays unreachable.

        :param name: subcommand name
        :type name: str
        :return: parser of the subcommand
        :rtype: argparse.ArgumentParser
        """
        # Argparse keeps subparsers in private attributes; getattr keeps the
        # lookup type-checker friendly (``argparse.Action`` declares neither)
        taken = getattr(  # noqa: B009
            self._subparsers, '_name_parser_map')
        aliases = tuple(kwargs.get('aliases', ()))
        for alias in (name, *aliases):
            if alias in RESERVED_COMMAND_NAMES:
                self._logger.warning(
                    'CLI entry point %r tries to register reserved '
                    'subcommand %r, ignoring', self._entry_point, alias
                )
                return argparse.ArgumentParser(prog=name, add_help=False)
            if alias in taken:
                self._logger.warning(
                    'Subcommand %r: provided by both %s and CLI entry '
                    'point %r; %s wins', alias,
                    self._origins.get(alias, 'the built-in command'),
                    self._entry_point,
                    self._origins.get(alias, 'the built-in command')
                )
                return argparse.ArgumentParser(prog=name, add_help=False)
        add_parser = getattr(  # noqa: B009
            self._subparsers, 'add_parser')
        parser: argparse.ArgumentParser = add_parser(name, **kwargs)
        for alias in (name, *aliases):
            self._origins[alias] = self._entry_point
        return parser


def load_cli_plugins(subparsers: argparse.Action) -> None:
    """Registers subcommands discovered from entry points.

    Every entry point of the :data:`CLI_GROUP` group provides a callable
    that receives a subparsers facade and builds its own command tree.
    Subcommands set ``parser.set_defaults(command_handler=...)`` so
    :func:`main` can dispatch to them.

    Registrations are isolated: plugins can neither add nor replace
    reserved subcommands (see :data:`RESERVED_COMMAND_NAMES`) nor another
    plugin's subcommand — such registrations are logged and ignored — and
    a broken registration is reverted, leaving no partial subcommands
    behind. A plugin can never break the ``tiny-pacs`` binary.

    :param subparsers: subparsers action of the top-level parser
    :type subparsers: argparse.Action
    """
    logger = logging.getLogger('tiny_pacs.cli')
    name_parser_map = getattr(  # noqa: B009
        subparsers, '_name_parser_map')
    choices_actions = getattr(subparsers, '_choices_actions', [])
    # The origin (entry point name) of every subcommand, so duplicate
    # registrations can be logged naming both parties
    origins = dict.fromkeys(name_parser_map, 'the built-in command')
    for ep in entry_points(group=CLI_GROUP):
        if ep.name in RESERVED_COMMAND_NAMES:
            logger.warning(
                'CLI entry point %r uses a reserved subcommand name, '
                'skipping', ep.name
            )
            continue
        parsers_before = dict(name_parser_map)
        actions_before = list(choices_actions)
        try:
            register = ep.load()
            register(_PluginSubParsers(subparsers, origins, ep.name,
                                       logger))
        except KeyboardInterrupt:
            raise
        except BaseException:
            logger.exception(
                'Failed to register CLI entry point %r, skipping', ep.name
            )
            name_parser_map.clear()
            name_parser_map.update(parsers_before)
            choices_actions[:] = actions_before


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses command line arguments.

    Invocations without a subcommand (``tiny-pacs -c config.yaml``) are
    treated as the ``run`` command, so the traditional command line keeps
    working. Plugin subcommands are discovered only when the invocation is
    not a built-in command, so ``run`` and ``config`` never import plugin
    modules.

    :param argv: arguments to parse, defaults to ``sys.argv[1:]``
    :return: parsed arguments
    """
    if argv is None:
        argv = sys.argv[1:]
    else:
        argv = list(argv)
    if not argv or (argv[0].startswith('-')
                    and argv[0] not in ('-h', '--help')):
        argv = ['run', *argv]
    load_plugins = argv[0] not in ('run', 'config')
    return build_parser(load_plugins).parse_args(argv)


if __name__ == '__main__':
    main()
