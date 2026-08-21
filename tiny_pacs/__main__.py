import argparse
import sys

from . import config, interactive, server


def main() -> None:
    args = parse_args()
    if args.command == 'config':
        config_command(args)
    else:
        run_command(args)


def run_command(args: argparse.Namespace) -> None:
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


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('-c', '--config', default=[], nargs='*',
                        help='Tiny PACS configuration')
    parser.add_argument('-a', '--aet', default=None,
                        help='Override Tiny PACS AE Title configuration')
    parser.add_argument('-p', '--port', default=None, type=int,
                        help='Override Tiny PACS port configuration')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Provide configuration values interactively')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='tiny-pacs',
        epilog='Running tiny-pacs without a command is equivalent to "tiny-pacs run".'
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
                               help='Provide configuration values interactively')

    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses command line arguments.

    Invocations without a subcommand (``tiny-pacs -c config.yaml``) are
    treated as the ``run`` command, so the traditional command line keeps
    working.

    :param argv: arguments to parse, defaults to ``sys.argv[1:]``
    :return: parsed arguments
    """
    if argv is None:
        argv = sys.argv[1:]
    else:
        argv = list(argv)
    if not argv or (argv[0].startswith('-') and argv[0] not in ('-h', '--help')):
        argv = ['run', *argv]
    return build_parser().parse_args(argv)


if __name__ == '__main__':
    main()
