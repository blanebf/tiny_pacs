#!/usr/bin/env python3
"""Verifies entry point discovery of the first-party extensions.

Installs nothing; expects ``tiny_pacs``, ``tiny_pacs_admin``,
``tiny_pacs_identity``, ``tiny_pacs_audit``, ``tiny_pacs_admin_web`` and
``tiny_pacs_dicomweb`` to be installed into the current environment (the
CI integration job installs the core and the extensions from the
repository). Checks both entry point groups end to end:

* ``tiny_pacs.components`` — the extension components are discovered by
  the first ``Config`` construction and attributed to their distribution;
* ``tiny_pacs.cli`` — the CLI parser registers every extension
  subcommand.

Exits non-zero when anything is missing, so it can run as a CI step.
"""
import argparse
import sys

EXPECTED_COMPONENTS = {
    'DeviceStore': 'tiny_pacs_admin',
    'Users': 'tiny_pacs_identity',
    'UserIdentityAuth': 'tiny_pacs_identity',
    'AuditLog': 'tiny_pacs_audit',
    'AdminWeb': 'tiny_pacs_admin_web',
    'DICOMWeb': 'tiny_pacs_dicomweb',
}

EXPECTED_SUBCOMMANDS = (
    'audit', 'components', 'db', 'devices', 'users', 'web-admin'
)


def check_components() -> list[str]:
    """Runs component discovery and returns the failed checks."""
    from tiny_pacs import config

    config.Config()
    failures = []
    for name, origin in EXPECTED_COMPONENTS.items():
        if name not in config.COMPONENT_REGISTRY:
            failures.append(f'component {name!r} was not discovered')
            continue
        found = config.get_component_origin(name)
        if found != origin:
            failures.append(
                f'component {name!r} is attributed to {found!r}, '
                f'expected {origin!r}'
            )
    return failures


def check_cli() -> list[str]:
    """Builds the CLI parser and returns the missing subcommands."""
    from tiny_pacs import __main__ as cli

    parser = cli.build_parser()
    subparsers = next(
        (action for action in parser._actions
         if isinstance(action, argparse._SubParsersAction)), None
    )
    names = set(subparsers.choices) if subparsers is not None else set()
    return [f'CLI subcommand {name!r} was not registered'
            for name in EXPECTED_SUBCOMMANDS if name not in names]


def main() -> int:
    """Verifies both entry point groups; returns a process exit code."""
    failures = check_components() + check_cli()
    for failure in failures:
        print(f'FAIL: {failure}', file=sys.stderr)
    if failures:
        return 1
    components = ', '.join(
        f'{name} ({origin})'
        for name, origin in EXPECTED_COMPONENTS.items()
    )
    subcommands = ', '.join(EXPECTED_SUBCOMMANDS)
    print(f'Entry points verified: components: {components}; '
          f'CLI subcommands: {subcommands}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
