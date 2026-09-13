#!/usr/bin/env python3
"""Prepares the core package for publishing to PyPI.

Inside the repository the ``admin`` and ``identity`` extras point at the
bundled extension packages through path dependencies, so they resolve
without the extensions being published. PyPI rejects distributions whose
dependencies reference local paths, so before the core is built for
publication the path constraints are rewritten to version constraints
tracking the extension's minor series (``>=0.1,<0.2`` for an extension
at ``0.1.x``).

Usage: ``python scripts/prepare_publish.py [path to the repository root]``

The script rewrites the given working copy in place; run it on a checkout
dedicated to the build (as the release workflow does), not in a working
development tree. Running it twice is a no-op.
"""
import re
import sys
from pathlib import Path

#: Extra dependency name -> directory of the bundled extension package
EXTENSION_DIRECTORIES = {
    'tiny-pacs-admin': 'extensions/tiny_pacs_admin',
    'tiny-pacs-identity': 'extensions/tiny_pacs_identity',
    'tiny-pacs-audit': 'extensions/tiny_pacs_audit',
    'tiny-pacs-admin-web': 'extensions/tiny_pacs_admin_web',
}

ROOT = Path(__file__).resolve().parent.parent


def version_constraint(directory: Path) -> str:
    """Builds the version constraint from the extension's current version.

    :param directory: directory of the extension package
    :return: a minor-series version constraint, e.g. ``>=0.1,<0.2``
    """
    try:
        text = (directory / 'pyproject.toml').read_text(encoding='utf-8')
    except OSError:
        sys.exit(f'Extension package not found at {directory}')
    match = re.search(r'^version\s*=\s*"(\d+)\.(\d+)\.', text, re.MULTILINE)
    if match is None:
        sys.exit(f'Cannot determine the extension version in {directory}')
    major, minor = match.groups()
    return f'>={major}.{minor},<{major}.{int(minor) + 1}'


def rewrite(root: Path) -> None:
    """Rewrites local extension dependencies of the core ``pyproject.toml``.

    Fails closed: after the rewrite every extra dependency must be a
    version constraint. A dependency line that no longer matches the
    expected path form (reformatted, renamed or moved) is not silently
    skipped — it aborts the rewrite, because building the core with a
    local path dependency would only fail later at the PyPI upload.

    :param root: repository root containing the core ``pyproject.toml``
    """
    pyproject = root / 'pyproject.toml'
    text = pyproject.read_text(encoding='utf-8')
    rewritten = False
    for name, directory in EXTENSION_DIRECTORIES.items():
        pattern = re.compile(
            rf'^{re.escape(name)} = \{{ path = "{re.escape(directory)}", '
            r'optional = true \}\s*$',
            re.MULTILINE,
        )
        if pattern.search(text) is None:
            continue
        constraint = version_constraint(root / directory)
        text = pattern.sub(
            f'{name} = {{ version = "{constraint}", optional = true }}',
            text,
        )
        rewritten = True
        print(f'{name}: path dependency replaced by "{constraint}"')
    for name in EXTENSION_DIRECTORIES:
        versioned = re.compile(
            rf'^{re.escape(name)} = \{{ version = "[^"]+", '
            r'optional = true \}\s*$',
            re.MULTILINE,
        )
        if versioned.search(text) is None:
            sys.exit(
                f'Dependency {name!r} is not a version constraint; '
                f'refusing to prepare the core for PyPI with a local '
                f'path dependency (unexpected line format?)'
            )
    if rewritten:
        pyproject.write_text(text, encoding='utf-8')
    else:
        print('No local extension dependencies to rewrite.')


if __name__ == '__main__':
    rewrite(Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT)
