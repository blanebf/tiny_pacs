import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from tiny_pacs import __main__ as cli
from tiny_pacs import launcher


def test_render_posix_uses_recorded_interpreter() -> None:
    python = '/opt/venv/bin/python3'
    text = launcher.render_posix('config.yaml', python)
    assert text.startswith('#!/usr/bin/env bash\n')
    assert f'PYTHON={shlex.quote(python)}' in text
    assert 'cd "$(dirname "$0")"' in text
    assert 'exec "$PYTHON" -m tiny_pacs "$@" -c config.yaml' in text
    # Fallback when the recorded interpreter is gone
    assert 'exec tiny-pacs "$@" -c config.yaml' in text
    assert '\r' not in text


def test_render_posix_quotes_special_characters() -> None:
    python = "/opt/my env/bin/python3"
    text = launcher.render_posix('my config.yaml', python)
    assert f'PYTHON={shlex.quote(python)}' in text
    assert f'-c {shlex.quote("my config.yaml")}' in text
    assert "PYTHON='/opt/my env/bin/python3'" in text


def test_render_posix_empty_interpreter_falls_back() -> None:
    text = launcher.render_posix('config.yaml', '')
    # ``[ -x '' ]`` is false, so the script skips the recorded
    # interpreter and execs tiny-pacs from the PATH
    assert "PYTHON=''" in text


def test_render_windows_uses_recorded_interpreter() -> None:
    python = r'C:\venv\Scripts\python.exe'
    text = launcher.render_windows('config.yaml', python)
    assert text.startswith('@echo off\n')
    assert 'cd /d "%~dp0"' in text
    assert f'set "TINY_PACS_PYTHON={python}"' in text
    assert 'set "TINY_PACS_CONFIG=config.yaml"' in text
    assert ('"%TINY_PACS_PYTHON%" -m tiny_pacs %* -c "%TINY_PACS_CONFIG%"'
            in text)
    # Fallback when the recorded interpreter is gone
    assert 'tiny-pacs %* -c "%TINY_PACS_CONFIG%"' in text
    assert 'exit /b %errorlevel%' in text
    # No parenthesized blocks, so arguments containing parentheses
    # survive %* expansion
    assert ') else (' not in text


def test_write_scripts_creates_both_files(tmp_path: Path) -> None:
    python = sys.executable
    written = launcher.write_scripts(tmp_path, 'tiny.yaml', python=python)
    assert [path.name for path in written] == [launcher.POSIX_SCRIPT,
                                               launcher.WINDOWS_SCRIPT]
    assert written == [tmp_path / 'cli.sh', tmp_path / 'cli.cmd']
    sh_bytes = (tmp_path / 'cli.sh').read_bytes()
    assert b'\r' not in sh_bytes
    assert b'-c tiny.yaml' in sh_bytes
    assert python.encode() in sh_bytes
    # Batch files use CRLF line endings throughout
    cmd_bytes = (tmp_path / 'cli.cmd').read_bytes()
    assert cmd_bytes.count(b'\r\n') == cmd_bytes.count(b'\n')
    assert b'set "TINY_PACS_CONFIG=tiny.yaml"' in cmd_bytes
    assert b'-c "%TINY_PACS_CONFIG%"' in cmd_bytes
    assert python.encode() in cmd_bytes


def test_write_scripts_defaults(tmp_path: Path) -> None:
    launcher.write_scripts(tmp_path)
    text = (tmp_path / 'cli.sh').read_text()
    assert '-c config.yaml' in text
    if sys.executable:
        assert shlex.quote(sys.executable) in text


@pytest.mark.skipif(os.name == 'nt', reason='POSIX permission bits')
def test_write_scripts_makes_shell_script_executable(
        tmp_path: Path) -> None:
    launcher.write_scripts(tmp_path)
    mode = (tmp_path / 'cli.sh').stat().st_mode
    assert mode & 0o777 == 0o755


def test_write_scripts_overwrites(tmp_path: Path) -> None:
    (tmp_path / 'cli.sh').write_text('# stale\n')
    launcher.write_scripts(tmp_path)
    assert '# stale' not in (tmp_path / 'cli.sh').read_text()


@pytest.mark.skipif(shutil.which('bash') is None, reason='bash required')
def test_generated_cli_sh_runs_subcommands(tmp_path: Path) -> None:
    cli.config_command(cli.parse_args(
        ['config', '-o', str(tmp_path / 'config.yaml'), '--launcher']))
    # Run from a foreign working directory: the script must change into
    # its own folder so "-c config.yaml" resolves next to it
    result = subprocess.run(
        ['bash', str(tmp_path / 'cli.sh'), 'config', 'show'],
        capture_output=True, text=True, timeout=300, cwd=tmp_path.parent
    )
    assert result.returncode == 0, result.stderr
    data = yaml.safe_load(result.stdout)
    assert data['ae']['port'] == 11112


@pytest.mark.skipif(shutil.which('bash') is None, reason='bash required')
def test_generated_cli_sh_passes_arguments_verbatim(
        tmp_path: Path) -> None:
    launcher.write_scripts(tmp_path, 'config.yaml')
    (tmp_path / 'config.yaml').write_text('ae:\n  port: 4242\n')
    result = subprocess.run(
        ['bash', str(tmp_path / 'cli.sh'), 'config', 'show'],
        capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    assert yaml.safe_load(result.stdout)['ae']['port'] == 4242
