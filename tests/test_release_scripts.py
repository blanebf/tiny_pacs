import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

CORE_PYPROJECT = (
    '[tool.poetry]\n'
    'name = "tiny_pacs"\n'
    'version = "0.3.0"\n'
    '\n'
    '[tool.poetry.dependencies]\n'
    'python = ">=3.10,<4.0"\n'
    'tiny-pacs-admin = { path = "extensions/tiny_pacs_admin", '
    'optional = true }\n'
    'tiny-pacs-identity = { path = "extensions/tiny_pacs_identity", '
    'optional = true }\n'
    '\n'
    '[tool.poetry.extras]\n'
    'admin = ["tiny-pacs-admin"]\n'
    'identity = ["tiny-pacs-identity"]\n'
)


def load_script(name: str) -> ModuleType:
    """Loads a script from the repository's scripts/ directory."""
    path = REPO_ROOT / 'scripts' / f'{name}.py'
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_extension(root: Path, directory: str, version: str) -> None:
    """Creates a bundled extension package with the given version."""
    extension = root / directory
    extension.mkdir(parents=True)
    (extension / 'pyproject.toml').write_text(
        f'[tool.poetry]\nname = "tiny"\nversion = "{version}"\n',
        encoding='utf-8'
    )


def prepare_tree(tmp_path: Path) -> ModuleType:
    """Writes a core pyproject with path extras and loads the script."""
    (tmp_path / 'pyproject.toml').write_text(CORE_PYPROJECT,
                                             encoding='utf-8')
    make_extension(tmp_path, 'extensions/tiny_pacs_admin', '0.1.0')
    make_extension(tmp_path, 'extensions/tiny_pacs_identity', '0.1.3')
    return load_script('prepare_publish')


def test_prepare_publish_rewrites_path_dependencies(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    script = prepare_tree(tmp_path)
    script.rewrite(tmp_path)
    text = (tmp_path / 'pyproject.toml').read_text(encoding='utf-8')
    assert ('tiny-pacs-admin = { version = ">=0.1,<0.2", optional = true }'
            in text)
    assert ('tiny-pacs-identity = { version = ">=0.1,<0.2", '
            'optional = true }' in text)
    assert 'path =' not in text
    assert 'admin = ["tiny-pacs-admin"]' in text
    assert 'identity = ["tiny-pacs-identity"]' in text
    output = capsys.readouterr().out
    assert 'tiny-pacs-admin: path dependency replaced by ">=0.1,<0.2"' \
        in output


def test_prepare_publish_tracks_minor_series(tmp_path: Path) -> None:
    (tmp_path / 'pyproject.toml').write_text(CORE_PYPROJECT,
                                             encoding='utf-8')
    make_extension(tmp_path, 'extensions/tiny_pacs_admin', '0.2.1')
    make_extension(tmp_path, 'extensions/tiny_pacs_identity', '0.3.0')
    script = load_script('prepare_publish')
    script.rewrite(tmp_path)
    text = (tmp_path / 'pyproject.toml').read_text(encoding='utf-8')
    assert ('tiny-pacs-admin = { version = ">=0.2,<0.3", optional = true }'
            in text)
    assert ('tiny-pacs-identity = { version = ">=0.3,<0.4", '
            'optional = true }' in text)


def test_prepare_publish_is_idempotent(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    script = prepare_tree(tmp_path)
    script.rewrite(tmp_path)
    text = (tmp_path / 'pyproject.toml').read_text(encoding='utf-8')
    script.rewrite(tmp_path)
    assert text == (tmp_path / 'pyproject.toml').read_text(encoding='utf-8')
    assert 'No local extension dependencies to rewrite.' \
        in capsys.readouterr().out


def test_prepare_publish_missing_extension(tmp_path: Path) -> None:
    (tmp_path / 'pyproject.toml').write_text(CORE_PYPROJECT,
                                             encoding='utf-8')
    script = load_script('prepare_publish')
    with pytest.raises(SystemExit):
        script.rewrite(tmp_path)


def test_prepare_publish_unreadable_version(tmp_path: Path) -> None:
    (tmp_path / 'pyproject.toml').write_text(CORE_PYPROJECT,
                                             encoding='utf-8')
    extension = tmp_path / 'extensions' / 'tiny_pacs_admin'
    extension.mkdir(parents=True)
    (extension / 'pyproject.toml').write_text('[tool.poetry]\n',
                                              encoding='utf-8')
    make_extension(tmp_path, 'extensions/tiny_pacs_identity', '0.1.0')
    script = load_script('prepare_publish')
    with pytest.raises(SystemExit):
        script.rewrite(tmp_path)


def test_prepare_publish_reformatted_path_dependency(
        tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    drifted = CORE_PYPROJECT.replace(
        'tiny-pacs-admin = { path = "extensions/tiny_pacs_admin", '
        'optional = true }',
        'tiny-pacs-admin = {path="extensions/tiny_pacs_admin",'
        'optional=true}',
    )
    (tmp_path / 'pyproject.toml').write_text(drifted, encoding='utf-8')
    make_extension(tmp_path, 'extensions/tiny_pacs_admin', '0.1.0')
    make_extension(tmp_path, 'extensions/tiny_pacs_identity', '0.1.0')
    script = load_script('prepare_publish')
    with pytest.raises(SystemExit):
        script.rewrite(tmp_path)
    # The original file is left untouched
    assert (tmp_path / 'pyproject.toml').read_text(encoding='utf-8') \
        == drifted


def test_prepare_publish_rewrites_real_pyproject(tmp_path: Path) -> None:
    """Guards against drift between the rewriter and the real files."""
    (tmp_path / 'pyproject.toml').write_text(
        (REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'),
        encoding='utf-8'
    )
    for directory in ('extensions/tiny_pacs_admin',
                      'extensions/tiny_pacs_identity'):
        (tmp_path / directory).mkdir(parents=True)
        (tmp_path / directory / 'pyproject.toml').write_text(
            (REPO_ROOT / directory / 'pyproject.toml')
            .read_text(encoding='utf-8'),
            encoding='utf-8'
        )
    script = load_script('prepare_publish')
    script.rewrite(tmp_path)
    text = (tmp_path / 'pyproject.toml').read_text(encoding='utf-8')
    assert 'path =' not in text
    assert 'admin = ["tiny-pacs-admin"]' in text
    assert 'identity = ["tiny-pacs-identity"]' in text


def test_verify_entry_points_script() -> None:
    pytest.importorskip('tiny_pacs_admin')
    pytest.importorskip('tiny_pacs_identity')
    result = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / 'scripts' / 'verify_entry_points.py')],
        capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
