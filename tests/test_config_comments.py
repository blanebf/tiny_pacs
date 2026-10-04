"""Tests of the generated configuration YAML comments."""
import pathlib

import pydantic
import pytest
import yaml  # type: ignore[import-untyped]

from tiny_pacs import component, config, config_comments, db, http, storage

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
LOCAL_DEBUG_CONFIG = REPO_ROOT / 'local-debug' / 'config.yaml'
GOLDEN_FILE = pathlib.Path(__file__).parent / 'data' / 'golden_config.yaml'


def test_sanitize_text_strips_rest_markup() -> None:
    sanitize = config_comments.sanitize_text
    assert sanitize(':class:`Database` config') == 'Database config'
    assert sanitize(
        ':class:`~tiny_pacs.events.Migrations`') == 'Migrations'
    assert sanitize(
        ':func:`logging.config.dictConfig`'
    ) == 'logging.config.dictConfig'
    assert sanitize('the ``on`` flag') == 'the on flag'
    assert sanitize(':class:`the title <~mod.Cls>`') == 'the title'
    assert sanitize('spaced   out\ntext') == 'spaced out text'


def test_parse_docstring_summary_and_multiline_ivars() -> None:
    summary, ivars = config_comments.parse_docstring(
        db.DatabaseConfig.__doc__)
    assert summary == 'Configuration of the Database component.'
    # Multi-line hanging continuation joined into one entry
    assert 'database is locked' in ivars['busy_timeout']
    assert 'PostgreSQL' in ivars['host']


def test_parse_docstring_stops_at_lists() -> None:
    from tiny_pacs.pacs import component as pacs_component
    summary, _ = config_comments.parse_docstring(
        pacs_component.PACS.__doc__)
    assert summary == 'Component that implements PACS services themselves.'
    assert 'Handles the following events' not in summary


def test_parse_docstring_rejects_non_strings() -> None:
    assert config_comments.parse_docstring(None) == ('', {})
    assert config_comments.parse_docstring(42) == ('', {})


def test_ivar_mro_subclass_wins() -> None:
    subclass = config_comments.model_field_comments(
        storage.FileStorageConfig)
    assert subclass['overwrite'].startswith('inherited from StorageConfig')
    base = config_comments.model_field_comments(storage.StorageConfig)
    assert 'rolls the record back' in base['overwrite']


def test_on_field_documented_for_every_component() -> None:
    config.load_component_plugins()
    for name, factory in config.COMPONENT_REGISTRY.items():
        comments = config_comments.model_field_comments(factory.config_model)
        assert 'on' in comments, name
        assert 'skipped' in comments['on'], name


def test_enum_and_bounds_facts() -> None:
    database = config_comments.model_field_comments(db.DatabaseConfig)
    assert 'one of: sqlite, postgres' in database['driver']
    server = config_comments.model_field_comments(http.HttpServerConfig)
    assert '0..65535' in server['port']
    assert '>=1' in server['threads']


def test_required_and_optional_facts() -> None:
    comments = config_comments.model_field_comments(config.TLSConfig)
    assert 'REQUIRED' in comments['certificate']
    assert 'optional' in comments['key']


def test_fact_not_duplicated_when_already_in_prose() -> None:
    class _Model(component.ComponentConfig):
        """Model.

        :ivar mode: SQLite URI open mode (``memory``, ``rwc``, ...)
        """

        mode: str = 'memory'

    comments = config_comments.model_field_comments(_Model)
    assert '(memory, rwc, ...)' in comments['mode']
    assert comments['mode'].count('memory') < 4


def test_field_description_wins_over_docstring() -> None:
    class _Model(component.ComponentConfig):
        """Model.

        :ivar value: docstring text
        """

        value: str = pydantic.Field(
            default='x', description='description text')

    comments = config_comments.model_field_comments(_Model)
    assert comments['value'] == 'description text'


def test_json_schema_extra_comment_honored() -> None:
    class _Model(component.ComponentConfig):
        """Model."""

        value: str = pydantic.Field(
            default='x', json_schema_extra={'comment': 'extra text'})

    comments = config_comments.model_field_comments(_Model)
    assert comments['value'] == 'extra text'


def test_undocumented_field_gets_facts_only_or_nothing() -> None:
    class _Model(component.ComponentConfig):
        """Model."""

        driver: db.DBDrivers = db.DBDrivers.SQLITE
        quiet: bool = True

    comments = config_comments.model_field_comments(_Model)
    assert comments['driver'] == 'one of: sqlite, postgres'
    assert 'quiet' not in comments


def test_secret_field_warning() -> None:
    comments = config_comments.model_field_comments(db.DatabaseConfig)
    assert 'plain text' in comments['password']
    assert '0600' in comments['password']


def test_doc_convention_every_field_documented() -> None:
    # Convention enforcement: every field of every registered component
    # config model (plus the core section models) carries an ``:ivar:``
    # entry (via the MRO) or a ``Field(description=)`` /
    # ``json_schema_extra['comment']``, so generated comments never
    # silently degrade for new components.
    config.load_component_plugins()
    models: set[type[pydantic.BaseModel]] = {
        factory.config_model
        for factory in config.COMPONENT_REGISTRY.values()
    }
    models.update({config.AEConfig, config.TLSConfig})
    for model in models:
        ivars = config_comments.model_field_ivars(model)
        for name, field in model.model_fields.items():
            extra = field.json_schema_extra
            has_extra = (isinstance(extra, dict) and 'comment' in extra)
            assert (name in ivars or field.description or has_extra), \
                f'{model.__name__}.{name} lacks :ivar:/description'


def test_component_comments_origins() -> None:
    header = config_comments.component_comments(
        'Database', config.COMPONENT_REGISTRY, config.COMPONENT_ORIGINS)
    assert len(header) == 1
    assert '(built-in)' in header[0]
    assert 'Handles database connections' in header[0]

    registry = dict(config.COMPONENT_REGISTRY)
    origins = dict(config.COMPONENT_ORIGINS)
    registry['_Ext'] = db.Database
    origins['_Ext'] = 'tiny-pacs-ext'
    header = config_comments.component_comments('_Ext', registry, origins)
    assert '(from tiny-pacs-ext, installed extension; off by default)' \
        in header[0]

    assert config_comments.component_comments(
        'Missing', config.COMPONENT_REGISTRY,
        config.COMPONENT_ORIGINS) == []


def _assert_structure_preserved(conf: config.Config) -> None:
    plain = config.dump_yaml(conf, comments=False)
    commented = config.dump_yaml(conf)
    # The safety fallback (plain text on a broken render) would also pass
    # the equality check, so insist on actual comments being present
    assert commented.startswith('# Generated by')
    assert yaml.safe_load(commented) == yaml.safe_load(plain)
    restored = config.Config()
    restored.update_config(yaml.safe_load(commented))
    assert restored == conf


def test_default_config_roundtrip_with_comments() -> None:
    _assert_structure_preserved(config.Config())


def test_configured_roundtrip_with_comments() -> None:
    conf = config.Config()
    conf.update_config({
        'ae': {'ae_title': 'T STL', 'port': 104, 'dump_ds': False,
               'tls': {'certificate': '/etc/pacs/cert.pem',
                       'key': '/etc/pacs/key.pem'}},
        'components': {
            'FileStorage': {'on': True, 'storage_dir': '/srv/pacs',
                            'overwrite': True},
            'Devices': {'devices': {'SRV ONE': {
                'aet': 'SRV ONE', 'address': 'srv.example.com',
                'port': 104}}},
            'Database': {'on': True, 'driver': 'sqlite',
                         'db_name': ':memory:'}
        }
    })
    _assert_structure_preserved(conf)


@pytest.mark.skipif(
    not LOCAL_DEBUG_CONFIG.exists(), reason='local-debug config not found')
def test_local_debug_config_roundtrip_with_comments() -> None:
    conf = config.Config()
    conf.update_config(str(LOCAL_DEBUG_CONFIG))
    _assert_structure_preserved(conf)


def test_scanner_hardening_exotic_log_section() -> None:
    conf = config.Config()
    conf.log['exotic'] = {
        'multiline': "first line\n  port: not a key\nlast ' line\n",
        'long_quoted': 'http://example.com/' + 'seg ment: x/' * 20,
        'unicode': 'Ünïcödé — ☃',
        'colons': 'a: b: c: d ' * 15
    }
    _assert_structure_preserved(conf)


def test_block_scalar_bodies_untouched() -> None:
    text = (
        'ae:\n'
        '  port: 11112\n'
        'log:\n'
        '  script: |\n'
        '    port: fake key\n'
        '    ae: not a section\n'
        '  version: 1\n'
    )
    comments = {
        ('ae',): ['AE section'],
        ('ae', 'port'): ['The port'],
        ('log', 'script'): ['A script']
    }
    rendered = config_comments._insert_comments(text, comments)
    assert yaml.safe_load(rendered) == yaml.safe_load(text)
    assert rendered.index('# AE section') < rendered.index('ae:')
    assert rendered.index('# The port') < rendered.index('port: 11112')
    assert rendered.index('# A script') < rendered.index('script: |')
    body = rendered.split('script: |')[1].split('version: 1')[0]
    assert '#' not in body


def test_wrapped_quoted_scalar_bodies_untouched() -> None:
    text = (
        "ae:\n"
        "  key: 'wrapped start\n"
        "    port: not a key\n"
        "    end'\n"
        "  port: 11112\n"
    )
    comments: dict[tuple[str, ...], list[str]] = {
        ('ae', 'port'): ['The port']
    }
    rendered = config_comments._insert_comments(text, comments)
    assert yaml.safe_load(rendered) == yaml.safe_load(text)
    body = rendered.split("key: 'wrapped start")[1].split("end'")[0]
    assert '#' not in body
    assert '# The port' in rendered


def test_broken_docstrings_degrade_to_no_comments() -> None:
    class _BrokenConfig(component.ComponentConfig):
        value: int = 3

    class _BrokenComponent(component.Component[_BrokenConfig]):
        config_model = _BrokenConfig

    _BrokenComponent.__doc__ = None
    _BrokenConfig.__doc__ = ':ivar value: broken `` :class:` unterminated'
    config.register_component('_Broken', _BrokenComponent)
    try:
        conf = config.Config()
        conf.update_config({'components': {'_Broken': {'value': 7}}})
        commented = config.dump_yaml(conf)
        plain = config.dump_yaml(conf, comments=False)
        assert yaml.safe_load(commented) == yaml.safe_load(plain)
        restored = config.Config()
        restored.update_config(yaml.safe_load(commented))
        assert restored == conf
    finally:
        del config.COMPONENT_REGISTRY['_Broken']
        del config.COMPONENT_ORIGINS['_Broken']


def test_write_yaml_with_comments(tmp_path: pathlib.Path) -> None:
    conf = config.Config()
    out_file = tmp_path / 'conf.yaml'
    config.write_yaml(conf, str(out_file))
    assert out_file.read_text().startswith('# Generated by')
    plain_file = tmp_path / 'plain.yaml'
    config.write_yaml(conf, str(plain_file), comments=False)
    assert '#' not in plain_file.read_text()


def test_extension_banner_once() -> None:
    registry = dict(config.COMPONENT_REGISTRY)
    origins = dict(config.COMPONENT_ORIGINS)
    registry['_ExtA'] = db.Database
    registry['_ExtB'] = db.Database
    origins['_ExtA'] = 'dist-a'
    origins['_ExtB'] = 'dist-b'
    conf = config.Config()
    conf.components['_ExtA'] = db.DatabaseConfig()
    conf.components['_ExtB'] = db.DatabaseConfig()
    comments = config_comments.build_comment_map(
        conf, registry, origins)
    banner_blocks = [
        blocks for blocks in comments.values()
        if config_comments.EXTENSION_BANNER in blocks
    ]
    assert len(banner_blocks) == 1
    assert comments[('ae',)] == ['Application entity settings.']
    assert ('log',) in comments


def test_golden_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # A built-in-only registry makes the generated document independent
    # of which extensions happen to be installed in the test environment
    registry = {
        name: factory
        for name, factory in config.COMPONENT_REGISTRY.items()
        if config.COMPONENT_ORIGINS.get(name) == 'built-in'
    }
    monkeypatch.setattr(config, 'COMPONENT_REGISTRY', registry)
    monkeypatch.setattr(
        config, 'COMPONENT_ORIGINS', dict.fromkeys(registry, 'built-in'))
    monkeypatch.setattr(config, '_plugins_loaded', True)
    conf = config.Config()
    generated = config.dump_yaml(conf)
    assert generated == GOLDEN_FILE.read_text()
