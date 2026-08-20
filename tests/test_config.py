from tiny_pacs import config, db, server, storage


def test_yaml_unquoted_on_key(tmp_path):
    # PyYAML (YAML 1.1) parses the bare ``on`` key as boolean ``True``,
    # producing ``{True: True}`` — components must still be enabled.
    conf_file = tmp_path / 'config.yaml'
    conf_file.write_text(
        'components:\n'
        '  Database:\n'
        '    on: true\n'
        '  InMemoryStorage:\n'
        '    on: true\n'
        '  FileStorage:\n'
        '    on: false\n'
    )
    conf = config.Config()
    conf.update_config(str(conf_file))
    srv = server.Server(conf)
    types = {type(c) for c in srv.components}
    assert db.Database in types
    assert storage.InMemoryStorage in types
    assert storage.FileStorage not in types
