"""TLS support tests.

A self-signed certificate is generated with the ``openssl`` binary; the tests
are skipped when it is not available.
"""
import shutil
import socket
import ssl
import subprocess
import uuid

import pytest
from pynetdicom2 import asceprovider, sopclass, ssl_ae, uids

from tiny_pacs import config, server

pytestmark = pytest.mark.skipif(
    shutil.which('openssl') is None,
    reason='openssl binary is required for TLS tests'
)


@pytest.fixture(scope='module')
def tls_cert(tmp_path_factory):
    tmp = tmp_path_factory.mktemp('tls')
    cert = tmp / 'cert.pem'
    key = tmp / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-nodes', '-subj', '/CN=localhost'],
        check=True, capture_output=True
    )
    return cert, key


@pytest.fixture
def tls_pacs(tls_cert):
    cert, key = tls_cert
    conf = config.Config()
    conf.update_config({
        'ae': {
            'port': 0,
            'tls': {'certificate': str(cert), 'key': str(key)}
        },
        'components': {'Database': {'on': True, 'db_name': str(uuid.uuid4())}}
    })
    _pacs = server.Server(conf)
    _pacs.start()
    yield _pacs
    _pacs.exit()


def _tls_echo(port: int, cert) -> bool:
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.check_hostname = False
    client_ctx.verify_mode = ssl.CERT_REQUIRED
    client_ctx.load_verify_locations(cafile=str(cert))

    client_ae = ssl_ae.SSLClientAE(client_ctx, 'TEST_CLIENT')
    client_ae.add_scu(sopclass.verification_scu)
    remote = asceprovider.RemoteAEConfig(
        aet='TINY_PACS', address='127.0.0.1', port=port
    )
    with client_ae.request_association(remote) as assoc:
        service = assoc.get_scu(uids.VERIFICATION_SOP_CLASS)
        status = service(1)
    return status.is_success


def test_tls_echo(tls_pacs: server.Server, tls_cert):
    cert, _ = tls_cert
    port = tls_pacs.ae.server.server_address[1]
    assert tls_pacs.ae.ssl_context is not None
    assert _tls_echo(port, cert)


def test_tls_failed_handshake_does_not_block_server(tls_pacs: server.Server,
                                                    tls_cert):
    cert, _ = tls_cert
    port = tls_pacs.ae.server.server_address[1]
    # A client that opens the TCP connection but never completes the TLS
    # handshake must not prevent the server from accepting new connections.
    broken = socket.create_connection(('127.0.0.1', port))
    broken.close()
    assert _tls_echo(port, cert)
