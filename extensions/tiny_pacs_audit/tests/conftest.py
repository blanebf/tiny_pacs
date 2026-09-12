"""Shared fixtures for the audit extension tests."""
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import trolleybus
from pynetdicom2 import pdu, userdataitems
from tiny_pacs import assoc_context
from tiny_pacs import db as core_db
from tiny_pacs import events as core_events

from tiny_pacs_audit.audit import AuditLog
from tiny_pacs_audit.models import AuditEventModel


def _sqlite_config(db_path: str) -> dict[str, Any]:
    """File-based SQLite configuration for offline administration."""
    return {'driver': 'sqlite', 'db_name': db_path, 'uri': False}


def _fake_asce(peer: str | None = None) -> Any:
    """Builds an association acceptor stand-in without a live socket."""
    if peer is None:
        return SimpleNamespace(dul=SimpleNamespace(dul_socket=None))
    socket = SimpleNamespace(getpeername=lambda: (peer, 104))
    return SimpleNamespace(dul=SimpleNamespace(dul_socket=socket))


@pytest.fixture
def audit_bus(tmp_path: Path) -> Iterator[Callable[..., Any]]:
    """Starts a headless event bus with a file SQLite database.

    The bus carries the ``Database`` component plus, optionally, the
    ``AuditLog`` component. Tests control what is instantiated; every
    started bus is stopped afterwards.

    :yield: factory ``(audit_conf=None, db_path=None, with_audit=True)``
            returning ``(bus, audit, database, db_path)``
    """
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []

    def start(
            audit_conf: dict[str, Any] | None = None,
            db_path: str | None = None,
            with_audit: bool = True
    ) -> tuple[trolleybus.EventBus, AuditLog | None, core_db.Database, str]:
        path = db_path or str(tmp_path / f'{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, _sqlite_config(path))
        audit = AuditLog(bus, audit_conf or {}) if with_audit else None
        bus.start()
        started.append((bus, database))
        return bus, audit, database, path

    yield start

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


@contextmanager
def association_context(
        calling_aet: str = 'MODALITY',
        called_aet: str = 'TINY_PACS',
        peer: str | None = None,
        username: str | None = None
) -> Iterator[assoc_context.AssocContext]:
    """Opens a core association context for the current thread.

    Mirrors what the AE does for an accepted association so DIMSE-path
    handlers (which read :func:`tiny_pacs.assoc_context.current`) can be
    exercised on a headless bus. The context is always closed on exit so
    it never leaks into another test.

    :yield: the opened association context
    """
    assoc = pdu.AAssociateRqPDU(
        called_ae_title=called_aet, calling_ae_title=calling_aet,
        variable_items=[
            pdu.UserInformationItem(
                [userdataitems.MaximumLengthSubItem(65536)])
        ]
    )
    context = assoc_context.open_context(_fake_asce(peer), assoc)
    if username is not None:
        context.username = username
    try:
        yield context
    finally:
        assoc_context.close_current()


def _identity_item(
        username: str | bytes = '',
        password: str | bytes = '',
        identity_type: int = 2,
        positive_response_req: int = 0
) -> userdataitems.UserIdentityNegotiationSubItem:
    """Builds a User Identity request sub-item."""
    return userdataitems.UserIdentityNegotiationSubItem(
        username, password, identity_type, positive_response_req
    )


def assoc_payload(
        calling_aet: str,
        identity_item: Any = None,
        peer: str | None = None,
        called_aet: str = 'TINY_PACS'
) -> core_events.AssocPayload:
    """Builds an association payload without a live network connection.

    :param calling_aet: calling AE title of the request
    :param identity_item: optional User Identity sub-item of the request
    :param peer: optional peer address reported by the fake socket
    :param called_aet: called AE title of the request
    :return: association payload
    """
    user_data: list[Any] = [userdataitems.MaximumLengthSubItem(65536)]
    if identity_item is not None:
        user_data.append(identity_item)
    user_info = pdu.UserInformationItem(user_data)
    assoc = pdu.AAssociateRqPDU(
        called_ae_title=called_aet, calling_ae_title=calling_aet,
        variable_items=[user_info]
    )
    asce = _fake_asce(peer)
    return core_events.AssocPayload(asce, assoc)


def seed_rows(db_path: str, rows: list[dict[str, Any]]) -> None:
    """Inserts audit records directly onto a file database.

    Opens a short-lived headless bus (so the audit tables exist and the
    model is bound to ``db_path``), inserts the rows and closes it again.

    :param db_path: file database to seed
    :param rows: field mappings passed to :meth:`AuditEventModel.create`
    """
    bus = trolleybus.EventBus()
    database = core_db.Database(bus, _sqlite_config(db_path))
    AuditLog(bus, {})
    bus.start()
    try:
        for row in rows:
            AuditEventModel.create(**row)
    finally:
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


@pytest.fixture
def config_file(tmp_path: Path) -> Iterator[Callable[..., str]]:
    """Writes audit configuration files for CLI tests.

    :yield: factory ``(extra='', db_name=None)`` returning the path of a
            YAML configuration with a file-based SQLite database
    """
    counter = {'n': 0}

    def write(extra: str = '', db_name: str | None = None) -> str:
        counter['n'] += 1
        name = db_name or str(tmp_path / f'{uuid.uuid4().hex}.db')
        path = tmp_path / f'conf_{counter["n"]}.yaml'
        path.write_text(
            'components:\n'
            '  Database:\n'
            '    on: true\n'
            f'    db_name: {name}\n'
            '    mode: rwc\n'
            f'{extra}'
        )
        return str(path)

    yield write
