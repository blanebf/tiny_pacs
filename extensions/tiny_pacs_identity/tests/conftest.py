"""Shared fixtures for the identity extension tests."""
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import trolleybus
from pynetdicom2 import pdu, userdataitems
from tiny_pacs import db as core_db
from tiny_pacs import devices as core_devices
from tiny_pacs import events as core_events

from tiny_pacs_identity.auth import UserIdentityAuth
from tiny_pacs_identity.users import Users


def _sqlite_config(db_path: str) -> dict[str, Any]:
    """File-based SQLite configuration for offline administration."""
    return {'driver': 'sqlite', 'db_name': db_path, 'uri': False}


@pytest.fixture
def identity_bus(tmp_path: Path) -> Iterator[Callable[..., Any]]:
    """Starts a headless event bus with a file SQLite database.

    The bus carries the ``Database`` and ``Users`` components plus,
    optionally, ``Devices``, ``DeviceStore`` and ``UserIdentityAuth``.
    Tests control what is instantiated; every started bus is stopped
    afterwards.

    :yield: factory ``(devices_conf=None, store_conf=None, users_conf=None,
            auth_conf=None, db_path=None, with_users=True)`` returning
            ``(bus, users, auth, database, db_path)``
    """
    started: list[tuple[trolleybus.EventBus, core_db.Database]] = []

    def start(
            devices_conf: dict[str, Any] | None = None,
            store_conf: dict[str, Any] | None = None,
            users_conf: dict[str, Any] | None = None,
            auth_conf: dict[str, Any] | None = None,
            db_path: str | None = None,
            with_users: bool = True
    ) -> tuple[trolleybus.EventBus, Users | None,
               UserIdentityAuth | None, core_db.Database, str]:
        path = db_path or str(tmp_path / f'{uuid.uuid4().hex}.db')
        bus = trolleybus.EventBus()
        database = core_db.Database(bus, _sqlite_config(path))
        if devices_conf is not None:
            core_devices.Devices(bus, devices_conf)
        if store_conf is not None:
            from tiny_pacs_admin.store import DeviceStore
            DeviceStore(bus, store_conf)
        users = Users(bus, users_conf or {}) if with_users else None
        auth = None
        if auth_conf is not None:
            auth = UserIdentityAuth(bus, auth_conf)
        bus.start()
        started.append((bus, database))
        return bus, users, auth, database, path

    yield start

    for bus, database in reversed(started):
        bus.stop()
        if database.db is not None and not database.db.is_closed():
            database.db.close()


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
        identity_item: Any = None
) -> core_events.AssocPayload:
    """Builds an association payload without a live network connection.

    :param calling_aet: calling AE title of the request
    :param identity_item: optional User Identity sub-item of the request
    :return: association payload with a well-formed User Information item
    """
    user_data: list[Any] = [userdataitems.MaximumLengthSubItem(65536)]
    if identity_item is not None:
        user_data.append(identity_item)
    user_info = pdu.UserInformationItem(user_data)
    assoc = pdu.AAssociateRqPDU(
        called_ae_title='TINY_PACS', calling_ae_title=calling_aet,
        variable_items=[user_info]
    )
    # No DUL socket: components must fall back to an empty peer address
    asce = SimpleNamespace(dul=SimpleNamespace(dul_socket=None))
    return core_events.AssocPayload(asce, assoc)  # type: ignore[arg-type]


@pytest.fixture
def config_file(tmp_path: Path) -> Iterator[Callable[..., str]]:
    """Writes identity configuration files for CLI tests.

    :yield: factory ``(db_name=None)`` returning the path of a YAML
            configuration with a file-based SQLite database
    """
    counter = {'n': 0}

    def write(db_name: str | None = None) -> str:
        counter['n'] += 1
        name = db_name or str(tmp_path / f'{uuid.uuid4().hex}.db')
        path = tmp_path / f'conf_{counter["n"]}.yaml'
        path.write_text(
            'components:\n'
            '  Database:\n'
            '    on: true\n'
            f'    db_name: {name}\n'
            '    mode: rwc\n'
        )
        return str(path)

    yield write
