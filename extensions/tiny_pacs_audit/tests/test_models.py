"""Tests of the audit model and its baseline migration."""
import datetime
from typing import Any

from tiny_pacs import db as core_db

from tiny_pacs_audit.models import AuditEventModel


def _version(name: str) -> int | None:
    row = core_db.SchemaVersion.get_or_none(
        core_db.SchemaVersion.component == name
    )
    return row.version if row is not None else None


def test_baseline_migration_fresh_db(audit_bus: Any) -> None:
    audit_bus()
    assert _version('AuditLog') == 1


def test_migration_restart_is_noop(audit_bus: Any) -> None:
    _, _, database, path = audit_bus()
    database.db.close()
    audit_bus(db_path=path)
    # The restart applies no migrations and keeps the recorded version
    assert _version('AuditLog') == 1


def test_model_roundtrip(audit_bus: Any) -> None:
    audit_bus()
    now = datetime.datetime.now(datetime.timezone.utc)
    AuditEventModel.create(
        timestamp=now,
        category='assoc',
        event='accepted',
        device_aet='MODALITY',
        username='alice',
        peer='10.0.0.9',
        status='accepted',
        details='{"identity_type": 2}'
    )
    row = AuditEventModel.get(AuditEventModel.device_aet == 'MODALITY')
    assert row.category == 'assoc'
    assert row.event == 'accepted'
    assert row.username == 'alice'
    assert row.peer == '10.0.0.9'
    assert row.status == 'accepted'
    assert row.details == '{"identity_type": 2}'
    assert row.timestamp is not None


def test_defaults_and_nulls(audit_bus: Any) -> None:
    audit_bus()
    AuditEventModel.create(category='admin', event='device-add',
                           status='success')
    row = AuditEventModel.get(AuditEventModel.event == 'device-add')
    # The nullable attribution columns and the default details blob
    assert row.device_aet is None
    assert row.username is None
    assert row.peer is None
    assert row.details == '{}'
    assert row.timestamp is not None
