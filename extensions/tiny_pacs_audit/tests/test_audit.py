"""Tests of the AuditLog component on a headless bus."""
import datetime
import json
from typing import Any, cast

import peewee
import pydantic
import pydicom
import pytest
from conftest import _identity_item, assoc_payload, association_context
from pydicom import uid
from pynetdicom2 import fsm, statuses
from tiny_pacs import assoc_context
from tiny_pacs import events as core_events

from tiny_pacs_audit.audit import AuditLog, AuditLogConfig
from tiny_pacs_audit.models import AuditEventModel

_SOP_CLASS = '1.2.840.10008.5.1.4.1.1.7'


def _ctx() -> fsm.PContextDef:
    return fsm.PContextDef(1, uid.UID(_SOP_CLASS),
                           uid.UID('1.2.840.10008.1.2'))


def _ds(**fields: Any) -> pydicom.Dataset:
    ds = pydicom.Dataset()
    ds.SOPClassUID = _SOP_CLASS
    ds.SOPInstanceUID = '1.2.3.4'
    ds.QueryRetrieveLevel = 'STUDY'
    for name, value in fields.items():
        setattr(ds, name, value)
    return ds


def _rows(bus: Any, **filters: Any) -> list[AuditEventModel]:
    return cast('list[AuditEventModel]', bus.send_one(
        core_events.AuditQuery, core_events.AuditFilter(**filters)))


def _session(calling_aet: str = 'MODALITY',
             **kwargs: Any) -> assoc_context.AssocContext:
    return assoc_context.AssocContext(
        calling_aet=calling_aet, called_aet='TINY_PACS', **kwargs)


# ----------------------------------------------------------------------
# Association lifecycle
# ----------------------------------------------------------------------

def test_assoc_records_attempt(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context(peer='10.0.0.9'):
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY'))
    row = _rows(bus, categories=['assoc'])[0]
    assert row.event == 'attempt'
    assert row.status == 'pending'
    assert row.device_aet == 'MODALITY'
    assert row.peer == '10.0.0.9'


def test_assoc_presented_identity_recorded(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context():
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('alice', 'secret', 2)))
    row = _rows(bus, events=['attempt'])[0]
    assert row.username == 'alice'
    details = json.loads(row.details)
    assert details['identity_type'] == 2
    assert details['identity_present'] is True
    # The password (secondary field) never reaches the record
    assert 'secret' not in row.details
    assert 'secret' not in str(row.username)


def test_assoc_username_control_chars_stripped(audit_bus: Any) -> None:
    """Peer-controlled usernames cannot forge rows or inject escapes."""
    bus, _, _, _ = audit_bus()
    with association_context():
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item('ali\r\nce\x1b[31m', 'x', 2)))
    row = _rows(bus, events=['attempt'])[0]
    # CR/LF and the ESC of an ANSI sequence are stripped; the printable
    # residue remains, so the value spans a single, inert report cell
    assert row.username == 'alice[31m'
    assert '\n' not in (row.username or '')
    assert '\r' not in (row.username or '')
    assert '\x1b' not in (row.username or '')


def test_assoc_without_identity(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context():
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY'))
    row = _rows(bus, events=['attempt'])[0]
    assert row.username is None
    details = json.loads(row.details)
    assert details['identity_present'] is False
    assert details['identity_type'] is None


def test_assoc_opaque_identity_type_only(audit_bus: Any) -> None:
    """Types 3-5 record their type but never the opaque credentials."""
    bus, _, _, _ = audit_bus()
    with association_context():
        bus.broadcast(core_events.Assoc, assoc_payload(
            'MODALITY', _identity_item(b'\xff\xfe-secretticket', b'', 3)))
    row = _rows(bus, events=['attempt'])[0]
    assert json.loads(row.details)['identity_type'] == 3
    assert row.username is None
    assert 'ticket' not in row.details


def test_assoc_rejected_records_reason(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context(peer='10.0.0.9'):
        bus.broadcast(core_events.AssocRejected,
                      core_events.AssocRejectedPayload(
                          assoc_payload('MODALITY').assoc,
                          'invalid credentials'))
    row = _rows(bus, events=['rejected'])[0]
    assert row.category == 'assoc'
    assert row.status == 'rejected'
    assert row.device_aet == 'MODALITY'
    assert json.loads(row.details)['reason'] == 'invalid credentials'


def test_assoc_rejected_invalid_called_aet(audit_bus: Any) -> None:
    """An invalid called AE title carries no request PDU."""
    bus, _, _, _ = audit_bus()
    bus.broadcast(core_events.AssocRejected,
                  core_events.AssocRejectedPayload(
                      None, 'called AE title not valid: BOGUS'))
    row = _rows(bus, events=['rejected'])[0]
    assert row.status == 'rejected'
    assert row.device_aet is None
    assert 'not valid' in json.loads(row.details)['reason']


def test_assoc_released_records_username(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    context = _session('MODALITY', peer='10.0.0.9', username='alice')
    bus.broadcast(core_events.AssocReleased, context)
    row = _rows(bus, events=['released'])[0]
    assert row.status == 'success'
    assert row.username == 'alice'
    assert row.device_aet == 'MODALITY'
    assert row.peer == '10.0.0.9'


def test_rejected_association_has_attempt_and_rejection(audit_bus: Any
                                                        ) -> None:
    """A rejected association records the attempt (+30) and the outcome."""
    bus, _, _, _ = audit_bus()
    payload = assoc_payload('MODALITY', _identity_item('alice', 'wrong', 2))
    with association_context(peer='10.0.0.9') as context:
        bus.broadcast(core_events.Assoc, payload)
        bus.broadcast(core_events.AssocRejected,
                      core_events.AssocRejectedPayload(
                          payload.assoc, 'invalid credentials'))
    assoc_rows = _rows(bus, categories=['assoc'])
    assert sorted(row.event for row in assoc_rows) == ['attempt', 'rejected']
    # Both rows of the association share the correlation id
    ids = {json.loads(row.details)['correlation_id'] for row in assoc_rows}
    assert ids == {context.correlation_id}


# ----------------------------------------------------------------------
# Service requests and outcomes
# ----------------------------------------------------------------------

def test_store_request_recorded_and_neutral(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    payload = core_events.StorePayload(_ctx(), b'', _session(username='alice'))
    results = bus.broadcast(core_events.Store, payload)
    # Audit is the only listener and returns the neutral success status
    assert results == [statuses.SUCCESS]
    row = _rows(bus, events=['store'])[0]
    assert row.category == 'service'
    assert row.device_aet == 'MODALITY'
    assert row.username == 'alice'
    assert json.loads(row.details)['sop_class'] == _SOP_CLASS


def test_store_without_session_falls_back_to_context(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context(calling_aet='CTX_AET', username='ctxuser'):
        # session=None on the payload; attribution comes from the thread
        bus.broadcast(core_events.Store,
                      core_events.StorePayload(_ctx(), b'', None))
    row = _rows(bus, events=['store'])[0]
    assert row.device_aet == 'CTX_AET'
    assert row.username == 'ctxuser'


def test_store_done_recorded(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context(calling_aet='MODALITY', username='alice',
                             peer='10.0.0.9'):
        bus.broadcast(core_events.StoreDone, _ds(SOPInstanceUID='1.2.3.999'))
    row = _rows(bus, events=['store-done'])[0]
    assert row.status == 'success'
    assert row.device_aet == 'MODALITY'
    assert row.username == 'alice'
    assert json.loads(row.details)['sop_instance'] == '1.2.3.999'


def test_store_failure_recorded(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context(calling_aet='MODALITY'):
        bus.broadcast(core_events.StoreFailure, _ds())
    row = _rows(bus, events=['store-failure'])[0]
    assert row.status == 'failure'
    assert row.category == 'service'


def test_find_recorded_and_empty_result(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    session = _session(username='alice')
    payload = core_events.FindPayload(_ctx(), _ds(), session)
    results = bus.broadcast(core_events.Find, payload)
    # Audit contributes no result datasets
    assert results == [()]
    row = _rows(bus, events=['find'])[0]
    assert row.device_aet == 'MODALITY'
    assert row.username == 'alice'
    assert json.loads(row.details)['level'] == 'STUDY'


def test_move_recorded(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    payload = core_events.MovePayload(_ctx(), _ds(), 'WS1', _session())
    results = bus.broadcast(core_events.Move, payload)
    assert results == [[]]
    row = _rows(bus, events=['move'])[0]
    assert json.loads(row.details)['destination'] == 'WS1'


def test_get_recorded(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    payload = core_events.GetPayload(_ctx(), _ds(), _session())
    results = bus.broadcast(core_events.Get, payload)
    assert results == [[]]
    assert _rows(bus, events=['get'])[0].device_aet == 'MODALITY'


def test_commitment_recorded(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    with association_context(calling_aet='MODALITY'):
        results = bus.broadcast(core_events.Commitment, [
            (uid.UID(_SOP_CLASS), uid.UID('1.2.3.1')),
            (uid.UID(_SOP_CLASS), uid.UID('1.2.3.2'))
        ])
    assert results == [([], [])]
    row = _rows(bus, events=['commitment'])[0]
    assert json.loads(row.details)['instances'] == 2
    assert row.device_aet == 'MODALITY'


# ----------------------------------------------------------------------
# AuditRecord from other components
# ----------------------------------------------------------------------

def test_audit_record_and_without_component(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(
            category='admin', event='device-add', device_aet='MRI',
            username='root', details={'address': '10.0.0.1'}))
    row = _rows(bus, categories=['admin'])[0]
    assert row.event == 'device-add'
    assert row.device_aet == 'MRI'
    assert row.username == 'root'
    assert json.loads(row.details) == {'address': '10.0.0.1'}


def test_disabled_component_records_nothing(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus(with_audit=False)
    results = bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(category='admin', event='device-add'))
    assert results == []
    assert not bus.has_listeners(core_events.AuditQuery)


# ----------------------------------------------------------------------
# Query API
# ----------------------------------------------------------------------

def _seed_query_rows() -> None:
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    rows = [
        ('assoc', 'accepted', 'MODALITY', 'alice', 'accepted', 0),
        ('assoc', 'rejected', 'CT', 'bob', 'rejected', 1),
        ('assoc', 'released', 'MODALITY', 'alice', 'success', 2),
        ('service', 'store', 'MODALITY', 'alice', 'success', 3),
        ('service', 'find', 'CT', 'carol', 'success', 4),
    ]
    for category, event, aet, user, status, hours in rows:
        AuditEventModel.create(
            timestamp=base + datetime.timedelta(hours=hours),
            category=category, event=event, device_aet=aet, username=user,
            status=status)


def test_query_filters(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    _seed_query_rows()
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

    assert len(_rows(bus, categories=['assoc'])) == 3
    assert len(_rows(bus, categories=['assoc', 'service'])) == 5
    assert len(_rows(bus, events=['rejected'])) == 1
    assert len(_rows(bus, device_aet='MODALITY')) == 3
    assert len(_rows(bus, username='alice')) == 3

    window = _rows(bus, since=base + datetime.timedelta(hours=1),
                   until=base + datetime.timedelta(hours=3))
    assert sorted(row.event for row in window) == [
        'rejected', 'released', 'store']

    assert [row.event for row in _rows(bus, limit=2)] == [
        'accepted', 'rejected']
    assert [row.event for row in _rows(bus, limit=2, offset=2)] == [
        'released', 'store']


def test_count(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    _seed_query_rows()
    assert bus.send_one(core_events.AuditCount,
                        core_events.AuditFilter(categories=['assoc'])) == 3
    assert bus.send_one(core_events.AuditCount,
                        core_events.AuditFilter()) == 5


def test_stats(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    _seed_query_rows()
    stats = bus.send_one(core_events.AuditStats, None)
    assert stats['total'] == 5
    assert stats['by_category'] == {'assoc': 3, 'service': 2}
    assert stats['by_event']['accepted'] == 1
    assert stats['by_status'] == {
        'accepted': 1, 'rejected': 1, 'success': 3}
    assert stats['oldest'] is not None
    assert stats['newest'] is not None


def test_stats_empty(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    stats = bus.send_one(core_events.AuditStats, None)
    assert stats['total'] == 0
    assert stats['oldest'] is None
    assert stats['newest'] is None
    assert stats['by_category'] == {}


def test_count_ignores_pagination(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    for _ in range(150):
        AuditEventModel.create(category='service', event='store',
                               device_aet='MODALITY', status='success')
    # The default filter limit (100) and an explicit small limit must not
    # truncate the reported total
    assert bus.send_one(core_events.AuditCount,
                        core_events.AuditFilter(categories=['service'])) == 150
    assert bus.send_one(
        core_events.AuditCount,
        core_events.AuditFilter(categories=['service'], limit=5)) == 150


def test_query_limit_is_capped(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import tiny_pacs_audit.audit as audit_module
    bus, _, _, _ = audit_bus()
    for _ in range(120):
        AuditEventModel.create(category='service', event='store',
                               status='success')
    monkeypatch.setattr(audit_module, 'MAX_QUERY_ROWS', 100)
    # A non-positive limit is clamped to the cap, never "unlimited"
    unlimited = bus.send_one(core_events.AuditQuery,
                             core_events.AuditFilter(limit=0))
    assert len(unlimited) == 100
    # An oversized limit is clamped to the cap as well
    oversized = bus.send_one(core_events.AuditQuery,
                             core_events.AuditFilter(limit=999_999))
    assert len(oversized) == 100


def test_cleanup_batches_large_delete(audit_bus: Any) -> None:
    from tiny_pacs_audit.audit import (
        DELETE_CHUNK,
        AuditCleanup,
        AuditCleanupOptions,
    )
    bus, _, _, _ = audit_bus()
    now = datetime.datetime.now(datetime.timezone.utc)
    old = now - datetime.timedelta(days=60)
    total = DELETE_CHUNK + 37
    for _ in range(total):
        AuditEventModel.create(timestamp=old, category='service',
                               event='store', status='success')
    # More rows than one delete batch: the loop removes them all
    removed = bus.send_one(
        AuditCleanup, AuditCleanupOptions(older_than_days=30))
    assert removed == total
    assert bus.send_one(core_events.AuditCount, core_events.AuditFilter()) == 0


# ----------------------------------------------------------------------
# Configuration, retention and redaction
# ----------------------------------------------------------------------

def test_config_validation() -> None:
    conf = AuditLogConfig.model_validate(
        {'retention_days': 30, 'record_details': False})
    assert conf.retention_days == 30
    assert conf.record_details is False
    with pytest.raises(pydantic.ValidationError):
        AuditLogConfig.model_validate({'retention_days': -1})


def test_record_details_disabled(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus(audit_conf={'record_details': False})
    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(
            category='admin', event='device-add',
            details={'address': '10.0.0.1'}))
    assert _rows(bus, categories=['admin'])[0].details == '{}'


def test_retention_purges_old_records(audit_bus: Any) -> None:
    _, _, database, path = audit_bus()
    now = datetime.datetime.now(datetime.timezone.utc)
    AuditEventModel.create(
        timestamp=now - datetime.timedelta(days=10), category='assoc',
        event='accepted', device_aet='OLD', status='accepted')
    AuditEventModel.create(
        timestamp=now - datetime.timedelta(hours=1), category='assoc',
        event='accepted', device_aet='NEW', status='accepted')
    database.db.close()

    bus2, _, _, _ = audit_bus(db_path=path, audit_conf={'retention_days': 5})
    rows = bus2.send_one(core_events.AuditQuery, core_events.AuditFilter())
    assert [row.device_aet for row in rows] == ['NEW']


def test_retention_zero_keeps_records(audit_bus: Any) -> None:
    _, _, database, path = audit_bus()
    now = datetime.datetime.now(datetime.timezone.utc)
    AuditEventModel.create(
        timestamp=now - datetime.timedelta(days=365), category='assoc',
        event='accepted', device_aet='ANCIENT', status='accepted')
    database.db.close()

    bus2, _, _, _ = audit_bus(db_path=path)
    rows = bus2.send_one(core_events.AuditQuery, core_events.AuditFilter())
    assert [row.device_aet for row in rows] == ['ANCIENT']


def test_cleanup_purges_and_reports(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    now = datetime.datetime.now(datetime.timezone.utc)
    AuditEventModel.create(
        timestamp=now - datetime.timedelta(days=40), category='assoc',
        event='accepted', device_aet='OLD', status='accepted')
    AuditEventModel.create(
        timestamp=now - datetime.timedelta(hours=1), category='assoc',
        event='accepted', device_aet='NEW', status='accepted')

    from tiny_pacs_audit.audit import AuditCleanup, AuditCleanupOptions
    removed = bus.send_one(
        AuditCleanup, AuditCleanupOptions(older_than_days=30))
    assert removed == 1
    assert [row.device_aet for row in _rows(bus)] == ['NEW']


def test_cleanup_refuses_zero_days(audit_bus: Any) -> None:
    _, audit, _, _ = audit_bus()
    assert audit is not None
    with pytest.raises(ValueError):
        audit.purge_older_than(0)


def test_column_values_truncated(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()
    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(
            category='x' * 50, event='y' * 50, device_aet='Z' * 50,
            username='u' * 100, status='s' * 50))
    row = _rows(bus)[0]
    assert len(row.category) == 16
    assert len(row.event) == 32
    assert len(row.device_aet or '') == 16
    assert len(row.username or '') == 64
    assert len(row.status) == 16


def test_non_serializable_details_dropped(audit_bus: Any) -> None:
    bus, _, _, _ = audit_bus()

    class Weird:
        def __str__(self) -> str:
            raise ValueError('no str for you')

    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(
            category='admin', event='device-add', details={'obj': Weird()}))
    assert _rows(bus, categories=['admin'])[0].details == '{}'


def test_details_values_bounded(audit_bus: Any) -> None:
    import tiny_pacs_audit.audit as audit_module
    bus, _, _, _ = audit_bus()
    ds = _ds(QueryRetrieveLevel='L' * 5000)
    bus.broadcast(core_events.Find,
                  core_events.FindPayload(_ctx(), ds, _session()))
    level = json.loads(_rows(bus, events=['find'])[0].details)['level']
    assert level is not None
    assert len(level) == audit_module.MAX_DETAILS_VALUE


def test_details_blob_capped(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import tiny_pacs_audit.audit as audit_module
    bus, _, _, _ = audit_bus()
    monkeypatch.setattr(audit_module, 'MAX_DETAILS_BLOB', 64)
    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(
            category='admin', event='device-add', details={'blob': 'x' * 500}))
    assert _rows(bus, categories=['admin'])[0].details == '{"truncated": true}'


# ----------------------------------------------------------------------
# Failure isolation
# ----------------------------------------------------------------------

def _always_fail(**kwargs: Any) -> Any:
    raise RuntimeError('audit db down')


def test_store_survives_write_failure(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _ = audit_bus()
    monkeypatch.setattr(AuditEventModel, 'create', _always_fail)
    payload = core_events.StorePayload(_ctx(), b'', _session())
    results = bus.broadcast(core_events.Store, payload)
    # The neutral success status is returned even though the write failed
    assert results == [statuses.SUCCESS]


def test_assoc_survives_write_failure(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _ = audit_bus()
    monkeypatch.setattr(AuditEventModel, 'create', _always_fail)
    with association_context():
        # Must not raise: an audit failure aborting ``Assoc`` would kill
        # the rejection chain and the association itself
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY'))
        bus.broadcast(core_events.AssocRejected,
                      core_events.AssocRejectedPayload(None, 'boom'))


def test_guard_swallows_construction_error(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _ = audit_bus()

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise ValueError('cannot build record')

    monkeypatch.setattr(AuditLog, '_write', boom)
    with association_context():
        bus.broadcast(core_events.Assoc, assoc_payload('MODALITY'))
    results = bus.broadcast(
        core_events.Store, core_events.StorePayload(_ctx(), b'', _session()))
    assert results == [statuses.SUCCESS]


def test_locked_sqlite_retries_then_writes(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _ = audit_bus()
    original = AuditEventModel.create
    calls = {'n': 0}

    def flaky(**kwargs: Any) -> Any:
        calls['n'] += 1
        if calls['n'] == 1:
            raise peewee.OperationalError('database is locked')
        return original(**kwargs)

    monkeypatch.setattr(AuditEventModel, 'create', flaky)
    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(category='admin', event='device-add'))
    assert calls['n'] == 2
    assert len(_rows(bus, categories=['admin'])) == 1


def test_locked_sqlite_drops_after_retry(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _ = audit_bus()
    calls = {'n': 0}

    def always_locked(**kwargs: Any) -> Any:
        calls['n'] += 1
        raise peewee.OperationalError('database is locked')

    monkeypatch.setattr(AuditEventModel, 'create', always_locked)
    bus.broadcast_nothrow(
        core_events.AuditRecord,
        core_events.AuditRecordPayload(category='admin', event='device-add'))
    # One initial attempt plus exactly one retry, then the record drops
    assert calls['n'] == 2
    assert len(_rows(bus, categories=['admin'])) == 0
