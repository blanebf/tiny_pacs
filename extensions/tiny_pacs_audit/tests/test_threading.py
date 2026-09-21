"""Threading-contract tests: correct per-thread association attribution.

The core :mod:`tiny_pacs.assoc_context` registry is indexed by thread id, on
the assumption that an association and every DIMSE-triggered event of that
association run on the same OS thread. These tests drive real concurrency
and assert the audit component reads the *right* context for each thread, so
service rows never attribute to the wrong device.

The write path is captured in memory (the insert is stubbed under a lock),
so the assertion is deterministic and independent of SQLite concurrency.
"""
import threading
from typing import Any

import pydicom
import pytest
from pydicom import uid
from tiny_pacs import events as core_events

from tiny_pacs_audit.audit import AuditLog

from .conftest import association_context


def _ds() -> pydicom.Dataset:
    ds = pydicom.Dataset()
    ds.SOPClassUID = '1.2.840.10008.5.1.4.1.1.7'
    ds.SOPInstanceUID = uid.generate_uid()
    return ds


def test_concurrent_contexts_attributed(
        audit_bus: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bus, _, _, _ = audit_bus()
    captured: list[dict[str, Any]] = []
    lock = threading.Lock()

    def fake_insert(self: AuditLog, row: dict[str, Any]) -> None:
        with lock:
            captured.append(dict(row))

    monkeypatch.setattr(AuditLog, '_insert', fake_insert)

    thread_count = 8
    iterations = 25
    aets = [f'THR{i}' for i in range(thread_count)]
    barrier = threading.Barrier(thread_count)

    def work(aet: str) -> None:
        # Force every thread to be inside its own association at once
        barrier.wait()
        with association_context(calling_aet=aet, username=aet.lower()):
            for _ in range(iterations):
                bus.broadcast(core_events.StoreDone, _ds())

    workers = [threading.Thread(target=work, args=(aet,)) for aet in aets]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    store_rows = [row for row in captured if row['event'] == 'store-done']
    assert len(store_rows) == thread_count * iterations
    # No context bled across threads: every AET owns exactly its own rows
    for aet in aets:
        mine = [row for row in store_rows if row['device_aet'] == aet]
        assert len(mine) == iterations
        assert all(row['username'] == aet.lower() for row in mine)
