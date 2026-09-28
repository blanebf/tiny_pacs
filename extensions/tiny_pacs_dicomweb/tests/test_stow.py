"""STOW-RS tests: storage pipeline, per-instance failure semantics,
audit emission and request validation.
"""
import io
from typing import Any

import pydicom
import pytest
from pydicom import uid as pydicom_uid
from pydicom.dataelem import DataElement
from pydicom.filebase import DicomFileLike
from pydicom.filewriter import write_dataset
from pynetdicom2 import fsm, uids
from tiny_pacs import events as core_events

from .conftest import (
    dicom_file_bytes,
    make_dataset,
    multipart_body,
    seed_archive,
    stow_push,
)


def raw_dataset_bytes(ds: pydicom.Dataset) -> bytes:
    """Serializes a dataset as implicit-VR elements without file meta."""
    buffer = io.BytesIO()
    wrapper = DicomFileLike(buffer)
    wrapper.is_implicit_VR = True
    wrapper.is_little_endian = True
    stripped = pydicom.Dataset()
    for element in ds:
        stripped.add(DataElement(element.tag, element.VR, element.value))
    write_dataset(wrapper, stripped)
    return buffer.getvalue()


@pytest.fixture
def stow(env: Any) -> Any:
    """An environment collecting ``StoreDataset`` and ``AuditRecord``."""
    environment = env(storage='file', collect_audit=True)
    payloads: list[core_events.StoreDatasetPayload] = []

    def collect(payload: core_events.StoreDatasetPayload) -> None:
        payloads.append(payload)

    environment.bus.subscribe(core_events.StoreDataset, collect)
    environment.stowed = payloads
    return environment


def _referenced(response: Any) -> list[dict[str, Any]]:
    return list(response.json.get('00081199', {}).get('Value', []))


def _failed(response: Any) -> list[dict[str, Any]]:
    return list(response.json.get('00081198', {}).get('Value', []))


# -----------------------------------------------------------------------
# Happy paths
# -----------------------------------------------------------------------

def test_store_two_instances(stow: Any) -> None:
    first = make_dataset(patient_id='S1')
    second = make_dataset(
        patient_id='S1', study_uid=first.StudyInstanceUID,
        series_uid=first.SeriesInstanceUID
    )
    response = stow_push(stow.client, [first, second])
    assert response.status == 200
    assert response.header('Content-Type') == 'application/dicom+json'
    referenced = _referenced(response)
    assert len(referenced) == 2
    assert not _failed(response)
    assert {
        entry['00081155']['Value'][0] for entry in referenced
    } == {str(first.SOPInstanceUID), str(second.SOPInstanceUID)}
    # the study-level retrieve URL is announced
    assert response.json['00081190']['vr'] == 'UR'
    retrieve_url = response.json['00081190']['Value'][0]
    assert retrieve_url == (
        f'http://localhost/dicomweb/studies/{first.StudyInstanceUID}'
    )
    # every stored file holds the pushed dataset verbatim (the file
    # meta information is storage-owned, as in the DIMSE path)
    from tiny_pacs_dicomweb.stow import _elements_after_meta
    for ds in (first, second):
        stored = list(stow.bus.send_any(
            core_events.GetFiles, [str(ds.SOPInstanceUID)]
        ))
        assert len(stored) == 1
        pushed = dicom_file_bytes(ds)
        with open(str(stored[0][2]), 'rb') as handle:
            raw = handle.read()
        assert _elements_after_meta(raw) == _elements_after_meta(pushed)
        assert pydicom.dcmread(io.BytesIO(raw)) \
            == pydicom.dcmread(io.BytesIO(pushed))


def test_stored_instances_feed_the_archive(stow: Any) -> None:
    """STOW pushes are visible to C-FIND and QIDO-RS alike."""
    ds = make_dataset(patient_id='S2', accession='STOW1')
    assert stow_push(stow.client, [ds]).status == 200

    identifier = pydicom.Dataset()
    identifier.QueryRetrieveLevel = 'STUDY'
    identifier.PatientID = 'S2'
    identifier.AccessionNumber = None
    identifier.StudyInstanceUID = None
    context = fsm.PContextDef(
        1, uids.STUDY_ROOT_FIND_SOP_CLASS,
        pydicom_uid.ImplicitVRLittleEndian
    )
    results = stow.bus.send_any(
        core_events.Find,
        core_events.FindPayload(context, identifier, None)
    )
    found = [row for row, _ in results if row.get('StudyInstanceUID')]
    assert len(found) == 1
    assert str(found[0].StudyInstanceUID) == str(ds.StudyInstanceUID)

    response = stow.client.get('/studies?PatientID=S2')
    assert response.status == 200
    assert len(response.json) == 1


def test_store_dataset_events_carry_the_stow_origin(stow: Any) -> None:
    ds = make_dataset(patient_id='S3')
    assert stow_push(stow.client, [ds]).status == 200
    relevant = [payload for payload in stow.stowed
                if str(payload.ds.SOPInstanceUID)
                == str(ds.SOPInstanceUID)]
    assert len(relevant) == 1
    assert relevant[0].origin == 'stow'
    assert relevant[0].transfer_syntax \
        == str(pydicom_uid.ExplicitVRLittleEndian)


def test_targeted_study_form(stow: Any) -> None:
    ds = make_dataset(patient_id='S4')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = stow.client.post(f'/studies/{ds.StudyInstanceUID}',
                                body, content_type)
    assert response.status == 200
    assert len(_referenced(response)) == 1
    assert response.json['00081190']['Value'][0].endswith(
        f'/studies/{ds.StudyInstanceUID}'
    )


def test_raw_dataset_part_with_transfer_syntax_parameter(stow: Any) -> None:
    """A part without the PS3.10 header is stored using its declared
    ``transfer-syntax`` parameter (implicit VR little endian here)."""
    ds = make_dataset(patient_id='S5')
    body, _ = multipart_body([(raw_dataset_bytes(ds),
                               'application/dicom; '
                               'transfer-syntax=1.2.840.10008.1.2')])
    content_type = ('multipart/related; type="application/dicom"; '
                    'boundary="tiny-pacs-test-boundary"')
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 200, response.text
    assert len(_referenced(response)) == 1
    stored = list(stow.bus.send_any(
        core_events.GetFiles, [str(ds.SOPInstanceUID)]
    ))
    assert len(stored) == 1
    assert str(stored[0][1]) == '1.2.840.10008.1.2'
    with open(str(stored[0][2]), 'rb') as handle:
        parsed = pydicom.dcmread(handle)
    assert str(parsed.PatientID) == 'S5'
    assert parsed.file_meta.TransferSyntaxUID \
        == pydicom_uid.ImplicitVRLittleEndian


# -----------------------------------------------------------------------
# Per-instance failure semantics
# -----------------------------------------------------------------------

def test_one_broken_part_fails_only_itself(stow: Any) -> None:
    good = make_dataset(patient_id='S6')
    parts = [
        (dicom_file_bytes(good), 'application/dicom'),
        (b'\x01\x02\x03not-a-dicom-object', 'application/dicom'),
    ]
    body, content_type = multipart_body(parts)
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 202, 'partial success per PS3.18'
    assert len(_referenced(response)) == 1
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]
    records = [record for record in stow.audit
               if record.event == 'stow']
    assert records and records[-1].details['instances'] == 1
    assert records[-1].details['failures'] == 1


def test_all_parts_broken_never_answers_500(stow: Any) -> None:
    body, content_type = multipart_body(
        [(b'garbage-one', 'application/dicom'),
         (b'garbage-two', 'application/dicom')]
    )
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 409, 'all-failed per PS3.18, never 500'
    assert not _referenced(response)
    assert len(_failed(response)) == 2
    records = [record for record in stow.audit
               if record.event == 'stow']
    assert records and records[-1].status == 'failure'


def test_unsupported_part_media_type_is_a_part_failure(stow: Any) -> None:
    good = make_dataset(patient_id='S7')
    body, content_type = multipart_body([
        (dicom_file_bytes(good), 'application/dicom'),
        (b'hello', 'text/plain'),
    ])
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 202
    assert len(_referenced(response)) == 1
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]


def test_target_study_mismatch_is_a_part_failure(stow: Any) -> None:
    ds = make_dataset(patient_id='S8')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = stow.client.post(
        f'/studies/{pydicom_uid.generate_uid()}', body, content_type
    )
    assert response.status == 409
    assert not _referenced(response)
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]
    assert failed[0]['00081155']['Value'] == [str(ds.SOPInstanceUID)]


def test_duplicate_instance_is_refused_without_overwrite(stow: Any) -> None:
    ds = make_dataset(patient_id='S9')
    assert stow_push(stow.client, [ds]).status == 200
    response = stow_push(stow.client, [ds])
    assert response.status == 409
    assert not _referenced(response)
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]


def test_duplicate_instance_is_replaced_with_overwrite(env: Any) -> None:
    environment = env(storage='file', overwrite=True)
    ds = make_dataset(patient_id='S10')
    assert stow_push(environment.client, [ds]).status == 200
    response = stow_push(environment.client, [ds])
    assert response.status == 200
    assert len(_referenced(response)) == 1
    assert not _failed(response)
    stored = list(environment.bus.send_any(
        core_events.GetFiles, [str(ds.SOPInstanceUID)]
    ))
    assert len(stored) == 1


# -----------------------------------------------------------------------
# Hardened part parsing
# -----------------------------------------------------------------------

def test_deflated_part_is_refused(stow: Any) -> None:
    """A deflated-transfer-syntax part is refused without inflating it:
    unbounded decompression would defeat the request body cap."""
    ds = make_dataset(patient_id='S20')
    payload = dicom_file_bytes(
        ds, pydicom_uid.DeflatedExplicitVRLittleEndian
    )
    body, content_type = multipart_body(
        [(payload, 'application/dicom')]
    )
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 409
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]
    assert not list(stow.bus.send_any(
        core_events.GetFiles, [str(ds.SOPInstanceUID)]
    ))


def test_traversal_sop_uid_is_refused(stow: Any) -> None:
    """A part whose SOP Instance UID is not a well-formed DICOM UID is
    refused before reaching the store pipeline: the UID becomes the
    storage file name, so an unvalidated value would write outside the
    storage directory (the core contains reads/deletes, not writes)."""
    ds = make_dataset(patient_id='S30', sop_uid='../../outside/pwned')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 409
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]
    assert '00081155' not in failed[0], \
        'the malformed UID must not be echoed into the failure entry'
    assert not stow.stowed, 'nothing may reach the archive'
    assert not list(stow.bus.send_any(
        core_events.GetFiles, ['../../outside/pwned']
    ))


def test_traversal_in_study_uid_is_refused(stow: Any) -> None:
    """Study/Series UIDs are reflected into retrieve URLs and archive
    rows: malformed values are refused like the SOP Instance UID."""
    ds = make_dataset(patient_id='S31', study_uid='../escape/study')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 409
    assert _failed(response)[0]['00081197']['Value'] == [0x0110]
    assert not stow.stowed


def test_invalid_target_study_uid_is_refused(stow: Any) -> None:
    """A target resource whose Study Instance UID is not a well-formed
    UID is refused outright."""
    ds = make_dataset(patient_id='S32')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = stow.client.post('/studies/not%20a%20uid', body,
                                content_type)
    assert response.status == 400
    assert not stow.stowed


def test_mixed_case_boundary_is_accepted(stow: Any) -> None:
    """RFC 2046 multipart boundaries are case-sensitive: an uppercase
    boundary must parse (regression: the Content-Type was lowercased
    before the MIME parser saw it)."""
    ds = make_dataset(patient_id='S33')
    boundary = 'TinyPacsMixedCase99'
    body = (f'--{boundary}\r\n'
            'Content-Type: application/dicom\r\n\r\n').encode('ascii') \
        + dicom_file_bytes(ds) \
        + f'\r\n--{boundary}--\r\n'.encode('ascii')
    content_type = ('multipart/related; type="application/dicom"; '
                    f'boundary={boundary}')
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 200, response.text
    assert len(_referenced(response)) == 1


def test_lying_meta_group_length_is_refused(stow: Any) -> None:
    """A part whose (0002,0000) group length disagrees with the actual
    meta attributes is refused instead of persisted corrupt."""
    ds = make_dataset(patient_id='S21')
    payload = bytearray(dicom_file_bytes(ds))
    # the group-length value sits at offset 140 (132 + tag 4 + VR 2 + len 2)
    declared = int.from_bytes(payload[140:144], 'little')
    payload[140:144] = (declared - 12).to_bytes(4, 'little')
    body, content_type = multipart_body(
        [(bytes(payload), 'application/dicom')]
    )
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 409
    failed = _failed(response)
    assert len(failed) == 1
    assert failed[0]['00081197']['Value'] == [0x0110]


def test_malformed_transfer_syntax_parameter_is_refused(
        stow: Any) -> None:
    """A raw-dataset part declaring a non-UID ``transfer-syntax`` is
    refused per-part (the value would otherwise reach the storage
    records and WADO response headers)."""
    ds = make_dataset(patient_id='S22')
    body, content_type = multipart_body(
        [(raw_dataset_bytes(ds), 'application/dicom; '
                                 'transfer-syntax=not a uid!')]
    )
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 409
    assert _failed(response)[0]['00081197']['Value'] == [0x0110]


def test_part_without_content_type_inherits_the_container_type(
        stow: Any) -> None:
    """RFC 2387: a part without its own Content-Type inherits the
    request's ``type="application/dicom"`` and stores normally."""
    ds = make_dataset(patient_id='S23')
    boundary = 'inherit-boundary'
    body = (
        f'--{boundary}\r\n\r\n'.encode('ascii')
        + dicom_file_bytes(ds)
        + f'\r\n--{boundary}--\r\n'.encode('ascii')
    )
    response = stow.client.post(
        '/studies', body,
        f'multipart/related; type="application/dicom"; '
        f'boundary="{boundary}"'
    )
    assert response.status == 200, response.text
    assert len(_referenced(response)) == 1
    stored = list(stow.bus.send_any(
        core_events.GetFiles, [str(ds.SOPInstanceUID)]
    ))
    assert len(stored) == 1


# -----------------------------------------------------------------------
# Request validation
# -----------------------------------------------------------------------

def test_wrong_request_media_type_answers_415(stow: Any) -> None:
    response = stow.client.post('/studies', b'anything',
                                'application/x-not-dicom')
    assert response.status == 415


def test_missing_body_answers_411(stow: Any) -> None:
    response = stow.client.post(
        '/studies', None, 'multipart/related; boundary="x"'
    )
    assert response.status == 411


def test_missing_boundary_answers_400(stow: Any) -> None:
    response = stow.client.post('/studies', b'--x--\r\n',
                                'multipart/related')
    assert response.status == 400


def test_empty_multipart_answers_400(stow: Any) -> None:
    body, content_type = multipart_body([])
    response = stow.client.post('/studies', body, content_type)
    assert response.status == 400


def test_oversized_body_answers_413(env: Any) -> None:
    """The whole body is capped at ``max_part_size`` (checked against
    ``Content-Length`` before any parsing happens)."""
    environment = env(storage='file', config={'max_part_size': 4096})
    big = make_dataset(patient_id='BIG')
    big.PatientComments = 'x' * 8192
    response = stow_push(environment.client, [big])
    assert response.status == 413


def test_xml_only_accept_answers_406(stow: Any) -> None:
    ds = make_dataset(patient_id='S11')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = stow.client.post('/studies', body, content_type,
                                headers={'Accept': 'application/xml'})
    assert response.status == 406


def test_missing_backends_degrade_to_503(env: Any) -> None:
    environment = env(pacs=False, storage='none')
    ds = make_dataset(patient_id='S12')
    body, content_type = multipart_body(
        [(dicom_file_bytes(ds), 'application/dicom')]
    )
    response = environment.client.post('/studies', body, content_type)
    assert response.status == 503
    assert 'not available' in response.text


# -----------------------------------------------------------------------
# Audit
# -----------------------------------------------------------------------

def test_stow_emits_service_audit_records(stow: Any) -> None:
    ds = make_dataset(patient_id='S13')
    assert stow_push(stow.client, [ds]).status == 200
    records = [record for record in stow.audit
               if record.event == 'stow']
    assert len(records) == 1
    record = records[0]
    assert record.category == 'service'
    assert record.status == 'success'
    assert record.details['origin'] == 'stow'
    assert record.details['instances'] == 1
    assert record.details['failures'] == 0
    assert record.details['study_instance_uid'] \
        == str(ds.StudyInstanceUID)


def test_archive_records_survive_a_store_without_storage(env: Any) -> None:
    """Sanity check of the seeding helper used by the query suites: the
    archive rows exist even though no storage materialized files."""
    environment = env(pacs=True, storage='none')
    ds = make_dataset(patient_id='S14')
    seed_archive(environment.bus, ds)
    response = environment.client.get('/studies?PatientID=S14')
    assert response.status == 200
    assert len(response.json) == 1
