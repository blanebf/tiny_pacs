"""QIDO-RS tests: DICOM JSON shape, matching parameters, pagination,
degradation and parity with the C-FIND front-end on the same data.
"""
from typing import Any

import pydicom
import pytest
from pydicom import uid as pydicom_uid
from pynetdicom2 import fsm, uids
from tiny_pacs import events as core_events

from .conftest import make_dataset, seed_archive

#: Tags rendered by the DICOM JSON responses of this suite
TAG_PATIENT_NAME = '00100010'
TAG_PATIENT_ID = '00100020'
TAG_STUDY_DATE = '00080020'
TAG_ACCESSION = '00080050'
TAG_STUDY_UID = '0020000D'
TAG_SERIES_UID = '0020000E'
TAG_SOP_UID = '00080018'
TAG_MODALITY = '00080060'


@pytest.fixture
def qido(env: Any) -> Any:
    """An environment seeded with a small two-study archive."""
    environment = env(storage='none', collect_audit=True)
    study_uid = pydicom_uid.generate_uid()
    series_uid = pydicom_uid.generate_uid()
    first = make_dataset(
        patient_id='P1', patient_name='Doe^John',
        study_uid=study_uid, series_uid=series_uid,
        study_date='20240102', modality='CT', accession='A100'
    )
    second = make_dataset(
        patient_id='P1', patient_name='Doe^John',
        study_uid=study_uid, series_uid=series_uid, study_date='20240102'
    )
    other = make_dataset(
        patient_id='P2', patient_name='Smith^Ann',
        study_date='20231231', modality='MR', accession='A200'
    )
    for ds in (first, second, other):
        seed_archive(environment.bus, ds)
    environment.studies = {'first': first, 'second': second,
                           'other': other}
    return environment


def _values(item: dict[str, Any], tag: str) -> list[Any]:
    return list(item.get(tag, {}).get('Value', []))


# -----------------------------------------------------------------------
# DICOM JSON conformance
# -----------------------------------------------------------------------

def test_study_query_returns_dicom_json(qido: Any) -> None:
    response = qido.client.get('/studies')
    assert response.status == 200
    assert response.header('Content-Type') == 'application/dicom+json'
    items = response.json
    assert isinstance(items, list) and len(items) == 2
    by_study = {
        _values(item, TAG_STUDY_UID)[0]: item for item in items
    }
    item = by_study[str(qido.studies['first'].StudyInstanceUID)]
    # every key is a plain 8-hex-digit tag string
    assert all(len(tag) == 8
               and all(char in '0123456789abcdefABCDEF' for char in tag)
               for tag in item)
    # every element is a PS3.18 F JSON object with a VR
    assert all(isinstance(value, dict) and 'vr' in value
               for value in item.values())
    assert _values(item, TAG_PATIENT_ID) == ['P1']
    assert item[TAG_PATIENT_NAME] == {
        'vr': 'PN', 'Value': [{'Alphabetic': 'Doe^John'}]
    }
    assert _values(item, TAG_STUDY_DATE) == ['20240102']
    assert _values(item, TAG_ACCESSION) == ['A100']


def test_series_and_instance_queries(qido: Any) -> None:
    first = qido.studies['first']
    response = qido.client.get(
        f'/studies/{first.StudyInstanceUID}/series'
    )
    assert response.status == 200
    series_items = response.json
    assert len(series_items) == 1
    assert _values(series_items[0], TAG_SERIES_UID) \
        == [str(first.SeriesInstanceUID)]
    assert series_items[0][TAG_MODALITY]['vr'] == 'CS'

    response = qido.client.get(
        f'/studies/{first.StudyInstanceUID}/series/'
        f'{first.SeriesInstanceUID}/instances'
    )
    assert response.status == 200
    instance_items = response.json
    assert len(instance_items) == 2
    assert {_values(item, TAG_SOP_UID)[0] for item in instance_items} \
        == {str(first.SOPInstanceUID),
            str(qido.studies['second'].SOPInstanceUID)}


# -----------------------------------------------------------------------
# Matching parameters
# -----------------------------------------------------------------------

def test_patient_id_matching(qido: Any) -> None:
    response = qido.client.get('/studies?PatientID=P2')
    assert response.status == 200
    items = response.json
    assert len(items) == 1
    assert _values(items[0], TAG_PATIENT_ID) == ['P2']


def test_patient_name_substring_matching(qido: Any) -> None:
    response = qido.client.get('/studies?PatientName=smith')
    assert response.status == 200
    assert len(response.json) == 1


def test_study_date_range_matching(qido: Any) -> None:
    response = qido.client.get('/studies?StudyDate=20240101-20240103')
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get('/studies?StudyDate=20231201-')
    assert response.status == 200
    assert len(response.json) == 2
    response = qido.client.get('/studies?StudyDate=-20240101')
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get('/studies?StudyDate=20240102')
    assert response.status == 200
    assert len(response.json) == 1


def test_accession_and_modality_matching(qido: Any) -> None:
    response = qido.client.get('/studies?AccessionNumber=A200')
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get('/studies?ModalitiesInStudy=MR')
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get('/studies?ModalitiesInStudy=CT')
    assert response.status == 200
    assert len(response.json) == 1


def test_modality_keyword_matching(qido: Any) -> None:
    """``Modality`` is the level-correct keyword of series/instance
    searches and filters the same archive field as
    ``ModalitiesInStudy``."""
    first = qido.studies['first']
    response = qido.client.get(
        f'/studies/{first.StudyInstanceUID}/series?Modality=CT'
    )
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get(
        f'/studies/{first.StudyInstanceUID}/series?Modality=MR'
    )
    assert response.status == 204


def test_invalid_birth_date_answers_400(qido: Any) -> None:
    """Exact-match date fields are validated like ``StudyDate``: the
    same malformed input must not be a 400 here and a silent no-match
    there."""
    assert qido.client.get('/studies?PatientBirthDate=1970-01-02') \
        .status == 400
    assert qido.client.get('/studies?PatientBirthDate=20249999') \
        .status == 400


def test_uid_matching(qido: Any) -> None:
    first = qido.studies['first']
    response = qido.client.get(
        f'/studies?StudyInstanceUID={first.StudyInstanceUID}'
    )
    assert response.status == 200 and len(response.json) == 1
    response = qido.client.get(
        f'/studies?SOPInstanceUID={first.SOPInstanceUID}'
    )
    assert response.status == 200 and len(response.json) == 1


def test_pinned_parent_wins_over_contradicting_parameter(
        qido: Any) -> None:
    first = qido.studies['first']
    other = qido.studies['other']
    response = qido.client.get(
        f'/studies/{first.StudyInstanceUID}/series'
        f'?StudyInstanceUID={other.StudyInstanceUID}'
    )
    assert response.status == 200, 'the path pin wins'
    assert len(response.json) == 1
    assert _values(response.json[0], TAG_SERIES_UID) \
        == [str(first.SeriesInstanceUID)]


# -----------------------------------------------------------------------
# Pagination
# -----------------------------------------------------------------------

def test_limit_and_offset(qido: Any) -> None:
    response = qido.client.get('/studies?limit=1')
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get('/studies?limit=1&offset=1')
    assert response.status == 200
    assert len(response.json) == 1
    response = qido.client.get('/studies?offset=5')
    assert response.status == 204


# -----------------------------------------------------------------------
# Failures and degradation
# -----------------------------------------------------------------------

def test_empty_result_is_no_content(qido: Any) -> None:
    response = qido.client.get('/studies?PatientID=UNKNOWN')
    assert response.status == 204
    assert response.body == b''


@pytest.mark.parametrize('query', [
    'StudyDate=20241301',          # not a calendar date
    'StudyDate=20240101-2024',     # malformed range bound
    'StudyDate=a-b',               # not dates at all
    'limit=abc',
    'offset=xyz',
])
def test_invalid_parameters_answer_400(qido: Any, query: str) -> None:
    response = qido.client.get(f'/studies?{query}')
    assert response.status == 400


@pytest.mark.parametrize('accept', [
    'application/dicom+xml',
    'application/xml',
    'multipart/related; type="application/xml"',
])
def test_xml_only_accept_answers_406(qido: Any, accept: str) -> None:
    response = qido.client.get('/studies', headers={'Accept': accept})
    assert response.status == 406


@pytest.mark.parametrize('accept', [
    'application/dicom+json',
    'application/json',
    '*/*',
    'application/*',
    '',
])
def test_json_accept_variants(qido: Any, accept: str) -> None:
    headers = {'Accept': accept} if accept else {}
    response = qido.client.get('/studies', headers=headers)
    assert response.status == 200


def test_missing_archive_listener_degrades_to_503(env: Any) -> None:
    environment = env(pacs=False, storage='none')
    response = environment.client.get('/studies')
    assert response.status == 503
    assert 'not available' in response.text


def test_broken_listener_does_not_leak_stack(env: Any) -> None:
    environment = env(pacs=False, storage='none')

    def broken(_: Any) -> Any:
        raise RuntimeError('backend exploded')

    environment.bus.subscribe(core_events.ArchiveStudyQuery, broken)
    response = environment.client.get('/studies')
    assert response.status == 500
    assert 'Traceback' not in response.text


# -----------------------------------------------------------------------
# Audit emission
# -----------------------------------------------------------------------

def test_qido_queries_emit_audit_records(qido: Any) -> None:
    qido.client.get('/studies?PatientID=P1')
    qido.client.get('/studies?PatientID=UNKNOWN')
    records = [record for record in qido.audit
               if record.event == 'qido']
    assert len(records) == 2
    assert all(record.category == 'service' for record in records)
    assert records[0].details == {'level': 'study', 'returned': 1}
    assert all(record.status == 'success' for record in records)


# -----------------------------------------------------------------------
# Parity with the C-FIND front-end
# -----------------------------------------------------------------------

def test_qido_matches_c_find_on_the_same_data(qido: Any) -> None:
    """The same archive answers C-FIND and QIDO-RS with the same
    attribute values (parity of the two query front-ends)."""
    identifier = pydicom.Dataset()
    identifier.QueryRetrieveLevel = 'STUDY'
    identifier.PatientID = 'P1'
    identifier.PatientName = None
    identifier.StudyDate = None
    identifier.AccessionNumber = None
    identifier.StudyInstanceUID = None
    context = fsm.PContextDef(
        1, uids.STUDY_ROOT_FIND_SOP_CLASS,
        pydicom_uid.ImplicitVRLittleEndian
    )
    results = qido.bus.send_any(
        core_events.Find,
        core_events.FindPayload(context, identifier, None)
    )
    found = [ds for ds, status in results if not status.is_pending
             or ds.get('StudyInstanceUID')]
    found = [ds for ds in found if ds.get('StudyInstanceUID')]

    response = qido.client.get('/studies?PatientID=P1')
    assert response.status == 200
    items = response.json
    assert len(items) == len(found) == 1
    item = items[0]
    cfind = found[0]
    assert _values(item, TAG_STUDY_UID) \
        == [str(cfind.StudyInstanceUID)]
    assert _values(item, TAG_PATIENT_ID) == [str(cfind.PatientID)]
    assert _values(item, TAG_STUDY_DATE) \
        == [str(cfind.get('StudyDate') or '')] or not cfind.get(
            'StudyDate')
    assert _values(item, TAG_ACCESSION) \
        == [str(cfind.get('AccessionNumber') or '')]


def test_object_metadata_form_of_object_routes(qido: Any) -> None:
    """``GET /studies/{uid}`` with a JSON ``Accept`` answers the QIDO-RS
    metadata form (negotiated by the object route of the WADO module)."""
    first = qido.studies['first']
    response = qido.client.get(
        f'/studies/{first.StudyInstanceUID}',
        headers={'Accept': 'application/dicom+json'}
    )
    assert response.status == 200
    assert len(response.json) == 1
    assert _values(response.json[0], TAG_STUDY_UID) \
        == [str(first.StudyInstanceUID)]
    response = qido.client.get(
        f'/studies/{pydicom_uid.generate_uid()}',
        headers={'Accept': 'application/json'}
    )
    assert response.status == 204
