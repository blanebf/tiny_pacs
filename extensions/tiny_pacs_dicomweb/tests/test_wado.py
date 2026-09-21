"""WADO-RS / WADO-URI retrieval tests.

Datasets are pushed through the STOW-RS endpoint (dogfooding the store
pipeline), then retrieved through WADO-RS and WADO-URI and byte-compared
against the files the storage actually holds.
"""
import io
from typing import Any

import pydicom
import pytest
from pydicom import uid as pydicom_uid
from tiny_pacs import events as core_events

from .conftest import (
    make_dataset,
    parse_multipart,
    seed_archive,
    stow_push,
)


@pytest.fixture
def wado(env: Any) -> Any:
    """An environment with two stored instances of one study/series."""
    environment = env(storage='file')
    first = make_dataset(patient_id='W1', study_date='20240102')
    second = make_dataset(
        patient_id='W1', study_uid=first.StudyInstanceUID,
        series_uid=first.SeriesInstanceUID
    )
    response = stow_push(environment.client, [first, second])
    assert response.status == 200, response.text
    raw: dict[str, bytes] = {}
    for ds in (first, second):
        stored = list(environment.bus.send_any(
            core_events.GetFiles, [str(ds.SOPInstanceUID)]
        ))
        assert len(stored) == 1
        with open(str(stored[0][2]), 'rb') as handle:
            raw[str(ds.SOPInstanceUID)] = handle.read()
    environment.first = first
    environment.second = second
    environment.raw = raw
    return environment


def _instance_path(env: Any, ds: pydicom.Dataset) -> str:
    return (f'/studies/{ds.StudyInstanceUID}/series/'
            f'{ds.SeriesInstanceUID}/instances/{ds.SOPInstanceUID}')


# -----------------------------------------------------------------------
# WADO-RS retrieval
# -----------------------------------------------------------------------

def test_study_retrieval_byte_compares(wado: Any) -> None:
    response = wado.client.get(f'/studies/{wado.first.StudyInstanceUID}')
    assert response.status == 200
    content_type = response.header('Content-Type') or ''
    assert content_type.startswith('multipart/related')
    assert 'type="application/dicom"' in content_type
    parts = parse_multipart(response)
    assert len(parts) == 2
    assert set(parts) == set(wado.raw.values()), \
        'every part is byte-identical to the stored file'
    assert response.body.endswith(b'--\r\n')


def test_instance_retrieval_defaults_to_single_part(wado: Any) -> None:
    response = wado.client.get(_instance_path(wado, wado.first))
    assert response.status == 200
    assert (response.header('Content-Type') or '').startswith(
        'application/dicom'
    )
    assert f'transfer-syntax={pydicom_uid.ExplicitVRLittleEndian}' \
        in (response.header('Content-Type') or '')
    assert response.body == wado.raw[str(wado.first.SOPInstanceUID)]


def test_instance_retrieval_honours_explicit_multipart(wado: Any) -> None:
    response = wado.client.get(
        _instance_path(wado, wado.first),
        headers={'Accept': 'multipart/related; type="application/dicom"'}
    )
    assert response.status == 200
    parts = parse_multipart(response)
    assert parts == [wado.raw[str(wado.first.SOPInstanceUID)]]


def test_series_retrieval_returns_the_series_instances(wado: Any) -> None:
    other = make_dataset(
        patient_id='W1', study_uid=wado.first.StudyInstanceUID
    )
    assert stow_push(wado.client, [other]).status == 200
    response = wado.client.get(
        f'/studies/{wado.first.StudyInstanceUID}/series/'
        f'{wado.first.SeriesInstanceUID}'
    )
    assert response.status == 200
    parts = parse_multipart(response)
    stored = [
        wado.raw[str(wado.first.SOPInstanceUID)],
        wado.raw[str(wado.second.SOPInstanceUID)]
    ]
    assert set(parts) == set(stored)


@pytest.mark.parametrize('path', [
    '/studies/1.2.3.4',
    '/studies/1.2.3.4/series/1.2.3.5',
    '/studies/1.2.3.4/series/1.2.3.5/instances/1.2.3.6',
])
def test_unknown_objects_answer_404(wado: Any, path: str) -> None:
    assert wado.client.get(path).status == 404


def test_transfer_syntax_negotiation(wado: Any) -> None:
    path = _instance_path(wado, wado.first)
    matching = (
        'multipart/related; type="application/dicom"; '
        f'transfer-syntax={pydicom_uid.ExplicitVRLittleEndian}'
    )
    assert wado.client.get(
        path, headers={'Accept': matching}
    ).status == 200
    other = (
        'multipart/related; type="application/dicom"; '
        'transfer-syntax=1.2.840.10008.1.2.4.50'
    )
    assert wado.client.get(path, headers={'Accept': other}).status == 406
    assert wado.client.get(
        path, headers={'Accept': 'application/dicom+xml'}
    ).status == 406


def test_transfer_syntax_wildcard_is_unconstrained(wado: Any) -> None:
    path = _instance_path(wado, wado.first)
    wildcard = 'multipart/related; type="application/dicom"; ' \
        'transfer-syntax=*'
    assert wado.client.get(
        path, headers={'Accept': wildcard}
    ).status == 200


def test_mixed_accept_ranges_are_not_refused(wado: Any) -> None:
    """RFC 9110: a request listing several acceptable representations
    must not be refused when any one of them is producible."""
    path = _instance_path(wado, wado.first)
    mixed = 'application/dicom+json, application/dicom'
    response = wado.client.get(path, headers={'Accept': mixed})
    assert response.status == 200
    assert response.body == wado.raw[str(wado.first.SOPInstanceUID)]


def test_multipart_type_parameter_is_validated(wado: Any) -> None:
    """A multipart range whose ``type`` parameter names an encoding we
    cannot produce is not silently answered with DICOM parts."""
    path = _instance_path(wado, wado.first)
    response = wado.client.get(
        path,
        headers={'Accept': 'multipart/related; '
                           'type="application/dicom+xml"'}
    )
    assert response.status == 406


def test_in_memory_storage_round_trip(env: Any) -> None:
    """``InMemoryStorage`` keeps datasets in memory; WADO re-serializes
    them as readable PS3.10 files."""
    environment = env(storage='memory')
    ds = make_dataset(patient_id='M1')
    assert stow_push(environment.client, [ds]).status == 200
    response = environment.client.get(_instance_path(environment, ds))
    assert response.status == 200
    parsed = pydicom.dcmread(io.BytesIO(response.body))
    assert parsed.SOPInstanceUID == ds.SOPInstanceUID
    assert str(parsed.PatientID) == 'M1'
    assert parsed.file_meta.TransferSyntaxUID \
        == pydicom_uid.ExplicitVRLittleEndian


def test_missing_storage_listener_degrades_to_503(env: Any) -> None:
    """Instance rows resolve (archive present) but nothing can fetch the
    files: the retrieval degrades instead of raising."""
    environment = env(pacs=True, storage='none')
    ds = make_dataset(patient_id='N1')
    seed_archive(environment.bus, ds)
    response = environment.client.get(_instance_path(environment, ds))
    assert response.status == 503
    assert 'not available' in response.text


# -----------------------------------------------------------------------
# WADO-URI
# -----------------------------------------------------------------------

def _wado_uri(env: Any, ds: pydicom.Dataset, extra: str = '') -> str:
    return (f'/wado?requestType=WADO&studyUID={ds.StudyInstanceUID}'
            f'&seriesUID={ds.SeriesInstanceUID}'
            f'&objectUID={ds.SOPInstanceUID}{extra}')


def test_wado_uri_single_object(wado: Any) -> None:
    response = wado.client.get(
        _wado_uri(wado, wado.first, '&contentType=application/dicom')
    )
    assert response.status == 200
    assert (response.header('Content-Type') or '') \
        .startswith('application/dicom')
    assert response.body == wado.raw[str(wado.first.SOPInstanceUID)]


def test_wado_uri_study_level_multipart(wado: Any) -> None:
    response = wado.client.get(
        f'/wado?requestType=WADO&studyUID={wado.first.StudyInstanceUID}'
        f'&contentType=multipart/related'
    )
    assert response.status == 200
    parts = parse_multipart(response)
    assert set(parts) == set(wado.raw.values())


def test_wado_uri_defaults_to_single_dicom(wado: Any) -> None:
    response = wado.client.get(_wado_uri(wado, wado.first))
    assert response.status == 200
    assert response.body == wado.raw[str(wado.first.SOPInstanceUID)]


def test_wado_uri_validation(wado: Any) -> None:
    study = wado.first.StudyInstanceUID
    sop = wado.first.SOPInstanceUID
    assert wado.client.get('/wado').status == 400, 'no requestType'
    assert wado.client.get('/wado?requestType=WADOURI').status == 400
    assert wado.client.get('/wado?requestType=WADO').status == 400, \
        'no studyUID'
    unsupported = (
        f'/wado?requestType=WADO&studyUID={study}&objectUID={sop}'
    )
    assert wado.client.get(unsupported + '&contentType=image/png') \
        .status == 406
    assert wado.client.get(unsupported + '&frameNumber=1').status == 406
    assert wado.client.get(
        unsupported + '&transferSyntax=1.2.840.10008.1.2.4.90'
    ).status == 406
    assert wado.client.get(
        unsupported + f'&transferSyntax={pydicom_uid.ExplicitVRLittleEndian}'
    ).status == 200
    # PS3.18 §8.7.3.6: the parameter is a comma-separated UID list or
    # the '*' wildcard; any listed UID matching the stored syntax serves
    assert wado.client.get(unsupported + '&transferSyntax=*').status == 200
    assert wado.client.get(
        unsupported + '&transferSyntax=1.2.840.10008.1.2.4.90,'
        f'{pydicom_uid.ExplicitVRLittleEndian}'
    ).status == 200
    assert wado.client.get(
        unsupported + '&transferSyntax=1.2.840.10008.1.2.4.90,'
        '1.2.840.10008.1.2.4.91'
    ).status == 406


def test_wado_uri_unknown_object_is_404(wado: Any) -> None:
    response = wado.client.get(
        '/wado?requestType=WADO&studyUID=1.2.3.4'
        '&objectUID=1.2.3.5&contentType=application/dicom'
    )
    assert response.status == 404


def test_wado_uri_single_content_type_on_multi_object_target(
        wado: Any
) -> None:
    response = wado.client.get(
        f'/wado?requestType=WADO&studyUID={wado.first.StudyInstanceUID}'
        f'&contentType=application/dicom'
    )
    assert response.status == 406
