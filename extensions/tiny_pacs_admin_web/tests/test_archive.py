"""Tests of the archive browser section.

Unit coverage of the search-form → ``ArchiveFilter`` translation (all
fields, empty form, invalid dates) and the pagination arithmetic, plus
in-process browsing of stored datasets (drill-down with pre-filled UID
filters, substring and date search, pagination), the viewer's read-only
access and the "not available" degradation without an archive listener.
The stored-dataset walkthrough over the shared ``HttpServer`` lives in
``test_e2e.py``.
"""
import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import pydicom
import pytest
import trolleybus
from conftest import WSGIClient, add_user, grant
from pydicom import uid
from pynetdicom2 import uids
from tiny_pacs import events as core_events

from tiny_pacs_admin_web import web as web_module

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


def _admin_client(env: SimpleNamespace, username: str = 'alice',
                  password: str = 'secret') -> WSGIClient:
    """Creates a user + admin grant and returns a logged-in client."""
    add_user(env.bus, username, password)
    grant(username, 'admin')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': username, 'password': password})
    assert response.status == 200
    return client


def _viewer_client(env: SimpleNamespace, username: str = 'bob',
                   password: str = 'wonder') -> WSGIClient:
    add_user(env.bus, username, password)
    grant(username, 'viewer')
    client = WSGIClient(env.app)
    response = client.post('/login',
                           {'username': username, 'password': password})
    assert response.status == 200
    return client


def _store(env: SimpleNamespace, patient_id: str,
           name: str = 'Doe^John', study_date: str = '20200115',
           modality: str = 'CT', accession: str = 'ACC1'
           ) -> pydicom.Dataset:
    """Records one dataset through the real store pipeline (bus only)."""
    ds = pydicom.Dataset()
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.PatientID = patient_id
    ds.PatientName = name
    ds.PatientBirthDate = '19800101'
    ds.PatientSex = 'M'
    ds.StudyDate = study_date
    ds.StudyTime = '101010'
    ds.AccessionNumber = accession
    ds.StudyDescription = 'Chest'
    ds.Modality = modality
    ds.SeriesNumber = '1'
    ds.InstanceNumber = '1'
    ds.StudyInstanceUID = uid.generate_uid()
    ds.SeriesInstanceUID = uid.generate_uid()
    ds.SOPInstanceUID = uid.generate_uid()
    ds.SOPClassUID = uids.BASIC_TEXT_SR_STORAGE
    env.bus.send_one(
        core_events.StoreDataset,
        core_events.StoreDatasetPayload(
            ds=ds, transfer_syntax=str(uid.ImplicitVRLittleEndian)
        )
    )
    return ds


# -----------------------------------------------------------------------
# Unit: form -> ArchiveFilter translation
# -----------------------------------------------------------------------

def test_form_translation_all_fields() -> None:
    values = {
        'patient_id': 'P1',
        'patient_name': 'Doe',
        'patient_birth_date': '19800101',
        'accession_number': 'ACC1',
        'study_date_from': '20200101',
        'study_date_to': '20201231',
        'modality': 'CT',
        'study_instance_uid': '1.2.3',
        'series_instance_uid': '1.2.3.4',
        'sop_instance_uid': '1.2.3.4.5',
    }
    flt = web_module._archive_filter(values, 100)
    assert flt == core_events.ArchiveFilter(
        patient_id='P1', patient_name='Doe', patient_birth_date='19800101',
        accession_number='ACC1', study_date_from='20200101',
        study_date_to='20201231', modality='CT',
        study_instance_uid='1.2.3', series_instance_uid='1.2.3.4',
        sop_instance_uid='1.2.3.4.5',
        limit=web_module.ARCHIVE_PAGE_SIZE, offset=100
    )


def test_form_translation_empty() -> None:
    empty = dict.fromkeys(web_module.ARCHIVE_FORM_FIELDS, '')
    flt = web_module._archive_filter(empty, 0)
    assert flt == core_events.ArchiveFilter(
        limit=web_module.ARCHIVE_PAGE_SIZE, offset=0
    )


@pytest.mark.parametrize('value,expected', [
    ('2020-01-15', '20200115'),
    ('20200115', '20200115'),
    (' 2020-01-15 ', '20200115'),
    ('19800229', '19800229'),
])
def test_normalize_da_accepts(value: str, expected: str) -> None:
    assert web_module._normalize_da(value, 'Study date from') == expected


@pytest.mark.parametrize('value', [
    '2020', '2020-1-5', '20201332', '2021-02-29', '00000000',
    'abcdefgh', '2020-01-155', 'yesterday',
])
def test_normalize_da_rejects(value: str) -> None:
    with pytest.raises(ValueError, match='Invalid'):
        web_module._normalize_da(value, 'Study date from')


def test_normalize_validates_every_date_field() -> None:
    raw = dict.fromkeys(web_module.ARCHIVE_FORM_FIELDS, '')
    raw['patient_birth_date'] = 'not-a-date'
    with pytest.raises(ValueError, match='Birth date'):
        web_module._archive_normalize(raw)
    raw['patient_birth_date'] = ''
    raw['study_date_to'] = '2020-13-01'
    with pytest.raises(ValueError, match='Study date to'):
        web_module._archive_normalize(raw)


def test_normalize_converts_dates() -> None:
    raw = dict.fromkeys(web_module.ARCHIVE_FORM_FIELDS, '')
    raw['study_date_from'] = '2020-01-15'
    raw['patient_id'] = 'P1'
    values = web_module._archive_normalize(raw)
    assert values['study_date_from'] == '20200115'
    assert values['patient_id'] == 'P1'


# -----------------------------------------------------------------------
# Unit: pagination arithmetic
# -----------------------------------------------------------------------

def test_pagination_empty() -> None:
    size = web_module.ARCHIVE_PAGE_SIZE
    assert web_module._archive_pagination(0, 0) == {
        'total': 0, 'offset': 0, 'page': 1, 'pages': 1,
        'has_prev': False, 'has_next': False,
        'prev_offset': 0, 'next_offset': size
    }


def test_pagination_middle_page() -> None:
    size = web_module.ARCHIVE_PAGE_SIZE
    page = web_module._archive_pagination(size * 2 + 7, size)
    assert page['page'] == 2
    assert page['pages'] == 3
    assert page['has_prev'] and page['has_next']
    assert page['prev_offset'] == 0
    assert page['next_offset'] == size * 2


def test_pagination_exact_multiple() -> None:
    size = web_module.ARCHIVE_PAGE_SIZE
    page = web_module._archive_pagination(size * 2, size)
    assert page['page'] == 2 and page['pages'] == 2
    assert page['has_next'] is False
    assert page['has_prev'] is True


def test_pagination_offset_beyond_total() -> None:
    size = web_module.ARCHIVE_PAGE_SIZE
    page = web_module._archive_pagination(12, size * 10)
    assert page['pages'] == 1
    assert page['page'] == 1, 'the displayed page is clamped'
    assert page['has_prev'] is True
    assert page['has_next'] is False
    assert page['prev_offset'] == size * 9


@pytest.mark.parametrize('raw,expected', [
    (None, 0), ('', 0), ('0', 0), ('50', 50), ('-5', 0), ('abc', 0),
    (' 25 ', 25),
])
def test_offset_parsing(raw: Any, expected: int) -> None:
    assert web_module._archive_offset(raw) == expected


# -----------------------------------------------------------------------
# Browsing stored datasets (real PACS listener, in-process WSGI)
# -----------------------------------------------------------------------

def test_patients_table_and_search(console: Callable[..., Any]) -> None:
    env = console(with_pacs=True)
    client = _admin_client(env)
    _store(env, 'P1')
    _store(env, 'P2', name='Smith^Ann', modality='DX')
    page = client.get('/archive/')
    assert page.status == 200
    assert 'Archive' in page.text
    assert page.text.count('<div class="field">') \
        == len(web_module.ARCHIVE_FORM_FIELDS), \
        'every label+input pair is one wrapped grid cell (layout)'
    assert 'P1' in page.text and 'P2' in page.text
    assert 'Doe^John' in page.text and 'Smith^Ann' in page.text
    assert '2 match(es)' in page.text
    assert '/archive/studies/P1' in page.text
    # Substring search on the name
    page = client.get('/archive/?patient_name=Smi')
    assert 'P2' in page.text
    assert '/archive/studies/P1' not in page.text
    # Exact filters
    page = client.get('/archive/?patient_id=P1')
    assert 'P1' in page.text and 'P2' not in page.text
    page = client.get('/archive/?modality=DX')
    assert 'P2' in page.text and 'P1' not in page.text
    page = client.get('/archive/?study_date_from=2020-01-01'
                      '&study_date_to=2020-12-31')
    assert 'P1' in page.text and 'P2' in page.text
    page = client.get('/archive/?study_date_from=2021-01-01')
    assert 'No matching records' in page.text


def test_invalid_date_rerenders_with_error(console: Callable[..., Any]
                                           ) -> None:
    env = console(with_pacs=True)
    client = _admin_client(env)
    _store(env, 'P1')
    page = client.get('/archive/?study_date_from=not-a-date')
    assert page.status == 200
    assert 'Invalid Study date from' in page.text
    assert 'Traceback' not in page.text
    assert 'value="not-a-date"' in page.text, 'the form keeps the input'


def test_drill_down_prefills_uid_filters(console: Callable[..., Any]
                                         ) -> None:
    env = console(with_pacs=True)
    client = _admin_client(env)
    ds = _store(env, 'P1')
    studies = client.get(f'/archive/studies/{quote("P1")}')
    assert studies.status == 200
    assert ds.StudyInstanceUID in studies.text
    assert 'ACC1' in studies.text
    assert f'/archive/series/{ds.StudyInstanceUID}' in studies.text
    assert 'value="P1"' in studies.text, 'the pinned patient ID'
    assert 'readonly' in studies.text

    series = client.get(f'/archive/series/{ds.StudyInstanceUID}')
    assert series.status == 200
    assert ds.SeriesInstanceUID in series.text
    assert 'CT' in series.text
    assert f'/archive/instances/{ds.SeriesInstanceUID}' in series.text
    assert f'value="{ds.StudyInstanceUID}"' in series.text

    instances = client.get(f'/archive/instances/{ds.SeriesInstanceUID}')
    assert instances.status == 200
    assert ds.SOPInstanceUID in instances.text
    assert f'value="{ds.SeriesInstanceUID}"' in instances.text
    # Deepest level: no further drill-down links
    assert 'Back to patients' in instances.text


def test_bare_prefix_redirects_to_section_root(console: Callable[..., Any]
                                               ) -> None:
    env = console(with_pacs=True)
    client = _admin_client(env)
    response = client.get('/archive', follow=False)
    assert response.status == 303
    location = response.header('Location')
    assert location is not None and location.endswith('/archive/')


def test_pagination_pages(console: Callable[..., Any],
                          monkeypatch: pytest.MonkeyPatch) -> None:
    env = console(with_pacs=True)
    client = _admin_client(env)
    for patient_id in ('P1', 'P2', 'P3', 'P4', 'P5'):
        _store(env, patient_id)
    monkeypatch.setattr(web_module, 'ARCHIVE_PAGE_SIZE', 2)
    page = client.get('/archive/')
    assert '5 match(es) — page 1 of 3' in page.text
    assert 'P1' in page.text and 'P2' in page.text
    assert 'P3' not in page.text
    assert 'offset=2' in page.text, 'the next-page link'
    page = client.get('/archive/?offset=2')
    assert 'page 2 of 3' in page.text
    assert 'P3' in page.text and 'P4' in page.text
    assert 'offset=0' in page.text and 'offset=4' in page.text
    page = client.get('/archive/?offset=4')
    assert 'page 3 of 3' in page.text
    assert 'P5' in page.text and 'P1' not in page.text
    assert 'offset=2' in page.text
    assert 'offset=6' not in page.text, 'no next page'


def test_pagination_keeps_search_context(console: Callable[..., Any],
                                         monkeypatch: pytest.MonkeyPatch
                                         ) -> None:
    env = console(with_pacs=True)
    client = _admin_client(env)
    for i in range(3):
        _store(env, f'P{i}', name='Common^Name')
    monkeypatch.setattr(web_module, 'ARCHIVE_PAGE_SIZE', 2)
    page = client.get('/archive/?patient_name=Common')
    assert '3 match(es) — page 1 of 2' in page.text
    link = page.text.split('href="/archive/?offset=2', 1)
    assert len(link) == 2
    assert 'patient_name=Common' in link[1].split('"', 1)[0], \
        'the next-page link carries the search context'


def test_archive_viewer_read_only(console: Callable[..., Any]) -> None:
    env = console(with_pacs=True)
    client = _viewer_client(env)
    _store(env, 'P1')
    page = client.get('/archive/')
    assert page.status == 200
    assert 'P1' in page.text
    assert page.text.count('<form method="post"') == 1, \
        'the only POST form is the nav logout; the section is read-only'
    assert '<form method="get"' in page.text, 'the search form is GET'
    # The section has no POST endpoints at all
    assert client.post('/archive/', follow=False).status == 405


def test_archive_not_available_without_listener(console: Callable[..., Any]
                                                ) -> None:
    env = console()  # no PACS component on this bus
    client = _admin_client(env)
    page = client.get('/archive/')
    assert page.status == 200
    assert 'not available' in page.text
    assert 'Traceback' not in page.text
    assert 'href="/archive/"' not in page.text, 'the nav hides the link'


def test_archive_listener_failure_degrades(console: Callable[..., Any],
                                           caplog: pytest.LogCaptureFixture
                                           ) -> None:
    env = console(with_pacs=True)

    def boom(_filter: Any) -> Any:
        raise RuntimeError('archive exploded')

    env.bus.subscribe(
        core_events.ArchivePatientQuery, boom,
        trolleybus.DEFAULT_PRIORITY + 40
    )
    client = _admin_client(env)
    with caplog.at_level(logging.ERROR):
        page = client.get('/archive/')
    assert page.status == 200
    assert 'not available' in page.text
    assert any('archive exploded' in record.getMessage()
               for record in caplog.records)


def test_offset_capped() -> None:
    assert web_module._archive_offset('99999999999') \
        == web_module.ARCHIVE_MAX_OFFSET
    assert web_module._archive_offset(
        str(web_module.ARCHIVE_MAX_OFFSET + 1)
    ) == web_module.ARCHIVE_MAX_OFFSET


def test_patient_id_with_slash_drills_down(console: Callable[..., Any]
                                           ) -> None:
    """Patient IDs (LO VR) legally contain '/': the quoted drill-down
    link must survive the WSGI percent-decoding into PATH_INFO."""
    env = console(with_pacs=True)
    client = _admin_client(env)
    ds = _store(env, 'P/1')
    page = client.get('/archive/')
    assert page.status == 200
    assert '/archive/studies/P%2F1' in page.text
    studies = client.get(f'/archive/studies/{quote("P/1", safe="")}')
    assert studies.status == 200
    assert ds.StudyInstanceUID in studies.text
    assert 'value="P/1"' in studies.text, 'the pinned patient ID'
