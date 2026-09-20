import logging
import pathlib
import sqlite3
import uuid

import peewee
import pytest
import trolleybus
from pydicom import Dataset

from tiny_pacs import db, events, pacs
from tiny_pacs.pacs import models


@pytest.fixture
def pacs_srv() -> pacs.PACS:
    bus = trolleybus.EventBus()
    _db = db.Database(bus, {'db_name': str(uuid.uuid4())})
    _pacs_srv = pacs.PACS(bus, {})
    bus.start()
    with _db.atomic():
        patient = models.Patient.create(
            patient_id='test1',
            patient_name='Test^Test^Test',
            patient_sex='M',
            patient_birth_date='19660101'
        )

        study1 = models.Study.create(
            patient=patient,
            study_instance_uid='1.2.3.4',
            study_date='20200101',
            accession_number='1234'
        )
        study1_series1 = models.Series.create(
            study=study1,
            series_instance_uid='1.2.3.4.5',
            modality='DX'
        )
        models.Instance.create(
            series=study1_series1,
            sop_instance_uid='1.2.3.4.5.6',
            sop_class_uid='2.3.4'
        )
        models.Instance.create(
            series=study1_series1,
            sop_instance_uid='1.2.3.4.5.7',
            sop_class_uid='2.3.4'
        )
        study1_series2 = models.Series.create(
            study=study1,
            series_instance_uid='1.2.3.4.6',
            modality='SR'
        )
        models.Instance.create(
            series=study1_series2,
            sop_instance_uid='1.2.3.4.6.6',
            sop_class_uid='2.3.5'
        )

        study2 = models.Study.create(
            patient=patient,
            study_instance_uid='1.2.3.5',
            study_date='20200201',
            accession_number='1235'
        )
        study2_series1 = models.Series.create(
            study=study2,
            series_instance_uid='1.2.3.5.5',
            modality='CT'
        )
        models.Instance.create(
            series=study2_series1,
            sop_instance_uid='1.2.3.5.5.6',
            sop_class_uid='2.3.7'
        )
        study2_series2 = models.Series.create(
            study=study2,
            series_instance_uid='1.2.3.5.6',
            modality='PET'
        )
        models.Instance.create(
            series=study2_series2,
            sop_instance_uid='1.2.3.5.6.6',
            sop_class_uid='2.3.7'
        )
    return _pacs_srv


def test_patient_find_no_filters(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    for patient in pacs_srv.c_find(request):
        assert patient.PatientName == 'Test^Test^Test'
        assert patient.PatientSex == 'M'


def test_patient_find_with_count(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.NumberOfPatientRelatedStudies = None
    for patient in pacs_srv.c_find(request):
        assert patient.PatientName == 'Test^Test^Test'
        assert patient.PatientSex == 'M'
        assert patient.NumberOfPatientRelatedStudies == 2


def test_patient_find_text_filter_positive(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientName = 'Test^*'
    for patient in pacs_srv.c_find(request):
        assert patient.PatientName == 'Test^Test^Test'
        assert patient.PatientSex == 'M'


def test_patient_find_text_filter_negative(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientName = 'Test1^*'
    assert not list(pacs_srv.c_find(request))


def test_patient_find_date_single_positive(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19660101'
    for patient in pacs_srv.c_find(request):
        assert patient.PatientName == 'Test^Test^Test'
        assert patient.PatientSex == 'M'


def test_patient_find_date_single_negative(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19660102'
    assert not list(pacs_srv.c_find(request))


def test_patient_find_date_range_positive(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19650101-19660102'
    for patient in pacs_srv.c_find(request):
        assert patient.PatientName == 'Test^Test^Test'
        assert patient.PatientSex == 'M'


def test_patient_find_date_range_negative(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19670101-19680102'
    assert not list(pacs_srv.c_find(request))


def test_study_find_no_patient_attrs(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].AccessionNumber == '1234'


def test_study_find_patient_attrs_no_filters(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].AccessionNumber == '1234'
    assert results[0].PatientName == 'Test^Test^Test'


def test_study_find_patient_attrs_with_filters_positive(
        pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = 'Test^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].AccessionNumber == '1234'
    assert results[0].PatientName == 'Test^Test^Test'


def test_study_find_patient_attrs_with_filters_negative(
        pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = 'Test1^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    assert not list(pacs_srv.c_find(request))


def test_study_find_modalities_in_study_no_filter(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    request.ModalitiesInStudy = None
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert set(results[0].ModalitiesInStudy) == set(['DX', 'SR'])


def test_series_find_patient_filter(pacs_srv: pacs.PACS) -> None:
    request = Dataset()
    request.PatientName = 'Test^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'SERIES'
    request.SeriesInstanceUID = None
    request.Modality = None
    results = list(pacs_srv.c_find(request))
    assert len(results) == 4


def test_store(pacs_srv: pacs.PACS) -> None:
    ds = Dataset()
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.PatientID = 'test_id'
    ds.PatientName = 'Store^Store^Stor'
    ds.PatientBirthDate = '19800101'
    ds.StudyInstanceUID = '1.2.5'
    ds.StudyDate = '20200301'
    ds.StudyTime = '101010'
    ds.SeriesInstanceUID = '1.2.5.6'
    ds.Modality = 'CT'
    ds.SOPInstanceUID = '1.2.5.6'
    ds.SOPClassUID = '2.3.4'

    pacs_srv.c_store(ds)

    request = Dataset()
    request.PatientName = 'Store^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'IMAGE'
    request.StudyInstanceUID = None
    request.SeriesInstanceUID = None
    request.SOPInstanceUID = None
    request.Modality = None
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1


def test_store_dataset(pacs_srv: pacs.PACS) -> None:
    done: list[Dataset] = []
    pacs_srv.bus.subscribe(events.StoreDone, done.append)
    payload = events.StoreDatasetPayload(ds=_store_ds(),
                                         transfer_syntax='1.2.840.10008.1.2')
    pacs_srv.on_store_dataset(payload)
    assert done == [payload.ds]

    request = Dataset()
    request.PatientName = 'Store^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'IMAGE'
    request.StudyInstanceUID = None
    request.SeriesInstanceUID = None
    request.SOPInstanceUID = None
    request.Modality = None
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1


def test_store_dataset_failure(pacs_srv: pacs.PACS) -> None:
    """A dataset that cannot be recorded broadcasts StoreFailure."""
    failures: list[Dataset] = []
    pacs_srv.bus.subscribe(events.StoreFailure, failures.append)
    ds = Dataset()
    ds.PatientID = 'no_study'
    # No StudyInstanceUID: recording the dataset must fail
    payload = events.StoreDatasetPayload(ds=ds,
                                         transfer_syntax='1.2.840.10008.1.2')
    with pytest.raises(AttributeError):
        pacs_srv.on_store_dataset(payload)
    assert failures == [ds]


def _store_ds() -> Dataset:
    ds = Dataset()
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.PatientID = 'test_id'
    ds.PatientName = 'Store^Store^Stor'
    ds.PatientBirthDate = '19800101'
    ds.StudyInstanceUID = '1.2.5'
    ds.StudyDate = '20200301'
    ds.StudyTime = '101010'
    ds.SeriesInstanceUID = '1.2.5.6'
    ds.Modality = 'CT'
    ds.SOPInstanceUID = '1.2.5.6'
    ds.SOPClassUID = '2.3.4'
    return ds


def test_archive_patient_query(pacs_srv: pacs.PACS) -> None:
    items = pacs_srv.on_archive_patients(events.ArchiveFilter())
    assert len(items) == 1
    assert items[0].total == 1
    assert items[0].uids == {'patient_id': 'test1'}
    assert items[0].attributes[0x00100020] == ('LO', 'test1')
    assert items[0].fields['patient_name'] == 'Test^Test^Test'

    items = pacs_srv.on_archive_patients(
        events.ArchiveFilter(patient_id='ghost')
    )
    assert items == []

    items = pacs_srv.on_archive_patients(
        events.ArchiveFilter(patient_name='Test')
    )
    assert len(items) == 1


def test_archive_study_query(pacs_srv: pacs.PACS) -> None:
    items = pacs_srv.on_archive_studies(events.ArchiveFilter())
    assert [item.uids['study_instance_uid'] for item in items] == \
        ['1.2.3.4', '1.2.3.5']
    assert all(item.total == 2 for item in items)
    assert items[0].uids['patient_id'] == 'test1'
    assert items[0].attributes[0x00080020] == ('DA', '20200101')

    items = pacs_srv.on_archive_studies(
        events.ArchiveFilter(study_date_from='20200115')
    )
    assert [i.uids['study_instance_uid'] for i in items] == ['1.2.3.5']

    items = pacs_srv.on_archive_studies(
        events.ArchiveFilter(accession_number='1234')
    )
    assert [i.uids['study_instance_uid'] for i in items] == ['1.2.3.4']

    items = pacs_srv.on_archive_studies(events.ArchiveFilter(modality='CT'))
    assert [i.uids['study_instance_uid'] for i in items] == ['1.2.3.5']

    items = pacs_srv.on_archive_studies(events.ArchiveFilter(limit=1))
    assert len(items) == 1
    assert items[0].total == 2

    items = pacs_srv.on_archive_studies(
        events.ArchiveFilter(limit=1, offset=1)
    )
    assert [i.uids['study_instance_uid'] for i in items] == ['1.2.3.5']


def test_archive_series_query(pacs_srv: pacs.PACS) -> None:
    items = pacs_srv.on_archive_series(events.ArchiveFilter())
    assert len(items) == 4
    assert all(item.total == 4 for item in items)
    assert items[0].uids['study_instance_uid'] == '1.2.3.4'

    items = pacs_srv.on_archive_series(
        events.ArchiveFilter(study_instance_uid='1.2.3.5',
                             series_instance_uid='1.2.3.5.6')
    )
    assert [i.uids['series_instance_uid'] for i in items] == ['1.2.3.5.6']

    items = pacs_srv.on_archive_series(
        events.ArchiveFilter(patient_id='test1', modality='DX')
    )
    assert [i.uids['series_instance_uid'] for i in items] == ['1.2.3.4.5']


def test_archive_instance_query(pacs_srv: pacs.PACS) -> None:
    items = pacs_srv.on_archive_instances(events.ArchiveFilter())
    assert len(items) == 5
    assert all(item.total == 5 for item in items)

    items = pacs_srv.on_archive_instances(
        events.ArchiveFilter(sop_instance_uid='1.2.3.4.5.6')
    )
    assert len(items) == 1
    assert items[0].uids['sop_instance_uid'] == '1.2.3.4.5.6'
    assert items[0].uids['patient_id'] == 'test1'
    assert items[0].attributes[0x00080016] == ('UI', '2.3.4')

    items = pacs_srv.on_archive_instances(
        events.ArchiveFilter(modality='CT')
    )
    assert [i.uids['sop_instance_uid'] for i in items] == ['1.2.3.5.5.6']


def test_archive_uniform_filter_semantics(pacs_srv: pacs.PACS) -> None:
    """Filters naming a lower level restrict the upper levels too.

    Fixture shape: patient ``test1`` owns studies ``1.2.3.4`` (20200101,
    accession 1234; series DX/SR) and ``1.2.3.5`` (20200201, accession
    1235; series CT/PET), every series owns instances.
    """
    # Study-level filters on the PATIENT query: patients match through
    # the studies they own
    by_accession = pacs_srv.on_archive_patients(
        events.ArchiveFilter(accession_number='1235')
    )
    assert [i.uids['patient_id'] for i in by_accession] == ['test1']
    assert pacs_srv.on_archive_patients(
        events.ArchiveFilter(accession_number='9999')
    ) == []
    by_date = pacs_srv.on_archive_patients(
        events.ArchiveFilter(study_date_from='20200115',
                             study_date_to='20200301')
    )
    assert [i.uids['patient_id'] for i in by_date] == ['test1']
    assert pacs_srv.on_archive_patients(
        events.ArchiveFilter(study_date_from='20210101')
    ) == []
    by_study = pacs_srv.on_archive_patients(
        events.ArchiveFilter(study_instance_uid='1.2.3.5')
    )
    assert [i.uids['patient_id'] for i in by_study] == ['test1']
    by_series = pacs_srv.on_archive_patients(
        events.ArchiveFilter(series_instance_uid='1.2.3.4.5')
    )
    assert [i.uids['patient_id'] for i in by_series] == ['test1']
    by_instance = pacs_srv.on_archive_patients(
        events.ArchiveFilter(sop_instance_uid='1.2.3.5.6.6')
    )
    assert [i.uids['patient_id'] for i in by_instance] == ['test1']
    assert pacs_srv.on_archive_patients(
        events.ArchiveFilter(patient_id='test1', accession_number='9999')
    ) == []

    # Series/instance filters on the STUDY query
    studies = pacs_srv.on_archive_studies(
        events.ArchiveFilter(series_instance_uid='1.2.3.5.5')
    )
    assert [i.uids['study_instance_uid'] for i in studies] == ['1.2.3.5']
    studies = pacs_srv.on_archive_studies(
        events.ArchiveFilter(sop_instance_uid='1.2.3.4.5.7')
    )
    assert [i.uids['study_instance_uid'] for i in studies] == ['1.2.3.4']

    # Instance filters on the SERIES query
    series = pacs_srv.on_archive_series(
        events.ArchiveFilter(sop_instance_uid='1.2.3.5.5.6')
    )
    assert [i.uids['series_instance_uid'] for i in series] == ['1.2.3.5.5']


def _identity_ds(**overrides: object) -> Dataset:
    """Complete storable dataset with overridable attributes.

    Every call carries a unique study/series/instance UID chain so
    repeated stores create separate studies.
    """
    suffix = str(uuid.uuid4().int)[:6]
    ds = Dataset()
    ds.SpecificCharacterSet = 'ISO_IR 192'
    ds.PatientID = 'identity_test'
    ds.PatientName = 'Identity^Test'
    ds.PatientSex = 'M'
    ds.PatientBirthDate = '19700101'
    ds.StudyInstanceUID = f'1.9.{suffix}'
    ds.StudyDate = '20210101'
    ds.SeriesInstanceUID = f'1.9.{suffix}.1'
    ds.Modality = 'CT'
    ds.SOPInstanceUID = f'1.9.{suffix}.1.1'
    ds.SOPClassUID = '1.2.840.10008.5.1.4.1.1.2'
    for key, value in overrides.items():
        setattr(ds, key, value)
    return ds


def test_store_same_patient_id_different_issuer(
        pacs_srv: pacs.PACS) -> None:
    """The identity key is (PatientID, IssuerOfPatientID)."""
    pacs_srv.c_store(_identity_ds(IssuerOfPatientID='HOSP1'))
    pacs_srv.c_store(_identity_ds(IssuerOfPatientID='HOSP2'))
    # Same identity key attaches instead of failing the unique constraint
    pacs_srv.c_store(_identity_ds(IssuerOfPatientID='HOSP2'))

    rows = models.Patient.select().where(
        models.Patient.patient_id == 'identity_test'
    ).order_by(models.Patient.issuer_of_patient_id)
    assert [r.issuer_of_patient_id for r in rows] == ['HOSP1', 'HOSP2']

    request = Dataset()
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientID = 'identity_test'
    request.IssuerOfPatientID = 'HOSP2'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].IssuerOfPatientID == 'HOSP2'


def test_store_conflicting_demographics_attaches_and_warns(
        pacs_srv: pacs.PACS, caplog: pytest.LogCaptureFixture) -> None:
    """Demographics never split a patient: the identity key wins."""
    ds = _identity_ds(PatientID='test1', PatientName='Other^Person',
                      PatientSex='F', PatientBirthDate='19991231')
    with caplog.at_level(logging.WARNING, logger='PatientAPI'):
        pacs_srv.c_store(ds)

    patient = models.Patient.get(models.Patient.patient_id == 'test1')
    # Attached to the existing record, stored demographics kept
    assert patient.patient_name == 'Test^Test^Test'
    assert patient.patient_sex == 'M'
    assert patient.patient_birth_date == '19660101'
    assert 'Demographic conflict' in caplog.text
    # Only the conflicting attribute names are logged, no PHI values
    assert 'PatientName' in caplog.text
    assert 'Other^Person' not in caplog.text
    assert models.Patient.select().count() == 1


def test_store_deidentified_dataset(
        pacs_srv: pacs.PACS, caplog: pytest.LogCaptureFixture) -> None:
    """PS3.15 E markers attach silently and are recorded."""
    ds = _identity_ds(PatientID='0', PatientIdentityRemoved='YES',
                      DeidentificationMethod='basic profile')
    with caplog.at_level(logging.WARNING, logger='PatientAPI'):
        pacs_srv.c_store(ds)
    # A later dataset without the markers but with conflicting
    # demographics still attaches silently: the record is de-identified
    pacs_srv.c_store(
        _identity_ds(PatientID='0', PatientSex='F',
                     PatientBirthDate='19010101')
    )

    records = models.Patient.select().where(
        models.Patient.patient_id == '0'
    )
    assert records.count() == 1
    patient = records.get()
    assert patient.patient_identity_removed == 'YES'
    assert patient.deidentification_method == 'basic profile'
    assert 'Demographic conflict' not in caplog.text


def test_store_deidentification_code_sequence(pacs_srv: pacs.PACS) -> None:
    """The method is derived from the code sequence when (0012,0063)
    is absent."""
    item = Dataset()
    item.CodeValue = '113100'
    item.CodingSchemeDesignator = 'DCM'
    item.CodeMeaning = 'Basic Confidentiality Profile'
    ds = _identity_ds(PatientID='code_seq')
    ds.DeidentificationMethodCodeSequence = [item]
    pacs_srv.c_store(ds)

    patient = models.Patient.get(models.Patient.patient_id == 'code_seq')
    assert patient.deidentification_method == \
        'Basic Confidentiality Profile'
    assert pacs_srv.patient_api.is_anonymized(ds, 'code_seq')


def test_store_default_placeholder_anonymous(
        pacs_srv: pacs.PACS, caplog: pytest.LogCaptureFixture) -> None:
    """The default placeholder ID is an anonymized bucket: conflicting
    demographics attach silently."""
    with caplog.at_level(logging.WARNING, logger='PatientAPI'):
        pacs_srv.c_store(_identity_ds(PatientID='ANONYMOUS'))
        pacs_srv.c_store(_identity_ds(PatientID='ANONYMOUS',
                                      PatientSex='F'))
    records = models.Patient.select().where(
        models.Patient.patient_id == 'ANONYMOUS'
    )
    assert records.count() == 1
    assert records.get().patient_sex == 'M'
    assert 'Demographic conflict' not in caplog.text
    # Placeholder recognition is case-insensitive
    ds = _identity_ds(PatientID='anonymous')
    assert pacs_srv.patient_api.is_anonymized(ds, 'anonymous')


def test_store_configured_anonymous_patient_ids(
        caplog: pytest.LogCaptureFixture) -> None:
    """The placeholder list is configurable and only silences those."""
    bus = trolleybus.EventBus()
    db.Database(bus, {'db_name': str(uuid.uuid4())})
    srv = pacs.PACS(bus, {'anonymous_patient_ids': ['0', 'research']})
    bus.start()

    with caplog.at_level(logging.WARNING, logger='PatientAPI'):
        srv.c_store(_identity_ds(PatientID='0'))
        srv.c_store(_identity_ds(PatientID='0', PatientSex='F'))
        srv.c_store(_identity_ds(PatientID='RESEARCH', PatientSex='M'))
        srv.c_store(_identity_ds(PatientID='RESEARCH', PatientSex='F'))
    assert models.Patient.select().where(
        models.Patient.patient_id == '0'
    ).count() == 1
    assert models.Patient.select().where(
        models.Patient.patient_id == 'RESEARCH'
    ).count() == 1
    assert 'Demographic conflict' not in caplog.text

    # A regular ID with conflicting demographics still warns
    srv.c_store(_identity_ds(PatientID='real_id', PatientSex='M'))
    srv.c_store(_identity_ds(PatientID='real_id', PatientSex='F'))
    assert 'Demographic conflict' in caplog.text


def test_store_missing_patient_id(pacs_srv: pacs.PACS) -> None:
    """Datasets without a Patient ID share one record."""
    first = _identity_ds()
    del first.PatientID
    second = _identity_ds()
    del second.PatientID
    pacs_srv.c_store(first)
    pacs_srv.c_store(second)

    records = models.Patient.select().where(models.Patient.patient_id == '')
    assert records.count() == 1
    assert records.get().issuer_of_patient_id == ''


def test_store_other_patient_names_multivalue(pacs_srv: pacs.PACS) -> None:
    """Wire-decoded multi-valued names are stored as a DICOM string,
    not as a bracketed repr (pydicom MultiValue is not a list)."""
    ds = _identity_ds(PatientID='multiname')
    ds.add_new(0x00101001, 'PN', r'Other^One\Other^Two')
    pacs_srv.c_store(ds)

    patient = models.Patient.get(models.Patient.patient_id == 'multiname')
    assert patient.other_patient_names == 'Other^One\\Other^Two'


def test_store_creation_race_retries_as_attach(
        pacs_srv: pacs.PACS, monkeypatch: pytest.MonkeyPatch) -> None:
    """A lost creation race attaches to the winner's record instead of
    failing the C-STORE."""
    api = pacs_srv.patient_api
    original = api.create_patient
    raced: list[int] = []

    def racing(
            ds: Dataset, patient_id: str, issuer: str
    ) -> models.Patient:
        if not raced:
            raced.append(1)
            # The concurrent winner creates the record first, then the
            # loser's insert violates the identity key
            original(ds, patient_id, issuer)
            raise peewee.IntegrityError('UNIQUE constraint failed')
        return original(ds, patient_id, issuer)

    monkeypatch.setattr(api, 'create_patient', racing)
    pacs_srv.c_store(_identity_ds(PatientID='race_winner'))
    assert models.Patient.select().where(
        models.Patient.patient_id == 'race_winner'
    ).count() == 1


_V1_PATIENT_DDL = (
    'CREATE TABLE "patient" ("id" INTEGER NOT NULL PRIMARY KEY, '
    '"patient_name" VARCHAR(324), "patient_id" VARCHAR(64) NOT NULL '
    'UNIQUE, "issuer_of_patient_id" VARCHAR(64), '
    '"patient_birth_date" VARCHAR(8), "patient_birth_time" VARCHAR(14), '
    '"patient_sex" VARCHAR(16), "other_patient_names" TEXT NOT NULL, '
    '"ethnic_group" VARCHAR(16), "patient_comments" TEXT NOT NULL)'
)


def test_patient_identity_migration(tmp_path: pathlib.Path) -> None:
    """A legacy version-1 SQLite database is upgraded in place.

    Data is preserved, NULL issuers normalize to the empty issuer, the
    foreign key children survive the rebuild and the unique key becomes
    the (patient_id, issuer_of_patient_id) pair.
    """
    db_name = str(tmp_path / 'pacs.db')
    con = sqlite3.connect(db_name)
    con.execute(_V1_PATIENT_DDL)
    con.execute(
        'INSERT INTO patient (id, patient_name, patient_id, '
        "issuer_of_patient_id, other_patient_names, patient_comments) "
        "VALUES (1, 'A^B', 'p1', 'HOSP', '', '')"
    )
    con.execute(
        'INSERT INTO patient (id, patient_name, patient_id, '
        "issuer_of_patient_id, other_patient_names, patient_comments) "
        "VALUES (2, 'C^D', 'p2', NULL, '', '')"
    )
    con.commit()
    con.close()

    bus = trolleybus.EventBus()
    database = db.Database(bus, {'driver': 'sqlite', 'db_name': db_name,
                                 'uri': False})
    pacs.PACS(bus, {})
    bus.start()

    row = db.SchemaVersion.get(db.SchemaVersion.component == 'PACS')
    assert row.version == 2
    rows = list(models.Patient.select().order_by(models.Patient.id))
    assert [(r.patient_id, r.issuer_of_patient_id) for r in rows] == \
        [('p1', 'HOSP'), ('p2', '')]
    assert rows[0].patient_name == 'A^B'
    assert rows[1].patient_identity_removed == ''
    assert rows[1].deidentification_method == ''

    # The foreign key children survive the rebuild intact
    assert database.db is not None
    study = models.Study.create(patient=rows[0], study_instance_uid='2.1')
    assert study.patient.patient_id == 'p1'
    cursor = database.db.execute_sql('PRAGMA foreign_key_check')
    assert cursor.fetchall() == []

    # The ID alone is no longer unique; the pair is
    models.Patient.create(patient_id='p1', issuer_of_patient_id='OTHER')
    with pytest.raises(peewee.IntegrityError):
        models.Patient.create(patient_id='p1', issuer_of_patient_id='HOSP')

    database.db.close()


def test_patient_identity_migration_idempotent(
        tmp_path: pathlib.Path) -> None:
    """Restarting over a migrated database applies nothing again."""
    db_name = str(tmp_path / 'pacs.db')
    con = sqlite3.connect(db_name)
    con.execute(_V1_PATIENT_DDL)
    con.commit()
    con.close()

    config = {'driver': 'sqlite', 'db_name': db_name, 'uri': False}
    bus = trolleybus.EventBus()
    database = db.Database(bus, config)
    pacs.PACS(bus, {})
    bus.start()
    models.Patient.create(patient_id='p1', issuer_of_patient_id='')
    assert database.db is not None
    database.db.close()

    bus = trolleybus.EventBus()
    database = db.Database(bus, config)
    pacs.PACS(bus, {})
    bus.start()
    row = db.SchemaVersion.get(db.SchemaVersion.component == 'PACS')
    assert row.version == 2
    assert models.Patient.select().count() == 1
    assert database.db is not None
    database.db.close()
