import uuid

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
