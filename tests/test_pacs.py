# -*- coding: utf-8 -*-
import uuid
import pytest

from pydicom import Dataset

from tiny_pacs import db
from tiny_pacs import event_bus
from tiny_pacs import pacs
from tiny_pacs.pacs import models


@pytest.fixture
def pacs_srv():
    bus = event_bus.EventBus()
    _db = db.Database(bus, {'db_name': str(uuid.uuid4())})
    _pacs_srv = pacs.PACS(bus, {})
    bus.broadcast(event_bus.DefaultChannels.ON_START)
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
            series_instance_uid = '1.2.3.4.6',
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


def test_patient_find_no_filters(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    for patient in pacs_srv.c_find(request):
       assert patient.PatientName == 'Test^Test^Test'
       assert patient.PatientSex == 'M'


def test_patient_find_with_count(pacs_srv: pacs.PACS):
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


def test_patient_find_text_filter_positive(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientName = 'Test^*'
    for patient in pacs_srv.c_find(request):
       assert patient.PatientName == 'Test^Test^Test'
       assert patient.PatientSex == 'M'


def test_patient_find_text_filter_negative(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientName = 'Test1^*'
    assert not list(pacs_srv.c_find(request))


def test_patient_find_date_single_positive(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19660101'
    for patient in pacs_srv.c_find(request):
       assert patient.PatientName == 'Test^Test^Test'
       assert patient.PatientSex == 'M'


def test_patient_find_date_single_negative(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19660102'
    assert not list(pacs_srv.c_find(request))


def test_patient_find_date_range_positive(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19650101-19660102'
    for patient in pacs_srv.c_find(request):
       assert patient.PatientName == 'Test^Test^Test'
       assert patient.PatientSex == 'M'


def test_patient_find_date_range_negative(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = None
    request.PatientSex = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'PATIENT'
    request.PatientBirthDate = '19670101-19680102'
    assert not list(pacs_srv.c_find(request))


def test_study_find_no_patient_attrs(pacs_srv: pacs.PACS):
    request = Dataset()
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].AccessionNumber == '1234'


def test_study_find_patient_attrs_no_filters(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = None
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].AccessionNumber == '1234'
    assert results[0].PatientName == 'Test^Test^Test'


def test_study_find_patient_attrs_with_filters_positive(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = 'Test^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert results[0].AccessionNumber == '1234'
    assert results[0].PatientName == 'Test^Test^Test'


def test_study_find_patient_attrs_with_filters_negative(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = 'Test1^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    assert not list(pacs_srv.c_find(request))


def test_study_find_modalities_in_study_no_filter(pacs_srv: pacs.PACS):
    request = Dataset()
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'STUDY'
    request.AccessionNumber = '1234'
    request.ModalitiesInStudy = None
    results = list(pacs_srv.c_find(request))
    assert len(results) == 1
    assert set(results[0].ModalitiesInStudy) == set(['DX', 'SR'])


def test_series_find_patient_filter(pacs_srv: pacs.PACS):
    request = Dataset()
    request.PatientName = 'Test^*'
    request.SpecificCharacterSet = 'ISO_IR 192'
    request.QueryRetrieveLevel = 'SERIES'
    request.SeriesInstanceUID = None
    request.Modality = None
    results = list(pacs_srv.c_find(request))
    assert len(results) == 4


def test_store(pacs_srv: pacs.PACS):
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
