# -*- coding: utf-8 -*-
import peewee
import pydicom
from pydicom.tag import Tag

from .models import Patient, Study, Series, Instance
from . import base_api


class PatientAPI(base_api.BaseAPI):
    def c_store(self, ds: pydicom.Dataset):
        """Gets or creates patient record for storage request

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing patient record, that matches incoming dataset
        :rtype: Patient
        """
        # TODO: Issue Patient ID if missing
        # TODO: Check for conflicting Patient IDs

        patient_id = getattr(ds, 'PatientID', None)
        patient_name = getattr(ds, 'PatientName', None)
        patient_sex = getattr(ds, 'PatientSex', None)
        patient_birth_date = getattr(ds, 'PatientBirthDate', None)
        query = Patient.select().where(Patient.patient_id == patient_id)
        if patient_name:
            query = query.where(Patient.patient_name ** str(patient_name))
        if patient_sex:
            query = query.where(Patient.patient_sex == patient_sex)
        if patient_birth_date:
            query = query.where(Patient.patient_birth_date == patient_birth_date)
        try:
            patient = query.get()
        except Patient.DoesNotExist:  # pylint: disable=no-member
            issuer_of_patient_id = getattr(ds, 'IssuerOfPatientID', None)
            patient_birth_time = getattr(ds, 'PatientBirthTime', None)
            other_patient_names = getattr(ds, 'OtherPatientNames', '')
            if isinstance(other_patient_names, list):
                other_patient_names = '\\'.join(other_patient_names)
            ethnic_group = getattr(ds, 'EthnicGroup', None)
            patient_comments = getattr(ds, 'PatientComments', '')
            patient = Patient.create(
                patient_id=patient_id,
                patient_name=patient_name,
                patient_sex=patient_sex,
                patient_birth_date=patient_birth_date,
                patient_birth_time=patient_birth_time,
                issuer_of_patient_id=issuer_of_patient_id,
                other_patient_names=other_patient_names,
                ethnic_group=ethnic_group,
                patient_comments=patient_comments
            )
            self.log.debug('Created new patient, Patient ID: %s', patient_id)
        return patient

    def c_find(self, ds: pydicom.Dataset):
        """C-FIND request handler for Patient level

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND result
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []

        select = [Patient]
        skipped = set()
        if 'NumberOfPatientRelatedStudies' in ds:
            _tag = Tag(0x0020, 0x1200)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(Study.id)\
                    .alias('number_of_patient_related_studies')  # pylint: disable=no-member
                )
            response_attrs.append(
                (_tag, 'number_of_patient_related_studies', 'IS', None)
            )
            joins.add((Patient, Study))
        if 'NumberOfPatientRelatedSeries' in ds:
            _tag = Tag(0x0020, 0x1202)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(Series.id)\
                    .alias('number_of_patient_related_series')  # pylint: disable=no-member
            )
            response_attrs.append(
                (_tag, 'number_of_patient_related_series', 'IS', None)
            )
            joins.update([(Patient, Study), (Study, Series)])
        if 'NumberOfPatientRelatedInstances' in ds:
            _tag = Tag(0x0020, 0x1204)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(Instance.id)\
                    .alias('number_of_patient_related_instances')  # pylint: disable=no-member
            )
            response_attrs.append(
                (_tag, 'number_of_patient_related_instances', 'IS', None)
            )
            joins.update([(Patient, Study), (Study, Series), (Series, Instance)])

        query = Patient.select(*select)
        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = self.build_filters(Patient, query, ds)
        response_attrs.extend(_response_attrs)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        yield from (
            self.encode_response(p, response_attrs, encoding) for p in query
        )
