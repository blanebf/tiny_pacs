# -*- coding: utf-8 -*-
import peewee
import pydicom
from pydicom.tag import Tag

from .. import db

from . import models
from . import base_api

class StudyAPI(base_api.BaseAPI):
    def c_store(self, patient: models.Patient, ds: pydicom.Dataset):
        """C-STORE handler

        :param patient: patient model for study
        :type patient: Patient
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing study record, that matches incoming dataset
        :rtype: Study
        """
        study_instance_uid = ds.StudyInstanceUID
        try:
            return models.Study.get(models.Study.study_instance_uid == study_instance_uid)
        except models.Study.DoesNotExist:  # pylint: disable=no-member
            study_date = getattr(ds, 'StudyDate', None)
            study_time = getattr(ds, 'StudyTime', None)
            accession_number = getattr(ds, 'AccessionNumber', None)
            study_id = getattr(ds, 'StudyID', None)
            study_description = getattr(ds, 'StudyDescription', None)
            referring_physician_name = getattr(
                ds, 'ReferringPhysicianName', None
            )
            name_of_physicians_reading_study = getattr(
                ds, 'NameOfPhysiciansReadingStudy', ''
            )

            if isinstance(name_of_physicians_reading_study, list):
                name_of_physicians_reading_study = '\\'.join(
                    name_of_physicians_reading_study
                )

            admitting_diagnoses_description = getattr(
                ds, 'AdmittingDiagnosesDescription', None
            )
            patient_age = getattr(ds, 'PatientAge', None)
            patient_size = getattr(ds, 'PatientSize', None)
            patient_weight = getattr(ds, 'PatientWeight', None)
            occupation = getattr(ds, 'Occupation', None)
            additional_patient_history = getattr(
                ds, 'AdditionalPatientHistory', ''
            )
            study = models.Study.create(
                patient=patient,
                study_instance_uid=study_instance_uid,
                study_date=study_date,
                study_time=study_time,
                accession_number=accession_number,
                study_id=study_id,
                study_description=study_description,
                referring_physician_name=referring_physician_name,
                name_of_physicians_reading_study=name_of_physicians_reading_study,
                admitting_diagnoses_description=admitting_diagnoses_description,
                patient_age=patient_age,
                patient_size=patient_size,
                patient_weight=patient_weight,
                occupation=occupation,
                additional_patient_history=additional_patient_history
            )
            self.log.debug('Create new study, Study Instance UID: %s', study_instance_uid)
            return study

    def c_find(self, ds: pydicom.Dataset):
        """C-FIND request handler for Study level

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND result
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []
        select = [models.Study]
        upper_level_filters = []

        patient_attrs = [e for e in ds if e.tag in models.Patient.mapping]
        skipped = set(e.tag for e in patient_attrs)
        if patient_attrs:
            upper_level_filters.extend(self.filter_upper_level(models.Patient, patient_attrs))
            for tag, attr, vr, _, attr_name in upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('patient', attr_name), vr, None))
            joins.add((models.Study, models.Patient))

        if 'ModalitiesInStudy' in ds:
            _tag = Tag(0x0008, 0x0061)
            skipped.add(_tag)
            # TODO: Add modalities in study filter
            agg_fun = self.bus.send_one(db.DBChannels.STRING_AGG)
            select.append(
                agg_fun(models.Series.modality, '\\').alias('modalities_in_study')
            )
            func = lambda v: '\\'.join(set(v.split('\\'))) if v else v
            response_attrs.append((_tag, 'modalities_in_study', 'CS', func))
            joins.add((models.Study, models.Series))
        if 'SOPClassesInStudy' in ds:
            _tag = Tag(0x0008, 0x0062)
            skipped.add(_tag)
            agg_fun = self.bus.send_one(db.DBChannels.STRING_AGG)
            select.append(
                agg_fun(models.Instance.sop_class_uid, '\\').alias('sop_classes_in_study')
            )
            func = lambda v: '\\'.join(set(v.split('\\'))) if v else v
            response_attrs.append((_tag, 'sop_classes_in_study', 'UI', func))
            joins.update([(models.Study, models.Series), (models.Series, models.Instance)])
        if 'NumberOfStudyRelatedSeries' in ds:
            _tag = Tag(0x0020, 0x1206)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(models.Series.id)\
                    .alias('number_of_study_related_series')  # pylint: disable=no-member
            )
            response_attrs.append(
                (_tag, 'number_of_study_related_series', 'IS', None)
            )
            joins.add((models.Study, models.Series))
        if 'NumberOfStudyRelatedInstances' in ds:
            _tag = Tag((0x0020, 0x1208))
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(models.Instance.id)\
                    .alias('number_of_study_related_instances')  # pylint: disable=no-member
            )
            response_attrs.append(
                (_tag, 'number_of_study_related_instances', 'IS', None)
            )
            joins.update([(models.Study, models.Series), (models.Series, models.Instance)])

        query = models.Study.select(*select)

        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = self.build_filters(models.Study, query, ds, skipped)
        response_attrs.extend(_response_attrs)
        for _, attr, vr, elem, _ in upper_level_filters:
            if not elem.value:
                continue
            query = self.build_filter(query, attr, vr, elem)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        if not query.count():
            return []
        yield from (self.encode_response(s, response_attrs, encoding) for s in query)
