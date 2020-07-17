# -*- coding: utf-8 -*-
import enum
from itertools import chain

import peewee
import pydicom
from pynetdicom2 import statuses

from . import ae
from . import component
from . import db
from . import event_bus
from . import storage


#: Set of tags excluded from generating queries based on C-FIND-RQ
EXCLUDED_ATTRS = set([
    0x00080052, # Query/Retrieve Level
    0x00080005, # Specific Character Set
    0x00201200, # Number of Patient Related Studies
    0x00201202, # Number of Patient Related Series
    0x00201204, # Number of Patient Related Instances
    0x00080061, # Modalities in Study
    0x00080062, # SOP Classes in Study
    0x00201070, # Other Study Numbers
    0x00201206, # Number of Study Related Series
    0x00201208, # Number of Study Related Instances
    0x00201209  # Number of Series Related Instances
])


#: List of text VRs
TEXT_VR = ['AE', 'CS', 'LO', 'LT', 'PN', 'SH', 'ST', 'UC', 'UR', 'UT']


class QRLevelRank(enum.Enum):
    """Rank of Query/Retrieve level"""

    #: QueryRetrieveLevel == 'PATIENT'
    PATIENT = 0

    #: QueryRetrieveLevel == 'STUDY'
    STUDY = 1

    #: QueryRetrieveLevel == 'SERIES'
    SERIES = 2

    #: QueryRetrieveLevel == 'IMAGE'
    IMAGE = 3


#: Map of QueryRetrieveLevel to `tiny_pacs.pacs.QRLevelRank`
QR_LEVEL = {
    'PATIENT': QRLevelRank.PATIENT,
    'STUDY': QRLevelRank.STUDY,
    'SERIES': QRLevelRank.SERIES,
    'IMAGE': QRLevelRank.IMAGE
}


class PACS(component.Component):
    """"Component that implements PACS services themselves.

    Provides handling to the following events:

        * :attr:`~tiny_pacs.ae.AEChannels.STORE`
        * :attr:`~tiny_pacs.ae.AEChannels.FIND`
        * :attr:`~tiny_pacs.ae.AEChannels.MOVE`
        * :attr:`~tiny_pacs.ae.AEChannels.COMMITMENT`

    Component also handles all relevant DB interactions, except for keeping
    track of stored datasets. That function is relegated to components in
    :module:`~tiny_pacs.storage`
    """

    def __init__(self, bus: event_bus.EventBus, config: dict):
        """Component initialization

        :param bus: event bus
        :type bus: event_bus.EventBus
        :param config: component configuration
        :type config: dict
        """
        super().__init__(bus, config)
        self.subscribe(ae.AEChannels.STORE, self.on_store)
        self.subscribe(ae.AEChannels.FIND, self.on_find)
        self.subscribe(ae.AEChannels.MOVE, self.on_move)
        self.subscribe(ae.AEChannels.COMMITMENT, self.on_commitment)
        self.subscribe(db.DBChannels.TABLES, self.tables)

    @staticmethod
    def tables():
        """Returns a list of tables for DB component

        :return: list of tables used by this component
        :rtype: list
        """
        return [Patient, Study, Series, Instance]

    def atomic(self):
        """Context manager for handling simple transactions

        :return: atomic transaction
        """
        return self.send_one(db.DBChannels.ATOMIC)

    def on_store(self, context, ds):
        """Handling of incoming storage request

        :param context: presentation context
        :type context: pynetdicom2.asceprovider.PContextDef
        :param ds: incoming dataset
        :type ds: file
        :return: C-STORE handling status
        :rtype: pynetdicom2.statuses.Status
        """
        self.log_info('Handling store request (%r)', context)
        try:
            ds = pydicom.dcmread(ds, stop_before_pixels=True)
            self.c_store(ds)
        except Exception as e:
            self.log_exception(f'Failed to store dataset: {e}')
            self.broadcast(storage.StorageChannels.ON_STORE_FAILURE, ds)
            return statuses.C_STORE_CANNON_UNDERSTAND
        else:
            self.log_info('Dataset successfully stored (%r)', context)
            self.broadcast(storage.StorageChannels.ON_STORE_DONE, ds)
            return statuses.SUCCESS

    def on_find(self, context, ds: pydicom.Dataset):
        """Handling of incoming find request

        :param context: presentation context
        :type context: pynetdicom2.asceprovider.PContextDef
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :yield: tuple of find result and pending status
        :rtype: tuple
        """
        results = self.c_find(ds)
        yield from ((r, statuses.C_FIND_PENDING) for r in results)

    def on_move(self, context, ds: pydicom.Dataset, destination: str):
        """Handling of incoming move request

        :param context: presentation context
        :type context: pynetdicom2.asceprovider.PContextDef
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :param destination: move destination
        :type destination: str
        :return: list of tuples: SOP Class UID, Transfer Syntax and either
                 filename or dataset
        :rtype: list
        """
        self.log_info('Handling move request to %s (%r)', destination, context)
        instances = [uid for _, _, uid in self.c_move_get_instances(ds)]
        self.log_debug('Moving instances: %r', instances)
        results = self.broadcast(storage.StorageChannels.ON_GET_FILES, instances)
        return list(chain.from_iterable(results))

    def on_commitment(self, uids: list):
        """Handling of incoming storage commitment request

        :param uids: list of tuple (SOP Instance UID, SOP Class UID)
        :type uids: list
        :return: tuple of two list: successes and failures
        :rtype: tuple
        """
        self.log_info('Handling Storage Commitment')
        self.log_debug('Verifying %r instances', uids)
        results = self.broadcast(storage.StorageChannels.ON_STORE_VERIFY, uids)
        success = chain.from_iterable(s for s, _ in results)
        failure = chain.from_iterable(f for _, f in results)
        return list(success), list(failure)

    def c_find(self, ds: pydicom.Dataset):
        """C-FIND implementation

        Translate incoming dataset to database query

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :yield: result dataset
        :rtype: pydicom.Dataset
        """
        level = ds.QueryRetrieveLevel
        self.log_info('Handling find request for level: %s', level)
        if level == 'PATIENT':
            yield from Patient.c_find(ds)
        elif level == 'STUDY':
            yield from Study.c_find(ds)
        elif level == 'SERIES':
            yield from Series.c_find(ds)
        elif level == 'IMAGE':
            yield from Instance.c_find(ds)

    def c_store(self, ds: pydicom.Dataset):
        """C-STORE implementation

        Store dataset attributes in a database

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        """
        with self.atomic():
            patient = Patient.c_store(ds)
            study = Study.c_store(patient, ds)
            series = Series.c_store(study, ds)
            Instance.c_store(series, ds)

    def c_move_get_instances(self, ds: pydicom.Dataset):
        """Gets instances for C-MOVE request

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :yield: tuple of Study Instance UID, Series Instance UID,
                SOP Instance UID
        :rtype: tuple
        """
        level = ds.QueryRetrieveLevel
        level = QR_LEVEL[level]
        query = Instance.select(
                    Instance.sop_instance_uid,
                    Series.series_instance_uid,
                    Study.study_instance_uid
            )\
            .join(Series)\
            .join(Study)\
            .join(Patient)

        if (level == QRLevelRank.PATIENT or
                (level.value > QRLevelRank.PATIENT.value and
                 hasattr(ds, 'PatientID'))):
            query = query.where(Patient.patient_id == ds.PatientID)

        if (level == QRLevelRank.STUDY or
                (level.value > QRLevelRank.STUDY.value and
                 hasattr(ds, 'StudyInstanceUID'))):
            study_uids = ds.StudyInstanceUID
            if not isinstance(study_uids, list):
                study_uids = [study_uids]
            query = query.where(Study.study_instance_uid << study_uids)

        if (level == QRLevelRank.SERIES or
                (level.value > QRLevelRank.SERIES.value and
                 hasattr(ds, 'SeriesInstanceUID'))):
            series_uids = ds.SeriesInstanceUID
            if not isinstance(series_uids, list):
                series_uids = [series_uids]
            query = query.where(Series.series_instance_uid << series_uids)

        if level == QRLevelRank.IMAGE:
            sop_instance_uids = ds.SOPInstanceUID
            if not isinstance(sop_instance_uids, list):
                sop_instance_uids = [sop_instance_uids]
            query = query.where(Instance.sop_instance_uid << sop_instance_uids)

        for instance in query:
            series = instance.series
            study = series.study
            yield (study.study_instance_uid,
                   series.series_instance_uid,
                   instance.sop_instance_uid)


class Patient(peewee.Model):
    """Patient model.

    Stores all C-FIND relevant patient attributes.
    """
    mapping = {
        0x00100010: ('patient_name', 'PN'),
        0x00100020: ('patient_id', 'LO'),
        0x00100021: ('issuer_of_patient_id', 'LO'),
        0x00100030: ('patient_birth_date', 'DA'),
        0x00100032: ('patient_birth_time', 'TM'),
        0x00100040: ('patient_sex', 'CS'),
        0x00101001: ('other_patient_names', 'PN'),
        0x00102160: ('ethnic_group', 'SH'),
        0x00104000: ('patient_comments', 'LT')
    }

    #: Patinet's Name (0010, 0010) PN
    patient_name = peewee.CharField(max_length=64*5+4, index=True, null=True)

    #: Patient's ID (0010, 0020) LO
    patient_id = peewee.CharField(max_length=64, unique=True)

    #: Issuer of Patient's ID (0010, 0021) LO
    issuer_of_patient_id = peewee.CharField(max_length=64, index=True,
                                            null=True)

    #: Patient's Birth Date (0010, 0030) DA
    patient_birth_date = peewee.CharField(max_length=8, index=True, null=True)

    #: Patient's Birth Time (0010, 0032) TM
    patient_birth_time = peewee.CharField(max_length=14, index=True, null=True)

    #: Patient's Sex (0010, 0040) CS
    patient_sex = peewee.CharField(max_length=16, index=True, null=True)

    #: Other Patient's Names (0010, 1001) PN
    other_patient_names = peewee.TextField(default='')

    #: Ethnic Group (0010, 2160) SH
    ethnic_group = peewee.CharField(max_length=16, index=True, null=True)

    #: Patient Comments (0010, 4000) LT
    patient_comments = peewee.TextField(default='')

    # Number of Patient Related Studies (0020,1200)
    # Number of Patient Related Series (0020,1202)
    # Number of Patient Related Instances (0020,1204)

    @classmethod
    def c_store(cls, ds: pydicom.Dataset):
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
        return patient

    @classmethod
    def c_find(cls, ds: pydicom.Dataset):
        """C-FIND request handler for Patient level

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND result
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []

        select = [Patient]
        if 'NumberOfPatientRelatedStudies' in ds:
            select.append(
                peewee.fn.Count(Study.id)\
                    .alias('number_of_patient_related_studies')  # pylint: disable=no-member
                )
            response_attrs.append(
                (0x00201200, 'number_of_patient_related_studies', 'IS')
            )
            joins.add((Patient, Study))
        if 'NumberOfPatientRelatedSeries' in ds:
            select.append(
                peewee.fn.Count(Series.id)\
                    .alias('number_of_patient_related_series')  # pylint: disable=no-member
            )
            response_attrs.append(
                (0x00201202, 'number_of_patient_related_series', 'IS')
            )
            joins.union([(Patient, Study), (Study, Series)])
        if 'NumberOfPatientRelatedInstances' in ds:
            select.append(
                peewee.fn.Count(Instance.id)\
                    .alias('number_of_patient_related_instances')  # pylint: disable=no-member
            )
            response_attrs.append(
                (0x00201204, 'number_of_patient_related_instances', 'IS')
            )
            joins.union([(Patient, Study), (Study, Series), (Series, Instance)])

        query = Patient.select(*select)
        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = _build_filters(cls, query, ds)
        response_attrs.extend(_response_attrs)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        yield from (
            _encode_response(p, response_attrs, encoding) for p in query
        )


class Study(peewee.Model):
    """Study model.

    Stores all relevant C-FIND attributes.
    """
    mapping = {
        0x00080020: ('study_date', 'DA'),
        0x00080030: ('study_time', 'TM'),
        0x00080050: ('accession_number', 'SH'),
        0x00200010: ('study_id', 'SH'),
        0x0020000D: ('study_instance_uid', 'UI'),
        0x00081030: ('study_description', 'LO'),
        0x00080090: ('referring_physician_name', 'PN'),
        0x00081060: ('name_of_physicians_reading_study', 'PN'),
        0x00081080: ('admitting_diagnoses_description', 'LO'),
        0x00101010: ('patient_age', 'AS'),
        0x00101020: ('patient_size', 'DS'),
        0x00101030: ('patient_weight', 'DS'),
        0x00102180: ('occupation', 'SH'),
        0x001021B0: ('additional_patient_history', 'LT')
    }

    #: Reference to Patient
    patient = peewee.ForeignKeyField(Patient)

    #: Study Date (0008, 0020) DA
    study_date = peewee.CharField(max_length=8, index=True, null=True)

    #: Study Time (0008, 0030) TM
    study_time = peewee.CharField(max_length=14, index=True, null=True)

    #: Accession Number (0008, 0050) SH
    accession_number = peewee.CharField(max_length=16, index=True, null=True)

    #: Study ID (0020, 0010) SH
    study_id = peewee.CharField(max_length=16, index=True, null=True)

    #: Study Instance UID (0020, 000D) UI
    study_instance_uid = peewee.CharField(max_length=64, unique=True)

    #: Study Description (0008,1030) LO
    study_description = peewee.CharField(max_length=64, index=True, null=True)

    #: Referring Physician Name (0008, 0090) PN
    referring_physician_name = peewee.CharField(max_length=5*64+4, index=True,
                                                null=True)

    #: Name Of Physicians Reading Study (0008, 1060) PN
    name_of_physicians_reading_study = peewee.TextField(default='')

    #: Admitting Diagnoses Description (0008, 1080) LO
    admitting_diagnoses_description = peewee.CharField(
        max_length=64, index=True, null=True
    )

    #: Patient Age (0010, 1010) AS
    patient_age = peewee.CharField(max_length=4, index=True, null=True)

    #: Patient Size (0010, 1020) DS
    patient_size = peewee.CharField(max_length=16, index=True, null=True)

    #: Patient Weight (0010, 1030) DS
    patient_weight = peewee.CharField(max_length=16, index=True, null=True)

    #: Occupation (0010, 2180) SH
    occupation = peewee.CharField(max_length=16, index=True, null=True)

    #: Additional Patient History (0010, 21B0) LT
    additional_patient_history = peewee.TextField(default='')

    # Modalities in Study (0008,0061)
    # SOP Classes in Study (0008,0062)
    # Other Study Numbers (0020,1070)
    # Number of Study Related Series (0020,1206)
    # Number of Study Related Instances (0020,1208)

    @classmethod
    def c_store(self, patient: Patient, ds: pydicom.Dataset):
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
            return Study.get(Study.study_instance_uid == study_instance_uid)
        except Study.DoesNotExist:  # pylint: disable=no-member
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
            return Study.create(
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

    @classmethod
    def c_find(cls, ds: pydicom.Dataset):
        """C-FIND request handler for Study level

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND result
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []
        select = [Study]
        upper_level_filters = []

        patient_attrs = [e for e in ds if e.tag in Patient.mapping]
        skipped = set(e.tag for e in patient_attrs)
        if patient_attrs:
            upper_level_filters.extend(_filter_upper_level(Patient, patient_attrs))
            for tag, attr, vr, _, attr_name in upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('patient', attr_name), vr))
            joins.add((Study, Patient))

        if 'ModalitiesInStudy' in ds:
            # TODO: Add modalities in study filter
            agg_fun = db.string_agg_func()
            select.append(
                agg_fun(Series.modality, '\\').alias('modalities_in_study')
            )
            response_attrs.append((0x00080061, 'modalities_in_study', 'CS'))
            joins.add((Study, Series))
        if 'SOPClassesInStudy' in ds:
            agg_fun = db.string_agg_func()
            select.append(
                agg_fun(Instance.sop_class_uid, '\\').alias('sop_classes_in_study')
            )
            response_attrs.append((0x00080062, 'sop_classes_in_study', 'UI'))
            joins.union([(Study, Series), (Series, Instance)])
        if 'NumberOfStudyRelatedSeries' in ds:
            select.append(
                peewee.fn.Count(Series.id)\
                    .alias('number_of_study_related_series')  # pylint: disable=no-member
            )
            response_attrs.append((0x00201202, 'number_of_study_related_series', 'IS'))
            joins.add((Study, Series))
        if 'NumberOfStudyRelatedInstances' in ds:
            select.append(
                peewee.fn.Count(Instance.id)\
                    .alias('number_of_study_related_instances')  # pylint: disable=no-member
            )
            response_attrs.append((0x00201204, 'number_of_study_related_instances', 'IS'))
            joins.union([(Study, Series), (Series, Instance)])

        query = Study.select(*select)

        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = _build_filters(cls, query, ds, skipped)
        response_attrs.extend(_response_attrs)
        for _, attr, vr, elem, _ in upper_level_filters:
            if not elem.value:
                continue
            query = _build_filter(query, attr, vr, elem)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        yield from (_encode_response(s, response_attrs, encoding) for s in query)


class Series(peewee.Model):
    """Series model.

    Stores all relevant C-FIND attributes.
    """
    mapping = {
        0x00080060: ('modality', 'CS'),
        0x00200011: ('series_number', 'IS'),
        0x0020000E: ('series_instance_uid', 'UI')
    }

    #: Reference to Study
    study = peewee.ForeignKeyField(Study)

    #: Modality (0008, 0060) CS
    modality = peewee.CharField(max_length=16, index=True, null=True)

    #: Series Number (0020, 0011) IS
    series_number = peewee.CharField(max_length=12, index=True, null=True)

    #: Series Instance UID (0020, 000E) UI
    series_instance_uid = peewee.CharField(max_length=64, unique=True)

    # Number of Series Related Instances (0020,1209)

    @classmethod
    def c_store(cls, study: Study, ds: pydicom.Dataset):
        """C-STORE handler

        :param study: study reference
        :type study: Study
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing series record, that matches incoming dataset
        :rtype: Series
        """
        series_instance_uid = ds.SeriesInstanceUID
        try:
            return Series.get(Series.series_instance_uid == series_instance_uid)
        except Series.DoesNotExist:  # pylint: disable=no-member
            modality = getattr(ds, 'Modality', None)
            series_number = getattr(ds, 'SeriesNumber', None)
            return Series.create(
                study=study,
                series_instance_uid=series_instance_uid,
                modality=modality,
                series_number=series_number
            )

    @classmethod
    def c_find(cls, ds: pydicom.Dataset):
        """C-FIND handler

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND results
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []
        select = [Series]
        upper_level_filters = []

        skipped = set()

        patient_attrs = [e for e in ds if e.tag in Patient.mapping]
        skipped.update(e.tag for e in patient_attrs)
        if patient_attrs:
            _upper_level_filters = _filter_upper_level(Patient, patient_attrs)
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('study', 'patient', attr_name), vr))
            joins.update([(Series, Study), (Study, Patient)])

        study_attrs = [e for e in ds if e.tag in Study.mapping]
        skipped.update(e.tag for e in study_attrs)
        if study_attrs:
            _upper_level_filters = _filter_upper_level(Study, study_attrs)
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('study', attr_name), vr))
            joins.update([(Series, Study)])

        if 'NumberOfSeriesRelatedInstances' in ds:
            select.append(
                peewee.fn.Count(Instance.id)\
                    .alias('number_of_study_related_series')  # pylint: disable=no-member
            )
            response_attrs.append((0x00201209, 'number_of_series_related_instances', 'IS'))
            joins.add((Series, Instance))

        query = Series.select(*select)

        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = _build_filters(cls, query, ds, skipped)
        response_attrs.extend(_response_attrs)
        for _, attr, vr, elem, _ in upper_level_filters:
            if not elem.value:
                continue
            query = _build_filter(query, attr, vr, elem)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        yield from (_encode_response(s, response_attrs, encoding) for s in query)


class Instance(peewee.Model):
    """Instance model.

    Stores all relevant C-FIND attributes.
    """
    mapping = {
        0x00200013: ('instance_number', 'IS'),
        0x00080018: ('sop_instance_uid', 'UI'),
        0x00080016: ('sop_class_uid', 'UI'),
        0x00400512: ('container_identifier', 'LO')
    }

    #: Series reference
    series = peewee.ForeignKeyField(Series)

    #: Instance Number (0020, 0013) IS
    instance_number = peewee.CharField(max_length=12, index=True, null=True)

    #: SOP Instance UID (0008, 0018) UI
    sop_instance_uid = peewee.CharField(max_length=64, unique=True)

    #: SOP Class UID (0008, 0016) UI
    sop_class_uid = peewee.CharField(max_length=64, index=True, null=True)

    #: Container Identifier (0040, 0512) LO
    container_identifier = peewee.CharField(max_length=64, index=True, null=True)

    # Available Transfer Syntax UID (0008,3002)
    # Related General SOP Class UID (0008,001A)

    @classmethod
    def c_store(cls, series: Series, ds: pydicom.Dataset):
        """C-STORE handler

        :param series: series reference
        :type series: Series
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing instance record, that matches incoming dataset
        :rtype: Instance
        """
        sop_instance_uid = ds.SOPInstanceUID
        try:
            return Instance.get(Instance.sop_instance_uid == sop_instance_uid)
        except Instance.DoesNotExist:  # pylint: disable=no-member
            instance_number = getattr(ds, 'InstanceNumber', None)
            sop_class_uid = getattr(ds, 'SOPClassUID', None)
            container_identifier = getattr(ds, 'ContainerIdentifier', None)
            return Instance.create(
                series=series,
                sop_instance_uid=sop_instance_uid,
                instance_number=instance_number,
                sop_class_uid=sop_class_uid,
                container_identifier=container_identifier
            )

    @classmethod
    def c_find(cls, ds: pydicom.Dataset):
        """C-FIND handler

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND results
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []
        select = [Instance]
        upper_level_filters = []

        skipped = set()

        patient_attrs = [e for e in ds if e.tag in Patient.mapping]
        skipped.update(e.tag for e in patient_attrs)
        if patient_attrs:
            _upper_level_filters = _filter_upper_level(Patient, patient_attrs)
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('series', 'study', 'patient', attr_name), vr))
            joins.update([(Instance, Series), (Series, Study), (Study, Patient)])

        study_attrs = [e for e in ds if e.tag in Study.mapping]
        skipped.union(e.tag for e in study_attrs)
        if study_attrs:
            _upper_level_filters = _filter_upper_level(Study, study_attrs)
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('series', 'study', attr_name), vr))
            joins.update([(Instance, Series), (Series, Study)])

        series_attrs = [e for e in ds if e.tag in Series.mapping]
        skipped.union(e.tag for e in series_attrs)
        if series_attrs:
            _upper_level_filters = _filter_upper_level(Series, series_attrs)
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('series', attr_name), vr))
            joins.add((Instance, Series))

        query = Instance.select(*select)

        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = _build_filters(cls, query, ds, skipped)
        response_attrs.extend(_response_attrs)
        for _, attr, vr, elem, _ in upper_level_filters:
            if not elem.value:
                continue
            query = _build_filter(query, attr, vr, elem)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        yield from (_encode_response(s, response_attrs, encoding) for s in query)


def _build_filters(model, query, ds: pydicom.Dataset, skipped=None):
    """Build filters for provided model

    :param model: PACS level model
    :type model: peewee.Model
    :param query: C-FIND SQL query
    :type query: peewee.Query
    :param ds: C-FIND request
    :type ds: pydicom.Dataset
    :param skipped: skipped attributes, defaults to None
    :type skipped: list, optional
    :return: query and response attributes
    :rtype: tuple
    """
    if skipped is None:
        skipped = set()
    response_attrs = []
    for elem in ds:
        if elem.tag in EXCLUDED_ATTRS or elem.tag in skipped:
            continue

        try:
            attr_name, vr = model.mapping[elem.tag]
        except KeyError:
            response_attrs.append((elem.tag, None, elem.VR))
            continue

        response_attrs.append((elem.tag, attr_name, vr))
        if elem.is_empty:
            continue

        attr = getattr(model, attr_name)
        query = _build_filter(query, attr, vr, elem)
    return query, response_attrs


def _build_filter(query, attr, vr, elem):
    """Build filter for specific attribute

    :param query: current SQL query
    :type query: peewee.Query
    :param attr: C-FIND request attribute
    :type attr: [type]
    :param vr: element VR
    :type vr: str
    :param elem: DICOM element
    :type elem: [type]
    :raises ValueError: raises ValueError for unsupported VR
    :return: query with added filter
    :rtype: peewee.Query
    """
    if vr in TEXT_VR:
        if vr == 'PN':
            value = str(elem.value)
        else:
            value = elem.value
        return _text_filter(query, attr, value)
    elif vr == 'DA':
        return _date_filter(query, attr, elem.value)
    elif vr == 'TM':
        return _time_filter(query, attr, elem.value)
    elif vr == 'DT':
        return _date_time_filter(query, attr, elem.value)
    raise ValueError(f'Unsupported VR: {vr}')


def _filter_upper_level(model, elements: list):
    """Build filter for upper C-FIND level

    :param model: peewee model
    :type model: peewee.Model
    :param elements: upper level elements
    :type elements: list
    :yield: tuple of tag, attribute, VR, element and attribute name
    :rtype: tuple
    """
    for elem in elements:
        attr_name, vr = model.mapping[elem.tag]
        attr = getattr(model, attr_name)
        yield elem.tag, attr, vr, elem, attr_name


def _encode_response(instance, response_attrs: list, encoding: str):
    """Creates a C-FIND response dataset

    :param instance: database model instance
    :type instance: peewee.Model
    :param response_attrs: list of response attributes (tag, attribute name in
                           the database model and VR)
    :type response_attrs: list
    :param encoding: response encoding
    :type encoding: str
    :return: C-FIND-RSP dataset
    :rtype: pydicom.Dataset
    """
    rsp = pydicom.Dataset()
    rsp.SpecificCharacterSet = encoding
    for tag, attr_name, vr in response_attrs:
        if attr_name is None:
            # Attribute not supported
            rsp.add_new(tag, vr, None)
        else:
            if not isinstance(attr_name, tuple):
                attr = getattr(instance, attr_name)
            else:
                attr = instance
                for field in attr_name:
                    attr = getattr(attr, field)
            rsp.add_new(tag, vr, attr)
    return rsp

def _text_filter(query: peewee.Query, attr, value: str):
    if isinstance(value, list):
        return query.where(attr << value)
    value = value.replace('?', '_')
    value = value.replace('*', '%')
    return query.where(attr ** value)


def _date_filter(query: peewee.Query, attr, value: str):
    if '-' in value:
        start, end = value.split('-')
        # TODO: Add normalization for shorter value
        return query.where((attr >= start) & (attr <= end))

    return query.where(attr == value)


def _time_filter(query: peewee.Query, attr, value: str):
    if '-' in value:
        start, end = value.split('-')
        # TODO: Add normalization for shorter value
        return query.where((attr >= start) & (attr <= end))

    return query.where(attr == value)


def _date_time_filter(query: peewee.Query, attr, value: str):
    if '-' in value:
        start, end = value.split('-')
        # TODO: Add normalization for shorter value
        return query.where((attr >= start) & (attr <= end))

    return query.where(attr == value)
