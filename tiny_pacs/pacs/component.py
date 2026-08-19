# -*- coding: utf-8 -*-
import enum
from itertools import chain
from typing import Iterator

import pydicom
import trolleybus
from pynetdicom2 import statuses

from .. import component
from .. import events

from . import models
from . import patient_api
from . import study_api
from . import series_api
from . import instance_api


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
TEXT_VR = ['AE', 'CS', 'LO', 'LT', 'PN', 'SH', 'ST', 'UC', 'UR', 'UT', 'UI']


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

        * :class:`~tiny_pacs.events.Store`
        * :class:`~tiny_pacs.events.Find`
        * :class:`~tiny_pacs.events.Move`
        * :class:`~tiny_pacs.events.Get`
        * :class:`~tiny_pacs.events.Commitment`

    Component also handles all relevant DB interactions, except for keeping
    track of stored datasets. That function is relegated to components in
    :module:`~tiny_pacs.storage`
    """

    def __init__(self, bus: trolleybus.EventBus, config: dict):
        """Component initialization

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: dict
        """
        super().__init__(bus, config)
        self.patient_api = patient_api.PatientAPI(bus)
        self.study_api = study_api.StudyAPI(bus)
        self.series_api = series_api.SeriesAPI(bus)
        self.instance_api = instance_api.InstanceAPI(bus)

        self.subscribe(events.Store, self.on_store)
        self.subscribe(events.Find, self.on_find)
        self.subscribe(events.Move, self.on_move)
        self.subscribe(events.Get, self.on_get)
        self.subscribe(events.Commitment, self.on_commitment)
        self.subscribe(events.Tables, self.tables)

    def tables(self, _: None = None):
        """Returns a list of tables for DB component

        :return: list of tables used by this component
        :rtype: list
        """
        return [models.Patient, models.Study, models.Series, models.Instance]

    def atomic(self):
        """Context manager for handling simple transactions

        :return: atomic transaction
        """
        return self.send_one(events.Atomic, None)

    def on_store(self, payload: events.StorePayload):
        """Handling of incoming storage request

        :param payload: presentation context and incoming dataset
        :type payload: events.StorePayload
        :return: C-STORE handling status
        :rtype: pynetdicom2.statuses.Status
        """
        context = payload.context
        self.log_info('Handling store request (%r)', context)
        try:
            ds = pydicom.dcmread(payload.ds, stop_before_pixels=True)
            self.c_store(ds)
        except Exception as error:  # pylint: disable=broad-except
            self.log_exception(f'Failed to store dataset: {error}')
            self.broadcast(events.StoreFailure, ds)
            return statuses.C_STORE_CANNOT_UNDERSTAND
        else:
            self.log_info('Dataset successfully stored (%r)', context)
            self.broadcast(events.StoreDone, ds)
            return statuses.SUCCESS

    def on_find(self, payload: events.FindPayload):
        """Handling of incoming find request

        :param payload: presentation context and incoming dataset
        :type payload: events.FindPayload
        :yield: tuple of find result and pending status
        :rtype: tuple
        """
        results = self.c_find(payload.ds)
        yield from ((r, statuses.C_FIND_PENDING) for r in results)

    def on_move(self, payload: events.MovePayload):
        """Handling of incoming move request

        :param payload: presentation context, incoming dataset and move
                        destination
        :type payload: events.MovePayload
        :return: list of tuples: SOP Class UID, Transfer Syntax and either
                 filename or dataset
        :rtype: list
        """
        destination = payload.destination
        self.log_info('Handling move request to %s (%r)', destination, payload.context)
        instances = [uid for _, _, uid in self.c_move_get_instances(payload.ds)]
        self.log_debug('Moving instances: %r', instances)
        results = self.broadcast(events.GetFiles, instances)
        return list(chain.from_iterable(results))

    def on_get(self, payload: events.GetPayload):
        """Handling of incoming get request

        :param payload: presentation context and incoming dataset
        :type payload: events.GetPayload
        :return: list of tuples: SOP Class UID, Transfer Syntax and either
                 filename or dataset
        :rtype: list
        """
        self.log_info('Handling get request (%r)', payload.context)
        instances = [uid for _, _, uid in self.c_move_get_instances(payload.ds)]
        self.log_debug('Getting instances: %r', instances)
        results = self.broadcast(events.GetFiles, instances)
        return list(chain.from_iterable(results))

    def on_commitment(self, uids: list):
        """Handling of incoming storage commitment request

        :param uids: list of tuple (SOP Class UID, SOP Instance UID)
        :type uids: list
        :return: tuple of two list: successes and failures
        :rtype: tuple
        """
        self.log_info('Handling Storage Commitment')
        self.log_debug('Verifying %r instances', uids)
        results = self.broadcast(events.StoreVerify, uids)
        success = chain.from_iterable(s for s, _ in results)
        failure = chain.from_iterable(f for _, f in results)
        return list(success), list(failure)

    def c_find(self, ds: pydicom.Dataset) -> Iterator[pydicom.Dataset]:
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
            yield from self.patient_api.c_find(ds)
        elif level == 'STUDY':
            yield from self.study_api.c_find(ds)
        elif level == 'SERIES':
            yield from self.series_api.c_find(ds)
        elif level == 'IMAGE':
            yield from self.instance_api.c_find(ds)

    def c_store(self, ds: pydicom.Dataset):
        """C-STORE implementation

        Store dataset attributes in a database

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        """
        with self.atomic():
            patient = self.patient_api.c_store(ds)
            study = self.study_api.c_store(patient, ds)
            series = self.series_api.c_store(study, ds)
            self.instance_api.c_store(series, ds)

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
        query = models.Instance.select(
            models.Instance.sop_instance_uid,
            models.Series.series_instance_uid,
            models.Study.study_instance_uid
            )\
            .join(models.Series)\
            .join(models.Study)\
            .join(models.Patient)

        if (level == QRLevelRank.PATIENT or
                (level.value > QRLevelRank.PATIENT.value and
                 hasattr(ds, 'PatientID'))):
            query = query.where(models.Patient.patient_id == ds.PatientID)

        if (level == QRLevelRank.STUDY or
                (level.value > QRLevelRank.STUDY.value and
                 hasattr(ds, 'StudyInstanceUID'))):
            study_uids = ds.StudyInstanceUID
            if not isinstance(study_uids, list):
                study_uids = [study_uids]
            query = query.where(models.Study.study_instance_uid << study_uids)

        if (level == QRLevelRank.SERIES or
                (level.value > QRLevelRank.SERIES.value and
                 hasattr(ds, 'SeriesInstanceUID'))):
            series_uids = ds.SeriesInstanceUID
            if not isinstance(series_uids, list):
                series_uids = [series_uids]
            query = query.where(models.Series.series_instance_uid << series_uids)

        if level == QRLevelRank.IMAGE:
            sop_instance_uids = ds.SOPInstanceUID
            if not isinstance(sop_instance_uids, list):
                sop_instance_uids = [sop_instance_uids]
            query = query.where(models.Instance.sop_instance_uid << sop_instance_uids)

        for instance in query:
            series = instance.series
            study = series.study
            yield (study.study_instance_uid, series.series_instance_uid, instance.sop_instance_uid)
