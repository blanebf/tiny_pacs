"""PACS component implementation.

Provides C-STORE, C-FIND, C-MOVE/C-GET, Storage Commitment handling and
the related database interactions.
"""
import enum
from collections.abc import Iterator
from itertools import chain
from typing import Any

import peewee
import pydicom
import trolleybus
from pydicom.uid import UID
from pynetdicom2 import statuses

from .. import component, events, schema
from . import (
    archive_api,
    instance_api,
    models,
    patient_api,
    series_api,
    study_api,
)


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


#: Tables owned by the PACS component, in foreign key creation order
TABLES: list[type[peewee.Model]] = [
    models.Patient, models.Study, models.Series, models.Instance
]

#: Schema migrations of the PACS tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create PACS tables')
]


class PACSConfig(component.ComponentConfig):
    """Configuration of the :class:`PACS` component.

    The PACS component currently has no settings beyond the common ``on``
    flag; this model exists so the component provides its own config type to
    the loader like every other component.
    """


class PACS(component.Component[PACSConfig]):
    """Component that implements PACS services themselves.

    Handles the following events:

        * :class:`~tiny_pacs.events.StoreDataset`
        * :class:`~tiny_pacs.events.Find`
        * :class:`~tiny_pacs.events.Move`
        * :class:`~tiny_pacs.events.Get`
        * :class:`~tiny_pacs.events.Commitment`
        * :class:`~tiny_pacs.events.ArchivePatientQuery`
        * :class:`~tiny_pacs.events.ArchiveStudyQuery`
        * :class:`~tiny_pacs.events.ArchiveSeriesQuery`
        * :class:`~tiny_pacs.events.ArchiveInstanceQuery`
        * :class:`~tiny_pacs.events.Migrations`

    Component also handles all relevant DB interactions, except for keeping
    track of stored datasets. That function is relegated to components in
    :mod:`~tiny_pacs.storage`
    """

    config_model = PACSConfig

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: PACSConfig | dict[str, Any]
    ) -> None:
        """Component initialization

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: PACSConfig or dict
        """
        super().__init__(bus, config)
        self.patient_api = patient_api.PatientAPI(bus)
        self.study_api = study_api.StudyAPI(bus)
        self.series_api = series_api.SeriesAPI(bus)
        self.instance_api = instance_api.InstanceAPI(bus)

        self.subscribe(events.StoreDataset, self.on_store_dataset)
        self.subscribe(events.Find, self.on_find)
        self.subscribe(events.Move, self.on_move)
        self.subscribe(events.Get, self.on_get)
        self.subscribe(events.Commitment, self.on_commitment)
        self.subscribe(events.ArchivePatientQuery, self.on_archive_patients)
        self.subscribe(events.ArchiveStudyQuery, self.on_archive_studies)
        self.subscribe(events.ArchiveSeriesQuery, self.on_archive_series)
        self.subscribe(events.ArchiveInstanceQuery, self.on_archive_instances)
        self.subscribe(events.Migrations, self.migrations)

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), TABLES, MIGRATIONS
        )

    def atomic(self) -> Any:
        """Context manager for handling simple transactions

        :return: atomic transaction
        """
        return self.send_one(events.Atomic, None)

    def on_store_dataset(self, payload: events.StoreDatasetPayload) -> None:
        """Handling of an incoming decoded dataset

        Records the dataset attributes in the database and broadcasts
        :class:`~tiny_pacs.events.StoreDone` on success or
        :class:`~tiny_pacs.events.StoreFailure` on failure.

        :param payload: decoded dataset, transfer syntax and origin
        :type payload: events.StoreDatasetPayload
        :raises Exception: re-raised after broadcasting ``StoreFailure``
                           when the dataset could not be recorded, so the
                           emitter can map the failure to its own error
                           reporting (the AE answers a C-STORE failure
                           status)
        """
        ds = payload.ds
        self.log_info('Handling store request (origin: %s)', payload.origin)
        try:
            self.c_store(ds)
        except Exception as error:
            self.log_exception(f'Failed to store dataset: {error}')
            self.broadcast(events.StoreFailure, ds)
            raise
        self.log_info('Dataset successfully stored (origin: %s)',
                      payload.origin)
        self.broadcast(events.StoreDone, ds)

    def on_archive_patients(
            self, payload: events.ArchiveFilter
    ) -> list[events.ArchiveItem]:
        """Handles `ArchivePatientQuery` event

        :param payload: archive query filter
        :type payload: events.ArchiveFilter
        :return: matching patient items
        :rtype: list[events.ArchiveItem]
        """
        return archive_api.patients(payload)

    def on_archive_studies(
            self, payload: events.ArchiveFilter
    ) -> list[events.ArchiveItem]:
        """Handles `ArchiveStudyQuery` event

        :param payload: archive query filter
        :type payload: events.ArchiveFilter
        :return: matching study items
        :rtype: list[events.ArchiveItem]
        """
        return archive_api.studies(payload)

    def on_archive_series(
            self, payload: events.ArchiveFilter
    ) -> list[events.ArchiveItem]:
        """Handles `ArchiveSeriesQuery` event

        :param payload: archive query filter
        :type payload: events.ArchiveFilter
        :return: matching series items
        :rtype: list[events.ArchiveItem]
        """
        return archive_api.series(payload)

    def on_archive_instances(
            self, payload: events.ArchiveFilter
    ) -> list[events.ArchiveItem]:
        """Handles `ArchiveInstanceQuery` event

        :param payload: archive query filter
        :type payload: events.ArchiveFilter
        :return: matching instance items
        :rtype: list[events.ArchiveItem]
        """
        return archive_api.instances(payload)

    def on_find(
            self,
            payload: events.FindPayload
    ) -> Iterator[tuple[pydicom.Dataset, statuses.Status]]:
        """Handling of incoming find request

        :param payload: presentation context and incoming dataset
        :type payload: events.FindPayload
        :yield: tuple of find result and pending status
        :rtype: tuple
        """
        results = self.c_find(payload.ds)
        yield from ((r, statuses.C_FIND_PENDING) for r in results)

    def on_move(self, payload: events.MovePayload) -> list[events.StoredFile]:
        """Handling of incoming move request

        :param payload: presentation context, incoming dataset and move
                        destination
        :type payload: events.MovePayload
        :return: list of stored files: tuples of SOP Class UID, Transfer
                 Syntax UID and either a file name, a dataset or a file
                 object
        :rtype: list[events.StoredFile]
        """
        destination = payload.destination
        self.log_info('Handling move request to %s (%r)', destination,
                      payload.context)
        instances = [
            uid for _, _, uid in self.c_move_get_instances(payload.ds)
        ]
        self.log_debug('Moving instances: %r', instances)
        results = self.broadcast(events.GetFiles, instances)
        return list(chain.from_iterable(results))

    def on_get(self, payload: events.GetPayload) -> list[events.StoredFile]:
        """Handling of incoming get request

        :param payload: presentation context and incoming dataset
        :type payload: events.GetPayload
        :return: list of stored files: tuples of SOP Class UID, Transfer
                 Syntax UID and either a file name, a dataset or a file
                 object
        :rtype: list[events.StoredFile]
        """
        self.log_info('Handling get request (%r)', payload.context)
        instances = [
            uid for _, _, uid in self.c_move_get_instances(payload.ds)
        ]
        self.log_debug('Getting instances: %r', instances)
        results = self.broadcast(events.GetFiles, instances)
        return list(chain.from_iterable(results))

    def on_commitment(
            self,
            uids: list[tuple[UID, UID]]
    ) -> tuple[list[tuple[UID, UID]], list[tuple[UID, UID]]]:
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

    def c_store(self, ds: pydicom.Dataset) -> None:
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

    def c_move_get_instances(
            self,
            ds: pydicom.Dataset
    ) -> Iterator[tuple[str, str, str]]:
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
            query = query.where(
                models.Series.series_instance_uid << series_uids
            )

        if level == QRLevelRank.IMAGE:
            sop_instance_uids = ds.SOPInstanceUID
            if not isinstance(sop_instance_uids, list):
                sop_instance_uids = [sop_instance_uids]
            query = query.where(
                models.Instance.sop_instance_uid << sop_instance_uids
            )

        for instance in query:
            series = instance.series
            study = series.study
            yield (study.study_instance_uid,
                   series.series_instance_uid,
                   instance.sop_instance_uid)
