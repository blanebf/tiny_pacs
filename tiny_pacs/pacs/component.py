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

#: Columns of the version-1 ``patient`` table in table order, used by
#: migration 2 to copy the rows into the rebuilt table
_PATIENT_V1_COLUMNS = (
    'id', 'patient_name', 'patient_id', 'issuer_of_patient_id',
    'patient_birth_date', 'patient_birth_time', 'patient_sex',
    'other_patient_names', 'ethnic_group', 'patient_comments'
)

#: Columns added by migration 2
_PATIENT_V2_COLUMNS = ('patient_identity_removed', 'deidentification_method')


def _upgrade_patient_identity(migrator: Any) -> list[Any]:
    """Migration 2: the DICOM patient identity key.

    Replaces the single-column unique constraint on ``patient_id`` with
    the (``patient_id``, ``issuer_of_patient_id``) pair (PS3.3 C.7.1.1:
    the issuer scopes the uniqueness of the ID), normalizes issuer-less
    rows to the empty issuer and adds the de-identification columns
    ``patient_identity_removed`` / ``deidentification_method``.

    Executed directly through raw SQL (no ``playhouse`` operations are
    returned): SQLite cannot drop an inline column constraint in place,
    so the table is rebuilt with the procedure recommended for SQLite
    DDL — foreign key enforcement is off by default and the child tables
    keep their ``REFERENCES "patient"`` clauses through the drop and
    rename — and the new table definition comes from the model itself,
    so migrated and fresh databases end up identical.

    :param migrator: driver-specific ``playhouse.migrate`` migrator
    :type migrator: Any
    :return: no declarative operations; the migration ran to completion
    :rtype: list
    """
    database = migrator.database
    if not database.table_exists('patient'):
        return []
    if isinstance(database, peewee.SqliteDatabase):
        _rebuild_patient_sqlite(database)
    else:
        _migrate_patient_postgres(database)
    return []


def _rebuild_patient_sqlite(database: peewee.SqliteDatabase) -> None:
    """Rebuilds the ``patient`` table in place on SQLite.

    :param database: the active SQLite connection wrapper
    :type database: peewee.SqliteDatabase
    """
    class _PatientV2(models.Patient):
        """Version-2 patient table under a temporary name."""

        class Meta:
            table_name = 'patient__v2'

    cursor = database.execute_sql('PRAGMA table_info("patient")')
    existing = {row[1] for row in cursor.fetchall()}
    target: list[str] = []
    source: list[str] = []
    for column in _PATIENT_V1_COLUMNS:
        if column not in existing and column != 'issuer_of_patient_id':
            continue
        target.append(f'"{column}"')
        if column == 'issuer_of_patient_id':
            source.append(
                'COALESCE("issuer_of_patient_id", \'\')'
                if column in existing else '\'\''
            )
        else:
            source.append(f'"{column}"')
    target.extend(f'"{c}"' for c in _PATIENT_V2_COLUMNS)
    source.extend(['\'\'', '\'\''])

    _PatientV2.create_table(safe=True)
    database.execute_sql(
        f'INSERT INTO "patient__v2" ({", ".join(target)}) '
        f'SELECT {", ".join(source)} FROM "patient"'
    )
    # Index names of the temporary table, collected before the rename
    # (sqlite_master rows follow the table)
    cursor = database.execute_sql(
        'SELECT "name" FROM "sqlite_master" WHERE "type" = \'index\' '
        'AND "tbl_name" = \'patient__v2\''
    )
    temp_indexes = [
        row[0] for row in cursor.fetchall()
        if not row[0].startswith('sqlite_autoindex')
    ]
    database.execute_sql('DROP TABLE "patient"')
    database.execute_sql('ALTER TABLE "patient__v2" RENAME TO "patient"')
    # The indexes created for the temporary table keep its name prefix;
    # drop them and recreate them from the model so the migrated
    # database matches a fresh one exactly
    for name in temp_indexes:
        database.execute_sql(f'DROP INDEX IF EXISTS "{name}"')
    models.Patient.create_table(safe=True)


def _migrate_patient_postgres(database: peewee.PostgresqlDatabase) -> None:
    """Applies the identity-key migration on PostgreSQL.

    ``DEFAULT ''`` clauses serve only to backfill the existing rows and
    are dropped again afterwards, so the migrated schema matches the
    fresh one exactly (peewee defaults are Python-side, never server
    defaults). The version-1 uniqueness of ``patient_id`` arrives both
    as a ``patient_patient_id_key`` constraint (hand-written schemas)
    and as a ``patient_patient_id`` unique index (peewee emits
    ``unique=True`` as ``CREATE UNIQUE INDEX``), so both forms are
    dropped.

    :param database: the active PostgreSQL connection wrapper
    :type database: peewee.PostgresqlDatabase
    """
    for statement in (
        'UPDATE "patient" SET "issuer_of_patient_id" = \'\' '
        'WHERE "issuer_of_patient_id" IS NULL',
        'ALTER TABLE "patient" ALTER COLUMN "issuer_of_patient_id" '
        'SET NOT NULL',
        'ALTER TABLE "patient" ADD COLUMN IF NOT EXISTS '
        '"patient_identity_removed" VARCHAR(16) NOT NULL DEFAULT \'\'',
        'ALTER TABLE "patient" ADD COLUMN IF NOT EXISTS '
        '"deidentification_method" TEXT NOT NULL DEFAULT \'\'',
        'ALTER TABLE "patient" ALTER COLUMN "patient_identity_removed" '
        'DROP DEFAULT',
        'ALTER TABLE "patient" ALTER COLUMN "deidentification_method" '
        'DROP DEFAULT',
        'ALTER TABLE "patient" DROP CONSTRAINT IF EXISTS '
        '"patient_patient_id_key"',
        'DROP INDEX IF EXISTS "patient_patient_id_key"',
        'DROP INDEX IF EXISTS "patient_patient_id"'
    ):
        database.execute_sql(statement)
    # Creates the composite unique index and the issuer index from the
    # current model definition (parity with fresh databases)
    models.Patient.create_table(safe=True)


#: Schema migrations of the PACS tables
MIGRATIONS: list[schema.Migration] = [
    schema.create_tables_migration(TABLES, 'Create PACS tables'),
    schema.Migration(
        2,
        'Patient identity key: (PatientID, IssuerOfPatientID) unique, '
        'de-identification columns',
        _upgrade_patient_identity
    )
]


class PACSConfig(component.ComponentConfig):
    """Configuration of the :class:`PACS` component.

    :ivar anonymous_patient_ids: Patient ID values (recognized
        case-insensitively) treated as de-identification placeholders
        of incoming datasets. Datasets carrying one of them — like the
        datasets with an empty Patient ID or with the normative PS3.15 E
        de-identification attributes — are stored without demographic
        conflict warnings, because the identity attributes of anonymized
        data carry no identity semantics (one shared record per distinct
        Patient ID string).
    """

    anonymous_patient_ids: list[str] = \
        list(patient_api.DEFAULT_ANONYMOUS_PATIENT_IDS)


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
        self.patient_api = patient_api.PatientAPI(
            bus, self.config.anonymous_patient_ids
        )
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

        issuer = getattr(ds, 'IssuerOfPatientID', '')
        if issuer:
            query = query.where(
                models.Patient.issuer_of_patient_id == str(issuer)
            )

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
