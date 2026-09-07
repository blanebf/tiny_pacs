"""Archive query API.

Answers the archive query events (:class:`~tiny_pacs.events.ArchiveFilter`
to :class:`~tiny_pacs.events.ArchiveItem` lists) from the PACS database
tables. The items carry both the DB-level attributes and the DICOM view
(tag to ``(VR, value)``) built from the model mappings, so consumers —
the web administration app, DICOMweb — never re-derive DICOM semantics
from the tables.
"""
from typing import Any

import peewee

from .. import events
from . import models

#: Models of the levels above (and including) each queried level, in
#: top-down order: their mappings and fields are merged into the result
LEVEL_CHAINS: dict[str, list[Any]] = {
    'patient': [models.Patient],
    'study': [models.Patient, models.Study],
    'series': [models.Patient, models.Study, models.Series],
    'instance': [models.Patient, models.Study, models.Series, models.Instance]
}

#: Walk from a level model to the model above it
_UPPER: dict[Any, str] = {
    models.Study: 'patient',
    models.Series: 'study',
    models.Instance: 'series'
}


def patients(payload: events.ArchiveFilter) -> list[events.ArchiveItem]:
    """Queries the archive on the PATIENT level.

    :param payload: archive query filter
    :type payload: events.ArchiveFilter
    :return: matching patient items with the total match count
    :rtype: list[events.ArchiveItem]
    """
    query: peewee.ModelSelect[Any] = models.Patient.select()
    query = _patient_filters(query, payload)
    if payload.modality:
        query = query.where(models.Patient.id << _patient_ids(payload))
    return _items('patient', models.Patient, query, payload)


def studies(payload: events.ArchiveFilter) -> list[events.ArchiveItem]:
    """Queries the archive on the STUDY level.

    The joined parent models are selected explicitly, so the whole page
    loads in a single query (no lazy per-row FK lookups).

    :param payload: archive query filter
    :type payload: events.ArchiveFilter
    :return: matching study items with the total match count
    :rtype: list[events.ArchiveItem]
    """
    query: peewee.ModelSelect[Any] = models.Study.select(
        models.Study, models.Patient
    ).join(models.Patient)
    query = _patient_filters(query, payload)
    query = _study_filters(query, payload)
    if payload.modality:
        query = query.where(models.Study.id << _study_ids(payload))
    return _items('study', models.Study, query, payload)


def series(payload: events.ArchiveFilter) -> list[events.ArchiveItem]:
    """Queries the archive on the SERIES level.

    The joined parent models are selected explicitly, so the whole page
    loads in a single query (no lazy per-row FK lookups).

    :param payload: archive query filter
    :type payload: events.ArchiveFilter
    :return: matching series items with the total match count
    :rtype: list[events.ArchiveItem]
    """
    query: peewee.ModelSelect[Any] = models.Series.select(
        models.Series, models.Study, models.Patient
    ).join(models.Study).join(models.Patient)
    query = _patient_filters(query, payload)
    query = _study_filters(query, payload)
    if payload.series_instance_uid:
        query = query.where(
            models.Series.series_instance_uid == payload.series_instance_uid
        )
    if payload.modality:
        query = query.where(models.Series.modality == payload.modality)
    return _items('series', models.Series, query, payload)


def instances(payload: events.ArchiveFilter) -> list[events.ArchiveItem]:
    """Queries the archive on the INSTANCE level.

    The joined parent models are selected explicitly, so the whole page
    loads in a single query (no lazy per-row FK lookups).

    :param payload: archive query filter
    :type payload: events.ArchiveFilter
    :return: matching instance items with the total match count
    :rtype: list[events.ArchiveItem]
    """
    query: peewee.ModelSelect[Any] = models.Instance.select(
        models.Instance, models.Series, models.Study, models.Patient
    ).join(models.Series)\
        .join(models.Study)\
        .join(models.Patient)
    query = _patient_filters(query, payload)
    query = _study_filters(query, payload)
    if payload.series_instance_uid:
        query = query.where(
            models.Series.series_instance_uid == payload.series_instance_uid
        )
    if payload.modality:
        query = query.where(models.Series.modality == payload.modality)
    if payload.sop_instance_uid:
        query = query.where(
            models.Instance.sop_instance_uid == payload.sop_instance_uid
        )
    return _items('instance', models.Instance, query, payload)


def _patient_ids(payload: events.ArchiveFilter) -> Any:
    """Patient ids owning at least one study with the filtered modality."""
    return models.Study.select(models.Study.patient)\
        .where(models.Study.id << _study_ids(payload))


def _study_ids(payload: events.ArchiveFilter) -> Any:
    """Study ids owning at least one series with the filtered modality."""
    return models.Series.select(models.Series.study)\
        .where(models.Series.modality == payload.modality)


def _patient_filters(
        query: 'peewee.ModelSelect[Any]',
        payload: events.ArchiveFilter
) -> 'peewee.ModelSelect[Any]':
    """Applies the patient-level filters of an archive query."""
    if payload.patient_id:
        query = query.where(models.Patient.patient_id == payload.patient_id)
    if payload.patient_name:
        query = query.where(
            models.Patient.patient_name ** f'%{payload.patient_name}%'
        )
    if payload.patient_birth_date:
        query = query.where(
            models.Patient.patient_birth_date == payload.patient_birth_date
        )
    return query


def _study_filters(
        query: 'peewee.ModelSelect[Any]',
        payload: events.ArchiveFilter
) -> 'peewee.ModelSelect[Any]':
    """Applies the study-level filters of an archive query."""
    if payload.accession_number:
        query = query.where(
            models.Study.accession_number == payload.accession_number
        )
    if payload.study_date_from:
        query = query.where(
            models.Study.study_date >= payload.study_date_from
        )
    if payload.study_date_to:
        query = query.where(models.Study.study_date <= payload.study_date_to)
    if payload.study_instance_uid:
        query = query.where(
            models.Study.study_instance_uid == payload.study_instance_uid
        )
    return query


def _items(
        level: str,
        model: Any,
        query: 'peewee.ModelSelect[Any]',
        payload: events.ArchiveFilter
) -> list[events.ArchiveItem]:
    """Counts, paginates and converts an archive query.

    :param level: queried level name (key of :data:`LEVEL_CHAINS`)
    :param model: model of the queried level, used for ordering and
                  pagination
    :param query: fully filtered query
    :param payload: archive query filter (limit/offset)
    :return: one item per row; every item carries the total match count
    :rtype: list[events.ArchiveItem]
    """
    total = query.count()
    rows = query.order_by(model.id)\
        .limit(payload.limit).offset(payload.offset)
    return [_item(level, row, total) for row in rows]


def _item(level: str, row: Any, total: int) -> events.ArchiveItem:
    """Converts one archive row into a query result item."""
    uids = _uids(level, row)
    fields: dict[str, Any] = {}
    attributes: dict[int, tuple[str, Any]] = {}
    for model in LEVEL_CHAINS[level]:
        target = _model_of(row, model)
        if target is None:
            continue
        for tag, (name, vr) in model.mapping.items():
            value = getattr(target, name, None)
            fields[name] = value
            attributes[tag] = (vr, value)
    return events.ArchiveItem(
        uids=uids, fields=fields, attributes=attributes, total=total
    )


def _uids(level: str, row: Any) -> dict[str, str]:
    """Collects the identifier chain of one archive row."""
    uids: dict[str, str] = {}
    patient = _model_of(row, models.Patient)
    if patient is not None:
        uids['patient_id'] = str(patient.patient_id)
    if level == 'patient':
        return uids
    study = _model_of(row, models.Study)
    if study is not None:
        uids['study_instance_uid'] = str(study.study_instance_uid)
    if level == 'study':
        return uids
    series = _model_of(row, models.Series)
    if series is not None:
        uids['series_instance_uid'] = str(series.series_instance_uid)
    if level == 'series':
        return uids
    uids['sop_instance_uid'] = str(row.sop_instance_uid)
    return uids


def _model_of(row: Any, model: Any) -> Any:
    """Returns the given level model of an archive row.

    The queried row is the level model itself; upper levels are reached
    through the foreign key references (``study.patient``,
    ``series.study`` …).
    """
    current: Any = row
    while current is not None and not isinstance(current, model):
        upper = _UPPER.get(type(current))
        if upper is None:
            return None
        current = getattr(current, upper, None)
    return current
