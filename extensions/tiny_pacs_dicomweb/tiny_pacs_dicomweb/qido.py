"""QIDO-RS: query studies, series and instances.

``GET {prefix}/studies``, ``GET {prefix}/studies/{uid}/series`` and
``GET {prefix}/studies/{uid}/series/{uid}/instances`` with the standard
matching parameters mapped onto :class:`~tiny_pacs.events.ArchiveFilter`;
responses are DICOM JSON (PS3.18 F) serialized from
``ArchiveItem.attributes`` via pydicom's JSON facilities — no hand-rolled
tag encoder.

v1 matching-parameter coverage (the archive query events back exactly
these; unknown parameters are ignored):

* ``PatientID``, ``PatientName`` (substring), ``PatientBirthDate``,
  ``AccessionNumber``, ``ModalitiesInStudy``, ``StudyInstanceUID``,
  ``SeriesInstanceUID``, ``SOPInstanceUID``;
* ``StudyDate`` with the standard single-date and range forms
  (``YYYYMMDD``, ``YYYYMMDD-YYYYMMDD``, ``YYYYMMDD-``, ``-YYYYMMDD``);
* ``limit``/``offset`` pagination (bounded), ``includefield`` (accepted;
  every stored column is returned anyway, non-stored fields are ignored)
  and ``fuzzymatching`` (ignored — ``PatientName`` always matches by
  substring).

Wildcard matching (``*``/``?``) is a deferred v1 limitation: values are
matched exactly by the archive filter semantics.
"""
import datetime
import json
from typing import Any

import bottle  # type: ignore[import-untyped]
import pydicom
from pydicom.dataelem import DataElement
from tiny_pacs import events

from .auth import environ_username
from .common import (
    DEFAULT_QUERY_LIMIT,
    JSON_CT,
    MAX_QUERY_LIMIT,
    MAX_QUERY_OFFSET,
    abort,
    accepts_json,
    audit,
    int_param,
    require,
)

#: Query level metadata: archive query event answered by the PACS
#: component, keyed by the QIDO-RS target resource
LEVELS: dict[str, Any] = {
    'study': events.ArchiveStudyQuery,
    'series': events.ArchiveSeriesQuery,
    'instance': events.ArchiveInstanceQuery,
}

#: Matching parameter -> ``ArchiveFilter`` field (single-value params)
SIMPLE_PARAMS: dict[str, str] = {
    'PatientID': 'patient_id',
    'PatientName': 'patient_name',
    'AccessionNumber': 'accession_number',
    'Modality': 'modality',
    'ModalitiesInStudy': 'modality',
    'PatientBirthDate': 'patient_birth_date',
    'StudyInstanceUID': 'study_instance_uid',
    'SeriesInstanceUID': 'series_instance_uid',
    'SOPInstanceUID': 'sop_instance_uid',
}


def register(state: Any, app: Any) -> None:
    """Registers the QIDO-RS routes on one bottle app.

    :param state: shared application state
    :param app: bottle application receiving the routes
    """

    @app.get('/studies')
    def query_studies() -> Any:
        return query_level(state, 'study')

    @app.get('/studies/<study_uid>/series')
    def query_series(study_uid: str) -> Any:
        return query_level(state, 'series', study_uid=study_uid)

    @app.get('/studies/<study_uid>/series/<series_uid>/instances')
    def query_instances(study_uid: str, series_uid: str) -> Any:
        return query_level(
            state, 'instance', study_uid=study_uid, series_uid=series_uid
        )


def query_level(state: Any, level: str, study_uid: str | None = None,
                series_uid: str | None = None, sop_uid: str | None = None
                ) -> Any:
    """Answers one QIDO-RS query.

    With an explicit object UID pin the same call serves the QIDO-RS
    object-metadata form of an object resource route
    (``GET /studies/{uid}`` a.s.o. with a JSON ``Accept``); the WADO-RS
    retrieval of the object routes negotiates between both forms.

    :param state: shared application state
    :param level: queried level: ``study``, ``series`` or ``instance``
    :param study_uid: pinned Study Instance UID (path segment)
    :param series_uid: pinned Series Instance UID (path segment)
    :param sop_uid: pinned SOP Instance UID (path segment)
    :return: DICOM JSON response or 204 without content
    """
    if not accepts_json(str(bottle.request.headers.get('Accept') or '')):
        abort(406, 'Only DICOM JSON responses are supported.')
    event = LEVELS[level]
    require(state, event, 'archive query')
    params = bottle.request.params
    try:
        archive_filter = _filter(params, study_uid, series_uid, sop_uid)
    except ValueError as error:
        abort(400, str(error))
    try:
        items: list[events.ArchiveItem] = list(
            state.bus.send_any(event, archive_filter) or []
        )
    except Exception:
        state.logger.exception('QIDO-RS %s query failed', level)
        abort(500)
    audit(state, 'qido', environ_username(),
          {'level': level, 'returned': len(items)})
    if not items:
        return bottle.HTTPResponse(status=204)
    body = json.dumps([item_json(item) for item in items])
    return bottle.HTTPResponse(
        body=body, status=200, headers={'Content-Type': JSON_CT}
    )


def _filter(params: Any, study_uid: str | None,
            series_uid: str | None, sop_uid: str | None
            ) -> events.ArchiveFilter:
    """Translates QIDO-RS query parameters into an archive filter.

    Path segments pin the UIDs of the parent levels and win over
    contradicting query parameters of the same key.

    :param params: bottle request parameters
    :param study_uid: pinned Study Instance UID
    :param series_uid: pinned Series Instance UID
    :param sop_uid: pinned SOP Instance UID
    :return: the filter to broadcast
    :rtype: events.ArchiveFilter
    :raises ValueError: raised for an invalid parameter value
    """
    kwargs: dict[str, Any] = {}
    for param, field_name in SIMPLE_PARAMS.items():
        value = str(params.get(param) or '').strip()
        if value:
            kwargs[field_name] = value
    if kwargs.get('patient_birth_date'):
        # exact-match dates are validated like StudyDate bounds: the
        # same input must not be a 400 in one field and a silent
        # no-match in another
        kwargs['patient_birth_date'] = _check_date(
            kwargs['patient_birth_date'], kwargs['patient_birth_date']
        )
    date_from, date_to = _study_date_range(
        str(params.get('StudyDate') or '').strip()
    )
    if date_from:
        kwargs['study_date_from'] = date_from
    if date_to:
        kwargs['study_date_to'] = date_to
    if study_uid:
        kwargs['study_instance_uid'] = study_uid
    if series_uid:
        kwargs['series_instance_uid'] = series_uid
    if sop_uid:
        kwargs['sop_instance_uid'] = sop_uid
    kwargs['limit'] = int_param(
        params.get('limit'), DEFAULT_QUERY_LIMIT, 1, MAX_QUERY_LIMIT,
        'limit'
    )
    kwargs['offset'] = int_param(
        params.get('offset'), 0, 0, MAX_QUERY_OFFSET, 'offset'
    )
    return events.ArchiveFilter(**kwargs)


def _study_date_range(value: str) -> tuple[str, str]:
    """Parses the ``StudyDate`` matching parameter.

    Accepts a single date plus the three PS3.18 range forms over
    ``YYYYMMDD`` dates (``YYYYMMDD-YYYYMMDD``, ``YYYYMMDD-`` and
    ``-YYYYMMDD``).

    :param value: raw parameter value (already stripped)
    :type value: str
    :return: inclusive ``(from, to)`` bounds; either empty when open
    :rtype: tuple[str, str]
    :raises ValueError: raised when the value is not a date or range
    """
    if not value:
        return '', ''
    if '-' not in value:
        day = _check_date(value, value)
        return day, day
    first, _, second = value.partition('-')
    if '-' in second:
        raise ValueError(
            f'Invalid StudyDate range {value!r}: expected at most one "-"'
        )
    low = _check_date(first, value) if first else ''
    high = _check_date(second, value) if second else ''
    return low, high


def _check_date(value: str, original: str) -> str:
    """Validates and normalizes one DICOM date (DA) bound.

    :param value: date without dashes
    :type value: str
    :param original: raw parameter value for the error message
    :type original: str
    :return: the normalized ``YYYYMMDD`` form
    :rtype: str
    :raises ValueError: raised when the value is not a calendar date
    """
    digits = value.strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(
            f'Invalid StudyDate parameter {original!r}: dates must be '
            f'YYYYMMDD'
        )
    try:
        datetime.datetime.strptime(digits, '%Y%m%d')
    except ValueError:
        raise ValueError(
            f'Invalid StudyDate parameter {original!r}: {digits!r} is '
            f'not a calendar date'
        ) from None
    return digits


def item_json(item: events.ArchiveItem) -> dict[str, Any]:
    """Serializes one archive item as PS3.18 DICOM JSON.

    The DICOM view (``ArchiveItem.attributes``: tag to ``(VR, value)``)
    is loaded into a :class:`pydicom.Dataset`, which the pydicom JSON
    encoder renders — the archive component owns the DICOM semantics,
    this module only serializes.

    :param item: archive query result
    :type item: events.ArchiveItem
    :return: the DICOM JSON object of the item
    :rtype: dict
    """
    ds = pydicom.Dataset()
    for tag, (vr, value) in item.attributes.items():
        if value is None:
            continue
        ds.add(DataElement(tag, vr, value))
    try:
        result: dict[str, Any] = ds.to_json_dict()
        return result
    except Exception:
        # A column holding a value its VR cannot render (corrupt row)
        # must not sink the whole page: serialize what is serializable
        fallback = pydicom.Dataset()
        for element in ds:
            try:
                single = pydicom.Dataset()
                single.add(element)
                single.to_json_dict()
                fallback.add(element)
            except Exception:
                continue
        return fallback.to_json_dict()
