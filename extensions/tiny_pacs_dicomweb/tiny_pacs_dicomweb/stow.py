"""STOW-RS: store DICOM objects over HTTP.

``POST {prefix}/studies[/{studyUID}]`` with ``multipart/related`` DICOM
parts (PS3.18 6.2). Every part is parsed with ``pydicom.dcmread`` and
pushed through the *existing* store pipeline: the file is materialized
via :class:`~tiny_pacs.events.GetFile` (using the part's transfer
syntax) and the dataset is fed to the archive via
:class:`~tiny_pacs.events.StoreDataset` with ``origin='stow'`` — the
same events an incoming C-STORE produces, so PACS, storage and audit
components treat both front-ends alike.

Responses are PS3.18 H success/failure-per-instance summaries: parse or
store failures map to the documented failure reason ``0x0110``
(processing failure, PS3.18 Table I.2-2), never to 500s. Per PS3.18
Table 10.5.3-1 the HTTP status is 200 when every part stored, 202 on
partial success and 409 when every part failed.

Every UID taken from a client part is validated as a well-formed DICOM
UID before it reaches the store pipeline: the SOP Instance UID becomes
the storage file name, so an unvalidated value could traverse out of
the storage directory.

Refused in v1 (no transcoding, bounded parsing): the deflated transfer
syntax — pydicom would inflate such parts without a decompressed-size
bound — and malformed or unknown transfer-syntax declarations.
"""
import io
import json
import re
from email.parser import BytesFeedParser
from typing import Any, BinaryIO

import bottle  # type: ignore[import-untyped]
import pydicom
from pydicom import uid
from pynetdicom2 import fsm
from tiny_pacs import events

from .auth import environ_username
from .common import (
    JSON_CT,
    abort,
    accepts_json,
    audit,
    base_url,
    require,
)

#: Failure Reason (0008,1197) of PS3.18 Table I.2-2: processing failure
#: (part not parseable, refused or failed to store). 0x0112 is *not* a
#: STOW-RS failure reason — the DICOM status registry defines it as
#: "no such SOP instance"/"SOP Class not supported".
FAILURE_PROCESSING = 0x0110

#: Length limit of a DICOM UID (PS3.5 §5) and of the DB columns holding it
MAX_UID_LENGTH = 64

#: Well-formed DICOM UID: dot-separated components starting 0-2, no
#: leading zeros (same shape validated in wado.py)
UID_PATTERN = re.compile(r'[0-2]((\.0)|(\.[1-9][0-9]*))*\Z')

#: Default part media type of a container without a ``type`` parameter
DICOM_PART_MEDIA_TYPE = 'application/dicom'

#: Media types a STOW part may carry
PART_MEDIA_TYPES = frozenset({'application/dicom',
                              'application/octet-stream'})

#: Default transfer syntax of raw-dataset parts without an explicit
#: ``transfer-syntax`` parameter
DEFAULT_TRANSFER_SYNTAX = uid.ExplicitVRLittleEndian

#: Refused transfer syntaxes: pydicom inflates deflated parts with no
#: decompressed-size bound (a decompression bomb would survive the
#: per-request byte cap); v1 stores parts verbatim and never transcodes
REFUSED_TRANSFER_SYNTAXES = frozenset({
    uid.UID('1.2.840.10008.1.2.1.99'),  # Deflated Explicit VR LE
})

#: Explicit-VR value representations carrying a 4-byte value length
LONG_VALUE_VRS = frozenset({
    b'OB', b'OD', b'OF', b'OL', b'OV', b'SQ', b'UC', b'UN', b'UR', b'UT'
})

#: Elements larger than this byte count are deferred during part parsing
#: (STOW-RS persists the raw bytes and only reads header attributes)
PARSE_DEFER_SIZE = 64 * 1024

#: PS3.10 file format marker and its offset after the 128-byte preamble
DICM_MARKER = b'DICM'
DICM_OFFSET = 128


def register(state: Any, app: Any) -> None:
    """Registers the STOW-RS routes on one bottle app.

    :param state: shared application state
    :param app: bottle application receiving the routes
    """

    @app.post('/studies')
    def store_studies() -> Any:
        return _store_request(state, None)

    @app.post('/studies/<study_uid>')
    def store_study(study_uid: str) -> Any:
        return _store_request(state, study_uid)


def _store_request(state: Any, study_uid: str | None) -> Any:
    """Answers one STOW-RS request.

    :param state: shared application state
    :param study_uid: Study Instance UID of the target resource, None
                      for the service-level ``/studies`` form
    :return: the PS3.18 H summary response
    """
    username = environ_username()
    if study_uid is not None and not _valid_uid(study_uid):
        abort(400, 'The target Study Instance UID is not a well-formed '
                   'DICOM UID.')
    if not accepts_json(str(bottle.request.headers.get('Accept') or '')):
        abort(406, 'Only DICOM JSON responses are supported.')
    require(state, events.GetFile, 'storage')
    require(state, events.StoreDataset, 'archive')
    # The header value keeps its original casing: RFC 2046 multipart
    # boundaries are case-sensitive, so only comparisons lowercase a
    # copy — never the value fed to the MIME parser.
    content_type = str(
        bottle.request.headers.get('Content-Type') or ''
    ).strip()
    if not content_type.lower().startswith('multipart/related'):
        abort(415, 'STOW-RS requires a multipart/related request body '
                   'with application/dicom parts.')
    body = _read_body(state)
    parts, failure = _parse_parts(state, content_type, body)
    if failure is not None:
        return failure
    if not parts:
        abort(400, 'The multipart/related body holds no parts.')
    environment: dict[str, Any] = bottle.request.environ
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for part in parts:
        _store_part(state, part, study_uid, environment, successes,
                    failures)
    response = _summary(environment, study_uid, successes, failures)
    audit(state, 'stow', username, {
        'origin': 'stow',
        'instances': len(successes),
        'failures': len(failures),
        'study_uid': study_uid,
        'study_instance_uid': _study_of(successes, failures)
    }, 'failure' if failures and not successes else 'success')
    return response


def _valid_uid(value: str | None) -> bool:
    """Whether a value is a well-formed DICOM UID.

    :param value: candidate UID string, None or empty when absent
    :type value: str or None
    :return: True when the value matches the PS3.5 UID form and fits
             the 64-character limit
    :rtype: bool
    """
    if not value:
        return False
    return len(value) <= MAX_UID_LENGTH \
        and UID_PATTERN.fullmatch(value) is not None


def _valid_optional_uid(value: str | None) -> bool:
    """Whether an optional UID attribute is absent or well-formed.

    :param value: candidate UID string, None or empty when absent
    :type value: str or None
    :return: True when the value is empty or a valid DICOM UID
    :rtype: bool
    """
    return not value or _valid_uid(value)


def _study_of(successes: list[dict[str, Any]],
              failures: list[dict[str, Any]]) -> str | None:
    """The Study Instance UID of the pushed objects, when uniform."""
    uids = {entry.get('_study') for entry in successes + failures
            if entry.get('_study')}
    if len(uids) == 1:
        return str(uids.pop())
    return None


def _read_body(state: Any) -> bytes:
    """Reads the request body within the configured size cap.

    :param state: shared application state
    :type state: AppState
    :return: the complete request body
    :rtype: bytes
    :raises bottle.HTTPError: 411 without ``Content-Length``, 413 when
                              the body exceeds ``max_part_size`` (the
                              cap bounds every single part as well)
    """
    length = bottle.request.content_length
    if length < 0:
        abort(411)
    if length > state.max_part_size:
        abort(413, f'The request body exceeds the {state.max_part_size} '
                   f'byte budget.')
    stream: BinaryIO = bottle.request.body
    data = stream.read(state.max_part_size + 1)
    if len(data) > state.max_part_size:
        abort(413, f'The request body exceeds the {state.max_part_size} '
                   f'byte budget.')
    return data


def _parse_parts(state: Any, content_type: str, body: bytes
                 ) -> tuple[list[Any], Any]:
    """Parses the ``multipart/related`` body into its DICOM parts.

    :param state: shared application state
    :param content_type: request ``Content-Type`` header value
    :param body: complete request body
    :return: tuple of payload/parameter pairs and an early-error
             response (None when the body parsed)
    """
    if 'boundary=' not in content_type.lower().replace(' ', ''):
        abort(400, 'The Content-Type carries no boundary parameter.')
    default_media = _container_type_parameter(content_type)
    header = f'Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n'
    try:
        # Fed in pieces instead of parsebytes(header + body): the
        # concatenation would double the peak memory of an upload that
        # is already fully buffered and capped by _read_body
        parser = BytesFeedParser()
        parser.feed(header.encode('latin-1', 'replace'))
        parser.feed(body)
        message = parser.close()
    except Exception:
        state.logger.warning('STOW-RS body could not be parsed as '
                             'multipart/related')
        return [], abort(400, 'The multipart/related body is malformed.')
    parts: list[Any] = []
    for part in message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        # RFC 2387: a part without its own Content-Type inherits the
        # container's ``type`` parameter (the stdlib email parser would
        # otherwise default it to text/plain and refuse a valid part)
        declared = part.get('Content-Type')
        if declared:
            media = declared.split(';')[0].strip().lower()
            params = {str(name).lower(): str(value)
                      for name, value in (part.get_params() or [])[1:]}
        else:
            media = default_media
            params = {}
        parts.append((payload, media, params))
    return parts, None


def _container_type_parameter(content_type: str) -> str:
    """Extracts the container media type of a multipart/related body.

    :param content_type: request ``Content-Type`` header value
    :type content_type: str
    :return: the ``type`` parameter (quoted or bare), defaulting to
             ``application/dicom`` per PS3.18 usage
    :rtype: str
    """
    for chunk in content_type.split(';')[1:]:
        key, _, value = chunk.partition('=')
        if key.strip().lower() == 'type':
            return value.strip().strip('"').lower()
    return DICOM_PART_MEDIA_TYPE


def _store_part(state: Any, part: Any, study_uid: str | None,
                environment: dict[str, Any],
                successes: list[dict[str, Any]],
                failures: list[dict[str, Any]]) -> None:
    """Stores one multipart part, recording the per-instance outcome.

    Parse and store failures never raise: they become entries of the
    PS3.18 H failure sequence.

    :param state: shared application state
    :param part: ``(payload bytes, media type parameters)`` pair
    :param study_uid: target Study Instance UID of the request
    :param environment: WSGI environ (retrieve URL building)
    :param successes: success entries to append to
    :param failures: failure entries to append to
    """
    payload, media_type, params = part
    media = str(media_type or '').strip().lower()
    if media and media not in PART_MEDIA_TYPES:
        state.logger.warning(
            'Refusing a STOW-RS part of media type %r', media
        )
        failures.append(_failure_entry(None, FAILURE_PROCESSING, None))
        return
    try:
        parsed = _parse_dicom(payload, params)
    except Exception:
        state.logger.warning('STOW-RS part could not be parsed as DICOM')
        failures.append(_failure_entry(None, FAILURE_PROCESSING, None))
        return
    ds, transfer_syntax, elements = parsed
    sop_class = str(getattr(ds, 'SOPClassUID', '') or '')
    sop_instance = str(getattr(ds, 'SOPInstanceUID', '') or '')
    target_study = str(getattr(ds, 'StudyInstanceUID', '') or '') or None
    series_uid = str(getattr(ds, 'SeriesInstanceUID', '') or '') or None
    if not _valid_uid(sop_class) or not _valid_uid(sop_instance) \
            or not _valid_optional_uid(target_study) \
            or not _valid_optional_uid(series_uid):
        # The SOP Instance UID becomes the storage file name: an
        # unvalidated value would let a crafted part write outside the
        # storage directory (the core guards reads/deletes, not writes).
        # The malformed UIDs are never echoed into the failure entry.
        state.logger.warning(
            'STOW-RS part with a missing or malformed UID refused'
        )
        failures.append(_failure_entry(None, FAILURE_PROCESSING, None))
        return
    if study_uid and target_study != study_uid:
        state.logger.warning(
            'Refusing a STOW-RS part of study %r for target study %r',
            target_study, study_uid
        )
        failures.append(
            _failure_entry((sop_class, sop_instance), FAILURE_PROCESSING,
                           target_study)
        )
        return
    try:
        stored = _materialize(state, ds, transfer_syntax, elements)
    except Exception:
        state.logger.exception(
            'STOW-RS storage of %s failed', sop_instance
        )
        failures.append(
            _failure_entry((sop_class, sop_instance), FAILURE_PROCESSING,
                           target_study)
        )
        return
    if not stored:
        failures.append(
            _failure_entry((sop_class, sop_instance), FAILURE_PROCESSING,
                           target_study)
        )
        return
    successes.append(
        _success_entry(environment, target_study or study_uid,
                       series_uid, sop_class, sop_instance)
    )


def _parse_dicom(
        payload: bytes, params: dict[str, str]
) -> tuple[pydicom.Dataset, uid.UID, bytes | memoryview]:
    """Parses one STOW-RS part into dataset, transfer syntax and the
    raw dataset-element bytes to persist.

    PS3.10 file-format parts keep their exact on-the-wire element bytes
    (the stored copy is byte-identical to what the client pushed, for
    any accepted transfer syntax including encapsulated ones); the file
    meta information is scanned by hand *before* handing the payload to
    pydicom, so a deflated part is refused without ever being inflated
    (decompression would not be bounded by the request body cap). Raw
    dataset parts declare ``transfer-syntax`` or default to Explicit VR
    Little Endian. Large elements are deferred during the parse: STOW
    persists the raw bytes and the archive only reads header attributes.

    :param payload: part payload bytes
    :param params: part media-type parameters
    :return: parsed dataset, transfer syntax and element bytes
    :rtype: tuple[pydicom.Dataset, pydicom.uid.UID, bytes or memoryview]
    :raises ValueError: raised when the part is not parseable DICOM or
                        declares a refused/malformed transfer syntax
    """
    if payload[DICM_OFFSET:DICM_OFFSET + 4] == DICM_MARKER:
        transfer_syntax, meta_end = _scan_meta(payload)
        ds = pydicom.dcmread(io.BytesIO(payload),
                             defer_size=PARSE_DEFER_SIZE)
        return ds, transfer_syntax, memoryview(payload)[meta_end:]
    declared = str(params.get('transfer-syntax') or '').strip()
    transfer_syntax = uid.UID(declared or DEFAULT_TRANSFER_SYNTAX)
    _check_transfer_syntax(transfer_syntax)
    ds = pydicom.dcmread(io.BytesIO(payload), force=True,
                         defer_size=PARSE_DEFER_SIZE)
    return ds, transfer_syntax, payload


def _check_transfer_syntax(transfer_syntax: uid.UID) -> None:
    """Validates one declared transfer syntax.

    :param transfer_syntax: the transfer syntax to validate
    :raises ValueError: raised when the UID is malformed or refused
    """
    if not transfer_syntax.is_valid:
        raise ValueError(
            f'malformed transfer syntax UID {str(transfer_syntax)!r}'
        )
    if transfer_syntax in REFUSED_TRANSFER_SYNTAXES:
        raise ValueError(
            f'transfer syntax {transfer_syntax} is refused '
            f'(unbounded decompression)'
        )


def _scan_meta(payload: bytes) -> tuple[uid.UID, int]:
    """Walks the file meta information of a PS3.10 part.

    The meta information is explicit VR little endian by definition; it
    is scanned attribute by attribute (never trusting the group length
    blindly) to locate the first dataset element and to extract the
    transfer syntax *before* pydicom sees the payload. A group length
    that disagrees with the scanned attributes is refused — otherwise a
    crafted part would parse as valid DICOM while the persisted element
    blob holds leftover meta bytes or a truncated dataset.

    :param payload: complete part bytes (PS3.10 file format)
    :type payload: bytes
    :return: transfer syntax UID and the offset of the first dataset
             element
    :rtype: tuple[pydicom.uid.UID, int]
    :raises ValueError: raised when the meta information is malformed,
                        carries no transfer syntax or the group length
                        disagrees with the attributes
    """
    header = DICM_OFFSET + 4
    end = len(payload)
    offset = header
    declared_length = -1
    group_start = header
    transfer_text = ''
    while offset + 8 <= end:
        tag = payload[offset:offset + 4]
        if (tag[0] | (tag[1] << 8)) != 0x0002:
            break
        vr = payload[offset + 4:offset + 6]
        if vr in LONG_VALUE_VRS:
            length = int.from_bytes(payload[offset + 8:offset + 12],
                                    'little')
            value = offset + 12
        elif vr.isalpha() and len(vr) == 2 and vr.isupper():
            length = int.from_bytes(payload[offset + 6:offset + 8],
                                    'little')
            value = offset + 8
        else:
            raise ValueError('malformed meta attribute VR')
        if length < 0 or value + length > end:
            raise ValueError('meta attribute length out of range')
        element = tag[2] | (tag[3] << 8)
        if element == 0x0000:
            if vr != b'UL' or length != 4:
                raise ValueError('malformed meta group length attribute')
            declared_length = int.from_bytes(
                payload[value:value + 4], 'little'
            )
            group_start = value + length
        elif element == 0x0010 and vr == b'UI':
            transfer_text = payload[value:value + length] \
                .decode('ascii', 'ignore').strip('\x00').strip()
        offset = value + length
    if offset == header:
        raise ValueError('no file meta attributes found')
    if declared_length >= 0 and group_start + declared_length != offset:
        raise ValueError(
            'the meta group length disagrees with the meta attributes'
        )
    if not transfer_text:
        raise ValueError('the meta information carries no transfer '
                         'syntax')
    transfer_syntax = uid.UID(transfer_text)
    _check_transfer_syntax(transfer_syntax)
    return transfer_syntax, offset


def _elements_after_meta(payload: bytes) -> bytes:
    """Splits a PS3.10 part behind its validated file meta information.

    :param payload: complete part bytes (PS3.10 file format)
    :type payload: bytes
    :return: the dataset element bytes
    :rtype: bytes
    :raises ValueError: raised when the meta information cannot be
                        validated
    """
    return payload[_scan_meta(payload)[1]:]


def _materialize(state: Any, ds: pydicom.Dataset,
                 transfer_syntax: uid.UID,
                 elements: bytes | memoryview) -> bool:
    """Persists one parsed dataset through the core store pipeline.

    Materializes the stored file via :class:`~tiny_pacs.events.GetFile`
    (preamble and file meta come from the storage component, exactly as
    in the C-STORE path), writes the part's element bytes verbatim and
    feeds the archive via :class:`~tiny_pacs.events.StoreDataset` with
    ``origin='stow'``.

    :param state: shared application state
    :param ds: parsed dataset
    :param transfer_syntax: the part's transfer syntax
    :param elements: raw dataset element bytes to persist
    :return: True when the object was stored
    :rtype: bool
    """
    command_set = pydicom.Dataset()
    command_set.AffectedSOPClassUID = ds.SOPClassUID
    command_set.AffectedSOPInstanceUID = ds.SOPInstanceUID
    context = fsm.PContextDef(1, uid.UID(str(ds.SOPClassUID)),
                              transfer_syntax)
    payload = events.GetFilePayload(context, command_set,
                                    transfer_syntax=str(transfer_syntax))
    file_object, _start = state.bus.send_one(events.GetFile, payload)
    if getattr(file_object, 'store_rejected', False):
        state.logger.warning(
            'Refusing STOW-RS of %s: already stored and overwrite is '
            'disabled', ds.SOPInstanceUID
        )
        _close(file_object)
        return False
    try:
        # The storage already wrote preamble and file meta information
        # (and left the stream positioned behind them): the element
        # bytes are appended verbatim, exactly like the DIMSE decoder
        # appends its incoming P-DATA stream. The returned start
        # position is where a *reader* restarts (0 = the whole PS3.10
        # file), not a write offset.
        file_object.write(elements)
        file_object.flush()
    except Exception:
        state.bus.broadcast_nothrow(events.StoreFailure, ds)
        _close(file_object)
        raise
    try:
        state.bus.broadcast(
            events.StoreDataset,
            events.StoreDatasetPayload(
                ds, str(transfer_syntax), origin='stow'
            )
        )
    except Exception:
        # The PACS component already broadcast StoreFailure before
        # re-raising, so the storage cleaned up its half-written file
        _close(file_object)
        raise
    _close(file_object)
    return True


def _close(file_object: Any) -> None:
    """Closes a materialization stream, swallowing close errors."""
    try:
        file_object.close()
    except Exception:  # pragma: no cover - defensive
        pass


def _success_entry(environment: dict[str, Any], study: str | None,
                   series: str | None, sop_class: str,
                   sop_instance: str) -> dict[str, Any]:
    """Builds one Referenced SOP Sequence entry (PS3.18 Table H.3-2)."""
    entry: dict[str, Any] = {
        '00081150': {'vr': 'UI', 'Value': [sop_class]},
        '00081155': {'vr': 'UI', 'Value': [sop_instance]},
    }
    if study and series:
        entry['00081190'] = {
            'vr': 'UR',
            'Value': [f'{base_url(environment)}/studies/{study}'
                      f'/series/{series}/instances/{sop_instance}']
        }
    entry['_study'] = study
    return entry


def _failure_entry(ident: tuple[str, str] | None, reason: int,
                   study: str | None) -> dict[str, Any]:
    """Builds one Failed SOP Sequence entry (PS3.18 Table H.3-3)."""
    entry: dict[str, Any] = {'00081197': {'vr': 'US', 'Value': [reason]}}
    if ident is not None:
        entry['00081150'] = {'vr': 'UI', 'Value': [ident[0]]}
        entry['00081155'] = {'vr': 'UI', 'Value': [ident[1]]}
    entry['_study'] = study
    return entry


def _summary(environment: dict[str, Any], study_uid: str | None,
             successes: list[dict[str, Any]],
             failures: list[dict[str, Any]]) -> bottle.HTTPResponse:
    """Builds the PS3.18 H response of one STOW-RS request.

    Per PS3.18 Table 10.5.3-1 the status is 200 when every part stored,
    202 on partial success and 409 when every part failed; parse and
    per-instance store failures never produce a 500.

    :param environment: WSGI environ (retrieve URL building)
    :param study_uid: target Study Instance UID of the request
    :param successes: Referenced SOP Sequence entries
    :param failures: Failed SOP Sequence entries
    :return: the JSON summary response
    :rtype: bottle.HTTPResponse
    """
    if failures:
        status = 409 if not successes else 202
    else:
        status = 200
    study = study_uid or _study_of(successes, failures)
    body: dict[str, Any] = {}
    if study:
        body['00081190'] = {
            'vr': 'UR',
            'Value': [f'{base_url(environment)}/studies/{study}']
        }
    if successes:
        body['00081199'] = {
            'vr': 'SQ',
            'Value': [_clean(entry) for entry in successes]
        }
    if failures:
        body['00081198'] = {
            'vr': 'SQ',
            'Value': [_clean(entry) for entry in failures]
        }
    return bottle.HTTPResponse(
        body=json.dumps(body), status=status,
        headers={'Content-Type': JSON_CT}
    )


def _clean(entry: dict[str, Any]) -> dict[str, Any]:
    """Drops the internal bookkeeping keys of one summary entry."""
    return {key: value for key, value in entry.items()
            if not key.startswith('_')}
