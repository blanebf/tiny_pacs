"""WADO-RS and WADO-URI retrieval.

Object retrieval resolves the requested study/series/instance UIDs
through :class:`~tiny_pacs.events.ArchiveInstanceQuery`, asks the
storage components for the stored files through
:class:`~tiny_pacs.events.GetFiles` and streams each one as
``application/dicom``; multi-object requests use ``multipart/related``
per PS3.18. v1 serves the stored transfer syntax only (no transcoding):
an ``Accept`` header (or the WADO-URI ``transferSyntax`` parameter)
is refused with 406 only when no listed range can be produced at all
(RFC 9110 semantics; the ``transfer-syntax=*`` wildcard means
unconstrained). Retrieval aggregates the ``GetFiles`` answers of
*every* installed storage component, exactly like the core C-GET/C-MOVE
paths.

The object routes (``GET /studies/{uid}`` a.s.o.) are shared with the
QIDO-RS object-metadata form per PS3.18 content negotiation: a JSON
``Accept`` gets the DICOM JSON metadata query
(:func:`~tiny_pacs_dicomweb.qido.query_level`), everything else the
retrieval below.

Security: file paths reach the response only from ``GetFiles`` results —
client-supplied strings are never used to open anything.
"""
import io
import re
import uuid
from collections.abc import Generator, Iterable
from itertools import chain
from typing import Any, BinaryIO

import bottle  # type: ignore[import-untyped]
import pydicom
from pydicom import uid
from pydicom.dataset import FileMetaDataset
from tiny_pacs import events

from . import qido
from .common import (
    CHUNK_SIZE,
    JSON_CT,
    MAX_RETRIEVE_INSTANCES,
    abort,
    require,
)

#: Media type of a retrieved DICOM object part
DICOM_CT = 'application/dicom'

#: Media type of a multi-part retrieval response
MULTIPART_CT = 'multipart/related'

#: Batch size of ``GetFiles`` calls: one archive/storage query per
#: batch, keeping every SQL ``IN`` clause far below the bind-parameter
#: limits of the supported database servers even for huge retrievals
GETFILES_BATCH = 500

#: Page size of the paged ``ArchiveInstanceQuery`` resolving a retrieval
#: target (reuses the storage batch so UID resolution and ``GetFiles``
#: round trips stay aligned)
RETRIEVE_PAGE = GETFILES_BATCH

#: Well-formed DICOM UID (PS3.5): the only shape ever reflected into a
#: ``transfer-syntax`` response header parameter
UID_PATTERN = re.compile(r'[0-2]((\.0)|(\.[1-9][0-9]*))*\Z')


def register(state: Any, app: Any) -> None:
    """Registers the WADO-RS/WADO-URI routes on one bottle app.

    :param state: shared application state
    :param app: bottle application receiving the routes
    """

    @app.get('/wado')
    def wado_uri() -> Any:
        return _wado_uri(state)

    @app.get('/studies/<study_uid>')
    def retrieve_study(study_uid: str) -> Any:
        return _object_route(state, 'study', study_uid=study_uid)

    @app.get('/studies/<study_uid>/series/<series_uid>')
    def retrieve_series(study_uid: str, series_uid: str) -> Any:
        return _object_route(state, 'series', study_uid=study_uid,
                             series_uid=series_uid)

    @app.get(
        '/studies/<study_uid>/series/<series_uid>/instances/<sop_uid>'
    )
    def retrieve_instance(study_uid: str, series_uid: str,
                          sop_uid: str) -> Any:
        return _object_route(state, 'instance', study_uid=study_uid,
                             series_uid=series_uid, sop_uid=sop_uid)


def _object_route(state: Any, level: str, study_uid: str | None = None,
                  series_uid: str | None = None,
                  sop_uid: str | None = None) -> Any:
    """Serves one object resource route with PS3.18 content negotiation.

    :param state: shared application state
    :param level: object level: ``study``, ``series`` or ``instance``
    :param study_uid: Study Instance UID of the route
    :param series_uid: Series Instance UID of the route
    :param sop_uid: SOP Instance UID of the route
    :return: the retrieval response or the QIDO-RS metadata response
    """
    accept = parse_accept(
        str(bottle.request.headers.get('Accept') or '')
    )
    json_ok, retrievable, wanted_ts = _negotiate_retrieve(accept)
    if not json_ok and not retrievable:
        abort(406, 'No acceptable representation can be produced; '
                   'the object routes serve DICOM retrieval '
                   f'({DICOM_CT}/{MULTIPART_CT}, stored transfer '
                   'syntax only) and DICOM JSON metadata.')
    if json_ok and not retrievable:
        return qido.query_level(state, level, study_uid=study_uid,
                                series_uid=series_uid, sop_uid=sop_uid)
    wants_multipart = any(media == MULTIPART_CT
                          and _multipart_type_ok(params)
                          for media, params in accept)
    return _retrieve(
        state, single=level == 'instance' and not wants_multipart,
        study_uid=study_uid, series_uid=series_uid, sop_uid=sop_uid,
        wanted_ts=wanted_ts
    )


def _negotiate_retrieve(
        accept: list[tuple[str, dict[str, str]]]
) -> tuple[bool, bool, set[str] | None]:
    """Negotiates one ``Accept`` header on an object route.

    Per RFC 9110 the request is only refused when *no* listed range can
    be produced — a mixed header (e.g. JSON metadata plus DICOM
    retrieval) is served with the route's primary representation, the
    retrieval. Transfer-syntax constraints are collected across every
    retrievable range (multiple ranges mean alternatives, so any match
    serves); an unconstrained or wildcarded range lifts them all.

    :param accept: parsed ``Accept`` ranges
    :type accept: list[tuple[str, dict]]
    :return: whether a JSON encoding is acceptable, whether a retrieval
             representation is acceptable and the transfer-syntax
             constraints (None when unconstrained)
    :rtype: tuple[bool, bool, set or None]
    """
    json_ok = False
    # An absent (empty) Accept accepts everything: the retrieval is the
    # primary representation of the object routes
    retrievable = not accept
    unconstrained = not accept
    wanted: set[str] = set()
    for media, params in accept:
        if media in (JSON_CT, 'application/json'):
            json_ok = True
            continue
        if media in ('*/*', 'application/*'):
            retrievable = True
            unconstrained = True
            continue
        if media == DICOM_CT:
            retrievable = True
            syntax = params.get('transfer-syntax', '').strip()
            if syntax and syntax != '*':
                wanted.add(syntax)
            else:
                unconstrained = True
            continue
        if media == MULTIPART_CT and _multipart_type_ok(params):
            retrievable = True
            syntax = params.get('transfer-syntax', '').strip()
            if syntax and syntax != '*':
                wanted.add(syntax)
            else:
                unconstrained = True
            continue
    constraints = None if unconstrained else (wanted or None)
    return json_ok, retrievable, constraints


def _multipart_type_ok(params: dict[str, str]) -> bool:
    """Whether a ``multipart/related`` range's ``type`` parameter is
    producible (absent, wildcarded or ``application/dicom``)."""
    wanted = params.get('type', '').strip().strip('"').lower()
    return wanted in ('', '*', DICOM_CT)


def _uri_transfer_syntax(raw: str) -> set[str] | None:
    """Parses the WADO-URI ``transferSyntax`` query parameter.

    Per PS3.18 §8.7.3.6 the parameter is a comma-separated list of
    Transfer Syntax UIDs or the single ``*`` wildcard: an entry is
    served when its stored syntax matches *any* listed UID, and a
    wildcard (or an absent parameter) lifts the constraint entirely —
    the same semantics :func:`_negotiate_retrieve` applies to the
    ``transfer-syntax`` parameters of WADO-RS ``Accept`` ranges.

    :param raw: raw ``transferSyntax`` parameter value
    :type raw: str
    :return: the set of acceptable Transfer Syntax UIDs, None when
             unconstrained
    :rtype: set[str] or None
    """
    tokens = {token.strip() for token in raw.split(',') if token.strip()}
    if not tokens or '*' in tokens:
        return None
    return tokens


def parse_accept(header: str) -> list[tuple[str, dict[str, str]]]:
    """Parses an ``Accept`` header into media ranges.

    :param header: raw ``Accept`` header value
    :type header: str
    :return: ``(media type, parameters)`` pairs in header order, with
             the quality parameter dropped
    :rtype: list[tuple[str, dict]]
    """
    ranges: list[tuple[str, dict[str, str]]] = []
    for chunk in header.split(','):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = [part.strip() for part in chunk.split(';')]
        media = parts[0].lower()
        params: dict[str, str] = {}
        for part in parts[1:]:
            key, _, value = part.partition('=')
            key = key.strip().lower()
            if key and key != 'q':
                params[key] = value.strip().strip('"')
        ranges.append((media, params))
    return ranges


# -----------------------------------------------------------------------
# Retrieval
# -----------------------------------------------------------------------

def _retrieve(state: Any, single: bool, study_uid: str | None,
              series_uid: str | None, sop_uid: str | None,
              wanted_ts: set[str] | None) -> Any:
    """Resolves, fetches and streams one retrieval request.

    :param state: shared application state
    :param single: the route addresses exactly one instance and the
                   client did not request ``multipart/related``
    :param study_uid: Study Instance UID filter
    :param series_uid: Series Instance UID filter
    :param sop_uid: SOP Instance UID filter
    :param wanted_ts: transfer-syntax constraints from ``Accept``,
                      None when unconstrained
    :return: the retrieval response
    """
    require(state, events.ArchiveInstanceQuery, 'archive query')
    uids = _resolve_instance_uids(state, study_uid, series_uid, sop_uid)
    if not uids:
        abort(404)
    require(state, events.GetFiles, 'storage')
    stored = _collect_stored(state, uids)
    if wanted_ts is not None:
        stored = [entry for entry in stored if str(entry[1]) in wanted_ts]
    if not stored:
        if wanted_ts is not None:
            abort(406, 'None of the requested transfer syntaxes matches '
                       'a stored object (no transcoding in v1).')
        abort(404)
    if single and len(stored) == 1:
        entry = stored[0]
        return bottle.HTTPResponse(
            body=_source_stream(entry),
            status=200,
            headers={'Content-Type': _single_content_type(entry)}
        )
    boundary = f'tiny-pacs-{uuid.uuid4().hex}'
    return bottle.HTTPResponse(
        body=_multipart(state, stored, boundary),
        status=200,
        headers={'Content-Type':
                 f'{MULTIPART_CT}; type="{DICOM_CT}"; '
                 f'boundary="{boundary}"'}
    )


def _collect_stored(state: Any, uids: list[str]
                    ) -> list[events.StoredFile]:
    """Fetches stored files from every storage component, in batches.

    Mirrors the core C-GET/C-MOVE aggregation (which broadcasts
    ``GetFiles`` and chains the per-storage results) instead of stopping
    at the first listener, so instances living in any installed storage
    are retrievable; batching keeps each SQL ``IN`` clause below the
    bind-parameter limits of the supported databases.

    :param state: shared application state
    :param uids: SOP Instance UIDs to fetch
    :return: the stored-file results of every storage component
    :rtype: list[events.StoredFile]
    """
    stored: list[events.StoredFile] = []
    try:
        for first in range(0, len(uids), GETFILES_BATCH):
            results = state.bus.broadcast(
                events.GetFiles, uids[first:first + GETFILES_BATCH]
            )
            stored.extend(chain.from_iterable(results))
    except Exception:
        state.logger.exception('WADO retrieval of %d instance(s) failed',
                               len(uids))
        abort(500)
    return stored


def _single_content_type(entry: events.StoredFile) -> str:
    """Builds the single-part response ``Content-Type`` safely.

    The recorded transfer syntax is a database value; only a well-formed
    DICOM UID is ever interpolated into the header (defense in depth —
    STOW-RS validates on the way in, DIMSE records negotiated syntaxes).

    :param entry: `(SOP Class UID, Transfer Syntax UID, source)` tuple
    :return: ``application/dicom`` plus the ``transfer-syntax``
             parameter when the UID is well-formed
    :rtype: str
    """
    syntax = str(entry[1]).strip()
    if syntax and len(syntax) <= 64 and UID_PATTERN.fullmatch(syntax):
        return f'{DICOM_CT}; transfer-syntax={syntax}'
    return DICOM_CT


def _resolve_instance_uids(state: Any, study_uid: str | None,
                           series_uid: str | None,
                           sop_uid: str | None) -> list[str]:
    """Resolves the SOP Instance UIDs of one retrieval target.

    The archive query is paged (``RETRIEVE_PAGE`` items per round trip)
    so a wide study never materializes its full item list — each item
    carries complete attribute and field dictionaries although the
    retrieval only reads the instance UID. The overall resolution stays
    bounded by :data:`MAX_RETRIEVE_INSTANCES`.

    :param state: shared application state
    :param study_uid: Study Instance UID filter
    :param series_uid: Series Instance UID filter
    :param sop_uid: SOP Instance UID filter
    :return: deduplicated SOP Instance UIDs in archive order
    :rtype: list[str]
    """
    uids: list[str] = []
    seen: set[str] = set()
    offset = 0
    try:
        while offset < MAX_RETRIEVE_INSTANCES:
            page = list(
                state.bus.send_any(
                    events.ArchiveInstanceQuery,
                    events.ArchiveFilter(
                        study_instance_uid=study_uid or None,
                        series_instance_uid=series_uid or None,
                        sop_instance_uid=sop_uid or None,
                        limit=RETRIEVE_PAGE,
                        offset=offset
                    )
                ) or []
            )
            for item in page:
                sop = str(item.uids.get('sop_instance_uid') or '')
                if sop and sop not in seen:
                    seen.add(sop)
                    uids.append(sop)
            if len(page) < RETRIEVE_PAGE:
                return uids
            offset += RETRIEVE_PAGE
        return uids
    except Exception:
        state.logger.exception('WADO instance resolution failed')
        abort(500)


def _multipart(state: Any, stored: list[events.StoredFile],
               boundary: str) -> Generator[bytes, None, None]:
    """Streams a ``multipart/related`` retrieval response.

    :param state: shared application state
    :param stored: stored files to serve
    :param boundary: multipart boundary token
    :yield: the response body chunk by chunk
    """
    for entry in stored:
        yield f'--{boundary}\r\n'.encode('ascii')
        yield f'Content-Type: {DICOM_CT}\r\n\r\n'.encode('ascii')
        try:
            yield from _source_stream(entry)
        except OSError:
            # A truncated multipart body must not look like a successful
            # transfer: re-raising aborts the chunked stream without the
            # terminating boundary, which the client reports as an error.
            state.logger.exception(
                'Failed to stream a stored file; aborting the multipart '
                'response'
            )
            raise
        yield b'\r\n'
    yield f'--{boundary}--\r\n'.encode('ascii')


def _source_stream(entry: events.StoredFile) -> Iterable[bytes]:
    """Streams one stored file as DICOM PS3.10 bytes.

    Handles the three ``StoredFile`` source forms: a file name produced
    by a file-backed storage, an in-memory dataset (re-serialized
    through pydicom) and an open binary stream.

    :param entry: `(SOP Class UID, Transfer Syntax UID, source)` tuple
    :type entry: events.StoredFile
    :return: generator of the file bytes, in chunks
    """
    sop_class, transfer_syntax, source = entry
    if isinstance(source, str):
        return _file_chunks(source)
    if isinstance(source, pydicom.Dataset):
        return [_serialize_dataset(source, sop_class, transfer_syntax)]
    return _stream_chunks(source)


def _file_chunks(path: str) -> Generator[bytes, None, None]:
    """Yield a stored file in chunks, closing it when done."""
    handle = open(path, 'rb')
    try:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                return
            yield chunk
    finally:
        handle.close()


def _stream_chunks(stream: BinaryIO) -> Generator[bytes, None, None]:
    """Yield an open binary source in chunks, closing it when done."""
    try:
        while True:
            chunk = stream.read(CHUNK_SIZE)
            if not chunk:
                return
            yield chunk
    finally:
        try:
            stream.close()
        except Exception:  # pragma: no cover - defensive
            pass


def _serialize_dataset(ds: pydicom.Dataset, sop_class: str | uid.UID,
                       transfer_syntax: str | uid.UID) -> bytes:
    """Serializes an in-memory stored dataset as a DICOM PS3.10 file.

    :param ds: stored dataset (without file meta information)
    :type ds: pydicom.Dataset
    :param sop_class: recorded SOP Class UID
    :param transfer_syntax: recorded Transfer Syntax UID
    :return: preamble + file meta + dataset bytes
    :rtype: bytes
    """
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = uid.UID(str(sop_class))
    meta.MediaStorageSOPInstanceUID = uid.UID(str(ds.SOPInstanceUID))
    meta.TransferSyntaxUID = uid.UID(str(transfer_syntax))
    ds.file_meta = meta
    buffer = io.BytesIO()
    pydicom.dcmwrite(buffer, ds, enforce_file_format=True)
    return buffer.getvalue()


# -----------------------------------------------------------------------
# WADO-URI
# -----------------------------------------------------------------------

def _wado_uri(state: Any) -> Any:
    """Answers a WADO-URI request (``GET {prefix}/wado``).

    Supported parameter forms: ``requestType=WADO`` with ``studyUID``
    (mandatory), optional ``seriesUID``/``objectUID``,
    ``contentType=application/dicom`` (default) or
    ``multipart/related``, and ``transferSyntax`` as the PS3.18
    comma-separated UID list or ``*`` wildcard (entries whose stored
    syntax matches none of the listed UIDs are refused, 406 — no
    transcoding in v1). Frame-level retrieval and converted media types
    are not produced in v1 (406).
    """
    params = bottle.request.params
    request_type = str(params.get('requestType') or '')
    if request_type.strip().upper() != 'WADO':
        abort(400, 'Missing or invalid requestType parameter; this '
                   'endpoint implements WADO-URI (requestType=WADO).')
    study_uid = str(params.get('studyUID') or '').strip()
    if not study_uid:
        abort(400, 'The studyUID parameter is required.')
    if str(params.get('frameNumber') or '').strip():
        abort(406, 'Frame-level retrieval is not supported in v1.')
    content_type = str(params.get('contentType') or '').strip().lower()
    if content_type.startswith(MULTIPART_CT):
        as_multipart = True
    elif content_type in ('', DICOM_CT):
        as_multipart = False
    else:
        abort(406, f'contentType {content_type!r} cannot be produced; '
                   f'v1 serves {DICOM_CT} (stored transfer syntax only).')
    series_uid = str(params.get('seriesUID') or '').strip() or None
    sop_uid = str(params.get('objectUID') or '').strip() or None
    wanted_ts = _uri_transfer_syntax(
        str(params.get('transferSyntax') or '')
    )
    require(state, events.ArchiveInstanceQuery, 'archive query')
    uids = _resolve_instance_uids(state, study_uid, series_uid, sop_uid)
    if not uids:
        abort(404)
    require(state, events.GetFiles, 'storage')
    stored = _collect_stored(state, uids)
    if wanted_ts is not None:
        stored = [entry for entry in stored if str(entry[1]) in wanted_ts]
        if not stored:
            abort(406, 'None of the requested transferSyntax UIDs matches '
                       'a stored object (no transcoding in v1).')
    if not stored:
        abort(404)
    if not as_multipart and len(stored) == 1:
        entry = stored[0]
        return bottle.HTTPResponse(
            body=_source_stream(entry), status=200,
            headers={'Content-Type': _single_content_type(entry)}
        )
    if not as_multipart:
        abort(406, 'A single-object contentType was requested but the '
                   'target holds several objects; request '
                   'multipart/related instead.')
    boundary = f'tiny-pacs-{uuid.uuid4().hex}'
    return bottle.HTTPResponse(
        body=_multipart(state, stored, boundary), status=200,
        headers={'Content-Type':
                 f'{MULTIPART_CT}; type="{DICOM_CT}"; '
                 f'boundary="{boundary}"'}
    )
