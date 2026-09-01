"""Event definitions for the tiny_pacs event system.

Events are built on top of :mod:`trolleybus`: every event is a class carrying
its payload type and listener result type. Events are never instantiated —
the event class itself is broadcast together with a single payload value.
Multi-value payloads are packed into small dataclasses.

Lifecycle events are provided by :mod:`trolleybus` itself
(:class:`trolleybus.OnStart`, :class:`trolleybus.OnStarted` and
:class:`trolleybus.OnExit`) and are emitted by
:meth:`trolleybus.EventBus.start` / :meth:`trolleybus.EventBus.stop`.
"""
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, BinaryIO, TypeAlias

import peewee
import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import asceprovider, fsm, pdu, statuses

if TYPE_CHECKING:
    from . import client, devices, schema  # noqa: F401


#: Single stored file: SOP Class UID, Transfer Syntax UID and either a file
#: name, a dataset or a file object. UID values can come either as
#: ``pydicom.uid.UID`` objects or as plain strings (e.g. from the database).
StoredFile: TypeAlias = tuple[
    str | uid.UID,
    str | uid.UID,
    str | pydicom.Dataset | BinaryIO
]


# ---------------------------------------------------------------------------
# AE events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AssocPayload:
    """Payload of the :class:`Assoc` event."""

    #: Association acceptor handling the incoming connection
    asce: asceprovider.AssociationAcceptor

    #: Association request parameters
    assoc: pdu.AAssociateRqPDU


class Assoc(trolleybus.Event[AssocPayload, None]):
    """Incoming association request."""


@dataclass(frozen=True)
class StorePayload:
    """Payload of the :class:`Store` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: Incoming dataset: file object (when SOP Class is stored in file) or
    #: raw encoded bytes
    ds: BinaryIO | bytes


class Store(trolleybus.Event[StorePayload, statuses.Status]):
    """Incoming C-STORE request. Handling result is a C-STORE status."""


@dataclass(frozen=True)
class FindPayload:
    """Payload of the :class:`Find` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: C-FIND request dataset
    ds: pydicom.Dataset


class Find(trolleybus.Event[FindPayload,
                            Iterable[tuple[pydicom.Dataset,
                                           statuses.Status]]]):
    """Incoming C-FIND request.

    Handling result is an iterable of ``(dataset, status)`` tuples.
    """


@dataclass(frozen=True)
class MovePayload:
    """Payload of the :class:`Move` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: C-MOVE request dataset
    ds: pydicom.Dataset

    #: C-MOVE destination AE title
    destination: str


class Move(trolleybus.Event[MovePayload, list[StoredFile]]):
    """Incoming C-MOVE request. Handling result is a list of stored files."""


@dataclass(frozen=True)
class GetPayload:
    """Payload of the :class:`Get` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: C-GET request dataset
    ds: pydicom.Dataset


class Get(trolleybus.Event[GetPayload, list[StoredFile]]):
    """Incoming C-GET request. Handling result is a list of stored files."""


class Commitment(
    trolleybus.Event[list[tuple[uid.UID, uid.UID]],
                     tuple[list[tuple[uid.UID, uid.UID]],
                           list[tuple[uid.UID, uid.UID]]]]):
    """Storage Commitment request.

    Payload is a list of ``(SOP Class UID, SOP Instance UID)`` tuples; handling
    result is a tuple of two lists — successfully stored instances and
    failures.
    """


@dataclass(frozen=True)
class GetFilePayload:
    """Payload of the :class:`GetFile` event."""

    #: Presentation context
    context: fsm.PContextDef

    #: Command dataset of the received message
    command_set: pydicom.Dataset


class GetFile(trolleybus.Event[GetFilePayload, tuple[BinaryIO, int]]):
    """Request a file object to store incoming dataset.

    Handling result is a tuple of the file object and the starting position
    of the dataset stream within it.
    """


class MainAET(trolleybus.Event[None, str]):
    """Request the main AE Title of the service."""


# ---------------------------------------------------------------------------
# Storage events
# ---------------------------------------------------------------------------

class StoreDone(trolleybus.Event[pydicom.Dataset, None]):
    """Dataset has been stored successfully. Payload is the stored dataset."""


class StoreFailure(trolleybus.Event[pydicom.Dataset, None]):
    """Dataset storage has failed. Payload is the dataset that failed."""


class GetFiles(trolleybus.Event[list[str], Iterable[StoredFile]]):
    """Request stored files by a list of SOP Instance UIDs."""


class StoreVerify(trolleybus.Event[list[tuple[uid.UID, uid.UID]],
                                   tuple[frozenset[tuple[uid.UID, uid.UID]],
                                         frozenset[tuple[uid.UID,
                                                         uid.UID]]]]):
    """Verify that provided SOP Instance UIDs are stored.

    Payload is a list of ``(SOP Class UID, SOP Instance UID)`` tuples;
    handling result is a tuple of two frozensets — successes and failures.
    """


# ---------------------------------------------------------------------------
# Database events
# ---------------------------------------------------------------------------

class Atomic(trolleybus.Event[None, Any]):
    """Request an atomic transaction context manager."""


class Tables(trolleybus.Event[None, list[type[peewee.Model]]]):
    """Request a list of database tables from components."""


class Migrations(trolleybus.Event[None, 'schema.ComponentMigrations']):
    """Request schema migrations from components.

    Handling result is the component's schema name, its tables and its
    migration list (see :class:`tiny_pacs.schema.ComponentMigrations`).
    """


class StringAgg(trolleybus.Event[None, Callable[..., peewee.Function]]):
    """Request the string aggregate SQL function for the current DB driver."""


# ---------------------------------------------------------------------------
# Device events
# ---------------------------------------------------------------------------

class DeviceByAE(trolleybus.Event[str, 'devices.DeviceConfig | None']):
    """Request device settings by AE Title."""


class DeviceConfigs(
    trolleybus.Event[None, "dict[str, 'devices.DeviceConfig']"]
):
    """Request the configured remote devices.

    Answered by device registry components with a mapping of AE titles to
    device configurations, e.g. so database-backed registries can import
    the YAML-configured devices.
    """


# ---------------------------------------------------------------------------
# Client events
# ---------------------------------------------------------------------------

class GetClient(trolleybus.Event[str, 'client.DICOMClient']):
    """Request a DICOM client for the provided AE Title."""
