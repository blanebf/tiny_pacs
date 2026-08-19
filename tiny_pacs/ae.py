"""Application entity implementation for tiny PACS.

Handles all relevant SCPs and emits appropriate events.
"""
import io
import logging
from collections.abc import Iterable, Iterator
from itertools import chain
from typing import BinaryIO, Union

import pydicom
import trolleybus

from pynetdicom2 import applicationentity
from pynetdicom2 import asceprovider
from pynetdicom2 import exceptions
from pynetdicom2 import fsm
from pynetdicom2 import sopclass
from pynetdicom2 import statuses
from pydicom import uid

from . import events
from . import services


class AE(applicationentity.AE):
    """Application Entity with SCP implementations.

    Adds all relevant SCPs for tiny PACS.
    """
    def __init__(self, bus: trolleybus.EventBus, config: dict,
                 bind_and_activate: bool = True):
        self.bus = bus
        self.log = logging.getLogger('AE')

        ae_title = config.get('ae_title', ['TINY_PACS'])
        port = config.get('port', 11112)
        supported_ts = config.get('supported_ts')
        max_pdu_length = config.get('max_pdu_length', 65536)

        self.dump_ds = config.get('dump_ds', False)

        if isinstance(ae_title, list):
            main_aet = ae_title[0]
            self.valid_aet = ae_title
        else:
            main_aet = ae_title
            self.valid_aet = [ae_title]

        super().__init__(main_aet, port, supported_ts, max_pdu_length,
                         bind_and_activate)
        self.add_scp(sopclass.verification_scp)
        self.add_scp(sopclass.qr_find_scp)
        self.add_scp(services.qr_move_scp)
        self.add_scp(services.qr_get_scp)
        self.add_scp(sopclass.storage_scp)
        self.add_scp(sopclass.StorageCommitment())
        self.bus.subscribe(events.MainAET, lambda _: self.get_main_aet())

    def get_main_aet(self) -> str:
        """Returns main AE title

        :return: main AE title
        :rtype: str
        """
        return self.valid_aet[0]

    def get_file(self, context: fsm.PContextDef,
                 command_set: pydicom.Dataset) -> tuple[BinaryIO, int]:
        return self.bus.send_one(
            events.GetFile, events.GetFilePayload(context, command_set)
        )

    def on_association_request(self, asce: asceprovider.AssociationAcceptor,
                               assoc):
        called_ae_title = assoc.called_ae_title.strip()
        calling_ae_title = assoc.calling_ae_title.strip()
        if called_ae_title not in self.valid_aet:
            self.log.error('Called AE Title is not valid: %s', called_ae_title)
            self.log.error('Valid AE Titles are %r', self.valid_aet)
            raise exceptions.AssociationRejectedError(1, 1, 7)

        self.log.info('Incoming association %s -> %s',
                      calling_ae_title, called_ae_title)
        if self.dump_ds:
            self.log.debug('ASSOCIATE-RQ %r', assoc)

        self.bus.broadcast(events.Assoc, events.AssocPayload(asce, assoc))

    def on_receive_store(self, context: fsm.PContextDef,
                         ds: Union[BinaryIO, bytes]) -> statuses.Status:
        self.log.info('Received C-STORE %r', context)
        if isinstance(ds, bytes):
            # Dataset arrives as raw bytes when its SOP Class UID is not in
            # the AE ``store_in_file`` set
            ds = io.BytesIO(ds)
        if self.dump_ds:
            try:
                _ds = pydicom.dcmread(ds, stop_before_pixels=True)

            except Exception:
                self.log.error(
                    'C-STORE failed to read dataset. C-STORE operation aborted'
                )
                raise
            else:
                self.log.debug('C-STORE dataset: %r', _ds)
            ds.seek(0)

        try:
            results = self.bus.broadcast(
                events.Store, events.StorePayload(context, ds)
            )
        except Exception as error:
            msg = f'C-STORE handling failed: {error}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        for status in results:
            if not status.is_success:
                return status
        return statuses.SUCCESS

    def on_receive_find(
            self, context: fsm.PContextDef, ds: pydicom.Dataset
    ) -> Iterator[tuple[pydicom.Dataset, statuses.Status]]:
        self.log.info('Received C-FIND %r', context)
        if self.dump_ds:
            self.log.debug('C-FIND dataset %r', ds)

        try:
            results = self.bus.broadcast(
                events.Find, events.FindPayload(context, ds)
            )
        except Exception as error:
            msg = f'C-FIND handling failed {error}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        yield from chain.from_iterable(results)

    def on_receive_move(  # type: ignore[override]
            self, context: fsm.PContextDef, ds: pydicom.Dataset,
            destination: str
    ) -> tuple[asceprovider.RemoteAEConfig, int,
               Iterator[events.StoredFile]]:
        self.log.info('Received C-MOVE to %s (%r)', destination, context)
        if self.dump_ds:
            self.log.debug('C-MOVE dataset %r', ds)

        remote_ae = self.bus.send_any(events.DeviceByAE, destination)
        if not remote_ae:
            msg = f'C-MOVE destination unknown: {destination}'
            self.log.error(msg)
            raise exceptions.EventHandlingError(msg)

        try:
            results = self.bus.broadcast(
                events.Move, events.MovePayload(context, ds, destination)
            )
        except Exception as error:
            msg = f'C-MOVE handling failed {error}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        datasets = list(chain.from_iterable(results))
        return asceprovider.RemoteAEConfig(**remote_ae), len(datasets), iter(datasets)

    def on_receive_get(
            self, context: fsm.PContextDef, ds: pydicom.Dataset
    ) -> Iterator[events.StoredFile]:
        """Handling of the C-GET request

        :param context: presentation context
        :type context: fsm.PContextDef
        :param ds: C-GET request dataset
        :type ds: pydicom.Dataset
        :raises exceptions.EventHandlingError: raised in case there is an error
                                               while handling C-GET request
        :return: response datasets as tuples of SOP Class UID, Transfer Syntax
                 UID and either file name or pydicom.Dataset
        """
        self.log.info('Received C-GET %r', context)
        if self.dump_ds:
            self.log.debug('C-GET dataset %r', ds)

        try:
            results = self.bus.broadcast(
                events.Get, events.GetPayload(context, ds)
            )
        except Exception as error:
            msg = f'C-GET handling failed {error}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)
        yield from chain.from_iterable(results)

    def on_commitment_request(
            self, remote_ae: str, uids: Iterable[tuple[uid.UID, uid.UID]]
    ) -> tuple[asceprovider.RemoteAEConfig,
               list[tuple[uid.UID, uid.UID]],
               list[tuple[uid.UID, uid.UID, int]]]:
        self.log.info('Received Storage Commitment request for %s', remote_ae)
        self.log.debug('Storage Commitment uids %r', uids)

        device = self.bus.send_any(events.DeviceByAE, remote_ae)
        if not device:
            msg = f'Storage Commitment destination unknown: {remote_ae}'
            self.log.error(msg)
            raise exceptions.EventHandlingError(msg)

        try:
            results = self.bus.broadcast(events.Commitment, list(uids))
        except Exception as error:
            msg = f'Storage Commitment handling failed: {error}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        success = list(chain.from_iterable(s for s, _ in results))
        failure = [
            (sop_class, sop_instance,
             sopclass.StorageCommitment.NO_SUCH_OBJECT_INSTANCE)
            for sop_class, sop_instance
            in chain.from_iterable(f for _, f in results)
        ]
        return asceprovider.RemoteAEConfig(**device), success, failure
