# -*- coding: utf-8 -*-
import enum
from itertools import chain
import logging

import pydicom

from pynetdicom2 import asceprovider
from pynetdicom2 import applicationentity
from pynetdicom2 import sopclass
from pynetdicom2 import exceptions
from pynetdicom2 import statuses

from . import event_bus
from . import devices
from . import services


class AEChannels(enum.Enum):
    ASSOC = 'on-assoc-request'
    STORE = 'on-receive-store'
    FIND = 'on-receive-find'
    MOVE = 'on-receive-move'
    GET = 'on-receive-get'
    COMMITMENT = 'on-receive-commitment'
    ON_GET_FILE = 'on-store-get-file'
    MAIN_AET = 'get-main-aet'


class AE(applicationentity.AE):
    def __init__(self, bus: event_bus.EventBus, config: dict):
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

        super().__init__(main_aet, port, supported_ts, max_pdu_length)
        self.add_scp(sopclass.verification_scp)
        self.add_scp(sopclass.qr_find_scp)
        self.add_scp(services.qr_move_scp)
        self.add_scp(sopclass.storage_scp)
        self.add_scp(sopclass.StorageCommitment())
        self.bus.subscribe(AEChannels.MAIN_AET, self.get_main_aet)

    def get_main_aet(self):
        return self.valid_aet[0]

    def get_file(self, context, command_set: pydicom.Dataset):
        return self.bus.send_one(AEChannels.ON_GET_FILE, context, command_set)

    def on_association_request(self, asce, assoc):
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

        self.bus.broadcast(AEChannels.ASSOC, asce, assoc)

    def on_receive_store(self, context, ds):
        self.log.info('Received C-STORE %r', context)
        if self.dump_ds:
            try:
                ds = pydicom.dcmread(ds, stop_before_pixels=True)

            except Exception:
                self.log.error(
                    'C-STORE failed to read dataset. C-STORE operation aborted'
                )
                raise
            else:
                self.log.debug('C-STORE dataset: %r', ds)

        try:
            results = self.bus.broadcast(AEChannels.STORE, context, ds)
        except Exception as e:
            msg = f'C-STORE handling failed: {e}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        for status in results:
            if not status.is_success:
                return status
        return statuses.SUCCESS

    def on_receive_find(self, context, ds):
        self.log.info('Received C-FIND %r', context)
        if self.dump_ds:
            self.log.debug('C-FIND dataset %r', ds)

        try:
            results = self.bus.broadcast(AEChannels.FIND, context, ds)
        except Exception as e:
            msg = f'C-FIND handling failed {e}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        yield from chain.from_iterable(results)

    def on_receive_move(self, context, ds, destination):
        self.log.info('Received C-MOVE to %s (%r)', destination, context)
        if self.dump_ds:
            self.log.debug('C-MOVE dataset %r', ds)

        remote_ae = self.bus.send_any(devices.DevicesChannels.DEVICE_BY_AE, destination)
        if not remote_ae:
            msg = f'C-MOVE destination unknown: {destination}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        try:
            results = self.bus.broadcast(AEChannels.MOVE, context, ds, destination)
        except Exception as e:
            msg = f'C-MOVE handling failed {e}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        datasets = chain.from_iterable(results)
        return remote_ae, datasets

    def on_receive_get(self, context: asceprovider.PContextDef,
                       ds: pydicom.Dataset):
        """Handling of the C-GET request

        :param context: presentation context
        :type context: asceprovider.PContextDef
        :param ds: C-GET request dataset
        :type ds: pydicom.Dataset
        :raises exceptions.EventHandlingError: raised in case there is an error
                                               while handling C-GET request
        :return: response datasets (file names or pydicom.Dataset(s))
        """
        self.log.info('Received C-GET %r', context)
        if self.dump_ds:
            self.log.debug('C-GET dataset %r', ds)

        try:
            results = self.bus.broadcast(AEChannels.GET, context, ds)
        except Exception as e:
            msg = f'C-GET handling failed {e}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)
        datasets = chain.from_iterable(results)
        return datasets

    def on_commitment_request(self, remote_ae, uids):
        self.log.info('Received Storage Commitment request for %s', remote_ae)
        self.log.debug('Storage Commitment uids %r', uids)

        remote_ae = self.bus.send_any(devices.DevicesChannels.DEVICE_BY_AE, remote_ae)
        if not remote_ae:
            msg = f'Storage Commitment destination unknown: {remote_ae}'
            self.log.error(msg)
            raise exceptions.EventHandlingError(msg)

        try:
            results = self.bus.broadcast(AEChannels.COMMITMENT, uids)
        except Exception as e:
            msg = f'Storage Commitment handling failed: {e}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg)

        success = chain.from_iterable(s for s, _ in results)
        failure = chain.from_iterable(f for _, f in results)
        return list(success), list(failure)
