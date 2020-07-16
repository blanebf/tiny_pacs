# -*- coding: utf-8 -*-
import enum
import logging

import pydicom

from pydicom import filereader
from pynetdicom2 import applicationentity
from pynetdicom2 import sopclass
from pynetdicom2 import uids

from . import ae
from . import component
from . import devices
from . import event_bus


class ClientChannels(enum.Enum):
    GET_CLIENT = 'get-client'


class FindRoot(enum.Enum):
    PAITNET = uids.PATIENT_ROOT_FIND_SOP_CLASS
    STUDY = uids.STUDY_ROOT_FIND_SOP_CLASS


class MoveRoot(enum.Enum):
    PATIENT = uids.PATIENT_ROOT_MOVE_SOP_CLASS
    STUDY = uids.STUDY_ROOT_MOVE_SOP_CLASS


class DICOMClientError(Exception):
    def __init__(self, status, *args):
        super().__init__(*args)
        self.status = status


class CEchoError(DICOMClientError):
    pass


class CFindError(DICOMClientError):
    pass


class CStoreError(DICOMClientError):
    pass


class CMoveError(DICOMClientError):
    pass


class DestinationUnknownError(Exception):
    pass


class Client(component.Component):
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self.subscribe(ClientChannels.GET_CLIENT, self.get)

    def get(self, remote_aet):
        remote_ae = self.send_any(devices.DevicesChannels.DEVICE_BY_AE, remote_aet)
        if not remote_ae:
            raise DestinationUnknownError()
        local_ae = self.send_one(ae.AEChannels.MAIN_AET)
        self.log_info('Getting DICOM client for %r', remote_ae)
        return DICOMClient(local_ae, remote_ae)


class DICOMClient:
    def __init__(self, local_ae, remote_ae):
        self.msg_id = 0
        self.local_ae = local_ae
        self.remote_ae = remote_ae
        self.aet = applicationentity.ClientAE(local_ae)
        self.log = logging.getLogger('DICOMClient')

    def echo(self):
        self.log.info('Sending C-ECHO request to %r', self.remote_ae)
        self.aet.add_scu(sopclass.verification_scu)
        with self.aet.request_association(self.remote_ae) as asce:
            service = asce.get_scu(uids.VERIFICATION_SOP_CLASS)
            self.msg_id += 1
            status = service(self.msg_id)
            if status.is_failure:
                self.log.error('C-ECHO failed %r', status)
                raise CEchoError(status)

    def find(self, ds, root=FindRoot.STUDY):
        self.aet.add_scu(sopclass.qr_find_scu)
        self.log.info('Sending C-FIND request to %r', self.remote_ae)
        with self.aet.request_association(self.remote_ae) as asce:
            self.log.debug('Association established with %r', self.remote_ae)
            service = asce.get_scu(root.value)
            self.msg_id += 1
            for result, status in service(ds, self.msg_id):
                if status.is_failure:
                    self.log.error('C-FIND operation failed %r', status)
                    raise CFindError(status)

                if not result:
                    continue

                yield result

    def store(self, ds, sop_class_uid=None, transfer_syntax=None):
        self.log.info('Sending C-STORE request to %r', self.remote_ae)
        if sop_class_uid is None or transfer_syntax is None:
            file_meta = filereader.read_file_meta_info(ds)
            sop_class_uid = file_meta.MediaStorageSOPClassUID
            transfer_syntax = file_meta.TransferSyntaxUID
        self.aet.supported_ts = frozenset([transfer_syntax])
        self.aet.supported_scu[sop_class_uid] = sopclass.storage_scu
        self.aet.update_context_def_list([sop_class_uid])
        with self.aet.request_association(self.remote_ae) as asce:
            self.log.debug('Association established with %r', self.remote_ae)
            self.store_with_asce(asce, ds, sop_class_uid)

    def store_with_asce(self, asce, ds, sop_class_uid):
        service = asce.get_scu(sop_class_uid)
        self.msg_id += 1
        status = service(ds, self.msg_id)
        if status.is_failure:
            self.log.error('C-STORE operation failed %r', status)
            raise CStoreError(status)

    def move(self, ds, root=MoveRoot.STUDY, dest_ae=None):
        if dest_ae is None:
            dest_ae = self.local_ae
        self.log.info('Sending C-MOVE request to %r -> %s', self.remote_ae, dest_ae)

        self.aet.add_scu(sopclass.qr_move_scu)
        with self.aet.request_association(self.remote_ae) as asce:
            self.log.debug('Association established with %r', self.remote_ae)
            self._move(asce, ds, dest_ae, root)

    def move_instance(self, study_uid, series_uid, instance_uid, dest_ae=None,
                      asce=None):
        if dest_ae is None:
            dest_ae = self.local_ae
        self.log.info('Sending C-MOVE request to %r -> %s', self.remote_ae, dest_ae)

        ds = pydicom.Dataset()
        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = series_uid
        ds.SOPInstnaceUID = instance_uid
        ds.QueryRetrieveLevel = 'IMAGE'
        self.aet.add_scu(sopclass.qr_move_scu)
        if asce is None:
            with self.aet.request_association(self.remote_ae) as asce:
                self.log.debug('Association established with %r', self.remote_ae)
                self._move(asce, ds, dest_ae, MoveRoot.STUDY)
        else:
            self._move(asce, ds, dest_ae, MoveRoot.STUDY)

    def _move(self, asce, ds, dest_ae, root):
        service = asce.get_scu(root.value)
        self.msg_id +=1
        for status, response in service(ds, dest_ae, self.msg_id):
            if status.is_failure:
                self.log.error('C-MOVE operation failed %r', status)
                raise CMoveError(status)

            if response.num_of_failed_sub_ops != 0:
                self.log.error('C-MOVE operation failed. One or more operation failed')
                raise CMoveError(status, 'Move operation failed')
