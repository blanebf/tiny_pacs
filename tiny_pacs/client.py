"""DICOM Client component implementation."""
import enum
import logging
from collections.abc import Iterator
from typing import Any

import pydicom
import trolleybus
from pydicom import filereader, uid
from pynetdicom2 import (
    applicationentity,
    asceprovider,
    sopclass,
    statuses,
    uids,
)

from . import component, devices, events


class FindRoot(enum.Enum):
    """Roots for C-FIND."""

    #: Patient root SOP Class UID
    PAITNET = uids.PATIENT_ROOT_FIND_SOP_CLASS

    #: Study root SOP Class UID
    STUDY = uids.STUDY_ROOT_FIND_SOP_CLASS


class MoveRoot(enum.Enum):
    """Roots for C-MOVE"""

    #: Patient root SOP Class UID
    PATIENT = uids.PATIENT_ROOT_MOVE_SOP_CLASS

    #: Study root SOP Class UID
    STUDY = uids.STUDY_ROOT_MOVE_SOP_CLASS


class DICOMClientError(Exception):
    """DICOM Client error.

    Base class for all DICOM-related client interaction errors.

    :ivar status: DICOM error status code
    """
    def __init__(self, status: statuses.Status, *args: object):
        super().__init__(*args)
        self.status = status


class CEchoError(DICOMClientError):
    """C-ECHO failure"""


class CFindError(DICOMClientError):
    """C-FIND failure"""


class CStoreError(DICOMClientError):
    """C-STORE failure"""


class CMoveError(DICOMClientError):
    """C-MOVE failure"""


class DestinationUnknownError(Exception):
    """C-MOVE destination unknown failure"""


class ClientConfig(component.ComponentConfig):
    """Configuration of the :class:`Client` component.

    The client has no settings of its own (device settings come from the
    :class:`~tiny_pacs.devices.Devices` component); this model exists so the
    component provides its own config type to the loader like every other
    component.
    """


class Client(component.Component[ClientConfig]):
    """Simple component that can create DICOM Client for provided AE Title."""

    config_model = ClientConfig

    def __init__(self, bus: trolleybus.EventBus,
                 config: ClientConfig | dict[str, Any]):
        super().__init__(bus, config)
        self.subscribe(events.GetClient, self.get)

    def get(self, remote_aet: str) -> 'DICOMClient':
        """Gets a DICOM client for provided AE Title

        :param remote_aet: remote AE title for the client
        :type remote_aet: str
        :raises DestinationUnknownError: raised if component can not find
                                         settings for provided AE Title
        :return: DICOM Client
        :rtype: DICOMClient
        """
        remote_ae = self.send_any(events.DeviceByAE, remote_aet)
        if not remote_ae:
            raise DestinationUnknownError()
        local_ae = self.send_one(events.MainAET, None)
        self.log_info('Getting DICOM client for %r', remote_ae)
        return DICOMClient(local_ae, remote_ae)


class DICOMClient:
    """DICOM Client implementation.

    Provides some convenience wrappers around common services in the SCU
    role.

    :ivar msg_id: current message ID
    :ivar local_ae: local AE Title
    :ivar remote_ae: remote AE Title
    :ivar aet: ClientAE instance for accessing DICOM services
    :ivar log: logger
    """

    def __init__(self, local_ae: str,
                 remote_ae: devices.DeviceConfig | dict[str, Any]):
        self.msg_id = 0
        self.local_ae = local_ae
        if isinstance(remote_ae, dict):
            remote_ae = devices.DeviceConfig.model_validate(remote_ae)
        # ``pynetdicom2`` requests associations from a ``RemoteAEConfig``; a
        # ``DeviceConfig`` carries the same fields plus nothing the
        # association layer needs, so convert at the boundary.
        self.remote_ae = remote_ae
        self.aet = applicationentity.ClientAE(local_ae)
        self.log = logging.getLogger('DICOMClient')

    def _remote_ae_config(self) -> asceprovider.RemoteAEConfig:
        """Returns the remote AE settings as a ``pynetdicom2`` config object.

        :return: remote AE configuration
        :rtype: asceprovider.RemoteAEConfig
        """
        return self.remote_ae.to_remote_ae()

    def echo(self) -> None:
        """Sends C-ECHO message (verification SCU)

        :raises CEchoError: raised when the C-ECHO-RSP status indicates
                            a failure
        """
        self.log.info('Sending C-ECHO request to %r', self.remote_ae)
        self.aet.add_scu(sopclass.verification_scu)
        with self.aet.request_association(self._remote_ae_config()) as asce:
            service = asce.get_scu(uids.VERIFICATION_SOP_CLASS)
            self.msg_id += 1
            status = service(self.msg_id)
            if status.is_failure:
                self.log.error('C-ECHO failed %r', status)
                raise CEchoError(status)

    def find(self, ds: pydicom.Dataset,
             root: FindRoot = FindRoot.STUDY) -> Iterator[pydicom.Dataset]:
        """Makes a Q/R C-FIND request

        :param ds: C-FIND request (search parameters)
        :type ds: pydicom.Dataset
        :param root: C-FIND root, defaults to FindRoot.STUDY
        :type root: FindRoot, optional
        :raises CFindError: raised if a C-FIND-RSP status indicates
                            a failure
        :yield: C-FIND results
        :rtype: Iterator[pydicom.Dataset]
        """
        self.aet.add_scu(sopclass.qr_find_scu)
        self.log.info('Sending C-FIND request to %r', self.remote_ae)
        with self.aet.request_association(self._remote_ae_config()) as asce:
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

    def store(self, ds: pydicom.Dataset | str,
              sop_class_uid: uid.UID | None = None,
              transfer_syntax: uid.UID | None = None) -> None:
        """Send a C-STORE request with provided dataset

        :param ds: dataset to store (filename or dataset itself)
        :type ds: pydicom.Dataset | str
        :param sop_class_uid: dataset SOP Class UID, defaults to None
        :type sop_class_uid: uid.UID, optional
        :param transfer_syntax: dataset Transfer Syntax UID, defaults to None
        :type transfer_syntax: uid.UID, optional
        """
        self.log.info('Sending C-STORE request to %r', self.remote_ae)
        if sop_class_uid is None or transfer_syntax is None:
            file_meta = filereader.read_file_meta_info(ds)
            sop_class_uid = file_meta.MediaStorageSOPClassUID
            transfer_syntax = file_meta.TransferSyntaxUID
        self.aet.supported_ts = frozenset([transfer_syntax])
        self.aet.supported_scu[sop_class_uid] = sopclass.storage_scu
        self.aet.update_context_def_list([sop_class_uid])
        with self.aet.request_association(self._remote_ae_config()) as asce:
            self.log.debug('Association established with %r', self.remote_ae)
            self.store_with_asce(asce, ds, sop_class_uid)

    def store_with_asce(self, asce: asceprovider.AssociationRequester,
                        ds: pydicom.Dataset | str,
                        sop_class_uid: uid.UID) -> None:
        """Make a C-STORE request with existing association

        :param asce: existing association
        :type asce: asceprovider.AssociationRequester
        :param ds: dataset to store (filename or dataset itself)
        :type ds: pydicom.Dataset | str
        :param sop_class_uid: dataset SOP Class UID
        :type sop_class_uid: uid.UID
        :raises CStoreError: raised if C-STORE failed
        """
        service = asce.get_scu(sop_class_uid)
        self.msg_id += 1
        status = service(ds, self.msg_id)
        if status.is_failure:
            self.log.error('C-STORE operation failed %r', status)
            raise CStoreError(status)

    def move(self, ds: pydicom.Dataset, root: MoveRoot = MoveRoot.STUDY,
             dest_ae: str | None = None) -> None:
        """Makes a C-MOVE request to destination AE Title (or self, if not
        specified)

        :param ds: C-MOVE request dataset
        :type ds: pydicom.Dataset
        :param root: C-MOVE root, defaults to MoveRoot.STUDY
        :type root: MoveRoot, optional
        :param dest_ae: destination AE Title, defaults to None
        :type dest_ae: str, optional
        """
        if dest_ae is None:
            dest_ae = self.local_ae
        self.log.info('Sending C-MOVE request to %r -> %s',
                      self.remote_ae, dest_ae)

        self.aet.add_scu(sopclass.qr_move_scu)
        with self.aet.request_association(self._remote_ae_config()) as asce:
            self.log.debug('Association established with %r', self.remote_ae)
            self._move(asce, ds, dest_ae, root)

    def move_instance(
            self, study_uid: uid.UID, series_uid: uid.UID,
            instance_uid: uid.UID, dest_ae: str | None = None,
            asce: asceprovider.AssociationRequester | None = None
    ) -> None:
        """Makes a C-MOVE request for a single instance to destination
        AE Title (or self, if not specified)

        :param study_uid: Study Instance UID
        :type study_uid: uid.UID
        :param series_uid: Series Instance UID
        :type series_uid: uid.UID
        :param instance_uid: SOP Instance UID
        :type instance_uid: uid.UID
        :param dest_ae: destination AE Title, defaults to None
        :type dest_ae: str, optional
        :param asce: existing association, defaults to None
        :type asce: asceprovider.AssociationRequester, optional
        """
        if dest_ae is None:
            dest_ae = self.local_ae
        self.log.info('Sending C-MOVE request to %r -> %s',
                      self.remote_ae, dest_ae)

        ds = pydicom.Dataset()
        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = series_uid
        ds.SOPInstanceUID = instance_uid
        ds.QueryRetrieveLevel = 'IMAGE'
        self.aet.add_scu(sopclass.qr_move_scu)
        if asce is None:
            with self.aet.request_association(
                    self._remote_ae_config()) as asce:
                self.log.debug('Association established with %r',
                               self.remote_ae)
                self._move(asce, ds, dest_ae, MoveRoot.STUDY)
        else:
            self._move(asce, ds, dest_ae, MoveRoot.STUDY)

    def _move(self, asce: asceprovider.AssociationRequester,
              ds: pydicom.Dataset, dest_ae: str, root: MoveRoot) -> None:
        service = asce.get_scu(root.value)
        self.msg_id += 1
        for status, response in service(ds, dest_ae, self.msg_id):
            if status.is_failure:
                self.log.error('C-MOVE operation failed %r', status)
                raise CMoveError(status)

            if response.num_of_failed_sub_ops != 0:
                self.log.error('C-MOVE operation failed. '
                               'One or more operation failed')
                raise CMoveError(status, 'Move operation failed')
