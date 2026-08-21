"""Application entity implementation for tiny PACS.

Handles all relevant SCPs and emits appropriate events.
"""
import functools
import io
import logging
import socket
import socketserver
import ssl
from collections.abc import Iterable, Iterator
from itertools import chain
from typing import Any, BinaryIO

import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import (
    applicationentity,
    asceprovider,
    exceptions,
    fsm,
    pdu,
    sopclass,
    statuses,
)

from . import events, services
from .config import AEConfig, TLSConfig


def make_tls_context(tls_config: TLSConfig) -> ssl.SSLContext:
    """Creates a server-side TLS context from the ``tls`` AE config section.

    :param tls_config: TLS configuration with the ``certificate`` and
                       (optionally) ``key`` entries pointing to PEM files,
                       plus an optional ``ca`` entry for verifying client
                       certificates
    :return: configured SSL context
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tls_config.certificate, tls_config.key)
    if tls_config.ca:
        context.load_verify_locations(tls_config.ca)
        context.verify_mode = ssl.CERT_REQUIRED
    return context


class _TLSThreadingTCPServer(socketserver.ThreadingTCPServer):
    """Threading TCP server that wraps incoming connections in TLS.

    Similar to pynetdicom2's private ``ssl_ae._SSLThreadingTCPServer``, except
    that the TLS handshake happens in the per-connection handler thread with a
    deadline, not on the accept loop thread: a stalled or failing handshake
    cannot block acceptance of new connections.
    """
    allow_reuse_address = True
    daemon_threads = True

    #: TLS handshake deadline in seconds
    handshake_timeout = 15.0

    def __init__(self, context: ssl.SSLContext,
                 server_address: tuple[str, int],
                 RequestHandlerClass: Any,
                 bind_and_activate: bool = True):
        super().__init__(server_address, RequestHandlerClass,
                         bind_and_activate)
        self.context = context

    # The supertype also allows datagram ``(bytes, socket)`` requests; a TLS
    # server only ever receives stream sockets.
    def process_request_thread(  # type: ignore[override]
            self, request: socket.socket,
            client_address: tuple[str, int]) -> None:
        try:
            request.settimeout(self.handshake_timeout)
            request = self.context.wrap_socket(request, server_side=True)
            request.settimeout(None)
        except OSError:
            logging.getLogger('AE').warning(
                'TLS handshake failed for %s', client_address
            )
            request.close()
            return
        super().process_request_thread(request, client_address)


class AE(applicationentity.AE):
    """Application Entity with SCP implementations.

    Adds all relevant SCPs for tiny PACS. When the config contains a ``tls``
    section (``certificate``, optional ``key`` and ``ca`` file names) all
    incoming connections are wrapped in TLS.
    """
    def __init__(self, bus: trolleybus.EventBus,
                 config: AEConfig | dict[str, Any],
                 bind_and_activate: bool = True):
        self.bus = bus
        self.log = logging.getLogger('AE')

        if not isinstance(config, AEConfig):
            config = AEConfig.model_validate(config)

        ae_title = config.ae_title
        self.valid_aet: list[str]
        if isinstance(ae_title, list):
            main_aet = ae_title[0]
            self.valid_aet = ae_title
        else:
            main_aet = ae_title
            self.valid_aet = [ae_title]

        self.dump_ds = config.dump_ds

        self.ssl_context: ssl.SSLContext | None = None
        if config.tls is not None:
            self.ssl_context = make_tls_context(config.tls)

        supported_ts = [uid.UID(ts) for ts in config.supported_ts]
        super().__init__(main_aet, config.port, supported_ts,
                         config.max_pdu_length, bind_and_activate)
        self.add_scp(sopclass.verification_scp)
        self.add_scp(sopclass.qr_find_scp)
        self.add_scp(services.qr_move_scp)
        self.add_scp(services.qr_get_scp)
        self.add_scp(sopclass.storage_scp)
        self.add_scp(sopclass.StorageCommitment())
        self.bus.subscribe(events.MainAET, lambda _: self.get_main_aet())

    def _create_server(self, port: int, bind_and_activate: bool,
                       max_pdu_length: int) -> socketserver.TCPServer:
        if self.ssl_context is None:
            return super()._create_server(port, bind_and_activate,
                                          max_pdu_length)
        # ``local_ae`` is typed against ``AEBaseProto`` whose
        # ``on_receive_move`` yields plain datasets; tiny_pacs intentionally
        # yields stored-file tuples instead (see the ``on_receive_move``
        # override below).
        return _TLSThreadingTCPServer(
            self.ssl_context, ('', port),
            functools.partial(
                applicationentity.RequestHandler,
                local_ae=self,  # type: ignore[arg-type]
                max_pdu_length=max_pdu_length
            ),
            bind_and_activate
        )

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
                               assoc: pdu.AAssociateRqPDU) -> None:
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

    def on_association_response(self, response: pdu.AAssociateAcPDU) -> None:
        """Handles response to an outgoing association request."""
        self.log.info('Outgoing association accepted')
        if self.dump_ds:
            self.log.debug('ASSOCIATE-AC %r', response)

    def on_abort(self, asce: asceprovider.Association,
                 exc: exceptions.AssociationAbortedError) -> None:
        """Handles association aborts."""
        self.log.warning('Association aborted: %r', exc)

    def on_dcm_timeout(self, asce: asceprovider.Association,
                       exc: exceptions.DCMTimeoutError) -> None:
        """Handles DICOM timeouts on associations."""
        self.log.warning('Association timed out: %r', exc)

    def on_receive_echo(self, context: fsm.PContextDef) -> statuses.Status:
        """Handles C-ECHO requests.

        Logged at DEBUG level: verification is the standard periodic
        connectivity probe, so per-echo logging would produce unbounded
        log volume on busy PACSes.
        """
        self.log.debug('Received C-ECHO %r', context)
        return statuses.SUCCESS

    def on_commitment_response(
            self, transaction_uid: uid.UID,
            success: Iterable[tuple[uid.UID, uid.UID]],
            failure: Iterable[tuple[uid.UID, uid.UID, int]]
    ) -> None:
        """Handles incoming Storage Commitment reports (N-EVENT-REPORT)."""
        success = list(success)
        failure = list(failure)
        self.log.info('Received Storage Commitment report, '
                      'Transaction UID %s', transaction_uid)
        self.log.debug('Storage Commitment report, committed: %r, failed: %r',
                       success, failure)

    def on_receive_store(self, context: fsm.PContextDef,
                         ds: BinaryIO | bytes) -> statuses.Status:
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
            raise exceptions.EventHandlingError(msg) from error

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
            raise exceptions.EventHandlingError(msg) from error

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
            raise exceptions.EventHandlingError(msg) from error

        datasets = list(chain.from_iterable(results))
        return remote_ae.to_remote_ae(), len(datasets), iter(datasets)

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
            raise exceptions.EventHandlingError(msg) from error
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
            raise exceptions.EventHandlingError(msg) from error

        success = list(chain.from_iterable(s for s, _ in results))
        failure = [
            (sop_class, sop_instance,
             sopclass.StorageCommitment.NO_SUCH_OBJECT_INSTANCE)
            for sop_class, sop_instance
            in chain.from_iterable(f for _, f in results)
        ]
        return device.to_remote_ae(), success, failure
