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
from typing import Any, BinaryIO, cast

import pydicom
import trolleybus
from pydicom import uid
from pynetdicom2 import (
    applicationentity,
    asceprovider,
    dimsemessages,
    exceptions,
    fsm,
    pdu,
    sopclass,
    statuses,
)

from . import assoc_context, events, services
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


class _ThreadingTCPServer(socketserver.ThreadingTCPServer):
    """Threading TCP server with the defaults used by DICOM SCPs."""
    allow_reuse_address = True
    daemon_threads = True


class _RequestHandler(applicationentity.RequestHandler):
    """Request handler that notifies the AE when an association ends.

    ``AssociationAcceptor.handle`` runs the whole association lifecycle
    on the connection thread; wrapping it here gives the AE a teardown
    hook for every exit path (release, abort, timeout or error), which
    closes the association context and broadcasts
    :class:`~tiny_pacs.events.AssocReleased`. Afterwards the handler
    broadcasts :class:`~tiny_pacs.events.CloseConnection` so the thread
    returns its pooled database connection: peewee tracks connections
    per thread and this thread dies with the association, so without the
    explicit close every association leaks one pooled connection.
    """

    def handle(self) -> None:
        try:
            super().handle()
        finally:
            local_ae = getattr(self.asce, 'ae', None)
            try:
                on_end = getattr(local_ae, 'on_association_end', None)
                if callable(on_end):
                    on_end(self.asce)
            finally:
                bus = getattr(local_ae, 'bus', None)
                if bus is not None:
                    bus.broadcast_nothrow(events.CloseConnection, None)


class AE(applicationentity.AE):
    """Application Entity with SCP implementations.

    Adds all relevant SCPs for tiny PACS. When the config contains a ``tls``
    section (``certificate``, optional ``key`` and ``ca`` file names) all
    incoming connections are wrapped in TLS.
    """
    def __init__(self, bus: trolleybus.EventBus,
                 config: AEConfig | dict[str, Any],
                 bind_and_activate: bool = True):
        """Initializes the AE.

        Sets up TLS when configured, registers all built-in SCPs and
        subscribes to ``MainAET`` requests.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: AE configuration
        :type config: AEConfig or dict
        :param bind_and_activate: bind the port immediately, defaults to
                True
        :type bind_and_activate: bool, optional
        """
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

        #: Service hooks contributed through
        #: :class:`~tiny_pacs.events.ServicesRegistry`, indexed by the
        #: abstract syntax they serve
        self.find_hooks: dict[uid.UID, events.ServiceHook] = {}
        for hooks in self.bus.broadcast(events.ServicesRegistry, None):
            for hook in hooks:
                self._add_service_hook(hook)

        self.bus.subscribe(events.MainAET, lambda _: self.get_main_aet())

    def _add_service_hook(self, hook: events.ServiceHook) -> None:
        """Registers a contributed service hook.

        The hook's SOP classes are added to the AE's presentation
        contexts; C-FIND requests arriving on them are broadcast as the
        event class the hook declared instead of the built-in
        :class:`~tiny_pacs.events.Find`. Hooks may only add SOP classes,
        never replace built-in services.

        :param hook: service hook contributed by a component
        :type hook: events.ServiceHook
        """
        classes = [uid.UID(sop) for sop in hook.sop_classes]
        new = [sop for sop in classes if sop not in self.supported_scp]
        if not new:
            self.log.warning(
                'Service hook %r: all SOP classes are already served by '
                'this AE, ignoring', hook.name
            )
            return
        if hook.on_find is None:
            self.log.warning(
                'Service hook %r declares no find event, ignoring',
                hook.name
            )
            return
        for sop in new:
            self.find_hooks[sop] = hook
            self.log.info(
                'Service hook %r serves SOP class %s', hook.name, sop
            )

        @sopclass.sop_classes(new)
        def hook_find_scp(
                asce: asceprovider.AssociationAcceptor,
                ctx: fsm.PContextDef,
                msg: dimsemessages.CFindRQMessage
        ) -> None:
            # Routing to the hook's event happens in ``on_receive_find``
            # through the presentation context's abstract syntax
            sopclass.qr_find_scp(asce, ctx, msg)

        self.add_scp(cast(Any, hook_find_scp))

    def _create_server(self, port: int, bind_and_activate: bool,
                       max_pdu_length: int) -> socketserver.TCPServer:
        # ``local_ae`` is typed against ``AEBaseProto`` whose
        # ``on_receive_move`` yields plain datasets; tiny_pacs intentionally
        # yields stored-file tuples instead (see the ``on_receive_move``
        # override below).
        handler = functools.partial(
            _RequestHandler,
            local_ae=self,  # type: ignore[arg-type]
            max_pdu_length=max_pdu_length
        )
        if self.ssl_context is None:
            return _ThreadingTCPServer(
                ('', port), handler, bind_and_activate
            )
        return _TLSThreadingTCPServer(
            self.ssl_context, ('', port), handler, bind_and_activate
        )

    def get_main_aet(self) -> str:
        """Returns main AE title

        :return: main AE title
        :rtype: str
        """
        return self.valid_aet[0]

    def get_file(self, context: fsm.PContextDef,
                 command_set: pydicom.Dataset) -> tuple[BinaryIO, int]:
        """Requests a file object to store the incoming dataset.

        Runs on the per-association DUL provider thread (the FSM decodes
        the incoming dataset there), so the thread-local database
        connection the storage handlers check out is released right after:
        the DUL thread dies with the association and never runs another
        teardown hook, and an unclosed pooled connection would leak.

        :param context: presentation context
        :type context: fsm.PContextDef
        :param command_set: command dataset of the received message
        :type command_set: pydicom.Dataset
        :return: file object and the dataset stream start position
        :rtype: tuple
        """
        try:
            return self.bus.send_one(
                events.GetFile, events.GetFilePayload(context, command_set)
            )
        finally:
            self.bus.broadcast_nothrow(events.CloseConnection, None)

    def on_association_request(self, asce: asceprovider.AssociationAcceptor,
                               assoc: pdu.AAssociateRqPDU) -> None:
        """Handles incoming association requests.

        Requests with an unknown called AE title are rejected and
        broadcast as :class:`~tiny_pacs.events.AssocRejected`; for valid
        requests the association context is opened *before* the
        :class:`~tiny_pacs.events.Assoc` broadcast, so every listener can
        attribute its work through :func:`tiny_pacs.assoc_context.current`.
        A rejection raised by an ``Assoc`` listener is broadcast as
        ``AssocRejected`` too, then re-raised so pynetdicom2 sends the
        A-ASSOCIATE-RJ.

        :param asce: association acceptor
        :type asce: asceprovider.AssociationAcceptor
        :param assoc: association request parameters
        :type assoc: pdu.AAssociateRqPDU
        :raises exceptions.AssociationRejectedError: raised when the called
                AE title is not valid for this AE or when an ``Assoc``
                listener rejects the association
        """
        called_ae_title = assoc.called_ae_title.strip()
        calling_ae_title = assoc.calling_ae_title.strip()
        if called_ae_title not in self.valid_aet:
            self.log.error('Called AE Title is not valid: %s', called_ae_title)
            self.log.error('Valid AE Titles are %r', self.valid_aet)
            self.bus.broadcast(
                events.AssocRejected,
                events.AssocRejectedPayload(
                    None, f'called AE title not valid: {called_ae_title}'
                )
            )
            raise exceptions.AssociationRejectedError(1, 1, 7)

        self.log.info('Incoming association %s -> %s',
                      calling_ae_title, called_ae_title)
        if self.dump_ds:
            self.log.debug('ASSOCIATE-RQ %r', assoc)

        assoc_context.open_context(asce, assoc)
        try:
            self.bus.broadcast(events.Assoc, events.AssocPayload(asce, assoc))
        except exceptions.AssociationRejectedError as error:
            reason = str(error) or 'rejected by an association listener'
            self.bus.broadcast(
                events.AssocRejected,
                events.AssocRejectedPayload(assoc, reason)
            )
            # The association never got established: drop the context
            # without an ``AssocReleased`` broadcast
            assoc_context.close_current()
            raise

    def on_association_end(
            self, asce: asceprovider.AssociationAcceptor | None = None
    ) -> None:
        """Handles the teardown of an incoming association.

        Closes the association context of the connection thread and
        broadcasts :class:`~tiny_pacs.events.AssocReleased` with it.
        Called by the request handler wrapper on every association exit
        path; associations that never opened a context (rejected before
        establishment) are a silent no-op.

        :param asce: association acceptor, unused
        :type asce: asceprovider.AssociationAcceptor or None
        """
        context = assoc_context.close_current()
        if context is None:
            return
        self.log.info('Association ended %s -> %s',
                      context.calling_aet, context.called_aet)
        try:
            self.bus.broadcast(events.AssocReleased, context)
        except Exception as error:
            # Teardown must not fail because of a listener
            self.log.exception('AssocReleased handling failed: %s', error)

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
        """Handles C-STORE requests.

        Raw dataset bytes are wrapped in a file object; the dataset is then
        broadcast as a :class:`~tiny_pacs.events.Store` event carrying the
        association context. The first non-success handler status is
        returned to the peer. The dataset is then decoded and broadcast as
        a :class:`~tiny_pacs.events.StoreDataset` event feeding the store
        pipeline; a decode or pipeline failure yields a C-STORE failure
        status.

        :param context: presentation context
        :type context: fsm.PContextDef
        :param ds: incoming dataset as a file object or raw bytes
        :type ds: BinaryIO or bytes
        :return: C-STORE status
        :rtype: statuses.Status
        :raises exceptions.EventHandlingError: raised if ``Store`` event
                handling fails
        """
        self.log.info('Received C-STORE %r', context)
        if isinstance(ds, bytes):
            # Dataset arrives as raw bytes when its SOP Class UID is not in
            # the AE ``store_in_file`` set
            ds = io.BytesIO(ds)

        try:
            results = self.bus.broadcast(
                events.Store,
                events.StorePayload(context, ds, assoc_context.current())
            )
        except Exception as error:
            msg = f'C-STORE handling failed: {error}'
            self.log.exception(msg)
            raise exceptions.EventHandlingError(msg) from error

        for status in results:
            if not status.is_success:
                return status

        try:
            decoded = pydicom.dcmread(ds, stop_before_pixels=True)
        except Exception:
            self.log.error(
                'C-STORE failed to read dataset. C-STORE operation aborted'
            )
            return statuses.C_STORE_CANNOT_UNDERSTAND
        if self.dump_ds:
            self.log.debug('C-STORE dataset: %r', decoded)

        try:
            self.bus.broadcast(
                events.StoreDataset,
                events.StoreDatasetPayload(
                    decoded, str(context.supported_ts)
                )
            )
        except Exception as error:
            self.log.error('C-STORE dataset processing failed: %s', error)
            return statuses.C_STORE_CANNOT_UNDERSTAND
        return statuses.SUCCESS

    def on_receive_find(
            self, context: fsm.PContextDef, ds: pydicom.Dataset
    ) -> Iterator[tuple[pydicom.Dataset, statuses.Status]]:
        """Handles C-FIND requests.

        Requests arriving on a SOP class contributed by a
        :class:`~tiny_pacs.events.ServiceHook` are broadcast as the event
        class the hook declared; every other request is broadcast as a
        :class:`~tiny_pacs.events.Find` event. The results of all handlers
        are yielded.

        :param context: presentation context
        :type context: fsm.PContextDef
        :param ds: C-FIND request dataset
        :type ds: pydicom.Dataset
        :yield: tuples of result dataset and status
        :raises exceptions.EventHandlingError: raised if event handling
                fails
        """
        self.log.info('Received C-FIND %r', context)
        if self.dump_ds:
            self.log.debug('C-FIND dataset %r', ds)

        payload = events.FindPayload(context, ds, assoc_context.current())
        hook = self.find_hooks.get(context.sop_class)
        event: type[trolleybus.Event[Any, Any]] = events.Find
        if hook is not None and hook.on_find is not None:
            event = hook.on_find
        try:
            results: Any = self.bus.broadcast(event, payload)
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
        """Handles C-MOVE requests.

        Resolves the destination AE title via
        :class:`~tiny_pacs.events.DeviceByAE` and broadcasts a
        :class:`~tiny_pacs.events.Move` event; the resulting stored files
        are forwarded to the C-MOVE implementation.

        :param context: presentation context
        :type context: fsm.PContextDef
        :param ds: C-MOVE request dataset
        :type ds: pydicom.Dataset
        :param destination: move destination AE title
        :type destination: str
        :return: destination AE configuration, number of files and the
                stored files
        :raises exceptions.EventHandlingError: raised when the destination
                is unknown or event handling fails
        """
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
                events.Move,
                events.MovePayload(context, ds, destination,
                                   assoc_context.current())
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
        :raises exceptions.EventHandlingError: raised if there is an error
                                               while handling the C-GET
                                               request
        :yield: stored files: tuples of SOP Class UID, Transfer Syntax UID
                and either a file name, a dataset or a file object
        :rtype: events.StoredFile
        """
        self.log.info('Received C-GET %r', context)
        if self.dump_ds:
            self.log.debug('C-GET dataset %r', ds)

        try:
            results = self.bus.broadcast(
                events.Get,
                events.GetPayload(context, ds, assoc_context.current())
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
        """Handles Storage Commitment requests.

        Broadcasts a :class:`~tiny_pacs.events.Commitment` event; missing
        instances are reported with the ``no such object instance`` failure
        reason.

        :param remote_ae: AE title the commitment report is sent to
        :type remote_ae: str
        :param uids: SOP Class / SOP Instance UID tuples to verify
        :type uids: Iterable[tuple[uid.UID, uid.UID]]
        :return: destination AE configuration, committed instances and
                failures with their failure reason
        :raises exceptions.EventHandlingError: raised when the destination
                is unknown or event handling fails
        """
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
