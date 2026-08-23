"""DICOM-services

Provides alternative implementation for standard DICOM-services for usage with
this PACS implementation.
"""
import functools
from collections.abc import Iterator
from itertools import count
from typing import TYPE_CHECKING, cast

from pydicom import uid
from pynetdicom2 import (
    applicationentity,
    asceprovider,
    dimsemessages,
    dsutils,
    exceptions,
    fsm,
    sopclass,
    statuses,
)

from . import events

if TYPE_CHECKING:
    from . import ae


def _send_ops_response(
        asce: asceprovider.AssociationAcceptor,
        ctx: fsm.PContextDef,
        msg: dimsemessages.CMoveRQMessage | dimsemessages.CGetRQMessage,
        _status: statuses.Status,
        nop: int,
        failed: int,
        warning: int,
        completed: int
) -> None:
    """Builds and sends a fresh C-MOVE/C-GET response.

    A new response message is created for every send: DIMSE messages are
    encoded lazily by the DUL provider thread, so reusing the same message
    object between sends would race with the encoding.
    """
    rsp: dimsemessages.CMoveRSPMessage | dimsemessages.CGetRSPMessage
    if isinstance(msg, dimsemessages.CMoveRQMessage):
        rsp = dimsemessages.CMoveRSPMessage()
    else:
        rsp = dimsemessages.CGetRSPMessage()
    rsp.message_id_being_responded_to = msg.message_id
    rsp.sop_class_uid = msg.sop_class_uid
    rsp.num_of_remaining_sub_ops = nop - completed
    rsp.num_of_completed_sub_ops = completed
    rsp.num_of_failed_sub_ops = failed
    rsp.num_of_warning_sub_ops = warning
    rsp.status = int(_status)
    asce.send(rsp, ctx.id)


def _final_status(failed: int, warning: int) -> statuses.Status:
    # PS3.4 C.4.2.1.4: when one or more sub-operations failed or produced a
    # warning, the final response carries Warning status, not Success.
    if failed or warning:
        return statuses.C_MOVE_WARNING
    return statuses.SUCCESS


@sopclass.sop_classes(sopclass.MOVE_SOP_CLASSES)
def qr_move_scp(
        asce: asceprovider.AssociationAcceptor,
        ctx: fsm.PContextDef,
        msg: dimsemessages.CMoveRQMessage
) -> None:
    """Query/Retrieve C-MOVE service implementation.

    :param asce: active association
    :type asce: asceprovider.AssociationAcceptor
    :param ctx: presentation context
    :type ctx: fsm.PContextDef
    :param msg: incoming message
    :type msg: dimsemessages.CMoveRQMessage
    """
    if not msg.data_set:
        # A C-MOVE-RQ without an Identifier cannot be processed (mirrors the
        # built-in service): respond with a failure status instead of aborting
        # the association.
        _send_ops_response(asce, ctx, msg, statuses.C_MOVE_UNABLE_TO_PROCESS,
                           0, 0, 0, 0)
        return

    ds = dsutils.decode(
        cast(bytes, msg.data_set),
        ctx.supported_ts.is_implicit_VR,
        ctx.supported_ts.is_little_endian
    )

    remote_ae, _, _gen = asce.ae.on_receive_move(ctx, ds, msg.move_destination)

    # tiny_pacs AE returns stored files tuples for its custom move service
    _files = cast('Iterator[events.StoredFile]', _gen)
    gen: list[events.StoredFile] = list(_files)

    nop = len(gen)
    if not nop:
        # nothing to move
        _send_ops_response(asce, ctx, msg, statuses.SUCCESS, 0, 0, 0, 0)
        return

    contexts = {(uid.UID(sop_class), uid.UID(ts)) for sop_class, ts, _ in gen}
    datasets = ((uid.UID(sop_class), d) for sop_class, _, d in gen)

    aet = asce.ae.local_ae.aet

    client = applicationentity.ClientAE(aet)
    # PS3.8 9.3.2.2: presentation context IDs shall be odd values
    for context, pc_id in zip(contexts, count(1, 2)):
        sop_class, ts = context
        client.supported_scu[sop_class] = sopclass.storage_scu
        pc_def = asceprovider.PContextDefList(pc_id, sop_class,
                                              frozenset([ts]))
        client.context_def_list[pc_id] = pc_def

    with client.request_association(remote_ae) as assoc:
        failed = 0
        warning = 0
        completed = 0
        success = 0
        for sop_class, data_set in datasets:
            # request an association with destination send C-STORE
            service = assoc.get_scu(sop_class)
            # PS3.7 9.1.1.1.3: message IDs must be non-zero; PS3.4 C.4.2.1.3:
            # C-STORE sub-operations of a C-MOVE carry the Move Originator
            # fields (the AE title and message ID of the C-MOVE request).
            status = service(data_set, completed + 1,
                             move_originator_aet=asce.remote_ae,
                             move_originator_message_id=msg.message_id)
            if status.is_failure:
                failed += 1
            elif status.is_warning:
                warning += 1
            else:
                success += 1
            completed += 1
            _send_ops_response(
                asce, ctx, msg, statuses.C_MOVE_PENDING,
                nop, failed, warning, completed
            )
        _send_ops_response(
            asce, ctx, msg, _final_status(failed, warning),
            nop, failed, warning, completed
        )


@sopclass.sop_classes(sopclass.GET_SOP_CLASSES)
def qr_get_scp(
        asce: asceprovider.AssociationAcceptor,
        ctx: fsm.PContextDef,
        msg: dimsemessages.CGetRQMessage
) -> None:
    """Query/Retrieve C-GET service implementation.

    :param asce: active association
    :type asce: asceprovider.AssociationAcceptor
    :param ctx: presentation context
    :type ctx: fsm.PContextDef
    :param msg: incoming message
    :type msg: dimsemessages.CGetRQMessage
    """
    if not msg.data_set:
        # A C-GET-RQ without an Identifier cannot be processed (mirrors the
        # built-in C-MOVE/C-FIND services): respond with a failure status
        # instead of aborting the association.
        _send_ops_response(
            asce, ctx, msg, statuses.C_GET_UNABLE_TO_PROCESS, 0, 0, 0, 0
        )
        return

    ds = dsutils.decode(cast(bytes, msg.data_set),
                        ctx.supported_ts.is_implicit_VR,
                        ctx.supported_ts.is_little_endian)

    gen: list[events.StoredFile] = list(
        cast('ae.AE', asce.ae).on_receive_get(ctx, ds)
    )

    nop = len(gen)
    if not nop:
        # nothing to get
        _send_ops_response(asce, ctx, msg, statuses.SUCCESS, 0, 0, 0, 0)
        return

    datasets = ((sop_class, ts, d) for sop_class, ts, d in gen)

    failed = 0
    warning = 0
    completed = 0
    success = 0
    for sop_class, ts, data_set in datasets:
        for _, context in asce.accepted_contexts.items():
            if context.sop_class == sop_class and context.supported_ts == ts:
                pc_id = context.id
                ts = context.supported_ts
                break
        else:
            err_msg = (f'SOP Class UID {sop_class} or Transfer Syntax '
                       f'is not supported {ts}')
            raise exceptions.NetDICOMError(err_msg)
        # C-GET sends C-STORE sub-operations over the same association, so
        # the storage SCU is used with an acceptor here.
        service = functools.partial(
            sopclass.storage_scu, asce,  # type: ignore[arg-type]
            fsm.PContextDef(pc_id, sop_class, ts)
        )
        # PS3.7 9.1.1.1.3: message IDs must be non-zero
        status = service(data_set, completed + 1)
        if status.is_failure:
            failed += 1
        elif status.is_warning:
            warning += 1
        else:
            success += 1
        completed += 1
        _send_ops_response(
            asce, ctx, msg, statuses.C_GET_PENDING,
            nop, failed, warning, completed
        )

    _send_ops_response(
        asce, ctx, msg, _final_status(failed, warning),
        nop, failed, warning, completed
    )
