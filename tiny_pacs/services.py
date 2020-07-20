# -*- coding: utf-8 -*-
from itertools import count

from pydicom import uid

from pynetdicom2 import asceprovider
from pynetdicom2 import applicationentity
from pynetdicom2 import dimsemessages
from pynetdicom2 import sopclass
from pynetdicom2 import statuses
from pynetdicom2 import dsutils


@sopclass.sop_classes(sopclass.MOVE_SOP_CLASSES)
def qr_move_scp(asce: asceprovider.AssociationAcceptor,
                ctx: asceprovider.PContextDef,
                msg: dimsemessages.CMoveRQMessage):
    """Query/Retrieve C-MOVE service implementation.

    :param asce: active association
    :type asce: asceprovider.AssociationAcceptor
    :param ctx: presentation context
    :type ctx: asceprovider.PContextDef
    :param msg: incoming message
    :type msg: dimsemessages.CMoveRQMessage
    """
    ds = dsutils.decode(msg.data_set, ctx.supported_ts.is_implicit_VR,
                        ctx.supported_ts.is_little_endian)

    # make response
    rsp = dimsemessages.CMoveRSPMessage()
    rsp.message_id_being_responded_to = msg.message_id
    rsp.sop_class_uid = msg.sop_class_uid
    remote_ae, gen = asce.ae.on_receive_move(ctx, ds, msg.move_destination)

    if not isinstance(gen, list):
        gen = list(gen)

    nop = len(gen)
    if not nop:
        # nothing to move
        _set_ops(rsp, 0, 0, 0, 0)
        rsp.status = int(statuses.SUCCESS)
        asce.send(rsp, ctx.id)
        return

    contexts = {(sop_class, ts) for sop_class, ts, _ in gen}
    datasets = ((sop_class, d) for sop_class, _, d in gen)

    aet = asce.ae.local_ae['aet']

    client = applicationentity.ClientAE(aet)
    for context, pc_id in zip(contexts, count(0, 2)):
        sop_class, ts = context
        client.supported_scu[sop_class] = sopclass.storage_scu
        pc_def = asceprovider.PContextDef(pc_id, uid.UID(sop_class), [ts])
        client.context_def_list[pc_id] = pc_def

    with client.request_association(remote_ae) as assoc:
        failed = 0
        warning = 0
        completed = 0
        success = 0
        rsp.status = int(statuses.C_MOVE_PENDING)
        for sop_class, data_set in datasets:
            # request an association with destination send C-STORE
            service = assoc.get_scu(sop_class)
            status = service(data_set, completed)
            if status.is_failure:
                failed += 1
            elif status.is_warning:
                warning += 1
            else:
                success +=1
            rsp.num_of_remaining_sub_ops = nop - completed
            rsp.num_of_completed_sub_ops = success
            rsp.num_of_failed_sub_ops = failed
            rsp.num_of_warning_sub_ops = warning
            completed += 1

            # send response
            asce.send(rsp, ctx.id)
        _set_ops(rsp, completed, failed, warning, success)
        rsp.status = int(statuses.SUCCESS)
        asce.send(rsp, ctx.id)


@sopclass.sop_classes(sopclass.GET_SOP_CLASSES)
def qr_get_scp(asce: asceprovider.AssociationAcceptor,
               ctx: asceprovider.PContextDef, msg: dimsemessages.CGetRQMessage):
    """Query/Retrieve C-GET service implementation.

    :param asce: active association
    :type asce: asceprovider.AssociationAcceptor
    :param ctx: presentation context
    :type ctx: asceprovider.PContextDef
    :param msg: incoming message
    :type msg: dimsemessages.CGetRQMessage
    """
    ds = dsutils.decode(msg.data_set, ctx.supported_ts.is_implicit_VR,
                        ctx.supported_ts.is_little_endian)

    # make response
    rsp = dimsemessages.CGetRSPMessage()
    rsp.message_id_being_responded_to = msg.message_id
    rsp.sop_class_uid = msg.sop_class_uid
    gen = asce.ae.on_receive_get(ctx, ds)

    if not isinstance(gen, list):
        gen = list(gen)

    nop = len(gen)
    if not nop:
        # nothing to move
        _set_ops(rsp, 0, 0, 0, 0)
        rsp.status = int(statuses.SUCCESS)
        asce.send(rsp, ctx.id)
        return

    datasets = ((sop_class, d) for sop_class, _, d in gen)

    failed = 0
    warning = 0
    completed = 0
    success = 0
    rsp.status = int(statuses.C_GET_PENDING)
    for sop_class, data_set in datasets:
        service = asce.get_scu(sop_class)
        status = service(data_set, completed)
        if status.is_failure:
            failed += 1
        elif status.is_warning:
            warning += 1
        else:
            success +=1
        rsp.num_of_remaining_sub_ops = nop - completed
        rsp.num_of_completed_sub_ops = success
        rsp.num_of_failed_sub_ops = failed
        rsp.num_of_warning_sub_ops = warning
        completed += 1

        # send response
        asce.send(rsp, ctx.id)

    _set_ops(rsp, completed, failed, warning, success)
    rsp.status = int(statuses.SUCCESS)
    asce.send(rsp, ctx.id)


def _set_ops(msg, nop, failed, warning, completed):
    msg.num_of_remaining_sub_ops = nop - completed
    msg.num_of_completed_sub_ops = completed
    msg.num_of_failed_sub_ops = failed
    msg.num_of_warning_sub_ops = warning
