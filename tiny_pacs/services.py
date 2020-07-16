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
def qr_move_scp(asce, ctx, msg):
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
        _send_response(asce, ctx, msg, 0, 0, 0, 0)

    contexts = {(sop_class, ts) for sop_class, ts, _ in gen}
    datasets = (d for _, _, d in gen)

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
        for data_set in datasets:
            # request an association with destination send C-STORE
            service = assoc.get_scu(data_set.SOPClassUID)
            status = service(data_set, completed)
            if status.is_failure:
                failed += 1
            elif status.is_warning:
                warning += 1
            else:
                success +=1
            rsp.status = int(statuses.C_MOVE_PENDING)
            rsp.num_of_remaining_sub_ops = nop - completed
            rsp.num_of_completed_sub_ops = success
            rsp.num_of_failed_sub_ops = failed
            rsp.num_of_warning_sub_ops = warning
            completed += 1

            # send response
            asce.send(rsp, ctx.id)
        _send_response(asce, ctx, msg, nop, failed, warning, success)


def _send_response(asce, ctx, msg, nop, failed, warning, completed):
    rsp = dimsemessages.CMoveRSPMessage()
    rsp.message_id_being_responded_to = msg.message_id
    rsp.sop_class_uid = msg.sop_class_uid
    rsp.num_of_remaining_sub_ops = nop - completed
    rsp.num_of_completed_sub_ops = completed
    rsp.num_of_failed_sub_ops = failed
    rsp.num_of_warning_sub_ops = warning
    rsp.status = int(statuses.SUCCESS)
    asce.send(rsp, ctx.id)