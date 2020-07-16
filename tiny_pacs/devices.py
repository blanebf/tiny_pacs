# -*- coding: utf-8 -*-
import enum

from pynetdicom2 import asceprovider
from pynetdicom2 import pdu

from . import ae
from . import component
from . import event_bus


class DevicesChannels(enum.Enum):
    DEVICE_BY_AE = 'device-by-ae'


class Devices(component.Component):
    def __init__(self, bus: event_bus.EventBus, config: dict):
        super().__init__(bus, config)
        self.devices = config.get('devices', {})
        self.auto_add = config.get('auto_add', True)
        self.default_port = config.get('default_port', 11112)
        if self.auto_add:
            self.subscribe(ae.AEChannels.ASSOC, self.add_device_from_asce)
        self.subscribe(DevicesChannels.DEVICE_BY_AE, self.device_by_ae)

    def device_by_ae(self, ae: str):
        return self.devices.get(ae)

    def add_device_from_asce(self, asce: asceprovider.AssociationAcceptor,
                             assoc: pdu.AAssociateRqPDU):
        # TODO add C-ECHO, to check availability
        remote_addr, _ = asce.client_address
        calling_ae_title = assoc.calling_ae_title.strip()
        self.log_info(
            'Adding new device %s/%s/%d',
            calling_ae_title, remote_addr, self.default_port
        )
        self.devices[calling_ae_title] = {
            'aet': calling_ae_title,
            'address': remote_addr,
            'port': self.default_port
        }
