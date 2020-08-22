# -*- coding: utf-8 -*-
import enum

from pynetdicom2 import asceprovider
from pynetdicom2 import pdu

from . import ae
from . import component
from . import event_bus
from . import questions


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

    @classmethod
    def interactive(cls):
        def add_device(value: str):
            aet, address, port = value.split()
            return {'aet': aet, 'address': address, 'port': int(port)}

        return questions.Questionnaire([
            questions.Question(
                'auto_add', 'Auto-add new devices on incoming connections?',
                lambda v: v.lower() == 'y', default='Y'
            ),
            questions.Question(
                'default_port', 'Enter default for new devices',
                lambda v: int(v), default='11112'
            ),
            DeviceQuestion(
                'devices', 'Add pre-configurated device '
                '(AET, address, port, separated by spaces)',
                add_device, True
            )
        ])

    def device_by_ae(self, ae: str):
        return self.devices.get(ae)

    def add_device_from_asce(self, asce: asceprovider.AssociationAcceptor,
                             assoc: pdu.AAssociateRqPDU):
        # TODO add C-ECHO, to check availability
        remote_addr, _ = asce.client_address
        calling_ae_title = assoc.calling_ae_title.strip()
        if calling_ae_title in self.devices:
            return
        self.log_info(
            'Adding new device %s/%s/%d',
            calling_ae_title, remote_addr, self.default_port
        )
        self.devices[calling_ae_title] = {
            'aet': calling_ae_title,
            'address': remote_addr,
            'port': self.default_port
        }


class DeviceQuestion(questions.Question):
    @property
    def value(self):
        if not self._value:
            return {}
        devices = (self.handler(v) for v in self._value)
        return {d['aet']: d for d in devices}
