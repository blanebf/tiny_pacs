from typing import Any

import trolleybus

from . import component, events, questions


class Devices(component.Component):
    def __init__(self, bus: trolleybus.EventBus, config: dict):
        super().__init__(bus, config)
        self.devices: dict[str, dict[str, Any]] = config.get('devices', {})
        self.auto_add: bool = config.get('auto_add', True)
        self.default_port: int = config.get('default_port', 11112)
        if self.auto_add:
            self.subscribe(events.Assoc, self.add_device_from_asce)
        self.subscribe(events.DeviceByAE, self.device_by_ae)

    @classmethod
    def interactive(cls) -> questions.Questionnaire:
        def add_device(value: str) -> dict[str, Any]:
            aet, address, port = value.split()
            return {'aet': aet, 'address': address, 'port': int(port)}

        return questions.Questionnaire([
            questions.Question(
                'auto_add', 'Auto-add new devices on incoming connections?',
                lambda v: v.lower() == 'y', default='Y'
            ),
            questions.Question(
                'default_port', 'Enter default for new devices',
                int, default='11112'
            ),
            DeviceQuestion(
                'devices', 'Add pre-configurated device '
                '(AET, address, port, separated by spaces)',
                add_device, True, default_repr='[]'
            )
        ])

    def device_by_ae(self, _ae: str) -> dict[str, Any] | None:
        return self.devices.get(_ae)

    def add_device_from_asce(self, payload: events.AssocPayload) -> None:
        # TODO add C-ECHO, to check availability
        asce = payload.asce
        assoc = payload.assoc
        # The association no longer exposes the peer address directly; obtain
        # it from the DUL provider socket instead.
        dul_socket = asce.dul.dul_socket
        remote_addr = dul_socket.getpeername()[0] if dul_socket else ''
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
    def value(self) -> dict[str, Any]:
        if not self._value:
            return {}
        devices = (self.handler(v) for v in self._value)
        return {d['aet']: d for d in devices}

    @value.setter
    def value(self, _value: Any) -> None:
        self._set_value(_value)
