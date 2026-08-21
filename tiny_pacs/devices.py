import dataclasses
from typing import Any

import pydantic
import trolleybus
from pynetdicom2 import asceprovider

from . import component, events, questions


class DeviceConfig(pydantic.BaseModel):
    """Configuration of a remote DICOM device.

    The fields are passed through to the DICOM association requestor, so
    additional association parameters accepted by
    :class:`pynetdicom2.asceprovider.RemoteAEConfig` are allowed as well.

    :ivar aet: remote AE title
    :ivar address: remote IP address or host name
    :ivar port: remote SCP TCP port
    :ivar username: DICOM user identity negotiation username
    :ivar password: DICOM user identity negotiation password
    """

    model_config = pydantic.ConfigDict(extra='allow')

    aet: str
    address: str
    port: int
    username: str | None = None
    password: str | None = None

    def to_remote_ae(self) -> asceprovider.RemoteAEConfig:
        """Builds a ``pynetdicom2`` remote AE configuration from this device.

        Only fields accepted by
        :class:`pynetdicom2.asceprovider.RemoteAEConfig` are forwarded, so
        extra device keys never crash association setup.

        :return: remote AE configuration
        :rtype: asceprovider.RemoteAEConfig
        """
        accepted = {
            field.name
            for field in dataclasses.fields(asceprovider.RemoteAEConfig)
        } - {'user_data'}
        kwargs = {
            key: value
            for key, value in self.model_dump(exclude_none=True).items()
            if key in accepted
        }
        return asceprovider.RemoteAEConfig(**kwargs)


class DevicesConfig(component.ComponentConfig):
    """Configuration of the :class:`Devices` component.

    :ivar devices: pre-configured devices by AE title
    :ivar auto_add: register calling AE titles automatically
    :ivar default_port: port used for auto-added devices
    """

    devices: dict[str, DeviceConfig] = pydantic.Field(default_factory=dict)
    auto_add: bool = True
    default_port: int = pydantic.Field(default=11112, ge=0, le=65535)


class Devices(component.Component[DevicesConfig]):
    config_model = DevicesConfig

    def __init__(self, bus: trolleybus.EventBus,
                 config: DevicesConfig | dict[str, Any]):
        super().__init__(bus, config)
        self.devices: dict[str, DeviceConfig] = self.config.devices
        self.auto_add: bool = self.config.auto_add
        self.default_port: int = self.config.default_port
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

    def device_by_ae(self, _ae: str) -> DeviceConfig | None:
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
        self.devices[calling_ae_title] = DeviceConfig(
            aet=calling_ae_title,
            address=remote_addr,
            port=self.default_port
        )


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
