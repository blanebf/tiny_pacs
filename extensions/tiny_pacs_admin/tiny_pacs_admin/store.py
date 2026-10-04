"""Database-backed device registry component.

Keeps remote DICOM devices in the database and works in tandem with the
built-in :class:`tiny_pacs.devices.Devices` component:

* YAML-configured devices are imported into the database at startup
  (upsert by AE title), so the configuration stays the source of truth
  for configured devices;
* :class:`~tiny_pacs.events.DeviceByAE` is answered from the database at
  a higher priority than ``Devices``: a DB hit wins, on a DB miss the
  in-memory registry still answers;
* auto-added devices from incoming associations are persisted with the
  configurable defaults from :class:`DeviceStoreConfig`. Disable
  ``Devices.auto_add`` when using this component so new devices are only
  bookkept in one place.

Devices carry a per-device identity policy consumed by authentication
extensions; the policy travels on the existing ``DeviceByAE`` path as an
extra :class:`tiny_pacs.devices.DeviceConfig` field, invisible to the
association layer.
"""
import json
from collections.abc import Mapping
from typing import Any

import peewee
import pydantic
import trolleybus
from tiny_pacs import component, devices, events, schema
from tiny_pacs.identity import IdentityPolicy

from . import models
from .models import DeviceModel


class DeviceStoreConfig(component.ComponentConfig):
    """Configuration of the :class:`DeviceStore` component.

    :ivar default_identity: identity policy assigned to auto-added devices
    :ivar default_port: TCP port assigned to auto-added devices
    """

    default_identity: IdentityPolicy = IdentityPolicy.NONE
    default_port: int = pydantic.Field(default=11112, ge=0, le=65535)


class DeviceStore(component.Component[DeviceStoreConfig]):
    """Component that keeps remote device configurations in the database.

    Handles the following events:

        * :class:`~tiny_pacs.events.Assoc` (persists auto-added devices)
        * :class:`~tiny_pacs.events.DeviceByAE`
        * :class:`~tiny_pacs.events.Migrations`
        * :class:`~tiny_pacs.events.DeviceList`
        * :class:`~tiny_pacs.events.DeviceAdd`
        * :class:`~tiny_pacs.events.DeviceUpdate`
        * :class:`~tiny_pacs.events.DeviceRemove`
        * :class:`~tiny_pacs.events.AutoAddIdentity`

    Device mutations are broadcast as :class:`~tiny_pacs.events.AuditRecord`
    so an installed audit component records them.
    """

    config_model = DeviceStoreConfig

    #: Higher priority than the default: DB hits win ``DeviceByAE``
    #: lookups against the in-memory registry and persisted auto-adds run
    #: before it sees the association.
    priority = trolleybus.DEFAULT_PRIORITY + 10

    def __init__(
            self,
            bus: trolleybus.EventBus,
            config: DeviceStoreConfig | dict[str, Any]
    ) -> None:
        """Component initialization.

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param config: component configuration
        :type config: DeviceStoreConfig or dict
        """
        super().__init__(bus, config)
        self.subscribe(events.DeviceByAE, self.device_by_ae)
        self.subscribe(events.Assoc, self.on_assoc)
        self.subscribe(events.Migrations, self.migrations)
        self.subscribe(events.DeviceList, self.device_list)
        self.subscribe(events.DeviceAdd, self.device_add)
        self.subscribe(events.DeviceUpdate, self.device_update)
        self.subscribe(events.DeviceRemove, self.device_remove)
        self.subscribe(events.AutoAddIdentity, self.auto_add_identity)

    def on_started(self) -> None:
        """Handles `OnStarted` event.

        Imports the YAML-configured devices into the database once the
        database component has finished its setup during `OnStart`.
        """
        super().on_started()
        self.sync_configured_devices()

    def migrations(self, _: None = None) -> schema.ComponentMigrations:
        """Returns schema migrations of the component tables

        :return: component migrations
        :rtype: schema.ComponentMigrations
        """
        return schema.ComponentMigrations(
            self.schema(), models.TABLES, models.MIGRATIONS
        )

    def sync_configured_devices(self) -> int:
        """Imports the YAML-configured devices into the database.

        Device registry components answer
        :class:`~tiny_pacs.events.DeviceConfigs` with their configured
        devices; every device is upserted by AE title, so the YAML
        configuration remains the source of truth for configured devices.
        The optional ``identity`` field is copied when set; when the
        configuration omits it, the DB value is left unchanged.

        :return: number of imported devices
        :rtype: int
        """
        imported = 0
        for configured in self.broadcast(events.DeviceConfigs, None):
            for device in configured.values():
                self._upsert_configured(device)
                imported += 1
        if imported:
            self.log_info(
                'Imported %d configured device(s) into the database',
                imported
            )
        return imported

    def _upsert_configured(self, device: devices.DeviceConfig) -> None:
        """Upserts one YAML-configured device by its AE title."""
        data = device.model_dump(exclude_none=True)
        aet = data.pop('aet')
        address = data.pop('address')
        port = data.pop('port')
        username = data.pop('username', None)
        password = data.pop('password', None)
        identity = data.pop('identity', None)
        extra = json.dumps(data)
        now = models._utcnow()
        with self.atomic():
            row = DeviceModel.get_or_none(DeviceModel.aet == aet)
            if row is None:
                DeviceModel.create(
                    aet=aet,
                    address=address,
                    port=port,
                    username=username,
                    password=password,
                    identity=(IdentityPolicy.NONE.value
                              if identity is None else str(identity)),
                    extra=extra,
                    created=now,
                    updated=now
                )
                return
            row.address = address
            row.port = port
            row.username = username
            row.password = password
            row.extra = extra
            if identity is not None:
                row.identity = str(identity)
            row.updated = now
            row.save()

    def device_by_ae(self, _ae: str) -> 'devices.DeviceConfig | None':
        """Handles `DeviceByAE` event

        Returns the DB device with its ``identity`` policy attached as an
        extra field, or None so lower-priority listeners (the in-memory
        ``Devices`` component) may still answer.

        :param _ae: AE title of the device
        :type _ae: str
        :return: device configuration or None for unknown AE titles
        :rtype: DeviceConfig or None
        """
        row = DeviceModel.get_or_none(DeviceModel.aet == _ae)
        if row is None:
            return None
        return self.to_device_config(row)

    @staticmethod
    def to_device_config(row: DeviceModel) -> devices.DeviceConfig:
        """Converts a device record into a device configuration.

        The identity policy rides on the configuration as an extra field;
        :meth:`tiny_pacs.devices.DeviceConfig.to_remote_ae` forwards only
        the fields known to the association layer, so the policy never
        leaks into association setup.

        :param row: device record
        :type row: DeviceModel
        :return: device configuration
        :rtype: DeviceConfig
        """
        extra = dict(json.loads(row.extra or '{}'))
        data: dict[str, Any] = {
            'aet': row.aet,
            'address': row.address,
            'port': row.port,
            'username': row.username,
            'password': row.password,
            'identity': row.identity
        }
        data.update(extra)
        return devices.DeviceConfig.model_validate(data)

    def on_assoc(self, payload: events.AssocPayload) -> None:
        """Handles `Assoc` event: persists the calling AE title.

        Unknown calling AE titles are added as new devices using the peer
        address and the configured defaults (:attr:`DeviceStoreConfig.
        default_port`, :attr:`DeviceStoreConfig.default_identity`).

        :param payload: association acceptor and request parameters
        :type payload: events.AssocPayload
        """
        aet = payload.assoc.calling_ae_title.strip()
        if not aet:
            return
        if DeviceModel.get_or_none(DeviceModel.aet == aet) is not None:
            return
        # The association no longer exposes the peer address directly;
        # obtain it from the DUL provider socket instead (mirrors the
        # built-in Devices component).
        dul_socket = payload.asce.dul.dul_socket
        remote_addr = dul_socket.getpeername()[0] if dul_socket else ''
        try:
            DeviceModel.create(
                aet=aet,
                address=remote_addr,
                port=self.config.default_port,
                identity=self.config.default_identity.value
            )
        except peewee.IntegrityError:
            # Another association from the same AE title was recorded
            # concurrently
            return
        self.log_info(
            'Auto-added device %s (%s:%d, identity: %s)',
            aet, remote_addr,
            self.config.default_port, self.config.default_identity.value
        )

    def device_list(self, _: None = None) -> list[DeviceModel]:
        """Handles `DeviceList` event

        :return: every registered device, ordered by AE title
        :rtype: list[DeviceModel]
        """
        return list(DeviceModel.select().order_by(DeviceModel.aet))

    def device_add(self, payload: dict[str, Any]) -> DeviceModel:
        """Handles `DeviceAdd` event

        :param payload: device fields; ``aet`` and ``address`` are
                        required, ``port`` and ``identity`` default to the
                        component configuration
        :type payload: dict
        :return: the created device record
        :rtype: DeviceModel
        :raises ValueError: raised when the payload is incomplete or
                            invalid, or when the device already exists
        """
        fields = self._payload_fields(payload, require=('aet', 'address'))
        fields.setdefault('port', self.config.default_port)
        fields.setdefault('identity', self.config.default_identity.value)
        fields.setdefault('extra', '{}')
        aet = fields['aet']
        now = models._utcnow()
        with self.atomic():
            if DeviceModel.get_or_none(DeviceModel.aet == aet) is not None:
                raise ValueError(f'Device {aet} already exists')
            row = DeviceModel.create(created=now, updated=now, **fields)
        self.log_info('Added device %s', aet)
        self._audit('device-add', aet, address=row.address, port=row.port,
                    identity=row.identity)
        return row

    def device_update(self, payload: dict[str, Any]) -> DeviceModel:
        """Handles `DeviceUpdate` event

        :param payload: device fields; ``aet`` selects the device, only
                        the provided fields are changed
        :type payload: dict
        :return: the updated device record
        :rtype: DeviceModel
        :raises ValueError: raised when ``aet`` is missing or unknown, or
                            when the payload is invalid
        """
        fields = self._payload_fields(payload, require=('aet',))
        if set(fields) == {'aet'}:
            raise ValueError('Nothing to update')
        aet = fields['aet']
        with self.atomic():
            row = DeviceModel.get_or_none(DeviceModel.aet == aet)
            if row is None:
                raise ValueError(f'Unknown device {aet}')
            extra = dict(json.loads(row.extra or '{}'))
            for key, value in fields.items():
                if key == 'aet':
                    continue
                if key == 'extra':
                    extra.update(json.loads(value))
                    continue
                setattr(row, key, value)
            row.extra = json.dumps(extra)
            row.updated = models._utcnow()
            row.save()
        self.log_info('Updated device %s', aet)
        # Only the changed field names are recorded, never their values:
        # the payload may carry credentials
        self._audit('device-update', aet,
                    fields=sorted(
                        key for key in fields if key not in ('aet', 'extra')
                    ))
        return row

    def device_remove(self, aet: str) -> bool:
        """Handles `DeviceRemove` event

        :param aet: AE title of the device to remove
        :type aet: str
        :return: whether the device existed and was removed
        :rtype: bool
        """
        deleted = bool(
            DeviceModel.delete().where(DeviceModel.aet == aet).execute()
        )
        if deleted:
            self.log_info('Removed device %s', aet)
            self._audit('device-remove', aet)
        return deleted

    def auto_add_identity(self, _: None = None) -> IdentityPolicy:
        """Handles `AutoAddIdentity` event

        :return: identity policy assigned to auto-added devices
        :rtype: IdentityPolicy
        """
        return self.config.default_identity

    def _payload_fields(
            self,
            payload: Mapping[str, Any],
            require: tuple[str, ...]
    ) -> dict[str, Any]:
        """Validates a device event payload.

        Known device columns are converted to their storage types; unknown
        keys are collected into the ``extra`` JSON document. Keys with
        None values are ignored.

        :param payload: raw device field mapping
        :param require: field names that must be present
        :return: normalized fields ready for model creation/update
        :rtype: dict
        :raises ValueError: raised for missing or invalid values
        """
        fields: dict[str, Any] = {}
        extra: dict[str, Any] = {}
        for key, value in payload.items():
            if value is None:
                continue
            if key == 'aet':
                aet = str(value).strip()
                if not aet:
                    raise ValueError('aet must not be empty')
                fields['aet'] = aet
            elif key in ('address', 'username', 'password'):
                fields[key] = str(value)
            elif key == 'port':
                fields['port'] = self._port(value)
            elif key == 'identity':
                fields['identity'] = IdentityPolicy(value).value
            elif key == 'extra':
                if not isinstance(value, Mapping):
                    raise ValueError('extra must be a mapping')
                extra.update(value)
            else:
                extra[key] = value
        for name in require:
            if name not in fields:
                raise ValueError(f'{name} is required')
        if extra:
            fields['extra'] = json.dumps(extra)
        return fields

    @staticmethod
    def _port(value: Any) -> int:
        """Validates a TCP port payload value."""
        try:
            port = int(value)
        except (TypeError, ValueError):
            raise ValueError(f'port must be an integer, got {value!r}') \
                from None
        if not 0 <= port <= 65535:
            raise ValueError('port must be within 0..65535')
        return port

    def atomic(self) -> Any:
        """Context manager for handling simple transactions

        :return: atomic transaction
        """
        return self.send_one(events.Atomic, None)

    def _audit(self, event: str, aet: str, **details: Any) -> None:
        """Broadcasts an audit record of a successful device mutation.

        Fire-and-forget: listener failures never affect the mutation, and
        no listener is required.

        :param event: audit event name, e.g. ``device-add``
        :type event: str
        :param aet: AE title of the mutated device
        :type aet: str
        """
        self.broadcast_nothrow(
            events.AuditRecord,
            events.AuditRecordPayload(
                category='admin', event=event, device_aet=aet,
                details=dict(details)
            )
        )
