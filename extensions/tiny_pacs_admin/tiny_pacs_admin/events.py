"""Device management events.

The events are handled by the
:class:`~tiny_pacs_admin.store.DeviceStore` component; payloads and
results carry the per-device :class:`~tiny_pacs_admin.models.IdentityPolicy`
like every other device field.
"""
from typing import Any

import trolleybus

from . import models


class DeviceList(trolleybus.Event[None, list[models.DeviceModel]]):
    """Request every registered device, ordered by AE title."""


class DeviceAdd(trolleybus.Event[dict[str, Any], models.DeviceModel]):
    """Register a new device.

    Payload is a device field mapping that must contain at least ``aet``
    and ``address``; the port and identity policy default to the
    component's configured defaults when omitted.
    """


class DeviceUpdate(trolleybus.Event[dict[str, Any], models.DeviceModel]):
    """Update an existing device.

    Payload is a device field mapping that must contain ``aet``; only the
    provided fields are changed.
    """


class DeviceRemove(trolleybus.Event[str, bool]):
    """Remove a device by AE title.

    The result is whether the device existed (and was removed).
    """
