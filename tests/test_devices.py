"""Tests of the Devices component."""
import trolleybus

from tiny_pacs import devices, events


def _bus_with_devices(config: dict) -> tuple[trolleybus.EventBus,
                                             devices.Devices]:
    bus = trolleybus.EventBus()
    component = devices.Devices(bus, config)
    return bus, component


def test_device_by_ae() -> None:
    bus, _ = _bus_with_devices({'devices': {
        'MY_AE': {'aet': 'MY_AE', 'address': '10.0.0.5', 'port': 104}
    }})
    device = bus.send_any(events.DeviceByAE, 'MY_AE')
    assert device is not None
    assert device.aet == 'MY_AE'
    assert bus.send_any(events.DeviceByAE, 'UNKNOWN') is None


def test_configured_devices_returns_copy() -> None:
    config = {'devices': {
        'MY_AE': {'aet': 'MY_AE', 'address': '10.0.0.5', 'port': 104}
    }}
    bus, component = _bus_with_devices(config)
    result = bus.send_one(events.DeviceConfigs, None)
    assert set(result) == {'MY_AE'}
    assert isinstance(result['MY_AE'], devices.DeviceConfig)
    # The result is a copy: mutating it does not touch the component
    result.clear()
    assert set(component.devices) == {'MY_AE'}


def test_configured_devices_empty() -> None:
    bus, _ = _bus_with_devices({})
    assert bus.send_one(events.DeviceConfigs, None) == {}
