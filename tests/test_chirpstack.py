"""Translate generated ChirpStack messages into flat immutable events."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from chirpstack_api import api, common, integration
from google.protobuf.json_format import MessageToJson

from lorawan_connection import EventType, GatewayReception, StatusEvent, UplinkEvent
from lorawan_connection.backend.chirpstack import ChirpStackConnection

from .conftest import DESCRIPTOR


def test_authorization_metadata_uses_api_key() -> None:
    connection = ChirpStackConnection(
        "http://localhost:1",
        "test-key",
        application_ids=["app"],
        network_id="network",
        channel=Mock(close=AsyncMock()),
    )
    assert connection.metadata == (("authorization", "Bearer test-key"),)


@pytest.mark.parametrize(
    "kind,message,fields",
    [
        (
            "up",
            integration.UplinkEvent(data=b"\x00\xff", f_port=42),
            {"data": b"\x00\xff", "f_port": 42, "receptions": ()},
        ),
        (
            "up",
            integration.UplinkEvent(
                data=b"payload",
                rx_info=[
                    {"gateway_id": "nearby", "rssi": -45, "snr": -1.5},
                    {"gateway_id": "rooftop", "rssi": -85, "snr": 4.0},
                ],
            ),
            {
                "receptions": (
                    GatewayReception("nearby", -45, -1.5),
                    GatewayReception("rooftop", -85, 4.0),
                )
            },
        ),
        ("join", integration.JoinEvent(dev_addr="abcdef01"), {"dev_addr": "abcdef01"}),
        (
            "status",
            integration.StatusEvent(margin=-2, battery_level=0),
            {
                "margin": -2,
                "battery_level": 0,
                "battery_level_unavailable": False,
                "external_power_source": False,
            },
        ),
        (
            "ack",
            integration.AckEvent(queue_item_id="queue", acknowledged=True),
            {"queue_item_id": "queue", "acknowledged": True},
        ),
        (
            "txack",
            integration.TxAckEvent(gateway_id="gateway", downlink_id=123),
            {"gateway_id": "gateway", "downlink_id": 123},
        ),
        (
            "log",
            integration.LogEvent(description="diagnostic", level=1, code=2),
            {"description": "diagnostic", "level": 1, "code": 2},
        ),
        (
            "location",
            integration.LocationEvent(
                location=common.Location(latitude=52.5, longitude=4.5, altitude=2)
            ),
            {"latitude": 52.5, "longitude": 4.5, "altitude": 2},
        ),
    ],
)
async def test_event_conversion(kind, message, fields) -> None:
    message.device_info.dev_eui = DESCRIPTOR.dev_eui

    async def stream(*args, **kwargs) -> AsyncIterator[api.LogItem]:
        yield api.LogItem(
            id=f"{int(datetime.now(UTC).timestamp() * 1000)}-1",
            description=kind,
            body=MessageToJson(message),
        )

    connection = ChirpStackConnection(
        "http://localhost:1",
        "key",
        application_ids=["app"],
        network_id="network",
        channel=Mock(close=AsyncMock()),
    )
    connection._internal_api.StreamDeviceEvents = stream
    events = [event async for event in connection._read_stream(DESCRIPTOR.dev_eui)]
    assert len(events) == 1
    event = events[0]
    assert event.type is EventType(kind)
    assert event.dev_eui == DESCRIPTOR.dev_eui
    assert not event.is_initial
    for name, value in fields.items():
        assert getattr(event, name) == value
    message.Clear()
    for name, value in fields.items():
        assert getattr(event, name) == value
    await connection.close()


async def test_inventory_observations_are_used_without_event_history() -> None:
    """Inventory metadata supplies the last uplink and status as a fallback."""
    connection = ChirpStackConnection(
        "http://localhost:1",
        "key",
        application_ids=["app"],
        network_id="network",
        channel=Mock(close=AsyncMock()),
    )
    connection.devices = {DESCRIPTOR.dev_eui: DESCRIPTOR}
    connection.available = True
    received_at = datetime(2026, 11, 21, 18, 59, 54, tzinfo=UTC)
    connection._inventory_latest = {
        DESCRIPTOR.dev_eui: (
            UplinkEvent(
                descriptor=DESCRIPTOR,
                received_at=received_at,
                data=b"",
                is_initial=True,
            ),
            StatusEvent(
                descriptor=DESCRIPTOR,
                received_at=received_at,
                is_initial=True,
                margin=8,
                battery_level_unavailable=False,
                battery_level=100,
            ),
        )
    }

    async def stream(*args, **kwargs):
        if False:
            yield

    connection._internal_api.StreamDeviceEvents = stream
    events = []
    await connection.async_subscribe(listener=events.append)

    assert [event.type for event in events] == [
        EventType.ADDED,
        EventType.UPLINK,
        EventType.STATUS,
    ]
    assert events[1].data == b""
    assert events[1].received_at == received_at
    assert events[2].margin == 8
    assert all(event.is_initial for event in events[1:])
    await connection.close()


async def test_initial_latest_observations_are_added_safe() -> None:
    """Initial Redis observations contain metadata and never retain payload bytes."""
    connection = ChirpStackConnection(
        "http://localhost:1",
        "key",
        application_ids=["app"],
        network_id="network",
        channel=Mock(close=AsyncMock()),
    )
    connection.devices = {DESCRIPTOR.dev_eui: DESCRIPTOR}
    connection.available = True

    up_old = integration.UplinkEvent(
        data=b"secret-old",
        f_port=42,
        rx_info=[{"gateway_id": "gateway-old", "rssi": -70, "snr": 5}],
    )
    up_new = integration.UplinkEvent(
        data=b"secret-new",
        f_port=43,
        rx_info=[
            {"gateway_id": "gateway-a", "rssi": -45, "snr": -1.5},
            {"gateway_id": "gateway-b", "rssi": -85, "snr": 4.0},
        ],
    )
    up_old.device_info.dev_eui = DESCRIPTOR.dev_eui
    up_new.device_info.dev_eui = DESCRIPTOR.dev_eui
    join = integration.JoinEvent(dev_addr="abcdef01")
    join.device_info.dev_eui = DESCRIPTOR.dev_eui
    status = integration.StatusEvent(
        margin=-2,
        external_power_source=True,
        battery_level_unavailable=False,
        battery_level=73,
    )
    status.device_info.dev_eui = DESCRIPTOR.dev_eui

    async def stream(*args, **kwargs):
        yield api.LogItem(
            id="1000000-0",
            description="up",
            body=MessageToJson(up_old),
        )
        yield api.LogItem(
            id="3000000-0",
            description="up",
            body=MessageToJson(up_new),
        )
        yield api.LogItem(
            id="2000000-0",
            description="join",
            body=MessageToJson(join),
        )
        yield api.LogItem(
            id="4000000-0",
            description="status",
            body=MessageToJson(status),
        )
        yield api.LogItem(id="broken", description="up", body="{}")

    connection._internal_api.StreamDeviceEvents = stream
    events = []
    await connection.async_subscribe(
        listener=events.append,
    )

    assert [event.type for event in events] == [
        EventType.ADDED,
        EventType.UPLINK,
        EventType.JOIN,
        EventType.STATUS,
    ]
    uplink = events[1]
    assert uplink.data == b""
    assert uplink.received_at == datetime.fromtimestamp(3000, UTC)
    assert uplink.receptions == (
        GatewayReception("gateway-a", -45, -1.5),
        GatewayReception("gateway-b", -85, 4.0),
    )
    assert events[2].received_at == datetime.fromtimestamp(2000, UTC)
    assert events[3].margin == -2
    assert events[3].external_power_source is True
    assert events[3].battery_level_unavailable is False
    assert events[3].battery_level == 73
    assert all(event.is_initial for event in events[1:])
    await connection.close()
