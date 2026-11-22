"""Verify connections without history still deliver inventory and live events."""

from unittest.mock import AsyncMock, Mock, patch

from lorawan_connection import EventType, UplinkEvent
from lorawan_connection.backend._tts_api import message
from lorawan_connection.backend.tts import TTSConnection
from lorawan_connection.mock import MockConnection

from .conftest import DESCRIPTOR, NOW


async def test_mock_does_not_replay_activity():
    connection = MockConnection([DESCRIPTOR])
    activity = UplinkEvent(descriptor=DESCRIPTOR, received_at=NOW, data=b"live")
    connection.emit(activity)
    events = []
    unsubscribe = await connection.async_subscribe(listener=events.append)
    assert [event.type for event in events] == [EventType.ADDED]
    connection.emit(activity)
    assert events[-1] is activity
    unsubscribe()
    connection.emit(activity)
    assert len(events) == 2


async def test_tts_does_not_replay_activity():
    channel = Mock(close=AsyncMock())
    with patch("grpc.aio.insecure_channel", return_value=channel):
        connection = TTSConnection(
            "http://localhost:1",
            "not-a-credential",
            application_ids=["app"],
            network_id="network",
        )
    ids = {
        "application_ids": {"application_id": "app"},
        "device_id": "sensor",
        "dev_eui": bytes.fromhex(DESCRIPTOR.dev_eui),
    }
    connection._registry.List = AsyncMock(
        return_value=message("EndDevices", end_devices=[{"ids": ids}])
    )
    try:
        await connection.refresh()
        connection.available = True
        activity = message(
            "ApplicationUp",
            end_device_ids=ids,
            uplink_message={"frm_payload": b"live", "f_port": 1},
        )
        await connection.handle_message(activity)
        events = []
        unsubscribe = await connection.async_subscribe(listener=events.append)
        assert [event.type for event in events] == [EventType.ADDED]
        await connection.handle_message(activity)
        assert events[-1].type == EventType.UPLINK
        assert events[-1].data == b"live"
        unsubscribe()
        await connection.handle_message(activity)
        assert len(events) == 2
    finally:
        await connection.close()
