"""Regression tests for ChirpStack history subscriptions."""

import asyncio
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import grpc
import pytest
from chirpstack_api import api, integration
from google.protobuf.json_format import MessageToJson

from lorawan_connection import Device, DeviceCollection, DeviceEvent, EventType
from lorawan_connection.backend.chirpstack import ChirpStackConnection

EUI = "0102030405060708"
OTHER_EUI = "0102030405060709"
OLD = datetime(2020, 1, 1, tzinfo=UTC)


def uplink(
    eui: str, when: datetime, port: int, data: bytes = b"payload"
) -> api.LogItem:
    message = integration.UplinkEvent(data=data, f_port=port)
    message.device_info.dev_eui = eui
    return api.LogItem(
        id=f"{int(when.timestamp() * 1000)}-0",
        description="up",
        body=MessageToJson(message),
    )


def status(eui: str, when: datetime, margin: int) -> api.LogItem:
    message = integration.StatusEvent(margin=margin, battery_level=42)
    message.device_info.dev_eui = eui
    return api.LogItem(
        id=f"{int(when.timestamp() * 1000)}-0",
        description="status",
        body=MessageToJson(message),
    )


class History:
    def __init__(
        self, *items: api.LogItem, blocked: bool = False, grpc_call: bool = False
    ) -> None:
        self.items = items
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = asyncio.Event()
        self.call = MagicMock(spec=grpc.aio.UnaryStreamCall) if grpc_call else None
        if self.call is not None:
            self.call.__aiter__.side_effect = self.stream
        if not blocked:
            self.release.set()

    def open(self):
        return self.call if self.call is not None else self.stream()

    async def stream(self):
        try:
            self.started.set()
            for item in self.items:
                yield item
            await self.release.wait()
        finally:
            self.closed.set()


class BatchedHistory(History):
    def __init__(self, batches: list[list[api.LogItem]], delay: float) -> None:
        super().__init__()
        self.batches = batches
        self.delay = delay

    async def stream(self):
        try:
            self.started.set()
            for index, batch in enumerate(self.batches):
                if index:
                    await asyncio.sleep(self.delay)
                for item in batch:
                    yield item
        finally:
            self.closed.set()


class Streams:
    def __init__(self) -> None:
        self.live = defaultdict(asyncio.Queue)
        self.live_started = defaultdict(asyncio.Event)
        self.histories = defaultdict(deque)
        self.calls = defaultdict(int)

    def __call__(self, request, **kwargs):
        eui = request.dev_eui
        self.calls[eui] += 1
        if self.calls[eui] != 1:
            history = (
                self.histories[eui].popleft() if self.histories[eui] else History()
            )
            return history.open()
        return self.live_stream(eui)

    async def live_stream(self, eui):
        self.live_started[eui].set()
        while True:
            yield await self.live[eui].get()


@asynccontextmanager
async def connected(*, inventories=None, battery_level=55):
    """Start the real connection with mocked inventory and local event streams."""
    inventories = inventories or {EUI: None}
    streams = Streams()
    connection = ChirpStackConnection(
        "http://localhost:1",
        "not-a-credential",
        application_ids=["app"],
        network_id="network",
        channel=Mock(close=AsyncMock()),
    )
    connection.applications = AsyncMock(return_value={"app": "App"})
    items = []
    for eui, seen in inventories.items():
        item = api.DeviceListItem(dev_eui=eui, name=eui, device_profile_id="profile")
        if seen is not None:
            item.last_seen_at.FromDatetime(seen)
            item.device_status.CopyFrom(
                api.DeviceStatus(margin=3, battery_level=battery_level)
            )
        items.append(item)
    connection._device_api.List = AsyncMock(
        return_value=api.ListDevicesResponse(total_count=len(items), result=items)
    )
    connection._profile_api.Get = AsyncMock(
        return_value=api.GetDeviceProfileResponse(device_profile=api.DeviceProfile())
    )
    connection._internal_api.StreamDeviceEvents = streams
    try:
        await connection.async_connect()
        async with asyncio.timeout(2):
            for eui in inventories:
                await streams.live_started[eui].wait()
        yield connection, streams
    finally:
        await connection.close()


async def wait_for_count(events: list[DeviceEvent], count: int) -> None:
    async with asyncio.timeout(2):
        while len(events) < count:
            await asyncio.sleep(0)


async def test_history_is_automatic_and_isolated_per_subscriber():
    async with connected() as (connection, streams):
        first, second = [], []
        first_history = History(uplink(EUI, OLD, 3))
        history = History(uplink(EUI, OLD, 7), status(EUI, OLD, 8))
        streams.histories[EUI].extend((first_history, history))
        unsubscribe_first = await connection.async_subscribe(listener=first.append)
        unsubscribe_second = await connection.async_subscribe(listener=second.append)
        assert [event.type for event in first] == [EventType.ADDED, EventType.UPLINK]
        assert first[-1].f_port == 3
        assert [event.type for event in second] == [
            EventType.ADDED,
            EventType.UPLINK,
            EventType.STATUS,
        ]
        assert second[1].data == b""
        assert second[1].f_port == 7
        assert second[2].margin == 8
        assert all(event.is_initial for event in (first[1], second[1], second[2]))
        assert first_history.closed.is_set()
        assert history.closed.is_set()
        unsubscribe_first()
        unsubscribe_second()


async def test_device_collection_does_not_forward_initial_observations():
    async with connected() as (connection, streams):
        streams.histories[EUI].extend(
            (
                History(uplink(EUI, OLD, 7), status(EUI, OLD, 8)),
                History(uplink(EUI, OLD, 7), status(EUI, OLD, 8)),
            )
        )
        with patch.object(Device, "handle_event", autospec=True) as handle_event:
            collection = DeviceCollection(connection)
            await collection.async_setup()
            assert [call.args[1].type for call in handle_event.call_args_list] == [
                EventType.ADDED
            ]
            assert collection.devices[EUI].latest_status is None

            events = []
            unsubscribe = await connection.async_subscribe(listener=events.append)
            assert [event.type for event in events] == [
                EventType.ADDED,
                EventType.UPLINK,
                EventType.STATUS,
            ]
            assert all(event.is_initial for event in events[1:])
            await streams.live[EUI].put(uplink(EUI, datetime.now(UTC), 9, b"live"))
            await wait_for_count(events, 4)
            assert handle_event.call_args.args[1].data == b"live"
            assert not handle_event.call_args.args[1].is_initial
            unsubscribe()
            collection.close()


async def test_live_during_history_and_late_subscriber_receives_own_history():
    async with connected() as (connection, streams):
        plain, first, late = [], [], []
        unsubscribe_plain = await connection.async_subscribe(listener=plain.append)
        history = History(uplink(EUI, OLD, 1), status(EUI, OLD, 4), blocked=True)
        streams.histories[EUI].append(history)
        task = asyncio.create_task(connection.async_subscribe(listener=first.append))
        try:
            await asyncio.wait_for(history.started.wait(), 2)
            await streams.live[EUI].put(uplink(EUI, datetime.now(UTC), 9, b"live"))
            await wait_for_count(first, 2)
            assert not task.done()
            assert first[1].data == b"live"
            assert plain[1].data == b"live"
            history.release.set()
            unsubscribe_first = await asyncio.wait_for(task, 2)
            assert [event.type for event in first] == [
                EventType.ADDED,
                EventType.UPLINK,
                EventType.UPLINK,
                EventType.STATUS,
            ]
            assert first[2].is_initial
            assert first[2].f_port == 1
            assert first[-1].margin == 4
            streams.histories[EUI].append(History(uplink(EUI, OLD, 1)))
            unsubscribe_late = await connection.async_subscribe(listener=late.append)
            assert [event.type for event in late] == [EventType.ADDED, EventType.UPLINK]
            assert late[-1].f_port == 1
            assert late[-1].data == b""
            assert len(plain) == 2
            assert len(first) == 4
            unsubscribe_first()
            unsubscribe_late()
            unsubscribe_plain()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_cancelled_subscription_closes_history_and_leaves_other_listener_live():
    async with connected() as (connection, streams):
        plain, cancelled = [], []
        unsubscribe_plain = await connection.async_subscribe(listener=plain.append)
        history = History(uplink(EUI, OLD, 1), blocked=True, grpc_call=True)
        streams.histories[EUI].append(history)
        task = asyncio.create_task(
            connection.async_subscribe(listener=cancelled.append)
        )
        try:
            await asyncio.wait_for(history.started.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert history.closed.is_set()
            history.call.cancel.assert_called_once_with()
            await streams.live[EUI].put(uplink(EUI, datetime.now(UTC), 9, b"live"))
            await wait_for_count(plain, 2)
            assert plain[-1].data == b"live"
            assert [event.type for event in cancelled] == [EventType.ADDED]
            unsubscribe_plain()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_multiple_history_batches_keep_latest_observation_per_type(monkeypatch):
    monkeypatch.setattr(
        "lorawan_connection.backend.chirpstack._HISTORY_IDLE_TIMEOUT", 0.3
    )
    async with connected() as (connection, streams):
        history = BatchedHistory(
            [
                [uplink(EUI, OLD, 1), status(EUI, OLD, 3)],
                [uplink(EUI, OLD + timedelta(seconds=2), 7)],
                [
                    status(EUI, OLD + timedelta(seconds=3), 8),
                    uplink(EUI, OLD + timedelta(seconds=1), 2),
                ],
            ],
            delay=0.01,
        )
        streams.histories[EUI].append(history)
        events = []
        unsubscribe = await connection.async_subscribe(listener=events.append)
        assert history.closed.is_set()
        assert [event.type for event in events] == [
            EventType.ADDED,
            EventType.UPLINK,
            EventType.STATUS,
        ]
        assert events[1].f_port == 7
        assert events[1].received_at == OLD + timedelta(seconds=2)
        assert events[1].data == b""
        assert events[2].margin == 8
        assert events[2].received_at == OLD + timedelta(seconds=3)
        unsubscribe()


async def test_history_does_not_replay_events_at_or_after_subscription_cutoff():
    async with connected() as (connection, streams):
        now = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
        cutoff = now.replace(microsecond=123000)
        streams.histories[EUI].append(
            History(
                uplink(EUI, cutoff - timedelta(milliseconds=1), 1),
                uplink(EUI, cutoff, 2),
                uplink(EUI, cutoff + timedelta(milliseconds=1), 3),
                status(EUI, cutoff, 8),
            )
        )
        events = []
        with patch(
            "lorawan_connection.backend.chirpstack.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = now
            unsubscribe = await connection.async_subscribe(listener=events.append)
        assert [event.type for event in events] == [EventType.ADDED, EventType.UPLINK]
        assert events[-1].received_at == cutoff - timedelta(milliseconds=1)
        assert events[-1].f_port == 1
        unsubscribe()


async def test_history_reads_for_multiple_devices_run_concurrently():
    async with connected(inventories={EUI: None, OTHER_EUI: None}) as (
        connection,
        streams,
    ):
        first = History(uplink(EUI, OLD, 1), blocked=True)
        second = History(uplink(OTHER_EUI, OLD, 2), blocked=True)
        streams.histories[EUI].append(first)
        streams.histories[OTHER_EUI].append(second)
        events = []
        task = asyncio.create_task(connection.async_subscribe(listener=events.append))
        try:
            async with asyncio.timeout(2):
                await first.started.wait()
                await second.started.wait()
            assert not task.done()
            first.release.set()
            second.release.set()
            unsubscribe = await asyncio.wait_for(task, 2)
            assert {
                (event.dev_eui, event.f_port)
                for event in events
                if event.type == EventType.UPLINK
            } == {(EUI, 1), (OTHER_EUI, 2)}
            unsubscribe()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    ("battery", "unavailable", "value"), [(-1, True, 0), (55, False, 55)]
)
async def test_snapshot_battery_sentinel_is_preserved_in_initial_status(
    battery, unavailable, value
):
    async with connected(inventories={EUI: OLD}, battery_level=battery) as (
        connection,
        streams,
    ):
        streams.histories[EUI].append(History())
        events = []
        unsubscribe = await connection.async_subscribe(listener=events.append)
        assert [event.type for event in events] == [
            EventType.ADDED,
            EventType.UPLINK,
            EventType.STATUS,
        ]
        assert events[-1].received_at == OLD
        assert events[-1].is_initial
        assert events[-1].battery_level_unavailable is unavailable
        assert events[-1].battery_level == value
        unsubscribe()


async def test_initial_observations_do_not_suppress_live_payloads():
    async with connected() as (connection, streams):
        plain, opted = [], []
        unsubscribe_plain = await connection.async_subscribe(listener=plain.append)
        recorded_at = datetime.now(UTC) - timedelta(seconds=1)
        streams.histories[EUI].append(History(uplink(EUI, recorded_at, 6)))
        unsubscribe_opted = await connection.async_subscribe(listener=opted.append)
        await streams.live[EUI].put(uplink(EUI, recorded_at, 6, b"duplicate"))
        await wait_for_count(plain, 2)
        await wait_for_count(opted, 3)
        assert plain[-1].data == opted[-1].data == b"duplicate"
        await streams.live[EUI].put(uplink(EUI, datetime.now(UTC), 9, b"new"))
        await wait_for_count(plain, 3)
        await wait_for_count(opted, 4)
        assert plain[-1].data == opted[-1].data == b"new"
        unsubscribe_plain()
        unsubscribe_opted()


async def test_idle_timeout_keeps_partial_history_and_live_subscription(monkeypatch):
    monkeypatch.setattr(
        "lorawan_connection.backend.chirpstack._HISTORY_IDLE_TIMEOUT", 0.05
    )
    async with connected() as (connection, streams):
        history = History(uplink(EUI, OLD, 6), blocked=True)
        streams.histories[EUI].append(history)
        events = []
        unsubscribe = await asyncio.wait_for(
            connection.async_subscribe(listener=events.append),
            2,
        )
        assert history.closed.is_set()
        assert [event.type for event in events] == [EventType.ADDED, EventType.UPLINK]
        assert events[-1].f_port == 6
        assert events[-1].data == b""
        await streams.live[EUI].put(uplink(EUI, datetime.now(UTC), 9, b"live"))
        await wait_for_count(events, 3)
        assert events[-1].data == b"live"
        unsubscribe()


async def test_overall_timeout_preserves_completed_device_observations(monkeypatch):
    monkeypatch.setattr("lorawan_connection.backend.chirpstack._HISTORY_TIMEOUT", 0.1)
    monkeypatch.setattr(
        "lorawan_connection.backend.chirpstack._HISTORY_IDLE_TIMEOUT", 1
    )
    async with connected(inventories={EUI: None, OTHER_EUI: OLD}) as (
        connection,
        streams,
    ):
        complete = History(uplink(EUI, OLD, 6))
        stalled = History(blocked=True)
        streams.histories[EUI].append(complete)
        streams.histories[OTHER_EUI].append(stalled)
        events = []
        unsubscribe = await asyncio.wait_for(
            connection.async_subscribe(listener=events.append),
            2,
        )
        assert complete.closed.is_set() and stalled.closed.is_set()
        activity = [event for event in events if event.type != EventType.ADDED]
        assert [(event.dev_eui, event.type) for event in activity] == [
            (EUI, EventType.UPLINK),
            (OTHER_EUI, EventType.UPLINK),
            (OTHER_EUI, EventType.STATUS),
        ]
        assert activity[0].f_port == 6
        await streams.live[OTHER_EUI].put(
            uplink(OTHER_EUI, datetime.now(UTC), 9, b"live")
        )
        await wait_for_count(events, 6)
        assert events[-1].data == b"live"
        unsubscribe()


@pytest.mark.parametrize("offset", [-1, 0, 1])
async def test_inventory_uplink_and_retained_status_after_inventory_replay(offset):
    async with connected(inventories={EUI: OLD}) as (connection, streams):
        streams.histories[EUI].append(
            History(
                uplink(EUI, OLD + timedelta(seconds=offset), 7),
                status(EUI, OLD + timedelta(seconds=offset), 8),
            )
        )
        events = []
        unsubscribe = await connection.async_subscribe(listener=events.append)
        assert [event.type for event in events] == [
            EventType.ADDED,
            EventType.UPLINK,
            EventType.STATUS,
        ]
        assert events[1].received_at == OLD + timedelta(seconds=max(offset, 0))
        assert events[1].f_port == (1 if offset < 0 else 7)
        assert events[2].received_at == OLD + timedelta(seconds=offset)
        assert events[2].margin == 8
        unsubscribe()


async def test_history_keeps_later_redis_sequence_in_same_millisecond():
    async with connected() as (connection, streams):
        later_uplink = uplink(EUI, OLD, 7)
        later_status = status(EUI, OLD, 8)
        later_uplink.id = later_uplink.id.removesuffix("-0") + "-1"
        later_status.id = later_status.id.removesuffix("-0") + "-2"
        streams.histories[EUI].append(
            History(
                uplink(EUI, OLD, 1), status(EUI, OLD, 3), later_uplink, later_status
            )
        )
        events = []
        unsubscribe = await connection.async_subscribe(listener=events.append)
        assert [event.type for event in events] == [
            EventType.ADDED,
            EventType.UPLINK,
            EventType.STATUS,
        ]
        assert events[1].received_at == events[2].received_at == OLD
        assert events[1].f_port == 7
        assert events[2].margin == 8
        unsubscribe()
