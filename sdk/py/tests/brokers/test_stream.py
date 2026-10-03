from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import (
    NexoClient,
    NotConnectedError,
    ProtocolError,
    ProvisionOutcome,
    ResourceConfigurationConflictError,
    ResourceNotFoundError,
)
from tests.utils.wait_for import wait_for


async def _create_stream(nexo: NexoClient, name: str, **retention):
    await nexo.stream.create(name, **retention)
    return await nexo.stream.get(name)


@pytest.mark.asyncio
class TestStream:
    async def test_happy_path_publish_subscribe(self, nexo: NexoClient):
        name = f"stream-basic-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list = []
        sub = await stream.group("g1").subscribe(lambda data: received.append(data))
        assert sub.active

        await stream.publish({"id": 1})
        await stream.publish({"id": 2})

        await wait_for(lambda: len(received) == 2)
        await sub.stop()
        await sub.stop()
        await sub.wait_closed()
        assert sub.completion.done()
        assert not sub.active
        await nexo.stream.delete(name)

    async def test_subscribe_nonexistent_stream(self, nexo: NexoClient):
        name = f"stream-missing-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        await nexo.stream.delete(name)
        with pytest.raises(ResourceNotFoundError):
            await stream.group("missing-group").subscribe(lambda _: None)

    async def test_independent_consumer_groups(self, nexo: NexoClient):
        name = f"stream-groups-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        client_a = await NexoClient.connect()
        client_b = await NexoClient.connect()

        try:
            recv_a: list = []
            recv_b: list = []

            sub_a = await (await client_a.stream.get(name)).group("group_A").subscribe(lambda d: recv_a.append(d))
            sub_b = await (await client_b.stream.get(name)).group("group_B").subscribe(lambda d: recv_b.append(d))

            await stream.publish({"msg": "hello"})

            await wait_for(lambda: len(recv_a) == 1 and len(recv_b) == 1)

            await sub_a.stop()
            await sub_b.stop()
        finally:
            client_a.disconnect()
            client_b.disconnect()

    async def test_same_group_no_duplicates(self, nexo: NexoClient):
        name = f"parallel-consumers-{uuid.uuid4()}"
        group = "parallel_group"
        stream = await _create_stream(nexo, name)

        client_a = await NexoClient.connect()
        client_b = await NexoClient.connect()

        try:
            received_a: set[int] = set()
            received_b: set[int] = set()

            def cb_a(d):
                if d["id"] in received_a:
                    raise Exception(f"Duplicate in A: {d['id']}")
                received_a.add(d["id"])

            def cb_b(d):
                if d["id"] in received_b:
                    raise Exception(f"Duplicate in B: {d['id']}")
                received_b.add(d["id"])

            sub_a = await (await client_a.stream.get(name)).group(group).subscribe(cb_a)
            sub_b = await (await client_b.stream.get(name)).group(group).subscribe(cb_b)

            for i in range(100):
                await stream.publish({"id": i})

            await wait_for(lambda: len(received_a) + len(received_b) == 100, timeout=10.0)

            overlap = received_a & received_b
            assert len(overlap) == 0

            await sub_a.stop()
            await sub_b.stop()
        finally:
            client_a.disconnect()
            client_b.disconnect()

    async def test_consumer_disconnect_zero_data_loss(self, nexo: NexoClient):
        temp_a = await NexoClient.connect()
        temp_b = await NexoClient.connect()

        try:
            name = f"stream-disconnect-{uuid.uuid4()}"
            group = "group_disconnect"
            stream = await _create_stream(nexo, name)

            all_received: set[int] = set()
            track = lambda d: all_received.add(d["i"])

            sub_a = await (await temp_a.stream.get(name)).group(group).subscribe(track)
            sub_b = await (await temp_b.stream.get(name)).group(group).subscribe(track)

            for i in range(20):
                await stream.publish({"i": i})
            await wait_for(lambda: len(all_received) == 20)

            temp_a.disconnect()

            await asyncio.sleep(0.5)

            for i in range(20, 60):
                await stream.publish({"i": i})

            await wait_for(lambda: len(all_received) == 60, timeout=10.0)

            for i in range(60):
                assert i in all_received, f"Missing message index {i}"

            await sub_b.stop()
            temp_b.disconnect()
        except Exception:
            temp_a.disconnect()
            temp_b.disconnect()
            raise

    async def test_history_sync_new_group_from_beginning(self, nexo: NexoClient):
        name = f"stream-history-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        for i in range(5):
            await stream.publish({"i": i})

        received: list = []
        sub = await stream.group("history-group").subscribe(lambda d: received.append(d))

        await wait_for(lambda: len(received) == 5)
        assert received[0]["i"] == 0
        assert received[4]["i"] == 4

        await sub.stop()

    async def test_stop_subscription_quickly(self, nexo: NexoClient):
        name = f"stream-fast-stop-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        sub = await stream.group("fast-stop-group").subscribe(lambda _: None)

        start = asyncio.get_event_loop().time()
        await sub.stop()
        elapsed = asyncio.get_event_loop().time() - start

        assert elapsed < 2.0

    async def test_stop_commits_started_callback_before_leave(self, nexo: NexoClient):
        name = f"stream-stop-processing-{uuid.uuid4()}"
        group = "stop-processing-group"
        stream = await _create_stream(nexo, name)

        started = asyncio.Event()
        release = asyncio.Event()
        callback_count = 0

        async def callback(_):
            nonlocal callback_count
            callback_count += 1
            started.set()
            await release.wait()

        sub = await stream.group(group).subscribe(callback)
        await stream.publish({"id": 1})
        await asyncio.wait_for(started.wait(), timeout=2.0)

        stopping = asyncio.create_task(sub.stop())
        await asyncio.sleep(0.1)
        assert not stopping.done()

        release.set()
        await stopping

        redelivered: list = []
        resumed = await stream.group(group).subscribe(lambda data: redelivered.append(data))
        await asyncio.sleep(0.2)
        await resumed.stop()

        assert callback_count == 1
        assert redelivered == []
        await nexo.stream.delete(name)

    async def test_stop_callback_timeout_is_reported(self, nexo: NexoClient):
        name = f"stream-stop-timeout-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        started = asyncio.Event()

        async def callback(_):
            started.set()
            await asyncio.Event().wait()

        sub = await stream.group("stop-timeout-group").subscribe(
            callback,
            stop_timeout_ms=100,
        )
        await stream.publish({"id": 1})
        await asyncio.wait_for(started.wait(), timeout=2.0)

        with pytest.raises(TimeoutError, match="stop timed out"):
            await sub.stop()

        await nexo.stream.delete(name)

    async def test_stop_exposes_batch_update_failure(self, nexo: NexoClient):
        name = f"stream-stop-update-failure-{uuid.uuid4()}"
        group = "stop-update-failure-group"
        stream = await _create_stream(nexo, name)

        started = asyncio.Event()
        release = asyncio.Event()

        async def callback(_):
            started.set()
            await release.wait()

        sub = await stream.group(group).subscribe(callback)
        await stream.publish({"id": 1})
        await asyncio.wait_for(started.wait(), timeout=2.0)
        await stream.group(group).seek("beginning")

        stopping = asyncio.create_task(sub.stop())
        release.set()
        with pytest.raises(Exception, match="epoch is fenced"):
            await stopping

        await nexo.stream.delete(name)

    async def test_active_subscription_rejoins_after_update_failure(self, nexo: NexoClient):
        name = f"stream-active-update-failure-{uuid.uuid4()}"
        group = "active-update-failure-group"
        stream = await _create_stream(nexo, name)

        attempts = 0
        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def callback(_):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                first_started.set()
                await release_first.wait()

        sub = await stream.group(group).subscribe(callback)
        await stream.publish({"id": 1})
        await asyncio.wait_for(first_started.wait(), timeout=2.0)
        await stream.group(group).seek("beginning")
        release_first.set()

        await wait_for(lambda: attempts == 2, timeout=5.0)
        await sub.stop()
        await nexo.stream.delete(name)

    async def test_preserve_ordering_concurrency_1(self, nexo: NexoClient):
        name = f"stream-order-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list[int] = []
        sub = await stream.group("order-group").subscribe(
            lambda d: received.append(d["i"]), concurrency=1
        )

        for i in range(30):
            await stream.publish({"i": i})

        await wait_for(lambda: len(received) == 30, timeout=10.0)

        for i in range(30):
            assert received[i] == i

        await sub.stop()

    async def test_parallel_concurrency_gt_1(self, nexo: NexoClient):
        name = f"stream-concurrent-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        CALLBACK_DELAY = 0.1
        COUNT = 20
        CONCURRENCY = 10

        received: list[int] = []
        in_flight = 0
        max_in_flight = 0

        async def cb(d):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(CALLBACK_DELAY)
            received.append(d["i"])
            in_flight -= 1

        sub = await stream.group("concurrent-group").subscribe(
            cb,
            batch_size=COUNT,
            concurrency=CONCURRENCY,
        )

        for i in range(COUNT):
            await stream.publish({"i": i})

        start = asyncio.get_event_loop().time()
        await wait_for(lambda: len(received) == COUNT, timeout=10.0)
        elapsed = asyncio.get_event_loop().time() - start

        assert len(set(received)) == COUNT
        assert max_in_flight > 1
        assert elapsed < COUNT * CALLBACK_DELAY * 0.6

        await sub.stop()

    async def test_fast_ack_does_not_wait_for_slow_callback_in_same_batch(self, nexo: NexoClient):
        name = f"stream-incremental-ack-{uuid.uuid4()}"
        group = "incremental-ack-group"
        stream = await _create_stream(nexo, name)
        await stream.publish_batch([
            {"data": {"id": 1}, "key": "A"},
            {"data": {"id": 2}, "key": "B"},
            {"data": {"id": 3}, "key": "A"},
        ])

        fast_finished = asyncio.Event()
        slow_started = asyncio.Event()
        release_slow = asyncio.Event()

        received: list[int] = []

        async def first_callback(data):
            received.append(data["id"])
            if data["id"] == 1:
                fast_finished.set()
            elif data["id"] == 2:
                slow_started.set()
                await release_slow.wait()

        first = await stream.group(group).subscribe(
            first_callback,
            batch_size=2,
            concurrency=2,
            max_buffered_messages=2,
        )
        await asyncio.wait_for(
            asyncio.gather(fast_finished.wait(), slow_started.wait()),
            timeout=2.0,
        )

        client_b = await NexoClient.connect()
        received_by_second: list[int] = []
        try:
            second = await (await client_b.stream.get(name)).group(group).subscribe(
                lambda data: received_by_second.append(data["id"]),
                batch_size=1,
            )
            await wait_for(
                lambda: 3 in received or 3 in received_by_second, timeout=2.0
            )
            release_slow.set()
            await first.stop()
            await second.stop()
        finally:
            client_b.disconnect()

        await nexo.stream.delete(name)

    async def test_seek_beginning_and_end(self, nexo: NexoClient):
        name = f"stream-seek-{uuid.uuid4()}"
        group = "seek-group"
        stream = await _create_stream(nexo, name)

        for i in range(10):
            await stream.publish({"i": i})

        await stream.group(group).seek("end")

        received_end: list = []
        sub_end = await stream.group(group).subscribe(lambda d: received_end.append(d))

        await stream.publish({"i": 10})
        await wait_for(lambda: len(received_end) == 1)
        assert received_end[0]["i"] == 10
        await sub_end.stop()

        await stream.group(group).seek("beginning")

        received_start: list = []
        sub_start = await stream.group(group).subscribe(lambda d: received_start.append(d))

        await wait_for(lambda: len(received_start) == 11)
        assert received_start[0]["i"] == 0
        assert received_start[10]["i"] == 10

        await sub_start.stop()

    async def test_stop_quickly_during_long_poll_idle(self, nexo: NexoClient):
        name = f"stream-stop-idle-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        sub = await stream.group("idle-stop-group").subscribe(lambda _: None)

        await asyncio.sleep(0.2)

        start = asyncio.get_event_loop().time()
        await sub.stop()
        elapsed = asyncio.get_event_loop().time() - start

        assert elapsed < 2.0

    async def test_no_delivery_after_stop(self, nexo: NexoClient):
        name = f"stream-stop-nodeliver-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list = []
        sub = await stream.group("nodeliver-group").subscribe(lambda d: received.append(d))

        await stream.publish({"id": 1})
        await wait_for(lambda: len(received) == 1)

        await sub.stop()

        await stream.publish({"id": 2})
        await asyncio.sleep(0.5)

        assert len(received) == 1

    async def test_publish_batch_returns_seq_numbers(self, nexo: NexoClient):
        name = f"stream-batch-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        seqs = await stream.publish_batch([
            {"data": "msg1"},
            {"data": "msg2"},
            {"data": "msg3"},
        ])

        assert len(seqs) == 3
        assert seqs[0] == 1
        assert seqs[1] == 2
        assert seqs[2] == 3

    async def test_publish_batch_with_keys(self, nexo: NexoClient):
        name = f"stream-batch-keys-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list[dict] = []
        sub = await stream.group("g-batch-keys").subscribe(
            lambda data, meta: received.append({"data": data, "key": meta.get("key")}),
        )

        await stream.publish_batch([
            {"data": "msg1", "key": "key-A"},
            {"data": "msg2", "key": "key-B"},
            {"data": "msg3"},
        ])

        await wait_for(lambda: len(received) == 3)
        await sub.stop()

    async def test_publish_with_string_key(self, nexo: NexoClient):
        name = f"stream-pub-key-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list[dict] = []
        sub = await stream.group("g-pub-key").subscribe(
            lambda data, meta: received.append({"data": data, "key": meta.get("key")}),
        )

        seq = await stream.publish({"x": 1}, key="my-key")
        assert seq > 0

        await wait_for(lambda: len(received) == 1)
        await sub.stop()

        assert received[0]["data"] == {"x": 1}
        assert received[0]["key"] is not None
        assert bytes(received[0]["key"]).decode("utf-8") == "my-key"

        await nexo.stream.delete(name)

    async def test_publish_with_bytes_key(self, nexo: NexoClient):
        name = f"stream-pub-rawkey-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list[dict] = []
        sub = await stream.group("g-pub-rawkey").subscribe(
            lambda data, meta: received.append({"data": data, "key": meta.get("key")}),
        )

        raw_key = b"\x01\x02\xff"
        await stream.publish("payload", key=raw_key)

        await wait_for(lambda: len(received) == 1)
        await sub.stop()

        assert received[0]["data"] == "payload"
        assert received[0]["key"] is not None
        assert bytes(received[0]["key"]) == raw_key

        await nexo.stream.delete(name)

    async def test_publish_bytes_data(self, nexo: NexoClient):
        name = f"stream-pub-uint8-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list = []
        sub = await stream.group("g-pub-uint8").subscribe(lambda data: received.append(data))

        payload = b"\x01\x02\xff\x00"
        await stream.publish(payload)

        await wait_for(lambda: len(received) == 1)
        await sub.stop()

        assert isinstance(received[0], (bytes, bytearray))
        assert bytes(received[0]) == payload

        await nexo.stream.delete(name)

    async def test_empty_publish_batch(self, nexo: NexoClient):
        name = f"stream-batch-empty-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        seqs = await stream.publish_batch([])
        assert len(seqs) == 0

    async def test_reject_empty_stream_keys(self, nexo: NexoClient):
        name = f"stream-empty-key-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        with pytest.raises(ValueError, match="must not be empty"):
            await stream.publish({"x": 1}, key="")
        with pytest.raises(ValueError, match="must not be empty"):
            await stream.publish_batch([{"data": {"x": 1}, "key": b""}])
        await nexo.stream.delete(name)

    async def test_reject_oversized_publish_batch(self, nexo: NexoClient):
        name = f"stream-large-batch-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        items = [{"data": None}] * 65_537
        with pytest.raises(ValueError, match="Publish batch too large"):
            await stream.publish_batch(items)
        await nexo.stream.delete(name)

    # ── Edge cases ──────────────────────────────────────────────

    async def test_exists_true_after_create_false_before(self, nexo: NexoClient):
        name = f"stream-exists-{uuid.uuid4()}"
        assert await nexo.stream.exists(name) is False
        await nexo.stream.create(name)
        assert await nexo.stream.exists(name) is True
        await nexo.stream.delete(name)
        assert await nexo.stream.exists(name) is False

    async def test_create_idempotent(self, nexo: NexoClient):
        name = f"stream-idempotent-{uuid.uuid4()}"
        created = await nexo.stream.create(
            name,
            max_age_ms=1234,
            max_bytes=5678,
        )
        unchanged = await nexo.stream.create(
            name,
            max_age_ms=1234,
            max_bytes=5678,
        )

        assert created.status is ProvisionOutcome.CREATED
        assert unchanged.status is ProvisionOutcome.UNCHANGED
        assert unchanged.definition == created.definition
        assert await nexo.stream.describe(name) == created.definition
        await nexo.stream.delete(name)

    async def test_create_config_conflict_is_typed(self, nexo: NexoClient):
        name = f"stream-conflict-{uuid.uuid4()}"
        await nexo.stream.create(name, max_age_ms=1000)

        with pytest.raises(ResourceConfigurationConflictError) as caught:
            await nexo.stream.create(name, max_age_ms=1001)

        assert caught.value.details["resourceKind"] == "stream"
        assert caught.value.details["resourceName"] == name
        assert caught.value.details["differences"] == [
            {"path": "config.retention.maxAgeMs", "requested": 1001, "actual": 1000}
        ]
        await nexo.stream.delete(name)

    async def test_reject_invalid_stream_name(self, nexo: NexoClient):
        with pytest.raises(Exception, match="Invalid stream name"):
            await nexo.stream.create("../outside")

    async def test_reject_invalid_seek_and_subscription_options(self, nexo: NexoClient):
        name = f"stream-invalid-options-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        group = stream.group("group")
        with pytest.raises(ValueError, match="Invalid seek target"):
            await group.seek("invalid")
        with pytest.raises(ValueError, match="batch_size must be an integer between"):
            await group.subscribe(lambda _: None, batch_size=0)
        with pytest.raises(ValueError, match="wait_ms must be a positive integer"):
            await group.subscribe(lambda _: None, wait_ms=0)
        with pytest.raises(ValueError, match="stop_timeout_ms must be a positive integer"):
            await group.subscribe(lambda _: None, stop_timeout_ms=0)
        await nexo.stream.delete(name)

    async def test_reject_invalid_continuous_executor_options(self, nexo: NexoClient):
        name = f"stream-opt-validation-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        group = stream.group("group")

        bad = [0, -1, 1.5, float("nan"), float("inf"), True, "10"]
        for v in bad:
            for kwarg in (
                "batch_size",
                "key_batch_size",
                "wait_ms",
                "concurrency",
                "stop_timeout_ms",
                "callback_timeout_ms",
                "max_buffered_messages",
                "max_buffered_bytes",
            ):
                with pytest.raises(ValueError, match=kwarg):
                    await group.subscribe(lambda _: None, **{kwarg: v})

        with pytest.raises(ValueError, match="wait_ms"):
            await group.subscribe(lambda _: None, wait_ms=0x80000000)
        with pytest.raises(ValueError, match="stop_timeout_ms"):
            await group.subscribe(lambda _: None, stop_timeout_ms=0x80000000)
        with pytest.raises(ValueError, match="callback_timeout_ms"):
            await group.subscribe(lambda _: None, callback_timeout_ms=0x80000000)
        with pytest.raises(ValueError, match="concurrency"):
            await group.subscribe(lambda _: None, concurrency=65537)
        with pytest.raises(ValueError, match="batch_size"):
            await group.subscribe(lambda _: None, batch_size=65537)
        with pytest.raises(ValueError, match="key_batch_size"):
            await group.subscribe(lambda _: None, key_batch_size=1025)
        with pytest.raises(ValueError, match="max_buffered_messages"):
            await group.subscribe(lambda _: None, max_buffered_messages=131073)
        with pytest.raises(ValueError, match="max_buffered_bytes"):
            await group.subscribe(lambda _: None, max_buffered_bytes=53)
        with pytest.raises(ValueError, match="max_buffered_bytes"):
            await group.subscribe(lambda _: None, max_buffered_bytes=0x100000000)
        await nexo.stream.delete(name)

    async def test_get_nonexistent_stream_fails(self, nexo: NexoClient):
        name = f"stream-get-missing-{uuid.uuid4()}"
        with pytest.raises(ResourceNotFoundError):
            await nexo.stream.get(name)

    async def test_exists_propagates_connection_error(self):
        disconnected = NexoClient()
        try:
            with pytest.raises(NotConnectedError):
                await disconnected.stream.exists("stream-disconnected")
        finally:
            disconnected.disconnect()

    async def test_operations_after_delete_fail(self, nexo: NexoClient):
        name = f"stream-del-ops-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        await nexo.stream.delete(name)
        with pytest.raises(Exception):
            await stream.publish({"x": 1})
        with pytest.raises(Exception):
            await stream.group("g-del").subscribe(lambda _: None)

    async def test_peek_dls_empty_returns_empty(self, nexo: NexoClient):
        name = f"stream-dls-empty-{uuid.uuid4()}"
        group = "g-dls-empty"
        stream = await _create_stream(nexo, name)
        sub = await stream.group(group).subscribe(lambda _: None)
        await sub.stop()
        dls = stream.group(group).dls
        entries = await dls.peek(limit=10, offset=0)
        assert entries == []
        assert callable(dls.replay)
        assert callable(dls.delete)
        await nexo.stream.delete(name)

    async def test_purge_dls_empty_returns_zero(self, nexo: NexoClient):
        name = f"stream-dls-purge-empty-{uuid.uuid4()}"
        group = "g-dls-purge"
        stream = await _create_stream(nexo, name)
        sub = await stream.group(group).subscribe(lambda _: None)
        await sub.stop()
        count = await stream.group(group).dls.purge()
        assert count == 0
        await nexo.stream.delete(name)

    async def test_resubscribe_same_group_after_stop(self, nexo: NexoClient):
        name = f"stream-resub-{uuid.uuid4()}"
        group = "g-resub"
        stream = await _create_stream(nexo, name)

        recv1: list = []
        sub1 = await stream.group(group).subscribe(lambda d: recv1.append(d))
        await stream.publish({"i": 1})
        await stream.publish({"i": 2})
        await wait_for(lambda: len(recv1) == 2)
        await sub1.stop()

        await stream.publish({"i": 3})

        recv2: list = []
        sub2 = await stream.group(group).subscribe(lambda d: recv2.append(d))
        await wait_for(lambda: len(recv2) == 1)
        assert recv2[0]["i"] == 3
        await sub2.stop()

        await nexo.stream.delete(name)

    async def test_multiple_groups_simultaneous_delivery(self, nexo: NexoClient):
        name = f"stream-multi-groups-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        client_a = await NexoClient.connect()
        client_b = await NexoClient.connect()
        try:
            recv_a: list = []
            recv_b: list = []
            recv_c: list = []

            sub_a = await (await client_a.stream.get(name)).group("multi-a").subscribe(lambda d: recv_a.append(d))
            sub_b = await (await client_b.stream.get(name)).group("multi-b").subscribe(lambda d: recv_b.append(d))
            sub_c = await stream.group("multi-c").subscribe(lambda d: recv_c.append(d))

            for i in range(5):
                await stream.publish({"i": i})

            await wait_for(lambda: len(recv_a) == 5 and len(recv_b) == 5 and len(recv_c) == 5)

            await sub_a.stop()
            await sub_b.stop()
            await sub_c.stop()
        finally:
            client_a.disconnect()
            client_b.disconnect()

        await nexo.stream.delete(name)

    # ── Per-key ordering end-to-end ──────────────────────────────

    async def test_per_key_ordering_same_key_one_at_a_time(self, nexo: NexoClient):
        name = f"stream-perkey-order-{uuid.uuid4()}"
        group = "perkey-order-group"
        stream = await _create_stream(nexo, name)

        # Publish all messages BEFORE subscribing so the first fetch sees all 6
        # and per-key blocking is active immediately.
        # Interleaved: A0, B0, A1, no-key, B1, A2
        await stream.publish({"i": 0}, key="user-A")
        await stream.publish({"i": 0}, key="user-B")
        await stream.publish({"i": 1}, key="user-A")
        await stream.publish({"i": 0})
        await stream.publish({"i": 1}, key="user-B")
        await stream.publish({"i": 2}, key="user-A")

        delivered: list[dict] = []
        first_batch_event = asyncio.Event()
        release_gate = asyncio.Event()
        first_batch_count = 0

        async def callback(d, meta):
            nonlocal first_batch_count
            key = bytes(meta["key"]).decode("utf-8") if meta.get("key") else "none"
            delivered.append({"key": key, "i": d["i"]})
            first_batch_count += 1
            if first_batch_count == 3:
                first_batch_event.set()
            # Block all callbacks so ACKs don't fire until we've verified
            await release_gate.wait()

        sub = await stream.group(group).subscribe(
            callback,
            batch_size=10,
            concurrency=10,
        )

        # Wait for first batch: A0, B0, no-key (3 messages)
        # With concurrency=10 the SDK would process all if the server returned them,
        # but the server only returns 3 due to per-key blocking.
        await asyncio.wait_for(first_batch_event.wait(), timeout=5.0)

        first_keys = sorted(d["key"] for d in delivered[:3])
        assert first_keys == ["none", "user-A", "user-B"]

        # No second message for any key should have been delivered yet
        # (ACKs are blocked, so keys remain in-flight on the server)
        a_messages = [d for d in delivered if d["key"] == "user-A"]
        b_messages = [d for d in delivered if d["key"] == "user-B"]
        assert len(a_messages) == 1
        assert len(b_messages) == 1

        # Release the gate so ACKs proceed and the test can clean up
        release_gate.set()

        await sub.stop()
        await nexo.stream.delete(name)

    async def test_same_key_messages_processed_strictly_sequentially(self, nexo: NexoClient):
        name = f"stream-keyed-seq-{uuid.uuid4()}"
        group = "keyed-seq-group"
        stream = await _create_stream(nexo, name)

        for i in range(6):
            await stream.publish({"i": i}, key="user-A")

        received: list[int] = []
        in_flight = 0

        async def callback(d):
            nonlocal in_flight
            in_flight += 1
            assert in_flight == 1
            await asyncio.sleep(0.005)
            received.append(d["i"])
            in_flight -= 1

        sub = await stream.group(group).subscribe(
            callback, batch_size=10, key_batch_size=10, concurrency=5
        )

        await wait_for(lambda: received == [0, 1, 2, 3, 4, 5])
        await sub.stop()
        await nexo.stream.delete(name)

    async def test_mid_batch_failure_does_not_replay_confirmed_prefix(
        self, nexo: NexoClient
    ):
        name = f"stream-keyed-retry-{uuid.uuid4()}"
        group = "keyed-retry-group"
        stream = await _create_stream(nexo, name)

        for i in range(5):
            await stream.publish({"i": i}, key="user-A")

        attempts: list[int] = []
        failed_once = False

        async def callback(d):
            nonlocal failed_once
            attempts.append(d["i"])
            if d["i"] == 2 and not failed_once:
                failed_once = True
                raise RuntimeError("boom")

        sub = await stream.group(group).subscribe(
            callback, batch_size=10, key_batch_size=10
        )

        await wait_for(lambda: attempts == [0, 1, 2, 2, 3, 4], timeout=45.0)
        await sub.stop()
        await nexo.stream.delete(name)

    async def test_same_key_not_leased_to_two_consumers(self, nexo: NexoClient):
        name = f"stream-keyed-exclusive-{uuid.uuid4()}"
        group = "keyed-exclusive-group"
        stream = await _create_stream(nexo, name)

        for i in range(4):
            await stream.publish({"i": i}, key="user-A")

        received_a: list[int] = []
        received_b: list[int] = []
        started_a = asyncio.Event()
        held_a = asyncio.Event()

        async def callback_a(d):
            received_a.append(d["i"])
            if d["i"] == 0:
                started_a.set()
            await held_a.wait()

        async def callback_b(d):
            received_b.append(d["i"])

        sub_a = await stream.group(group).subscribe(callback_a)
        await asyncio.wait_for(started_a.wait(), timeout=5.0)

        sub_b = await stream.group(group).subscribe(callback_b)
        await asyncio.sleep(0.3)
        assert received_b == []

        stopping = asyncio.create_task(sub_a.stop())
        await asyncio.sleep(0)
        held_a.set()
        await stopping
        await wait_for(lambda: received_b == [1, 2, 3])
        await sub_b.stop()
        await nexo.stream.delete(name)

    async def test_stop_mid_batch_keeps_prefix_and_redelivers_tail(
        self, nexo: NexoClient
    ):
        name = f"stream-stop-midbatch-{uuid.uuid4()}"
        group = "stop-midbatch-group"
        stream = await _create_stream(nexo, name)

        for i in range(5):
            await stream.publish({"i": i}, key="user-A")

        received_a: list[int] = []
        started_third = asyncio.Event()
        held_third = asyncio.Event()

        async def callback_a(d):
            received_a.append(d["i"])
            if d["i"] == 2:
                started_third.set()
                await held_third.wait()

        sub_a = await stream.group(group).subscribe(callback_a)
        await asyncio.wait_for(started_third.wait(), timeout=5.0)

        stopping = asyncio.create_task(sub_a.stop())
        await asyncio.sleep(0)
        held_third.set()
        await stopping
        assert received_a == [0, 1, 2]

        received_b: list[int] = []
        sub_b = await stream.group(group).subscribe(lambda d: received_b.append(d["i"]))
        await wait_for(lambda: received_b == [3, 4])
        await sub_b.stop()
        await nexo.stream.delete(name)

    async def test_continuous_refill_independent_keys_while_batch_held(
        self, nexo: NexoClient
    ):
        name = f"stream-refill-{uuid.uuid4()}"
        group = "refill-group"
        stream = await _create_stream(nexo, name)

        for i in range(4):
            await stream.publish({"i": i}, key="key-A")
        for i in range(6):
            await stream.publish({"i": i}, key="key-B")

        started_a: list[int] = []
        done_b: list[int] = []
        in_flight = 0
        max_in_flight = 0
        gate_a = asyncio.Event()
        a_held = True

        async def callback(d, meta=None):
            nonlocal in_flight, max_in_flight, a_held
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            try:
                if meta["key"] == b"key-A":
                    started_a.append(d["i"])
                    if a_held:
                        a_held = False
                        await gate_a.wait()
                else:
                    done_b.append(d["i"])
            finally:
                in_flight -= 1

        sub = await stream.group(group).subscribe(
            callback,
            batch_size=4,
            key_batch_size=2,
            concurrency=2,
            max_buffered_messages=8,
        )
        try:
            await wait_for(lambda: done_b == [0, 1, 2, 3, 4, 5], timeout=10.0)
            assert started_a == [0]
            assert max_in_flight <= 2
        finally:
            gate_a.set()
            await sub.stop()
        await nexo.stream.delete(name)

    async def test_callback_timeout_cancels_and_keeps_other_keys_running(
        self, nexo: NexoClient
    ):
        name = f"stream-cb-timeout-{uuid.uuid4()}"
        group = "cb-timeout-group"
        stream = await _create_stream(nexo, name)

        for i in range(2):
            await stream.publish({"i": i}, key="key-A")
        for i in range(4):
            await stream.publish({"i": i}, key="key-B")

        started_a: list[int] = []
        done_b: list[int] = []
        in_flight = 0
        max_in_flight = 0
        cancelled = asyncio.Event()
        gate_a = asyncio.Event()

        async def callback(d, meta=None):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            try:
                if meta["key"] == b"key-A":
                    started_a.append(d["i"])
                    try:
                        await gate_a.wait()
                    except asyncio.CancelledError:
                        cancelled.set()
                        raise
                else:
                    done_b.append(d["i"])
            finally:
                in_flight -= 1

        sub = await stream.group(group).subscribe(
            callback,
            batch_size=4,
            key_batch_size=2,
            concurrency=2,
            max_buffered_messages=8,
            callback_timeout_ms=60,
        )
        try:
            await wait_for(lambda: done_b == [0, 1, 2, 3], timeout=10.0)
            await asyncio.wait_for(cancelled.wait(), timeout=5.0)
            assert started_a == [0]
            assert max_in_flight <= 2
        finally:
            gate_a.set()
            await sub.stop()
        await nexo.stream.delete(name)

    async def test_local_key_gate_survives_seek_with_uncooperative_callback(
        self, nexo: NexoClient
    ):
        name = f"stream-seek-key-gate-{uuid.uuid4()}"
        group = "seek-key-gate-group"
        stream = await _create_stream(nexo, name)
        await stream.publish({"i": 1}, key="key-A")
        await stream.publish({"i": 2}, key="key-B")

        a_started: list[int] = []
        done_b = 0
        a_in_flight = 0
        a_peak = 0
        first_a = True
        started_a = asyncio.Event()
        gate_a = asyncio.Event()
        cancel_seen = asyncio.Event()

        async def callback(_d, meta=None):
            nonlocal done_b, a_in_flight, a_peak, first_a
            if meta["key"] == b"key-A":
                a_in_flight += 1
                a_peak = max(a_peak, a_in_flight)
                try:
                    a_started.append(meta["seq"])
                    if first_a:
                        first_a = False
                        started_a.set()
                        while True:
                            try:
                                await gate_a.wait()
                                break
                            except asyncio.CancelledError:
                                cancel_seen.set()
                finally:
                    a_in_flight -= 1
            else:
                done_b += 1

        sub = await stream.group(group).subscribe(
            callback,
            batch_size=4,
            key_batch_size=2,
            concurrency=2,
            callback_timeout_ms=30000,
        )
        try:
            await asyncio.wait_for(started_a.wait(), timeout=10.0)
            await wait_for(lambda: done_b == 1, timeout=10.0)

            await stream.group(group).seek("beginning")
            await wait_for(lambda: done_b == 2, timeout=10.0)
            await asyncio.wait_for(cancel_seen.wait(), timeout=10.0)
            assert a_started == [1]
            assert a_peak == 1
            assert sub.error is None

            gate_a.set()
            await wait_for(lambda: len(a_started) == 2, timeout=10.0)
            assert a_peak == 1
        finally:
            gate_a.set()
            await sub.stop()
            await nexo.stream.delete(name)


def _write_fetch_page(batches, lease_ms=30000):
    from nexo.protocol.codec import FrameWriter
    from nexo.protocol.generated import HEADER_SIZE, DataType

    payload = bytes([DataType.STRING]) + b"x"
    w = FrameWriter().begin()
    w.u64(lease_ms).u32(len(batches))
    for key, seqs, receipt in batches:
        w.uuid(receipt)
        w.u16(len(key) if key else 0)
        if key:
            w.raw_bytes(key)
        w.u32(len(seqs))
        for seq in seqs:
            w.u64(seq).u64(0).u32(len(payload)).raw_bytes(payload)
    return w.finish(0, 0)[HEADER_SIZE:]


_LIMITS = dict(max_messages=10, max_key_messages=5, max_key_batches=4, max_bytes=1 << 20)


def _decode_page(buf, **overrides):
    from nexo.brokers.stream import _decode_fetch_page
    from nexo.protocol.codec import Cursor

    return _decode_fetch_page(Cursor(bytes(buf)), **{**_LIMITS, **overrides})


class TestDecodeFetchPage:
    def test_decode_fetch_valid_mixed_page(self):
        r1, r2, r3 = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
        buf = _write_fetch_page(
            [(b"a", [1, 2], r1), (b"b", [3], r2), (None, [4], r3)]
        )
        lease_ms, batches = _decode_page(buf)
        assert lease_ms == 30000
        assert [[m["seq"] for m in b["messages"]] for b in batches] == [[1, 2], [3], [4]]
        assert batches[2]["key"] is None
        assert batches[2]["messages"][0]["data"] == "x"

    def test_decode_fetch_rejects_oversized_response(self):
        buf = _write_fetch_page([])
        with pytest.raises(ProtocolError):
            _decode_page(buf, max_bytes=4)

    def test_decode_fetch_rejects_invalid_lease_ms(self):
        with pytest.raises(ProtocolError):
            _decode_page(_write_fetch_page([], lease_ms=0))
        with pytest.raises(ProtocolError):
            _decode_page(_write_fetch_page([], lease_ms=0x100000000))

    def test_decode_fetch_rejects_batch_count_beyond_payload(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import HEADER_SIZE, DataType

        payload = bytes([DataType.STRING]) + b"x"
        w = FrameWriter().begin()
        w.u64(30000).u32(2)
        w.uuid(uuid.uuid4().hex).u16(1).raw_bytes(b"a").u32(1)
        w.u64(1).u64(0).u32(len(payload)).raw_bytes(payload)
        with pytest.raises(ProtocolError):
            _decode_page(w.finish(0, 0)[HEADER_SIZE:])

    def test_decode_fetch_rejects_zero_message_batch(self):
        buf = _write_fetch_page([(b"a", [], uuid.uuid4().hex)])
        with pytest.raises(ProtocolError):
            _decode_page(buf)

    def test_decode_fetch_rejects_non_singleton_keyless_batch(self):
        buf = _write_fetch_page([(None, [1, 2], uuid.uuid4().hex)])
        with pytest.raises(ProtocolError):
            _decode_page(buf)

    def test_decode_fetch_rejects_keyed_batch_above_limit(self):
        buf = _write_fetch_page([(b"a", [1, 2, 3, 4, 5, 6], uuid.uuid4().hex)])
        with pytest.raises(ProtocolError):
            _decode_page(buf)

    def test_decode_fetch_rejects_more_keyed_batches_than_requested(self):
        buf = _write_fetch_page(
            [(bytes([i + 1]), [i + 1], uuid.uuid4().hex) for i in range(5)]
        )
        with pytest.raises(ProtocolError):
            _decode_page(buf)

    def test_decode_fetch_rejects_more_messages_than_requested(self):
        buf = _write_fetch_page([(b"a", [1, 2, 3, 4], uuid.uuid4().hex)])
        with pytest.raises(ProtocolError):
            _decode_page(buf, max_messages=3, max_key_batches=1)

    def test_decode_fetch_rejects_out_of_order_keyed_seqs(self):
        buf = _write_fetch_page([(b"a", [2, 1], uuid.uuid4().hex)])
        with pytest.raises(ProtocolError):
            _decode_page(buf)

    def test_decode_fetch_rejects_duplicate_receipts_and_keys(self):
        dup = uuid.uuid4().hex
        buf = _write_fetch_page([(b"a", [1], dup), (b"b", [2], dup)])
        with pytest.raises(ProtocolError):
            _decode_page(buf)
        buf = _write_fetch_page(
            [(b"a", [1], uuid.uuid4().hex), (b"a", [2], uuid.uuid4().hex)]
        )
        with pytest.raises(ProtocolError):
            _decode_page(buf)

    def test_decode_fetch_rejects_trailing_bytes(self):
        buf = _write_fetch_page([(None, [1], uuid.uuid4().hex)])
        with pytest.raises(ProtocolError):
            _decode_page(buf + b"\x00")
        with pytest.raises(ProtocolError):
            _decode_page(_write_fetch_page([]) + b"\x00")

    def test_decode_fetch_copies_key_and_raw_payload(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import DataType, HEADER_SIZE

        raw = bytes([DataType.RAW]) + b"\x07" * 64
        w = FrameWriter().begin()
        w.u64(30000).u32(1)
        w.uuid(uuid.uuid4().hex).u16(1).raw_bytes(b"a").u32(1)
        w.u64(1).u64(0).u32(len(raw)).raw_bytes(raw)
        buf = w.finish(0, 0)[HEADER_SIZE:]
        _, batches = _decode_page(buf)
        assert batches[0]["key"] == b"a"
        assert isinstance(batches[0]["key"], bytes)
        assert batches[0]["messages"][0]["data"] == b"\x07" * 64
        assert isinstance(batches[0]["messages"][0]["data"], bytes)


class _FakeStreamConnection:
    def __init__(self) -> None:
        self.is_connected = True
        self.joins: list[str] = []
        self.leaves: list[tuple[str, str]] = []
        self.updates: list[dict] = []
        self.fetches: list[dict] = []
        self.order: list[int] = []
        self.pending_fetch: asyncio.Future | None = None
        self.fetch_queue: list = []
        self.fetch_handler = None
        self.update_handler = None
        self.next_join_epochs: list[str] = []
        self.join_hold: asyncio.Future | None = None
        self.epoch = "e" * 32

    async def send(self, opcode, build=None, *, timeout_ms=None):
        from nexo.errors import NotMemberError
        from nexo.protocol.codec import Cursor, FrameWriter
        from nexo.protocol.generated import HEADER_SIZE, ResponseStatus, StreamOpcode

        w = FrameWriter().begin()
        if build is not None:
            build(w)
        cursor = Cursor(w.finish(1, opcode)[HEADER_SIZE:])
        self.order.append(opcode)
        if opcode == StreamOpcode.S_JOIN:
            epoch = (
                self.next_join_epochs.pop(0) if self.next_join_epochs else self.epoch
            )
            self.joins.append(epoch)
            member = f"consumer-{len(self.joins)}"
            if self.join_hold is not None and len(self.joins) > 1:
                await self.join_hold
            out = (
                FrameWriter()
                .begin()
                .u64(0)
                .uuid(epoch)
                .string(member)
                .finish(0, opcode)
            )
            return ResponseStatus.DATA, Cursor(out[HEADER_SIZE:])
        if opcode == StreamOpcode.S_LEAVE:
            cursor.read_string()
            cursor.read_string()
            consumer = cursor.read_string()
            epoch = cursor.read_uuid()
            self.leaves.append((consumer, epoch))
            pending = self.pending_fetch
            if pending is not None and not pending.done():
                self.pending_fetch = None
                pending.set_exception(NotMemberError("member left"))
            return ResponseStatus.OK, Cursor(b"")
        if opcode == StreamOpcode.S_FETCH:
            cursor.read_string()
            cursor.read_string()
            cursor.read_string()
            cursor.read_uuid()
            self.fetches.append(
                {
                    "max_messages": cursor.read_u32(),
                    "max_key_messages": cursor.read_u32(),
                    "max_key_batches": cursor.read_u32(),
                    "max_bytes": cursor.read_u32(),
                    "wait_ms": cursor.read_u32(),
                }
            )
            nxt = (
                self.fetch_queue.pop(0)
                if self.fetch_queue
                else self.fetch_handler() if self.fetch_handler is not None else None
            )
            if asyncio.isfuture(nxt) or asyncio.iscoroutine(nxt):
                nxt = await nxt
            if isinstance(nxt, BaseException):
                raise nxt
            if nxt is not None:
                return ResponseStatus.DATA, Cursor(nxt)
            loop = asyncio.get_running_loop()
            self.pending_fetch = loop.create_future()
            return await self.pending_fetch
        if opcode == StreamOpcode.S_UPDATE_BATCHES:
            cursor.read_string()
            cursor.read_string()
            consumer = cursor.read_string()
            epoch = cursor.read_uuid()
            count = cursor.read_u32()
            items = []
            for _ in range(count):
                items.append(
                    {
                        "receipt": cursor.read_uuid(),
                        "completed": cursor.read_u32(),
                        "flags": cursor.read_u8(),
                    }
                )
            self.updates.append(
                {"consumer": consumer, "epoch": epoch, "items": items}
            )
            if self.update_handler is not None:
                payload = self.update_handler(items)
                if asyncio.isfuture(payload) or asyncio.iscoroutine(payload):
                    payload = await payload
            else:
                payload = FrameWriter().begin().u32(0).finish(0, 0)[HEADER_SIZE:]
            return ResponseStatus.DATA, Cursor(payload)
        raise RuntimeError(f"unexpected opcode {opcode}")


def _mock_page(batches, lease_ms=30000):
    from nexo.protocol.codec import FrameWriter
    from nexo.protocol.generated import DataType, HEADER_SIZE

    payload = bytes([DataType.STRING]) + b"x"
    w = FrameWriter().begin()
    w.u64(lease_ms).u32(len(batches))
    for key, seqs, receipt in batches:
        w.uuid(receipt)
        w.u16(len(key) if key else 0)
        if key:
            w.raw_bytes(key)
        w.u32(len(seqs))
        for seq in seqs:
            w.u64(seq).u64(0).u32(len(payload)).raw_bytes(payload)
    return w.finish(0, 0)[HEADER_SIZE:]


RECEIPT_A = "a" * 32
RECEIPT_B = "b" * 32
RECEIPT_C = "c" * 32
RECEIPT_D = "d" * 32
EPOCH_2 = "2" * 32


def _mock_group(conn):
    from nexo.brokers.stream import NexoStreamGroup
    from nexo.utils.logger import Logger

    return NexoStreamGroup(conn, "s", "g", Logger(level="OFF"))


@pytest.mark.asyncio
class TestStreamContinuousExecutor:
    async def test_renews_held_batch_through_coalesced_updates(self):
        from nexo.protocol.generated import StreamOpcode

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page([(b"a", [1], RECEIPT_A)], lease_ms=90)
        )

        held = asyncio.Event()
        sub = await _mock_group(fake).subscribe(
            lambda _d: held.wait(), callback_timeout_ms=1000
        )
        try:
            await wait_for(
                lambda: len(
                    [
                        u
                        for u in fake.updates
                        if any(
                            i["receipt"] == RECEIPT_A
                            and i["completed"] == 0
                            and i["flags"] == 0
                            for i in u["items"]
                        )
                    ]
                )
                >= 2,
                timeout=3.0,
            )
            held.set()
            await wait_for(
                lambda: any(
                    i["receipt"] == RECEIPT_A and i["completed"] == 1 and i["flags"] == 0
                    for u in fake.updates
                    for i in u["items"]
                ),
                timeout=3.0,
            )
        finally:
            held.set()
            await sub.stop()

    async def test_stale_batch_fenced_without_rejoin(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import ErrorCode, HEADER_SIZE

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page(
                [
                    (b"a", [1, 2], RECEIPT_A),
                    (b"b", [10, 11], RECEIPT_B),
                ],
                lease_ms=60,
            )
        )
        fake.fetch_queue.append(_mock_page([(b"c", [12], RECEIPT_C)], lease_ms=60))

        fenced_a = False

        def update_handler(items):
            nonlocal fenced_a
            failures = []
            for i in items:
                if i["receipt"] == RECEIPT_A and not fenced_a:
                    fenced_a = True
                    failures.append(i)
            w = FrameWriter().begin()
            w.u32(len(failures))
            for f in failures:
                w.uuid(f["receipt"]).u8(ErrorCode.FENCED)
            return w.finish(0, 0)[HEADER_SIZE:]

        fake.update_handler = update_handler

        got_a: list[int] = []
        got_b: list[int] = []
        held_a = asyncio.Event()
        first_a = True

        async def callback(_d, meta=None):
            nonlocal first_a
            seq = meta["seq"]
            if meta["key"] == b"a":
                got_a.append(seq)
                if first_a:
                    first_a = False
                    await held_a.wait()
            else:
                got_b.append(seq)

        sub = await _mock_group(fake).subscribe(callback, callback_timeout_ms=5000)
        try:
            await wait_for(lambda: fenced_a, timeout=3.0)
            await wait_for(lambda: sorted(got_b) == [10, 11, 12], timeout=3.0)
            assert len(fake.joins) == 1
            held_a.set()
            await asyncio.sleep(0.05)
            assert got_a == [1]
        finally:
            held_a.set()
            await sub.stop()

    async def test_rejoins_with_new_epoch_after_session_fence(self):
        from nexo.errors import FencedError

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page(
                [
                    (b"a", [1, 2], RECEIPT_A),
                    (b"b", [10], RECEIPT_B),
                ]
            )
        )
        fake.next_join_epochs.extend([fake.epoch, EPOCH_2])
        a_started = asyncio.Event()
        phase = 0

        async def fetch_handler():
            nonlocal phase
            if phase == 0:
                await a_started.wait()
                phase = 1
                return FencedError("epoch is fenced")
            if phase == 1:
                phase = 2
                return _mock_page([(b"a", [1, 2], RECEIPT_C)])
            return None

        fake.fetch_handler = fetch_handler

        got_a: list[int] = []
        got_b: list[int] = []
        held_a = asyncio.Event()
        first_a = True

        async def callback(_d, meta=None):
            nonlocal first_a
            seq = meta["seq"]
            if meta["key"] == b"a":
                got_a.append(seq)
                if first_a:
                    first_a = False
                    a_started.set()
                    await held_a.wait()
            else:
                got_b.append(seq)

        sub = await _mock_group(fake).subscribe(callback, callback_timeout_ms=5000)
        try:
            await wait_for(
                lambda: fake.joins == [fake.epoch, EPOCH_2], timeout=3.0
            )
            held_a.set()
            await wait_for(lambda: len(got_a) >= 3, timeout=3.0)
            assert got_b == [10]
            await asyncio.sleep(0.05)
            assert any(
                i["receipt"] == RECEIPT_C
                for u in fake.updates
                if u["epoch"] == EPOCH_2
                for i in u["items"]
            )
            assert all(
                all(i["receipt"] == RECEIPT_C for i in u["items"])
                for u in fake.updates
                if u["epoch"] == EPOCH_2
            )
            assert got_a == [1, 1, 2]
        finally:
            held_a.set()
            await sub.stop()

    async def test_stop_during_pending_fetch_leaves_without_waiting_long_poll(self):
        from nexo.protocol.generated import (
            FLAG_STREAM_S_UPDATE_BATCH_RELEASE,
            StreamOpcode,
        )

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(_mock_page([(b"a", [1, 2], RECEIPT_A)]))

        got: list[int] = []
        held = asyncio.Event()

        async def callback(_d, meta=None):
            got.append(meta["seq"])
            await held.wait()

        sub = await _mock_group(fake).subscribe(
            callback, batch_size=4, wait_ms=20000
        )
        await wait_for(lambda: got == [1], timeout=3.0)
        await wait_for(lambda: fake.pending_fetch is not None, timeout=3.0)

        loop = asyncio.get_running_loop()
        started = loop.time()
        stopping = asyncio.create_task(sub.stop())
        await asyncio.sleep(0)
        held.set()
        await stopping
        assert loop.time() - started < 5.0

        last_update = len(fake.order) - 1 - fake.order[::-1].index(
            StreamOpcode.S_UPDATE_BATCHES
        )
        leave_idx = fake.order.index(StreamOpcode.S_LEAVE)
        assert -1 < last_update < leave_idx
        assert len(fake.leaves) == 1
        assert any(
            i["receipt"] == RECEIPT_A
            and i["completed"] == 1
            and i["flags"] & FLAG_STREAM_S_UPDATE_BATCH_RELEASE
            for u in fake.updates
            for i in u["items"]
        )
        assert got == [1]

    async def test_never_exceeds_buffered_credits_while_callbacks_held(self):
        fake = _FakeStreamConnection()

        pages = 0

        def fetch_handler():
            nonlocal pages
            pages += 1
            if pages > 2:
                return None
            limit = max(1, fake.fetches[-1]["max_messages"])
            return _mock_page(
                [(None, [i + 1], uuid.uuid4().hex) for i in range(min(4, limit))]
            )

        fake.fetch_handler = fetch_handler
        gate = asyncio.Event()
        running = 0
        max_running = 0
        calls = 0

        async def callback(_d, meta=None):
            nonlocal running, max_running, calls
            calls += 1
            running += 1
            max_running = max(max_running, running)
            try:
                await gate.wait()
            finally:
                running -= 1

        sub = await _mock_group(fake).subscribe(
            callback,
            batch_size=4,
            key_batch_size=2,
            concurrency=2,
            max_buffered_messages=6,
            wait_ms=20000,
        )
        try:
            await wait_for(lambda: calls >= 2, timeout=3.0)
            await asyncio.sleep(0.2)
            assert len(fake.fetches) == 1
            assert fake.fetches[0]["max_messages"] <= 6
            assert fake.fetches[0]["max_bytes"] <= 20 * 1024 * 1024
            assert fake.fetches[0]["max_key_batches"] <= 2
            assert max_running <= 2

            gate.set()
            await wait_for(lambda: len(fake.fetches) >= 2, timeout=3.0)
            assert len(fake.fetches) <= 8
            assert max_running <= 2
            assert any(
                i["completed"] == 1
                for u in fake.updates
                for i in u["items"]
            )
        finally:
            gate.set()
            await sub.stop()

    async def test_reserves_callback_slots_before_starting_runners(self):
        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page(
                [
                    (None, [1], RECEIPT_A),
                    (None, [2], RECEIPT_B),
                    (None, [3], RECEIPT_C),
                    (None, [4], RECEIPT_D),
                ]
            )
        )

        running = 0
        max_running = 0
        gate = asyncio.Event()

        async def callback(_d, meta=None):
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            try:
                await gate.wait()
            finally:
                running -= 1

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=2, max_buffered_messages=8
        )
        try:
            await wait_for(lambda: running >= 2, timeout=3.0)
            await asyncio.sleep(0.03)
            assert max_running <= 2
            gate.set()
            await wait_for(
                lambda: sum(
                    1
                    for u in fake.updates
                    for i in u["items"]
                    if i["completed"] == 1
                )
                == 4,
                timeout=3.0,
            )
            assert max_running <= 2
        finally:
            gate.set()
            await sub.stop()

    async def test_leaves_once_on_fatal_update_outcome_with_pending_fetch(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import ErrorCode, HEADER_SIZE

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(_mock_page([(b"a", [1], RECEIPT_A)]))

        def update_handler(items):
            w = FrameWriter().begin()
            w.u32(len(items))
            for f in items:
                w.uuid(f["receipt"]).u8(ErrorCode.INVALID_ARGUMENT)
            return w.finish(0, 0)[HEADER_SIZE:]

        fake.update_handler = update_handler

        sub = await _mock_group(fake).subscribe(
            lambda _d: None, callback_timeout_ms=5000
        )
        await wait_for(
            lambda: sub.error is not None and "rejected" in str(sub.error),
            timeout=5.0,
        )
        await wait_for(lambda: len(fake.leaves) == 1, timeout=3.0)
        with pytest.raises(Exception, match="rejected"):
            await asyncio.wait_for(sub.wait_closed(), timeout=3.0)
        with pytest.raises(Exception):
            await sub.stop()

    async def test_cooperative_cancel_does_not_fail_subscription(self):
        from nexo.protocol.generated import FLAG_STREAM_S_UPDATE_BATCH_RETRY

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(_mock_page([(b"a", [1, 2], RECEIPT_A)]))
        fake.fetch_queue.append(_mock_page([(b"b", [10], RECEIPT_B)]))
        fake.fetch_queue.append(_mock_page([(b"b", [11], RECEIPT_C)]))

        got_b: list[int] = []
        cancelled = asyncio.Event()
        gate_a = asyncio.Event()

        async def callback(_d, meta=None):
            if meta["key"] == b"a":
                try:
                    await gate_a.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
            else:
                got_b.append(meta["seq"])

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=1, callback_timeout_ms=40
        )
        try:
            await asyncio.wait_for(cancelled.wait(), timeout=3.0)
            await wait_for(lambda: sorted(got_b) == [10, 11], timeout=3.0)
            await wait_for(
                lambda: any(
                    i["receipt"] == RECEIPT_A
                    and i["completed"] == 0
                    and i["flags"] & FLAG_STREAM_S_UPDATE_BATCH_RETRY
                    for u in fake.updates
                    for i in u["items"]
                ),
                timeout=3.0,
            )
            gate_a.set()
            await asyncio.sleep(0.08)
            assert sub.error is None
            assert all(
                i["receipt"] != RECEIPT_A or i["completed"] == 0
                for u in fake.updates
                for i in u["items"]
            )
        finally:
            gate_a.set()
            await sub.stop()

    async def test_fails_on_duplicate_outcome_receipt_in_update_response(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import ErrorCode, HEADER_SIZE

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page(
                [(b"a", [1], RECEIPT_A), (b"b", [2], RECEIPT_B)],
                lease_ms=60,
            )
        )

        loop = asyncio.get_running_loop()
        update_gate: asyncio.Future = loop.create_future()
        first = True

        def ok_payload():
            return FrameWriter().begin().u32(0).finish(0, 0)[HEADER_SIZE:]

        def update_handler(items):
            nonlocal first
            if first:
                first = False
                return update_gate
            w = FrameWriter().begin()
            w.u32(len(items))
            for _f in items:
                w.uuid(items[0]["receipt"]).u8(ErrorCode.FENCED)
            return w.finish(0, 0)[HEADER_SIZE:]

        fake.update_handler = update_handler

        gate = asyncio.Event()
        sub = await _mock_group(fake).subscribe(
            lambda _d: gate.wait(), concurrency=2, callback_timeout_ms=60000
        )
        try:
            await wait_for(lambda: len(fake.updates) == 1, timeout=3.0)
            await asyncio.sleep(0.06)
            if not update_gate.done():
                update_gate.set_result(ok_payload())
            await wait_for(
                lambda: sub.error is not None
                and "duplicate" in str(sub.error),
                timeout=5.0,
            )
            with pytest.raises(Exception):
                await sub.stop()
        finally:
            if not update_gate.done():
                update_gate.set_result(ok_payload())
            gate.set()

    async def test_fails_subscription_when_timed_out_callbacks_never_settle(self):
        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page([(b"a", [1], RECEIPT_A), (b"b", [2], RECEIPT_B)])
        )

        releases: list[asyncio.Event] = []
        calls = 0

        async def callback(_d, meta=None):
            nonlocal calls
            calls += 1
            gate = asyncio.Event()
            releases.append(gate)
            while not gate.is_set():
                try:
                    await gate.wait()
                except asyncio.CancelledError:
                    continue

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=2, callback_timeout_ms=40
        )

        try:
            await wait_for(
                lambda: sub.error is not None
                and "capacity exhausted" in str(sub.error),
                timeout=5.0,
            )
            assert calls == 2
            await wait_for(lambda: len(fake.leaves) == 1, timeout=3.0)
            with pytest.raises(Exception, match="capacity exhausted"):
                await asyncio.wait_for(sub.wait_closed(), timeout=3.0)

            loop = asyncio.get_running_loop()
            started = loop.time()
            with pytest.raises(Exception, match="capacity exhausted"):
                await sub.stop()
            assert loop.time() - started < 1.0
        finally:
            for gate in releases:
                gate.set()

    async def test_capacity_stays_balanced_when_abort_races_settlement(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import ErrorCode, HEADER_SIZE

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page([(b"a", [1], RECEIPT_A)], lease_ms=90)
        )
        fake.fetch_queue.append(
            _mock_page([(b"b", [10], RECEIPT_B)], lease_ms=90)
        )
        fenced_a = False

        def update_handler(items):
            nonlocal fenced_a
            failures = []
            for i in items:
                if i["receipt"] == RECEIPT_A and not fenced_a:
                    fenced_a = True
                    failures.append(i)
            w = FrameWriter().begin()
            w.u32(len(failures))
            for f in failures:
                w.uuid(f["receipt"]).u8(ErrorCode.FENCED)
            return w.finish(0, 0)[HEADER_SIZE:]

        fake.update_handler = update_handler

        got: list[int] = []
        a_gate = asyncio.Event()
        first_a = True

        async def callback(_d, meta=None):
            nonlocal first_a
            if meta["key"] == b"a" and first_a:
                first_a = False
                try:
                    await a_gate.wait()
                except asyncio.CancelledError:
                    a_gate.set()
                    raise
            else:
                got.append(meta["seq"])

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=1, callback_timeout_ms=60000
        )
        try:
            await wait_for(lambda: fenced_a, timeout=3.0)
            await wait_for(lambda: got == [10], timeout=3.0)
            assert fake.joins == [fake.epoch]
            await asyncio.sleep(0.4)
            assert sub.error is None
        finally:
            a_gate.set()
            await sub.stop()

    async def test_leaves_member_whose_join_completes_during_stop(self):
        from nexo.errors import FencedError

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(FencedError("epoch is fenced"))
        fake.next_join_epochs.extend([fake.epoch, EPOCH_2])
        loop = asyncio.get_running_loop()
        fake.join_hold = loop.create_future()
        sub = await _mock_group(fake).subscribe(lambda _d: None)
        await wait_for(
            lambda: fake.joins == [fake.epoch, EPOCH_2], timeout=3.0
        )

        stopping = asyncio.ensure_future(sub.stop())
        await asyncio.sleep(0.05)
        assert not any(c == "consumer-2" for c, _ in fake.leaves)
        fake.join_hold.set_result(None)
        await asyncio.wait_for(stopping, timeout=5.0)

        assert ("consumer-2", EPOCH_2) in fake.leaves

    async def test_reports_unsettled_join_during_teardown_and_leaves_late_member(
        self,
    ):
        from nexo.errors import FencedError

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(FencedError("epoch is fenced"))
        fake.next_join_epochs.extend([fake.epoch, EPOCH_2])
        loop = asyncio.get_running_loop()
        fake.join_hold = loop.create_future()
        sub = await _mock_group(fake).subscribe(lambda _d: None)
        await wait_for(
            lambda: fake.joins == [fake.epoch, EPOCH_2], timeout=3.0
        )

        with pytest.raises(Exception, match="join did not settle"):
            await sub.stop()
        await wait_for(lambda: sub.error is not None, timeout=1.0)
        fake.join_hold.set_result(None)
        await wait_for(
            lambda: ("consumer-2", EPOCH_2) in fake.leaves, timeout=3.0
        )

    async def test_accepts_successor_before_previous_update_response(self):
        from nexo.protocol.codec import FrameWriter
        from nexo.protocol.generated import HEADER_SIZE

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(_mock_page([(b"a", [1], RECEIPT_A)]))
        loop = asyncio.get_running_loop()
        fetch_gate = loop.create_future()
        reply_gate = loop.create_future()
        fetches_seen = 0

        async def fetch_handler():
            nonlocal fetches_seen
            fetches_seen += 1
            if fetches_seen == 1:
                await fetch_gate
                return _mock_page([(b"a", [2], RECEIPT_B)])
            return None

        first = True

        async def update_handler(items):
            nonlocal first
            if first:
                first = False
                assert items == [
                    {"receipt": RECEIPT_A, "completed": 1, "flags": 0}
                ]
                fetch_gate.set_result(None)
                await reply_gate
            return FrameWriter().begin().u32(0).finish(0, 0)[HEADER_SIZE:]

        fake.fetch_handler = fetch_handler
        fake.update_handler = update_handler
        got: list[int] = []

        async def callback(_d, meta=None):
            got.append(meta["seq"])

        sub = await _mock_group(fake).subscribe(callback, concurrency=2)
        try:
            await wait_for(lambda: got == [1, 2], timeout=3.0)
            assert len(fake.fetches) >= 2
            assert fake.joins == [fake.epoch]
            assert sub.error is None
            reply_gate.set_result(None)
            await wait_for(
                lambda: any(
                    i["receipt"] == RECEIPT_B and i["completed"] == 1
                    for u in fake.updates
                    for i in u["items"]
                ),
                timeout=3.0,
            )
        finally:
            if not reply_gate.done():
                reply_gate.set_result(None)
            await sub.stop()

    async def test_waits_for_uncooperative_callback_before_regranted_key(self):
        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page([(b"a", [1], RECEIPT_A), (b"b", [10], RECEIPT_C)])
        )
        loop = asyncio.get_running_loop()
        regrant_gate = loop.create_future()
        regrant_calls = 0

        async def fetch_handler():
            nonlocal regrant_calls
            if regrant_calls == 0:
                regrant_calls = 1
                await regrant_gate
                return _mock_page([(b"a", [2], RECEIPT_B)])
            if regrant_calls == 1:
                regrant_calls = 2
                return _mock_page([(b"b", [11], RECEIPT_D)])
            return None

        fake.fetch_handler = fetch_handler
        held_a = asyncio.Event()
        cancellation_seen = asyncio.Event()
        got_a: list[int] = []
        got_b: list[int] = []
        in_flight = 0
        peak = 0
        a_in_flight = 0
        a_peak = 0
        first_a = True

        async def callback(_d, meta=None):
            nonlocal in_flight, peak, a_in_flight, a_peak, first_a
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                seq = meta["seq"]
                if meta["key"] == b"a":
                    a_in_flight += 1
                    a_peak = max(a_peak, a_in_flight)
                    try:
                        got_a.append(seq)
                        if first_a:
                            first_a = False
                            while not held_a.is_set():
                                try:
                                    await held_a.wait()
                                except asyncio.CancelledError:
                                    cancellation_seen.set()
                    finally:
                        a_in_flight -= 1
                else:
                    got_b.append(seq)
            finally:
                in_flight -= 1

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=2, callback_timeout_ms=5000
        )
        try:
            await wait_for(lambda: got_a == [1] and got_b == [10], timeout=3.0)
            regrant_gate.set_result(None)
            await wait_for(lambda: sorted(got_b) == [10, 11], timeout=3.0)
            await asyncio.sleep(0.1)
            await wait_for(lambda: cancellation_seen.is_set(), timeout=3.0)
            assert got_a == [1]
            assert sub.error is None
            held_a.set()
            await wait_for(lambda: got_a == [1, 2], timeout=3.0)
            assert a_peak == 1
            assert peak <= 2
            assert fake.joins == [fake.epoch]
            assert all(
                not (i["receipt"] == RECEIPT_A and i["completed"] > 0)
                for u in fake.updates
                for i in u["items"]
            )
            await wait_for(
                lambda: any(
                    i["receipt"] == RECEIPT_B and i["completed"] == 1
                    for u in fake.updates
                    for i in u["items"]
                ),
                timeout=3.0,
            )
        finally:
            held_a.set()
            await sub.stop()

    async def test_waits_for_uncooperative_callback_before_regranted_key_across_epoch_reset(
        self,
    ):
        from nexo.errors import FencedError

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(_mock_page([(b"a", [1], RECEIPT_A)]))
        fake.next_join_epochs.extend([fake.epoch, EPOCH_2])
        phase = 0
        a_started = asyncio.Event()

        async def fetch_handler():
            nonlocal phase
            if phase == 0:
                await a_started.wait()
                phase = 1
                return FencedError("epoch is fenced")
            if phase == 1:
                phase = 2
                return _mock_page(
                    [(b"a", [1], RECEIPT_B), (b"b", [10], RECEIPT_C)]
                )
            return None

        fake.fetch_handler = fetch_handler
        held_a = asyncio.Event()
        cancellation_seen = asyncio.Event()
        got_a: list[int] = []
        got_b: list[int] = []
        a_in_flight = 0
        a_peak = 0
        first_a = True

        async def callback(_d, meta=None):
            nonlocal a_in_flight, a_peak, first_a
            if meta["key"] == b"a":
                a_in_flight += 1
                a_peak = max(a_peak, a_in_flight)
                try:
                    got_a.append(meta["seq"])
                    if first_a:
                        first_a = False
                        a_started.set()
                        while not held_a.is_set():
                            try:
                                await held_a.wait()
                            except asyncio.CancelledError:
                                cancellation_seen.set()
                finally:
                    a_in_flight -= 1
            else:
                got_b.append(meta["seq"])

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=2, callback_timeout_ms=5000
        )
        try:
            await wait_for(
                lambda: fake.joins == [fake.epoch, EPOCH_2], timeout=3.0
            )
            await wait_for(lambda: got_b == [10], timeout=3.0)
            await asyncio.sleep(0.1)
            await wait_for(lambda: cancellation_seen.is_set(), timeout=3.0)
            assert got_a == [1]
            assert sub.error is None
            held_a.set()
            await wait_for(lambda: got_a == [1, 1], timeout=3.0)
            assert a_peak == 1
            assert all(
                u["epoch"] != EPOCH_2
                or all(i["receipt"] != RECEIPT_A for i in u["items"])
                for u in fake.updates
            )
        finally:
            held_a.set()
            await sub.stop()

    async def test_releases_unstarted_blocked_batch_during_stop(self):
        from nexo.protocol.generated import FLAG_STREAM_S_UPDATE_BATCH_RELEASE

        fake = _FakeStreamConnection()
        fake.fetch_queue.append(_mock_page([(b"a", [1], RECEIPT_A)]))
        loop = asyncio.get_running_loop()
        regrant_gate = loop.create_future()
        regrant_sent = False

        async def fetch_handler():
            nonlocal regrant_sent
            if not regrant_sent:
                regrant_sent = True
                await regrant_gate
                return _mock_page([(b"a", [2], RECEIPT_B)])
            return None

        fake.fetch_handler = fetch_handler
        held_a = asyncio.Event()
        cancellation_seen = asyncio.Event()
        got_a: list[int] = []

        async def callback(_d, meta=None):
            if meta["key"] == b"a":
                got_a.append(meta["seq"])
                if meta["seq"] == 1:
                    while not held_a.is_set():
                        try:
                            await held_a.wait()
                        except asyncio.CancelledError:
                            cancellation_seen.set()

        sub = await _mock_group(fake).subscribe(
            callback, concurrency=2, callback_timeout_ms=5000
        )
        try:
            await wait_for(lambda: got_a == [1], timeout=3.0)
            regrant_gate.set_result(None)
            await wait_for(lambda: cancellation_seen.is_set(), timeout=3.0)
            await asyncio.sleep(0.1)
            assert got_a == [1]
            await sub.stop()
            assert got_a == [1]
            held_a.set()
            await asyncio.sleep(0.1)
            assert got_a == [1]
            assert any(
                i["receipt"] == RECEIPT_B
                and i["completed"] == 0
                and (i["flags"] & FLAG_STREAM_S_UPDATE_BATCH_RELEASE) != 0
                for u in fake.updates
                for i in u["items"]
            )
        finally:
            held_a.set()

    async def test_cleanup_storage_bounded_across_repeated_resets(self):
        from nexo.errors import FencedError

        fake = _FakeStreamConnection()
        epochs = [str(i + 1).zfill(32) for i in range(21)]
        fake.next_join_epochs.extend(epochs)
        resets = 0
        final_sent = False

        def fetch_handler():
            nonlocal resets, final_sent
            if resets >= 20:
                if not final_sent:
                    final_sent = True
                    return _mock_page([(b"a", [1], RECEIPT_C)])
                return None
            resets += 1
            return FencedError("epoch is fenced")

        fake.fetch_handler = fetch_handler

        got: list[int] = []
        sub = await _mock_group(fake).subscribe(
            lambda _d, meta=None: got.append(meta["seq"])
        )
        inner = sub._stop_fn.__self__
        try:
            await wait_for(lambda: len(fake.joins) == 21, timeout=8.0)
            await wait_for(lambda: got == [1], timeout=3.0)
            await wait_for(lambda: len(fake.leaves) == 20, timeout=3.0)
            await wait_for(
                lambda: any(
                    u["epoch"] == epochs[20]
                    and any(i["receipt"] == RECEIPT_C for i in u["items"])
                    for u in fake.updates
                ),
                timeout=3.0,
            )
            assert all(u["epoch"] in epochs for u in fake.updates)
            assert inner._cleanup_task is None or isinstance(
                inner._cleanup_task, asyncio.Task
            )
            assert not hasattr(inner, "_cleanup_tasks")
            assert sub.error is None
        finally:
            await sub.stop()

    async def test_keyless_drain_uses_bounded_reusable_workers(self):
        loop = asyncio.get_running_loop()
        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page(
                [(None, [i], f"{i:032x}") for i in range(1, 101)],
                lease_ms=30000,
            )
        )
        fake.fetch_handler = lambda: None

        counts: dict[str, int] = {}

        def factory(loop_, coro, context=None):
            name = getattr(coro, "__qualname__", type(coro).__name__)
            counts[name] = counts.get(name, 0) + 1
            if context is not None:
                return asyncio.Task(coro, loop=loop_, context=context)
            return asyncio.Task(coro, loop=loop_)

        in_flight = 0
        peak = 0
        got: list[int] = []

        async def callback(d, meta=None):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            got.append(meta["seq"])
            in_flight -= 1

        previous_factory = loop.get_task_factory()
        loop.set_task_factory(factory)
        try:
            sub = await _mock_group(fake).subscribe(
                callback,
                concurrency=1,
                batch_size=100,
                max_buffered_messages=200,
            )
            try:
                await wait_for(lambda: len(got) == 100, timeout=5.0)
                await sub.stop()
                assert sorted(got) == list(range(1, 101))
                assert peak == 1
                assert len(fake.updates) <= 4
                assert counts.get(callback.__qualname__, 0) == 0
                worker_starts = counts.get(
                    "StreamSubscription._run_worker", 0
                )
                assert 1 <= worker_starts <= 4
            finally:
                await sub.stop()
        finally:
            loop.set_task_factory(previous_factory)

    async def test_released_batches_drop_payload_and_ready_references(self):
        fake = _FakeStreamConnection()
        fake.fetch_queue.append(
            _mock_page(
                [(None, [i], f"{i:032x}") for i in range(1, 9)],
                lease_ms=30000,
            )
        )
        fake.fetch_handler = lambda: None

        held = asyncio.Event()
        started = 0

        async def callback(_d, meta=None):
            nonlocal started
            started += 1
            await held.wait()

        sub = await _mock_group(fake).subscribe(callback, batch_size=8)
        inner = sub._stop_fn.__self__
        try:
            await wait_for(lambda: started == 8, timeout=3.0)
            batches = list(inner._batches.values())
            assert len(batches) == 8
            held.set()
            await sub.stop()
            assert inner._buffered_messages == 0
            assert inner._buffered_bytes == 0
            assert all(b.messages == [] for b in batches)
            assert all(
                slot is None for slot in inner._ready[: inner._ready_head]
            )
        finally:
            held.set()
            await sub.stop()
