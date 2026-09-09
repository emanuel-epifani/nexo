from __future__ import annotations

import asyncio
from pathlib import Path
import shutil
import uuid

import pytest

from nexo import (
    NexoClient,
    NotConnectedError,
    ProvisionOutcome,
    ResourceConfigurationConflictError,
    ResourceNotFoundError,
)
from tests.utils.wait_for import wait_for


async def _create_stream(nexo: NexoClient, name: str, **retention):
    await nexo.stream.create(name, **retention)
    return await nexo.stream.get(name)


STREAM_DATA_DIR = Path(__file__).resolve().parents[4] / "data" / "streams"


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

    async def test_stop_exposes_ack_failure(self, nexo: NexoClient):
        name = f"stream-stop-ack-failure-{uuid.uuid4()}"
        group = "stop-ack-failure-group"
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
        with pytest.raises(Exception, match=r"stream ACK request\(s\) failed"):
            await stopping

        await nexo.stream.delete(name)

    async def test_active_subscription_rejoins_after_ack_failure(self, nexo: NexoClient):
        name = f"stream-active-ack-failure-{uuid.uuid4()}"
        group = "active-ack-failure-group"
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

    async def test_preserve_ordering_default_concurrency(self, nexo: NexoClient):
        name = f"stream-order-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)

        received: list[int] = []
        sub = await stream.group("order-group").subscribe(lambda d: received.append(d["i"]))

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

        async def first_callback(data):
            if data["id"] == 1:
                fast_finished.set()
            elif data["id"] == 2:
                slow_started.set()
                await release_slow.wait()

        first = await stream.group(group).subscribe(
            first_callback,
            batch_size=2,
            concurrency=2,
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
            await wait_for(lambda: received_by_second == [3], timeout=2.0)
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

    async def test_publish_storage_write_failure(self, nexo: NexoClient):
        name = f"stream-write-failure-{uuid.uuid4()}"
        stream = await _create_stream(nexo, name)
        stream_path = STREAM_DATA_DIR / name
        shutil.rmtree(stream_path)
        stream_path.write_text("not-a-directory")

        try:
            with pytest.raises(Exception, match="Storage append failed"):
                await stream.publish({"x": 1})
        finally:
            stream_path.unlink(missing_ok=True)
            await nexo.stream.delete(name)

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
