from __future__ import annotations

import asyncio
from pathlib import Path
import shutil
import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


STREAM_DATA_DIR = Path(__file__).resolve().parents[4] / "data" / "streams"


@pytest.mark.asyncio
class TestStream:
    async def test_happy_path_publish_subscribe(self, nexo: NexoClient):
        topic = f"stream-basic-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list = []
        sub = await nexo.stream(topic).subscribe("g1", lambda data: received.append(data))

        await nexo.stream(topic).publish({"id": 1})
        await nexo.stream(topic).publish({"id": 2})

        await wait_for(lambda: len(received) == 2)
        await sub.stop()

    async def test_subscribe_nonexistent_stream(self, nexo: NexoClient):
        topic = f"stream-missing-{uuid.uuid4()}"
        with pytest.raises(Exception):
            await nexo.stream(topic).subscribe("missing-group", lambda _: None)

    async def test_independent_consumer_groups(self, nexo: NexoClient):
        topic = f"stream-groups-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        client_a = await NexoClient.connect()
        client_b = await NexoClient.connect()

        try:
            recv_a: list = []
            recv_b: list = []

            sub_a = await client_a.stream(topic).subscribe("group_A", lambda d: recv_a.append(d))
            sub_b = await client_b.stream(topic).subscribe("group_B", lambda d: recv_b.append(d))

            await nexo.stream(topic).publish({"msg": "hello"})

            await wait_for(lambda: len(recv_a) == 1 and len(recv_b) == 1)

            await sub_a.stop()
            await sub_b.stop()
        finally:
            client_a.disconnect()
            client_b.disconnect()

    async def test_same_group_no_duplicates(self, nexo: NexoClient):
        topic = f"parallel-consumers-{uuid.uuid4()}"
        group = "parallel_group"
        await nexo.stream(topic).create()

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

            sub_a = await client_a.stream(topic).subscribe(group, cb_a)
            sub_b = await client_b.stream(topic).subscribe(group, cb_b)

            for i in range(100):
                await nexo.stream(topic).publish({"id": i})

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
            topic = f"stream-disconnect-{uuid.uuid4()}"
            group = "group_disconnect"
            await nexo.stream(topic).create()

            all_received: set[int] = set()
            track = lambda d: all_received.add(d["i"])

            sub_a = await temp_a.stream(topic).subscribe(group, track)
            sub_b = await temp_b.stream(topic).subscribe(group, track)

            for i in range(20):
                await nexo.stream(topic).publish({"i": i})
            await wait_for(lambda: len(all_received) == 20)

            temp_a.disconnect()

            await asyncio.sleep(0.5)

            for i in range(20, 60):
                await nexo.stream(topic).publish({"i": i})

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
        topic = f"stream-history-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        for i in range(5):
            await nexo.stream(topic).publish({"i": i})

        received: list = []
        sub = await nexo.stream(topic).subscribe("history-group", lambda d: received.append(d))

        await wait_for(lambda: len(received) == 5)
        assert received[0]["i"] == 0
        assert received[4]["i"] == 4

        await sub.stop()

    async def test_stop_subscription_quickly(self, nexo: NexoClient):
        topic = f"stream-fast-stop-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        sub = await nexo.stream(topic).subscribe("fast-stop-group", lambda _: None)

        start = asyncio.get_event_loop().time()
        await sub.stop()
        elapsed = asyncio.get_event_loop().time() - start

        assert elapsed < 2.0

    async def test_preserve_ordering_default_concurrency(self, nexo: NexoClient):
        topic = f"stream-order-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list[int] = []
        sub = await nexo.stream(topic).subscribe("order-group", lambda d: received.append(d["i"]))

        for i in range(30):
            await nexo.stream(topic).publish({"i": i})

        await wait_for(lambda: len(received) == 30, timeout=10.0)

        for i in range(30):
            assert received[i] == i

        await sub.stop()

    async def test_parallel_concurrency_gt_1(self, nexo: NexoClient):
        topic = f"stream-concurrent-{uuid.uuid4()}"
        await nexo.stream(topic).create()

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

        sub = await nexo.stream(topic).subscribe(
            "concurrent-group", cb,
            {"batch_size": COUNT, "concurrency": CONCURRENCY},
        )

        for i in range(COUNT):
            await nexo.stream(topic).publish({"i": i})

        start = asyncio.get_event_loop().time()
        await wait_for(lambda: len(received) == COUNT, timeout=10.0)
        elapsed = asyncio.get_event_loop().time() - start

        assert len(set(received)) == COUNT
        assert max_in_flight > 1
        assert elapsed < COUNT * CALLBACK_DELAY * 0.6

        await sub.stop()

    async def test_seek_beginning_and_end(self, nexo: NexoClient):
        topic = f"stream-seek-{uuid.uuid4()}"
        group = "seek-group"
        await nexo.stream(topic).create()

        for i in range(10):
            await nexo.stream(topic).publish({"i": i})

        await nexo.stream(topic).seek(group, "end")

        received_end: list = []
        sub_end = await nexo.stream(topic).subscribe(group, lambda d: received_end.append(d))

        await nexo.stream(topic).publish({"i": 10})
        await wait_for(lambda: len(received_end) == 1)
        assert received_end[0]["i"] == 10
        await sub_end.stop()

        await nexo.stream(topic).seek(group, "beginning")

        received_start: list = []
        sub_start = await nexo.stream(topic).subscribe(group, lambda d: received_start.append(d))

        await wait_for(lambda: len(received_start) == 11)
        assert received_start[0]["i"] == 0
        assert received_start[10]["i"] == 10

        await sub_start.stop()

    async def test_stop_quickly_during_long_poll_idle(self, nexo: NexoClient):
        topic = f"stream-stop-idle-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        sub = await nexo.stream(topic).subscribe("idle-stop-group", lambda _: None)

        await asyncio.sleep(0.2)

        start = asyncio.get_event_loop().time()
        await sub.stop()
        elapsed = asyncio.get_event_loop().time() - start

        assert elapsed < 2.0

    async def test_no_delivery_after_stop(self, nexo: NexoClient):
        topic = f"stream-stop-nodeliver-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list = []
        sub = await nexo.stream(topic).subscribe("nodeliver-group", lambda d: received.append(d))

        await nexo.stream(topic).publish({"id": 1})
        await wait_for(lambda: len(received) == 1)

        await sub.stop()

        await nexo.stream(topic).publish({"id": 2})
        await asyncio.sleep(0.5)

        assert len(received) == 1

    async def test_publish_batch_returns_seq_numbers(self, nexo: NexoClient):
        topic = f"stream-batch-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        seqs = await nexo.stream(topic).publish_batch([
            {"data": "msg1"},
            {"data": "msg2"},
            {"data": "msg3"},
        ])

        assert len(seqs) == 3
        assert seqs[0] == 1
        assert seqs[1] == 2
        assert seqs[2] == 3

    async def test_publish_batch_with_keys(self, nexo: NexoClient):
        topic = f"stream-batch-keys-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list[dict] = []
        sub = await nexo.stream(topic).subscribe(
            "g-batch-keys",
            lambda data, meta: received.append({"data": data, "key": meta.get("key")}),
        )

        await nexo.stream(topic).publish_batch([
            {"data": "msg1", "key": "key-A"},
            {"data": "msg2", "key": "key-B"},
            {"data": "msg3"},
        ])

        await wait_for(lambda: len(received) == 3)
        await sub.stop()

    async def test_publish_with_string_key(self, nexo: NexoClient):
        topic = f"stream-pub-key-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list[dict] = []
        sub = await nexo.stream(topic).subscribe(
            "g-pub-key",
            lambda data, meta: received.append({"data": data, "key": meta.get("key")}),
        )

        seq = await nexo.stream(topic).publish({"x": 1}, {"key": "my-key"})
        assert seq > 0

        await wait_for(lambda: len(received) == 1)
        await sub.stop()

        assert received[0]["data"] == {"x": 1}
        assert received[0]["key"] is not None
        assert bytes(received[0]["key"]).decode("utf-8") == "my-key"

        await nexo.stream(topic).delete()

    async def test_publish_with_bytes_key(self, nexo: NexoClient):
        topic = f"stream-pub-rawkey-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list[dict] = []
        sub = await nexo.stream(topic).subscribe(
            "g-pub-rawkey",
            lambda data, meta: received.append({"data": data, "key": meta.get("key")}),
        )

        raw_key = b"\x01\x02\xff"
        await nexo.stream(topic).publish("payload", {"key": raw_key})

        await wait_for(lambda: len(received) == 1)
        await sub.stop()

        assert received[0]["data"] == "payload"
        assert received[0]["key"] is not None
        assert bytes(received[0]["key"]) == raw_key

        await nexo.stream(topic).delete()

    async def test_publish_bytes_data(self, nexo: NexoClient):
        topic = f"stream-pub-uint8-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        received: list = []
        sub = await nexo.stream(topic).subscribe("g-pub-uint8", lambda data: received.append(data))

        payload = b"\x01\x02\xff\x00"
        await nexo.stream(topic).publish(payload)

        await wait_for(lambda: len(received) == 1)
        await sub.stop()

        assert isinstance(received[0], (bytes, bytearray))
        assert bytes(received[0]) == payload

        await nexo.stream(topic).delete()

    async def test_empty_publish_batch(self, nexo: NexoClient):
        topic = f"stream-batch-empty-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        seqs = await nexo.stream(topic).publish_batch([])
        assert len(seqs) == 0

    # ── Edge cases ──────────────────────────────────────────────

    async def test_exists_true_after_create_false_before(self, nexo: NexoClient):
        topic = f"stream-exists-{uuid.uuid4()}"
        assert await nexo.stream(topic).exists() is False
        await nexo.stream(topic).create()
        assert await nexo.stream(topic).exists() is True
        await nexo.stream(topic).delete()
        assert await nexo.stream(topic).exists() is False

    async def test_create_idempotent(self, nexo: NexoClient):
        topic = f"stream-idempotent-{uuid.uuid4()}"
        await nexo.stream(topic).create()
        await nexo.stream(topic).create()
        assert await nexo.stream(topic).exists() is True
        await nexo.stream(topic).delete()

    async def test_publish_nonexistent_stream_fails(self, nexo: NexoClient):
        topic = f"stream-pub-missing-{uuid.uuid4()}"
        with pytest.raises(Exception):
            await nexo.stream(topic).publish({"x": 1})

    async def test_publish_storage_write_failure(self, nexo: NexoClient):
        topic = f"stream-write-failure-{uuid.uuid4()}"
        stream = nexo.stream(topic)
        topic_path = STREAM_DATA_DIR / topic
        await stream.create()
        shutil.rmtree(topic_path)
        topic_path.write_text("not-a-directory")

        try:
            with pytest.raises(Exception, match="Storage append failed"):
                await stream.publish({"x": 1})
        finally:
            topic_path.unlink(missing_ok=True)
            await stream.delete()

    async def test_operations_after_delete_fail(self, nexo: NexoClient):
        topic = f"stream-del-ops-{uuid.uuid4()}"
        await nexo.stream(topic).create()
        await nexo.stream(topic).delete()
        with pytest.raises(Exception):
            await nexo.stream(topic).publish({"x": 1})
        with pytest.raises(Exception):
            await nexo.stream(topic).subscribe("g-del", lambda _: None)

    async def test_peek_dlt_empty_returns_empty(self, nexo: NexoClient):
        topic = f"stream-dlt-empty-{uuid.uuid4()}"
        group = "g-dlt-empty"
        await nexo.stream(topic).create()
        sub = await nexo.stream(topic).subscribe(group, lambda _: None)
        await sub.stop()
        entries = await nexo.stream(topic).peek_dlt(group, 10, 0)
        assert entries == []
        await nexo.stream(topic).delete()

    async def test_purge_dlt_empty_returns_zero(self, nexo: NexoClient):
        topic = f"stream-dlt-purge-empty-{uuid.uuid4()}"
        group = "g-dlt-purge"
        await nexo.stream(topic).create()
        sub = await nexo.stream(topic).subscribe(group, lambda _: None)
        await sub.stop()
        count = await nexo.stream(topic).purge_dlt(group)
        assert count == 0
        await nexo.stream(topic).delete()

    async def test_resubscribe_same_group_after_stop(self, nexo: NexoClient):
        topic = f"stream-resub-{uuid.uuid4()}"
        group = "g-resub"
        await nexo.stream(topic).create()

        recv1: list = []
        sub1 = await nexo.stream(topic).subscribe(group, lambda d: recv1.append(d))
        await nexo.stream(topic).publish({"i": 1})
        await nexo.stream(topic).publish({"i": 2})
        await wait_for(lambda: len(recv1) == 2)
        await sub1.stop()

        await nexo.stream(topic).publish({"i": 3})

        recv2: list = []
        sub2 = await nexo.stream(topic).subscribe(group, lambda d: recv2.append(d))
        await wait_for(lambda: len(recv2) == 1)
        assert recv2[0]["i"] == 3
        await sub2.stop()

        await nexo.stream(topic).delete()

    async def test_multiple_groups_simultaneous_delivery(self, nexo: NexoClient):
        topic = f"stream-multi-groups-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        client_a = await NexoClient.connect()
        client_b = await NexoClient.connect()
        try:
            recv_a: list = []
            recv_b: list = []
            recv_c: list = []

            sub_a = await client_a.stream(topic).subscribe("multi-a", lambda d: recv_a.append(d))
            sub_b = await client_b.stream(topic).subscribe("multi-b", lambda d: recv_b.append(d))
            sub_c = await nexo.stream(topic).subscribe("multi-c", lambda d: recv_c.append(d))

            for i in range(5):
                await nexo.stream(topic).publish({"i": i})

            await wait_for(lambda: len(recv_a) == 5 and len(recv_b) == 5 and len(recv_c) == 5)

            await sub_a.stop()
            await sub_b.stop()
            await sub_c.stop()
        finally:
            client_a.disconnect()
            client_b.disconnect()

        await nexo.stream(topic).delete()
