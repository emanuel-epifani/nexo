from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


async def _destroy_and_reconnect(client: NexoClient) -> None:
    conn = client._conn
    if conn._writer is not None:
        conn._writer.close()
    await wait_for(lambda: not conn.is_connected)
    await wait_for(lambda: conn.is_connected, timeout=5.0)


async def _send_with_retry(fn, timeout: float = 5.0):
    import time

    last_error = None
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            return await fn()
        except Exception as e:
            last_error = e
            await asyncio.sleep(0.2)
    if last_error:
        raise last_error


@pytest.mark.asyncio
class TestReconnection:
    async def test_pubsub_auto_resubscribe(self, nexo: NexoClient):
        topic = f"reconnect-pubsub-{uuid.uuid4()}"
        received: list[str] = []

        await nexo.pubsub(topic).subscribe(lambda m: received.append(m))

        await _destroy_and_reconnect(nexo)

        await _send_with_retry(lambda: nexo.pubsub(topic).publish("after-crash"))

        await wait_for(lambda: "after-crash" in received)

    async def test_queue_resume_consuming(self, nexo: NexoClient):
        q_name = f"reconnect-queue-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()
        received: list = []

        await q.subscribe(lambda msg: received.append(msg))

        await _destroy_and_reconnect(nexo)

        await _send_with_retry(lambda: q.push({"status": "recovered"}))

        await wait_for(lambda: {"status": "recovered"} in received)

    async def test_stream_resume_consuming(self, nexo: NexoClient):
        stream_name = f"reconnect-stream-{uuid.uuid4()}"
        group = f"g-{uuid.uuid4()}"
        await nexo.stream(stream_name).create()
        received: list = []

        await nexo.stream(stream_name).subscribe(group, lambda m: received.append(m))

        await _destroy_and_reconnect(nexo)

        await _send_with_retry(lambda: nexo.stream(stream_name).publish({"status": "recovered"}))

        await wait_for(lambda: {"status": "recovered"} in received, timeout=10.0)

    async def test_queue_inflight_redelivered(self, nexo: NexoClient):
        q_name = f"reconnect-inflight-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create({"visibility_timeout_ms": 2000})

        await q.push({"important": True})

        received: list = []
        first_delivery = True

        async def cb(msg):
            nonlocal first_delivery
            received.append(msg)
            if first_delivery:
                first_delivery = False
                conn = nexo._conn
                if conn._writer is not None:
                    conn._writer.close()
                raise Exception("crash before ack")

        await q.subscribe(cb)

        await wait_for(lambda: len(received) >= 2, timeout=10.0)

        assert received[0] == {"important": True}
        assert received[1] == {"important": True}

    async def test_queue_push_during_disconnect_fails(self, nexo: NexoClient):
        q_name = f"reconnect-push-err-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        conn = nexo._conn
        if conn._writer is not None:
            conn._writer.close()
        await wait_for(lambda: not conn.is_connected)

        with pytest.raises(Exception):
            await q.push({"test": True})

        await wait_for(lambda: conn.is_connected, timeout=5.0)
        await _send_with_retry(lambda: q.push({"test": "after-reconnect"}))

    async def test_queue_survive_double_crash(self, nexo: NexoClient):
        q_name = f"reconnect-double-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()
        received: list = []

        await q.subscribe(lambda msg: received.append(msg))

        await _destroy_and_reconnect(nexo)
        await _destroy_and_reconnect(nexo)

        await _send_with_retry(lambda: q.push({"value": "once"}))

        await wait_for(lambda: {"value": "once"} in received)

        await asyncio.sleep(0.5)
        match_count = sum(1 for m in received if m == {"value": "once"})
        assert match_count == 1

    async def test_queue_stop_during_disconnect(self, nexo: NexoClient):
        q_name = f"reconnect-stop-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()
        received: list = []

        sub = await q.subscribe(lambda msg: received.append(msg))

        conn = nexo._conn
        if conn._writer is not None:
            conn._writer.close()
        await wait_for(lambda: not conn.is_connected)

        sub["stop"]()

        await wait_for(lambda: conn.is_connected, timeout=5.0)

        await _send_with_retry(lambda: q.push({"shouldNotArrive": True}))

        await asyncio.sleep(1.0)
        assert received == []

    async def test_pubsub_topics_before_and_after_crash(self, nexo: NexoClient):
        topic_before = f"reconnect-ps-before-{uuid.uuid4()}"
        topic_after = f"reconnect-ps-after-{uuid.uuid4()}"
        received_before: list[str] = []
        received_after: list[str] = []

        await nexo.pubsub(topic_before).subscribe(lambda m: received_before.append(m))

        await _destroy_and_reconnect(nexo)

        await nexo.pubsub(topic_after).subscribe(lambda m: received_after.append(m))

        await _send_with_retry(lambda: nexo.pubsub(topic_before).publish("msg-before"))
        await nexo.pubsub(topic_after).publish("msg-after")

        await wait_for(lambda: "msg-before" in received_before)
        await wait_for(lambda: "msg-after" in received_after)

    async def test_stream_inflight_redelivered(self, nexo: NexoClient):
        stream_name = f"reconnect-stream-inflight-{uuid.uuid4()}"
        group = f"g-inflight-{uuid.uuid4()}"
        await nexo.stream(stream_name).create()

        await nexo.stream(stream_name).publish({"important": True})

        received: list = []
        first_delivery = True

        async def cb(msg):
            nonlocal first_delivery
            received.append(msg)
            if first_delivery:
                first_delivery = False
                conn = nexo._conn
                if conn._writer is not None:
                    conn._writer.close()
                raise Exception("crash before ack")

        await nexo.stream(stream_name).subscribe(group, cb)

        await wait_for(lambda: len(received) >= 2, timeout=30.0)

        assert received[0] == {"important": True}
        assert received[1] == {"important": True}

    async def test_stream_same_key_serial_ordering(self, nexo: NexoClient):
        stream_name = f"key-order-tcp-{uuid.uuid4()}"
        group = f"g-key-order-{uuid.uuid4()}"
        await nexo.stream(stream_name).create()

        await nexo.stream(stream_name).publish({"n": 1}, {"key": "order-key"})
        await nexo.stream(stream_name).publish({"n": 2}, {"key": "order-key"})
        await nexo.stream(stream_name).publish({"n": 3}, {"key": "order-key"})

        received: list[int] = []
        in_flight = False

        def cb(data):
            nonlocal in_flight
            if in_flight:
                raise Exception("Concurrent same-key delivery detected")
            in_flight = True
            received.append(data["n"])
            in_flight = False

        sub = await nexo.stream(stream_name).subscribe(
            group, cb,
            {"batch_size": 1, "concurrency": 1},
        )

        await wait_for(lambda: len(received) == 3, timeout=10.0)

        assert received == [1, 2, 3]

        await sub["stop"]()
