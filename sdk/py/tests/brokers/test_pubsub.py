from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


@pytest.mark.asyncio
class TestPubSub:
    async def test_exact_match_and_ignore_noise(self, nexo: NexoClient):
        target = f"chat/room1-{uuid.uuid4()}"
        noise = f"chat/room2-{uuid.uuid4()}"
        received: list = []

        await nexo.pubsub(target).subscribe(lambda data: received.append(data))
        await nexo.pubsub(target).publish({"msg": "target"})
        await nexo.pubsub(noise).publish({"msg": "noise"})

        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "target"
        await nexo.pubsub(target).unsubscribe()

    async def test_single_level_wildcard(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"home-{base_id}/+/temp"
        received: list = []

        await nexo.pubsub(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub(f"home-{base_id}/kitchen/temp").publish({"id": "match-1"})
        await nexo.pubsub(f"home-{base_id}/garage/temp").publish({"id": "match-2"})
        await nexo.pubsub(f"home-{base_id}/kitchen/light").publish({"id": "fail-suffix"})
        await nexo.pubsub(f"home-{base_id}/kitchen/cupboard/temp").publish({"id": "fail-deep"})
        await nexo.pubsub(f"office-{base_id}/kitchen/temp").publish({"id": "fail-prefix"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["match-1", "match-2"]
        await nexo.pubsub(pattern).unsubscribe()

    async def test_clear_retained(self, nexo: NexoClient):
        topic = f"clear-retained-{uuid.uuid4()}"

        await nexo.pubsub(topic).publish("dark", {"retain": True})

        received: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: received.append(data))
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "dark"

        await nexo.pubsub(topic).clear()
        await nexo.pubsub(topic).unsubscribe()

        after_clear: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: after_clear.append(data))
        await asyncio.sleep(0.2)
        assert after_clear == []

    async def test_reject_invalid_ttl(self, nexo: NexoClient):
        topic = f"ttl-invalid-{uuid.uuid4()}"

        with pytest.raises(ValueError, match="Invalid ttl"):
            await nexo.pubsub(topic).publish("x", {"ttl": -1})

        with pytest.raises(ValueError, match="Invalid ttl"):
            await nexo.pubsub(topic).publish("x", {"ttl": 1.5})

    async def test_multi_level_wildcard(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"sensors-{base_id}/#"
        received: list = []

        await nexo.pubsub(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub(f"sensors-{base_id}/main").publish({"id": "root"})
        await nexo.pubsub(f"sensors-{base_id}/a/b/c").publish({"id": "deep"})
        await nexo.pubsub(f"other-{base_id}/main").publish({"id": "fail-prefix"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["deep", "root"]
        await nexo.pubsub(pattern).unsubscribe()

    async def test_async_callback(self, nexo: NexoClient):
        topic = f"async-cb-{uuid.uuid4()}"
        received: list = []

        async def async_handler(data):
            await asyncio.sleep(0.01)
            received.append(data)

        await nexo.pubsub(topic).subscribe(async_handler)
        await nexo.pubsub(topic).publish({"msg": "hello"})
        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "hello"
        await nexo.pubsub(topic).unsubscribe()

    async def test_slow_callback_does_not_block_store(self, nexo: NexoClient):
        pubsub_topic = f"slow-cb-{uuid.uuid4()}"
        store_key = f"store-key-{uuid.uuid4()}"
        callback_started = asyncio.Event()

        async def slow_handler(data):
            callback_started.set()
            await asyncio.sleep(0.5)

        await nexo.pubsub(pubsub_topic).subscribe(slow_handler)
        await nexo.pubsub(pubsub_topic).publish({"msg": "trigger"})

        await wait_for(callback_started.is_set)

        import time
        t0 = time.monotonic()
        await nexo.store.map.set(store_key, "value")
        elapsed = time.monotonic() - t0

        assert elapsed < 0.3, f"Store operation blocked by PubSub callback: {elapsed:.3f}s"
        await nexo.pubsub(pubsub_topic).unsubscribe()

    async def test_parallel_subscriptions(self, nexo: NexoClient):
        topic_a = f"par-a-{uuid.uuid4()}"
        topic_b = f"par-b-{uuid.uuid4()}"
        order: list[str] = []

        async def handler_a(data):
            await asyncio.sleep(0.1)
            order.append("a")

        async def handler_b(data):
            order.append("b")

        await nexo.pubsub(topic_a).subscribe(handler_a)
        await nexo.pubsub(topic_b).subscribe(handler_b)

        await nexo.pubsub(topic_a).publish({"msg": "x"})
        await nexo.pubsub(topic_b).publish({"msg": "y"})

        await wait_for(lambda: len(order) == 2)
        assert order == ["b", "a"], f"Subscription B should not wait for A: {order}"
        await nexo.pubsub(topic_a).unsubscribe()
        await nexo.pubsub(topic_b).unsubscribe()

    # ── Edge cases ──────────────────────────────────────────────

    async def test_retained_delivered_to_new_subscriber(self, nexo: NexoClient):
        topic = f"retained-new-{uuid.uuid4()}"

        await nexo.pubsub(topic).publish("retained-value", {"retain": True})

        received: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: received.append(data))
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "retained-value"

        await nexo.pubsub(topic).unsubscribe()
        await nexo.pubsub(topic).clear()

    async def test_retained_overwrite_on_second_publish(self, nexo: NexoClient):
        topic = f"retained-overwrite-{uuid.uuid4()}"

        await nexo.pubsub(topic).publish("first", {"retain": True})
        await nexo.pubsub(topic).publish("second", {"retain": True})

        received: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: received.append(data))
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "second"

        await nexo.pubsub(topic).unsubscribe()
        await nexo.pubsub(topic).clear()

    async def test_retained_not_delivered_after_ttl_expiry(self, nexo: NexoClient):
        topic = f"retained-ttl-{uuid.uuid4()}"

        await nexo.pubsub(topic).publish("temp-retained", {"retain": True, "ttl": 1})

        await asyncio.sleep(1.2)

        received: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: received.append(data))
        await asyncio.sleep(0.3)
        assert received == []

        await nexo.pubsub(topic).unsubscribe()

    async def test_unsubscribe_stops_delivery(self, nexo: NexoClient):
        topic = f"unsub-stop-{uuid.uuid4()}"

        received: list = []
        await nexo.pubsub(topic).subscribe(lambda data: received.append(data))

        await nexo.pubsub(topic).publish({"msg": "before"})
        await wait_for(lambda: len(received) == 1)

        await nexo.pubsub(topic).unsubscribe()

        await nexo.pubsub(topic).publish({"msg": "after"})
        await asyncio.sleep(0.3)
        assert len(received) == 1

    async def test_combined_wildcards_plus_and_hash(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"combo-{base_id}/+/b/#"

        received: list = []
        await nexo.pubsub(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub(f"combo-{base_id}/x/b/y/z").publish({"id": "deep-match"})
        await nexo.pubsub(f"combo-{base_id}/x/b").publish({"id": "shallow-match"})
        await nexo.pubsub(f"combo-{base_id}/x/c/y").publish({"id": "fail-wrong-segment"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["deep-match", "shallow-match"]

        await nexo.pubsub(pattern).unsubscribe()

    async def test_broadcast_to_3_plus_subscribers(self, nexo: NexoClient):
        topic = f"broadcast-{uuid.uuid4()}"

        client1 = await NexoClient.connect()
        client2 = await NexoClient.connect()
        try:
            recv1: list = []
            recv2: list = []
            recv3: list = []

            await client1.pubsub(topic).subscribe(lambda d: recv1.append(d))
            await client2.pubsub(topic).subscribe(lambda d: recv2.append(d))
            await nexo.pubsub(topic).subscribe(lambda d: recv3.append(d))

            await nexo.pubsub(topic).publish({"msg": "broadcast"})

            await wait_for(lambda: len(recv1) == 1 and len(recv2) == 1 and len(recv3) == 1)
            assert recv1[0]["msg"] == "broadcast"
            assert recv2[0]["msg"] == "broadcast"
            assert recv3[0]["msg"] == "broadcast"

            await client1.pubsub(topic).unsubscribe()
            await client2.pubsub(topic).unsubscribe()
            await nexo.pubsub(topic).unsubscribe()
        finally:
            client1.disconnect()
            client2.disconnect()

    async def test_disconnect_cleanup_does_not_break_topic(self, nexo: NexoClient):
        topic = f"disconnect-cleanup-{uuid.uuid4()}"

        temp_client = await NexoClient.connect()
        await temp_client.pubsub(topic).subscribe(lambda _: None)
        await asyncio.sleep(0.1)

        temp_client.disconnect()
        await asyncio.sleep(0.3)

        received: list = []
        await nexo.pubsub(topic).subscribe(lambda d: received.append(d))
        await nexo.pubsub(topic).publish({"msg": "after-disconnect"})

        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "after-disconnect"

        await nexo.pubsub(topic).unsubscribe()

    async def test_retained_with_wildcard_plus(self, nexo: NexoClient):
        base_id = uuid.uuid4()

        await nexo.pubsub(f"ret-plus-{base_id}/x").publish("val-x", {"retain": True})
        await nexo.pubsub(f"ret-plus-{base_id}/y").publish("val-y", {"retain": True})

        received: list[str] = []
        await nexo.pubsub(f"ret-plus-{base_id}/+").subscribe(lambda d: received.append(d))

        await wait_for(lambda: len(received) == 2)
        assert sorted(received) == ["val-x", "val-y"]

        await nexo.pubsub(f"ret-plus-{base_id}/+").unsubscribe()
        await nexo.pubsub(f"ret-plus-{base_id}/x").clear()
        await nexo.pubsub(f"ret-plus-{base_id}/y").clear()

    async def test_retained_with_wildcard_hash(self, nexo: NexoClient):
        base_id = uuid.uuid4()

        await nexo.pubsub(f"ret-hash-{base_id}/a/b/c").publish("deep", {"retain": True})

        received: list[str] = []
        await nexo.pubsub(f"ret-hash-{base_id}/#").subscribe(lambda d: received.append(d))

        await wait_for(lambda: len(received) == 1)
        assert received[0] == "deep"

        await nexo.pubsub(f"ret-hash-{base_id}/#").unsubscribe()
        await nexo.pubsub(f"ret-hash-{base_id}/a/b/c").clear()
