from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient, SlowConsumerError
from tests.utils.wait_for import wait_for


@pytest.mark.asyncio
class TestPubSub:
    async def test_exact_match_and_ignore_noise(self, nexo: NexoClient):
        target = f"chat/room1-{uuid.uuid4()}"
        noise = f"chat/room2-{uuid.uuid4()}"
        received: list = []

        sub = await nexo.pubsub.topic(target).subscribe(lambda data: received.append(data))
        await nexo.pubsub.topic(target).publish({"msg": "target"})
        await nexo.pubsub.topic(noise).publish({"msg": "noise"})

        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "target"
        await sub.stop()

    async def test_single_level_wildcard(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"home-{base_id}/+/temp"
        received: list = []

        sub = await nexo.pubsub.pattern(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub.topic(f"home-{base_id}/kitchen/temp").publish({"id": "match-1"})
        await nexo.pubsub.topic(f"home-{base_id}/garage/temp").publish({"id": "match-2"})
        await nexo.pubsub.topic(f"home-{base_id}/kitchen/light").publish({"id": "fail-suffix"})
        await nexo.pubsub.topic(f"home-{base_id}/kitchen/cupboard/temp").publish({"id": "fail-deep"})
        await nexo.pubsub.topic(f"office-{base_id}/kitchen/temp").publish({"id": "fail-prefix"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["match-1", "match-2"]
        await sub.stop()

    async def test_clear_retained(self, nexo: NexoClient):
        topic = f"clear-retained-{uuid.uuid4()}"

        await nexo.pubsub.topic(topic).publish("dark", retain=True)

        received: list[str] = []
        first = await nexo.pubsub.topic(topic).subscribe(
            lambda data: received.append(data)
        )
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "dark"

        await nexo.pubsub.topic(topic).clear_retained()
        await first.stop()

        after_clear: list[str] = []
        second = await nexo.pubsub.topic(topic).subscribe(
            lambda data: after_clear.append(data)
        )
        await asyncio.sleep(0.2)
        assert after_clear == []
        await second.stop()

    async def test_clear_retained_does_not_deliver_to_current_subscribers(self, nexo: NexoClient):
        topic = f"clear-spurious-{uuid.uuid4()}"

        await nexo.pubsub.topic(topic).publish("dark", retain=True)

        received: list[str] = []
        sub = await nexo.pubsub.topic(topic).subscribe(
            lambda data: received.append(data)
        )
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "dark"

        # Clear retained while the subscriber is still active.
        await nexo.pubsub.topic(topic).clear_retained()

        # The active subscriber must NOT receive a spurious empty message.
        await asyncio.sleep(0.3)
        assert len(received) == 1

        await sub.stop()
        await nexo.pubsub.topic(topic).clear_retained()

    async def test_reject_invalid_ttl(self, nexo: NexoClient):
        topic = f"ttl-invalid-{uuid.uuid4()}"

        with pytest.raises(ValueError, match="Invalid ttl"):
            await nexo.pubsub.topic(topic).publish("x", ttl=-1)

        with pytest.raises(ValueError, match="Invalid ttl"):
            await nexo.pubsub.topic(topic).publish("x", ttl=1.5)

    async def test_multi_level_wildcard(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"sensors-{base_id}/#"
        received: list = []

        sub = await nexo.pubsub.pattern(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub.topic(f"sensors-{base_id}/main").publish({"id": "root"})
        await nexo.pubsub.topic(f"sensors-{base_id}/a/b/c").publish({"id": "deep"})
        await nexo.pubsub.topic(f"other-{base_id}/main").publish({"id": "fail-prefix"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["deep", "root"]
        await sub.stop()

    async def test_async_callback(self, nexo: NexoClient):
        topic = f"async-cb-{uuid.uuid4()}"
        received: list = []

        async def async_handler(data):
            await asyncio.sleep(0.01)
            received.append(data)

        sub = await nexo.pubsub.topic(topic).subscribe(async_handler)
        await nexo.pubsub.topic(topic).publish({"msg": "hello"})
        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "hello"
        await sub.stop()

    async def test_slow_callback_does_not_block_store(self, nexo: NexoClient):
        pubsub_topic = f"slow-cb-{uuid.uuid4()}"
        store_key = f"store-key-{uuid.uuid4()}"
        callback_started = asyncio.Event()

        async def slow_handler(data):
            callback_started.set()
            await asyncio.sleep(0.5)

        sub = await nexo.pubsub.topic(pubsub_topic).subscribe(slow_handler)
        await nexo.pubsub.topic(pubsub_topic).publish({"msg": "trigger"})

        await wait_for(callback_started.is_set)

        import time
        t0 = time.monotonic()
        await nexo.store.map.set(store_key, "value")
        elapsed = time.monotonic() - t0

        assert elapsed < 0.3, f"Store operation blocked by PubSub callback: {elapsed:.3f}s"
        await sub.stop()

    async def test_parallel_subscriptions(self, nexo: NexoClient):
        topic_a = f"par-a-{uuid.uuid4()}"
        topic_b = f"par-b-{uuid.uuid4()}"
        order: list[str] = []

        async def handler_a(data):
            await asyncio.sleep(0.1)
            order.append("a")

        async def handler_b(data):
            order.append("b")

        sub_a = await nexo.pubsub.topic(topic_a).subscribe(handler_a)
        sub_b = await nexo.pubsub.topic(topic_b).subscribe(handler_b)

        await nexo.pubsub.topic(topic_a).publish({"msg": "x"})
        await nexo.pubsub.topic(topic_b).publish({"msg": "y"})

        await wait_for(lambda: len(order) == 2)
        assert order == ["b", "a"], f"Subscription B should not wait for A: {order}"
        await sub_a.stop()
        await sub_b.stop()

    async def test_two_local_listeners_share_pattern_and_stop_independently(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = nexo.pubsub.pattern(f"local-{base_id}/+")
        topic = nexo.pubsub.topic(f"local-{base_id}/value")
        received_a: list[str] = []
        received_b: list[str] = []
        concrete_topics: list[str] = []

        sub_a = await pattern.subscribe(lambda data: received_a.append(data))

        def handler_b(data, meta):
            received_b.append(data)
            concrete_topics.append(meta["topic"])

        sub_b = await pattern.subscribe(handler_b)
        await topic.publish("both")
        await wait_for(lambda: received_a == ["both"] and received_b == ["both"])
        assert concrete_topics == [topic.name]

        await sub_a.stop()
        assert not sub_a.active
        assert sub_b.active

        await topic.publish("only-b")
        await wait_for(lambda: received_b == ["both", "only-b"])
        assert received_a == ["both"]
        await sub_b.stop()

    async def test_overflow_stops_only_local_listener(self, nexo: NexoClient):
        topic = nexo.pubsub.topic(f"overflow-{uuid.uuid4()}")
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_handler(_):
            started.set()
            await release.wait()

        sub = await topic.subscribe(blocked_handler, queue_capacity=1)
        await topic.publish("one")
        await started.wait()
        await topic.publish("two")
        await topic.publish("three")

        await wait_for(lambda: not sub.active)
        assert isinstance(sub.error, SlowConsumerError)
        release.set()
        await sub.wait_closed()

    # ── Edge cases ──────────────────────────────────────────────

    async def test_retained_delivered_to_new_subscriber(self, nexo: NexoClient):
        topic = f"retained-new-{uuid.uuid4()}"

        await nexo.pubsub.topic(topic).publish("retained-value", retain=True)

        received: list[str] = []
        sub = await nexo.pubsub.topic(topic).subscribe(
            lambda data: received.append(data)
        )
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "retained-value"

        await sub.stop()
        await nexo.pubsub.topic(topic).clear_retained()

    async def test_retained_overwrite_on_second_publish(self, nexo: NexoClient):
        topic = f"retained-overwrite-{uuid.uuid4()}"

        await nexo.pubsub.topic(topic).publish("first", retain=True)
        await nexo.pubsub.topic(topic).publish("second", retain=True)

        received: list[str] = []
        sub = await nexo.pubsub.topic(topic).subscribe(
            lambda data: received.append(data)
        )
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "second"

        await sub.stop()
        await nexo.pubsub.topic(topic).clear_retained()

    async def test_retained_not_delivered_after_ttl_expiry(self, nexo: NexoClient):
        topic = f"retained-ttl-{uuid.uuid4()}"

        await nexo.pubsub.topic(topic).publish("temp-retained", retain=True, ttl=1)

        await asyncio.sleep(1.2)

        received: list[str] = []
        sub = await nexo.pubsub.topic(topic).subscribe(lambda data: received.append(data))
        await asyncio.sleep(0.3)
        assert received == []

        await sub.stop()

    async def test_unsubscribe_stops_delivery(self, nexo: NexoClient):
        topic = f"unsub-stop-{uuid.uuid4()}"

        received: list = []
        sub = await nexo.pubsub.topic(topic).subscribe(lambda data: received.append(data))

        await nexo.pubsub.topic(topic).publish({"msg": "before"})
        await wait_for(lambda: len(received) == 1)

        await sub.stop()

        await nexo.pubsub.topic(topic).publish({"msg": "after"})
        await asyncio.sleep(0.3)
        assert len(received) == 1

    async def test_combined_wildcards_plus_and_hash(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"combo-{base_id}/+/b/#"

        received: list = []
        sub = await nexo.pubsub.pattern(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub.topic(f"combo-{base_id}/x/b/y/z").publish({"id": "deep-match"})
        await nexo.pubsub.topic(f"combo-{base_id}/x/b").publish({"id": "shallow-match"})
        await nexo.pubsub.topic(f"combo-{base_id}/x/c/y").publish({"id": "fail-wrong-segment"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["deep-match", "shallow-match"]

        await sub.stop()

    async def test_broadcast_to_3_plus_subscribers(self, nexo: NexoClient):
        topic = f"broadcast-{uuid.uuid4()}"

        client1 = await NexoClient.connect()
        client2 = await NexoClient.connect()
        try:
            recv1: list = []
            recv2: list = []
            recv3: list = []

            sub1 = await client1.pubsub.topic(topic).subscribe(lambda d: recv1.append(d))
            sub2 = await client2.pubsub.topic(topic).subscribe(lambda d: recv2.append(d))
            sub3 = await nexo.pubsub.topic(topic).subscribe(lambda d: recv3.append(d))

            await nexo.pubsub.topic(topic).publish({"msg": "broadcast"})

            await wait_for(lambda: len(recv1) == 1 and len(recv2) == 1 and len(recv3) == 1)
            assert recv1[0]["msg"] == "broadcast"
            assert recv2[0]["msg"] == "broadcast"
            assert recv3[0]["msg"] == "broadcast"

            await sub1.stop()
            await sub2.stop()
            await sub3.stop()
        finally:
            client1.disconnect()
            client2.disconnect()

    async def test_disconnect_cleanup_does_not_break_topic(self, nexo: NexoClient):
        topic = f"disconnect-cleanup-{uuid.uuid4()}"

        temp_client = await NexoClient.connect()
        await temp_client.pubsub.topic(topic).subscribe(lambda _: None)
        await asyncio.sleep(0.1)

        temp_client.disconnect()
        await asyncio.sleep(0.3)

        received: list = []
        sub = await nexo.pubsub.topic(topic).subscribe(lambda d: received.append(d))
        await nexo.pubsub.topic(topic).publish({"msg": "after-disconnect"})

        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "after-disconnect"

        await sub.stop()

    async def test_retained_with_wildcard_plus(self, nexo: NexoClient):
        base_id = uuid.uuid4()

        await nexo.pubsub.topic(f"ret-plus-{base_id}/x").publish("val-x", retain=True)
        await nexo.pubsub.topic(f"ret-plus-{base_id}/y").publish("val-y", retain=True)

        received: list[str] = []
        sub = await nexo.pubsub.pattern(f"ret-plus-{base_id}/+").subscribe(lambda d: received.append(d))

        await wait_for(lambda: len(received) == 2)
        assert sorted(received) == ["val-x", "val-y"]

        await sub.stop()
        await nexo.pubsub.topic(f"ret-plus-{base_id}/x").clear_retained()
        await nexo.pubsub.topic(f"ret-plus-{base_id}/y").clear_retained()

    async def test_retained_with_wildcard_hash(self, nexo: NexoClient):
        base_id = uuid.uuid4()

        await nexo.pubsub.topic(f"ret-hash-{base_id}/a/b/c").publish("deep", retain=True)

        received: list[str] = []
        sub = await nexo.pubsub.pattern(f"ret-hash-{base_id}/#").subscribe(lambda d: received.append(d))

        await wait_for(lambda: len(received) == 1)
        assert received[0] == "deep"

        await sub.stop()
        await nexo.pubsub.topic(f"ret-hash-{base_id}/a/b/c").clear_retained()

    async def test_slow_consumer_disconnect_does_not_affect_others(self, nexo: NexoClient):
        topic = f"slow-consumer-{uuid.uuid4()}"

        fast_received: list = []
        fast_sub = await nexo.pubsub.topic(topic).subscribe(lambda d: fast_received.append(d))

        slow_client = await NexoClient.connect()
        slow_received: list = []
        slow_sub = await slow_client.pubsub.topic(topic).subscribe(lambda d: slow_received.append(d))

        await nexo.pubsub.topic(topic).publish({"msg": "first"})
        await wait_for(lambda: len(fast_received) == 1)
        await wait_for(lambda: len(slow_received) == 1)

        # Simulate server-initiated disconnect (as would happen for slow consumer)
        conn = slow_client._conn
        if conn._writer is not None:
            conn._writer.close()
        await wait_for(lambda: not conn.is_connected, timeout=3.0)

        # Fast subscriber should still receive messages
        await nexo.pubsub.topic(topic).publish({"msg": "second"})
        await wait_for(lambda: len(fast_received) == 2)
        assert fast_received[1]["msg"] == "second"

        # Slow client should auto-reconnect and resubscribe
        await wait_for(lambda: conn.is_connected, timeout=5.0)
        await asyncio.sleep(0.5)  # allow resubscribe

        await nexo.pubsub.topic(topic).publish({"msg": "after-reconnect"})
        await wait_for(lambda: any(r.get("msg") == "after-reconnect" for r in slow_received), timeout=5.0)

        await fast_sub.stop()
        await slow_sub.stop()
        slow_client.disconnect()
