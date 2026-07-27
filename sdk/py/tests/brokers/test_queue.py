from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


@pytest.mark.asyncio
class TestQueue:
    async def test_reject_batch_size_zero(self, nexo: NexoClient):
        q_name = f"queue-batch-zero-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        async def _noop(_):
            pass

        with pytest.raises(ValueError, match="batchSize must be >= 1"):
            await q.subscribe(_noop, {"batch_size": 0})
        await q.delete()

    async def test_reject_concurrency_zero(self, nexo: NexoClient):
        q_name = f"queue-conc-zero-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        async def _noop2(_):
            pass

        with pytest.raises(ValueError, match="concurrency must be >= 1"):
            await q.subscribe(_noop2, {"concurrency": 0})
        await q.delete()

    async def test_no_dlq_on_graceful_shutdown(self, nexo: NexoClient):
        q_name = f"queue-shutdown-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create(
            {"visibility_timeout_ms": 500, "max_deliveries": 3}
        )

        await q.push("msg1")
        await q.push("msg2")

        async def _slow(_):
            await asyncio.sleep(1)

        sub = await q.subscribe(
            _slow,
            {"batch_size": 2, "wait_ms": 100, "concurrency": 1},
        )

        await asyncio.sleep(0.2)
        await sub.stop()

        dlq_result = await q.dlq.peek(10)
        assert dlq_result["total"] == 0

        received: list[str] = []
        sub2 = await q.subscribe(
            lambda data: received.append(data),
            {"batch_size": 5, "wait_ms": 500, "concurrency": 1},
        )

        await wait_for(lambda: "msg2" in received)
        await sub2.stop()
        await q.delete()

    async def test_stop_consumer_when_queue_deleted(self, nexo: NexoClient):
        q_name = f"queue-deleted-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push("msg1")

        async def _slow2(_):
            await asyncio.sleep(0.5)

        sub = await q.subscribe(
            _slow2,
            {"batch_size": 1, "wait_ms": 500, "concurrency": 1},
        )

        await asyncio.sleep(0.2)
        await q.delete()
        await asyncio.sleep(2.0)
        await sub.stop()

    async def test_stop_consumer_nonexistent_queue(self, nexo: NexoClient):
        q_name = f"queue-nonexist-{uuid.uuid4()}"
        q = nexo.queue(q_name)

        sub = await q.subscribe(
            lambda _: None,
            {"batch_size": 1, "wait_ms": 100, "concurrency": 1},
        )

        await asyncio.sleep(0.5)
        await sub.stop()

    async def test_full_lifecycle_push_subscribe_ack(self, nexo: NexoClient):
        q_name = f"queue-life-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()
        payload = {"task": "process_me"}

        received: list = []
        sub = await q.subscribe(lambda data: received.append(data))

        await q.push(payload)

        await wait_for(lambda: received == [payload])
        await sub.stop()

    async def test_move_failed_to_dlq(self, nexo: NexoClient):
        q_name = f"queue-dlq-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create(
            {"max_deliveries": 1, "visibility_timeout_ms": 100}
        )

        await q.push("fail_payload")

        async def fail_cb(_):
            raise Exception("Simulated Failure")

        sub = await q.subscribe(fail_cb)

        await asyncio.sleep(1.0)
        await sub.stop()

        dlq_result = await q.dlq.peek(10)
        assert dlq_result["total"] == 1
        assert dlq_result["items"][0]["data"] == "fail_payload"

    async def test_priority_high_before_low(self, nexo: NexoClient):
        q_name = f"queue-prio-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push("low", {"priority": 0})
        await q.push("high", {"priority": 10})

        received: list[str] = []
        sub = await q.subscribe(
            lambda msg: received.append(msg),
            {"concurrency": 1},
        )

        await wait_for(lambda: len(received) == 2)
        await sub.stop()

        assert received == ["high", "low"]

    async def test_dlq_workflow_peek_move_delete_purge(self, nexo: NexoClient):
        q_name = f"dlq-test-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create(
            {"visibility_timeout_ms": 5000, "max_deliveries": 1}
        )

        await q.push({"order": "order1"})
        await q.push({"order": "order2"})
        await q.push({"order": "order3"})

        received: list = []
        OLD_CONSUME_WAIT_MS = 100

        async def fail_cb(msg):
            received.append(msg)
            raise Exception("Simulated processing error")

        sub = await q.subscribe(
            fail_cb,
            {"batch_size": 10, "wait_ms": OLD_CONSUME_WAIT_MS},
        )

        await wait_for(lambda: len(received) == 3)
        await sub.stop()

        await asyncio.sleep((OLD_CONSUME_WAIT_MS + 100) / 1000.0)

        async def check_dlq():
            dlq_result = await q.dlq.peek(10)
            assert dlq_result["total"] == 3
            assert len(dlq_result["items"]) == 3

        await wait_for(check_dlq)

        dlq_result = await q.dlq.peek(10)
        assert dlq_result["items"][0]["attempts"] >= 1

        target_msg = next(i for i in dlq_result["items"] if i["data"]["order"] == "order3")
        other_msg = next(i for i in dlq_result["items"] if i["data"]["order"] == "order2")

        msg_to_replay_id = target_msg["id"]
        msg_to_delete_id = other_msg["id"]

        moved = await q.dlq.move_to_queue(msg_to_replay_id)
        assert moved is True

        replayed: list = []
        sub2 = await nexo.queue(q_name).subscribe(
            lambda msg: replayed.append(msg),
            {"batch_size": 1, "wait_ms": 100, "concurrency": 1},
        )

        await wait_for(lambda: len(replayed) == 1)
        assert replayed[0]["order"] == "order3"
        await sub2.stop()

        deleted = await q.dlq.delete(msg_to_delete_id)
        assert deleted is True

        dlq_after_delete = await q.dlq.peek(10)
        assert dlq_after_delete["total"] == 1
        assert len(dlq_after_delete["items"]) == 1

        purged_count = await q.dlq.purge()
        assert purged_count == 1

        dlq_after_purge = await q.dlq.peek(10)
        assert dlq_after_purge["total"] == 0
        assert len(dlq_after_purge["items"]) == 0

        await q.delete()

    async def test_serialize_callbacks_concurrency_1(self, nexo: NexoClient):
        q_name = f"queue-conc1-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        COUNT = 10
        CALLBACK_DELAY = 0.05

        in_flight = 0
        max_in_flight = 0
        received: list[int] = []

        async def cb(msg):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(CALLBACK_DELAY)
            received.append(msg["i"])
            in_flight -= 1

        sub = await q.subscribe(
            cb,
            {"batch_size": COUNT, "concurrency": 1},
        )

        for i in range(COUNT):
            await q.push({"i": i})

        await wait_for(lambda: len(received) == COUNT)
        await sub.stop()

        assert max_in_flight == 1
        assert len(set(received)) == COUNT

    async def test_parallel_concurrency_gt_1(self, nexo: NexoClient):
        q_name = f"queue-conc-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        COUNT = 20
        CALLBACK_DELAY = 0.1
        CONCURRENCY = 10

        in_flight = 0
        max_in_flight = 0
        received: list[int] = []

        async def cb(msg):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(CALLBACK_DELAY)
            received.append(msg["i"])
            in_flight -= 1

        sub = await q.subscribe(
            cb,
            {"batch_size": COUNT, "concurrency": CONCURRENCY},
        )

        for i in range(COUNT):
            await q.push({"i": i})

        start = asyncio.get_event_loop().time()
        await wait_for(lambda: len(received) == COUNT, timeout=10.0)
        elapsed = asyncio.get_event_loop().time() - start
        await sub.stop()

        assert len(set(received)) == COUNT
        assert max_in_flight > 1
        assert elapsed < COUNT * CALLBACK_DELAY * 0.6

    async def test_multiple_parallel_subscribers(self, nexo: NexoClient):
        q_name = f"queue-multi-sub-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        COUNT = 20
        received: list[int] = []

        sub_a = await q.subscribe(
            lambda msg: received.append(msg["i"]),
            {"batch_size": 1, "wait_ms": 200, "concurrency": 1},
        )
        sub_b = await q.subscribe(
            lambda msg: received.append(msg["i"]),
            {"batch_size": 1, "wait_ms": 200, "concurrency": 1},
        )

        for i in range(COUNT):
            await q.push({"i": i})

        await wait_for(lambda: len(received) == COUNT)
        await sub_a.stop()
        await sub_b.stop()

        assert sorted(received) == list(range(COUNT))

    async def test_nack_persists_failure_reason(self, nexo: NexoClient):
        q_name = f"nack-reason-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create(
            {"max_deliveries": 0, "visibility_timeout_ms": 10000}
        )

        await q.push({"task": "fail_me"})

        async def fail_cb(_):
            raise Exception("Specific Failure Reason")

        sub = await q.subscribe(fail_cb)

        await asyncio.sleep(0.5)
        await sub.stop()

        dlq_result = await q.dlq.peek(10)
        assert dlq_result["total"] == 1
        assert dlq_result["items"][0]["failure_reason"] == "Specific Failure Reason"

    async def test_push_batch(self, nexo: NexoClient):
        q_name = f"batch-push-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push_batch([
            {"data": "msg1"},
            {"data": "msg2"},
            {"data": "msg3"},
        ])

        received: list[str] = []
        sub = await q.subscribe(
            lambda data: received.append(data),
            {"batch_size": 10, "wait_ms": 500, "concurrency": 1},
        )

        await wait_for(lambda: len(received) == 3)
        await sub.stop()
        await q.delete()

    async def test_push_batch_mixed_priorities(self, nexo: NexoClient):
        q_name = f"batch-prio-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push_batch([
            {"data": "low", "options": {"priority": 0}},
            {"data": "high", "options": {"priority": 10}},
            {"data": "mid", "options": {"priority": 5}},
        ])

        received: list[str] = []
        sub = await q.subscribe(
            lambda data: received.append(data),
            {"batch_size": 3, "wait_ms": 500, "concurrency": 1},
        )

        await wait_for(lambda: len(received) == 3)
        assert received[0] == "high"
        assert received[1] == "mid"
        assert received[2] == "low"
        await sub.stop()
        await q.delete()

    async def test_empty_push_batch(self, nexo: NexoClient):
        q_name = f"batch-empty-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push_batch([])
        await q.delete()

    # ── Edge cases ──────────────────────────────────────────────

    async def test_exists_true_after_create_false_before(self, nexo: NexoClient):
        q_name = f"queue-exists-{uuid.uuid4()}"
        assert await nexo.queue(q_name).exists() is False
        q = await nexo.queue(q_name).create()
        assert await q.exists() is True
        await q.delete()
        assert await nexo.queue(q_name).exists() is False

    async def test_create_idempotent(self, nexo: NexoClient):
        q_name = f"queue-idempotent-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()
        await nexo.queue(q_name).create()
        assert await q.exists() is True
        await q.delete()

    async def test_consume_empty_queue_no_wait_returns_immediately(self, nexo: NexoClient):
        q_name = f"queue-empty-nowait-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        received: list = []
        sub = await q.subscribe(
            lambda data: received.append(data),
            {"batch_size": 5, "wait_ms": 50, "concurrency": 1},
        )

        await asyncio.sleep(0.2)
        assert received == []
        await sub.stop()
        await q.delete()

    async def test_partial_batch_when_fewer_than_batch_size(self, nexo: NexoClient):
        q_name = f"queue-partial-batch-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push("a")
        await q.push("b")
        await q.push("c")

        received: list[str] = []
        sub = await q.subscribe(
            lambda data: received.append(data),
            {"batch_size": 10, "wait_ms": 100, "concurrency": 1},
        )

        await wait_for(lambda: len(received) == 3)
        assert received == ["a", "b", "c"]
        await sub.stop()
        await q.delete()

    async def test_long_polling_wakeup_on_push(self, nexo: NexoClient):
        q_name = f"queue-longpoll-wake-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        received: list = []
        sub = await q.subscribe(
            lambda data: received.append(data),
            {"batch_size": 1, "wait_ms": 5000, "concurrency": 1},
        )

        await asyncio.sleep(0.2)

        import time
        push_start = time.monotonic()
        await q.push("wakeup")
        await wait_for(lambda: len(received) == 1)
        elapsed = time.monotonic() - push_start

        assert elapsed < 2.0
        assert received[0] == "wakeup"
        await sub.stop()
        await q.delete()

    async def test_fifo_ordering_same_priority(self, nexo: NexoClient):
        q_name = f"queue-fifo-same-prio-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        for i in range(10):
            await q.push({"i": i}, {"priority": 5})

        received: list[int] = []
        sub = await q.subscribe(
            lambda data: received.append(data["i"]),
            {"batch_size": 10, "wait_ms": 100, "concurrency": 1},
        )

        await wait_for(lambda: len(received) == 10)
        for i in range(10):
            assert received[i] == i
        await sub.stop()
        await q.delete()

    async def test_push_nonexistent_queue_fails(self, nexo: NexoClient):
        q_name = f"queue-push-missing-{uuid.uuid4()}"
        with pytest.raises(Exception):
            await nexo.queue(q_name).push("data")

    async def test_push_deleted_queue_fails(self, nexo: NexoClient):
        q_name = f"queue-push-deleted-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()
        await q.delete()
        with pytest.raises(Exception):
            await q.push("data")
