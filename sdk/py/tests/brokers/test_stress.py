"""Nexo Python SDK — Stress / Benchmark Suite

Last run: 2026-07-22 | MacBook Pro M4 Pro | Battery High Performance

THROUGHPUT (50k ops, 50 concurrent workers):
  STORE SET             70,273 ops/sec | p50: 0.56ms | p99: 2.09ms | MAX: 9.26ms
  STORE GET             86,524 ops/sec | p50: 0.53ms | p99: 1.51ms | MAX: 4.01ms
  QUEUE PUSH            55,476 ops/sec | p50: 0.67ms | p99: 3.14ms | MAX: 14.96ms
  QUEUE PUSH BATCH     522,429 ops/sec | p50: 0.09ms | p99: 0.11ms | MAX: 0.11ms
  STREAM PUBLISH        65,469 ops/sec | p50: 0.67ms | p99: 1.76ms | MAX: 2.37ms
  STREAM PUB BATCH     516,439 ops/sec | p50: 0.10ms | p99: 0.10ms | MAX: 0.10ms
  PUBSUB PUBLISH        69,743 ops/sec | p50: 0.62ms | p99: 2.33ms | MAX: 4.70ms
  QUEUE CONSUME+ACK     65,963 ops/sec
  STREAM SUB+ACK        18,319 ops/sec

LATENCY (100k sequential ops):
  STORE SET             10,033 ops/sec | p50: 0.06ms | p99: 0.51ms | MAX: 59.02ms
  STORE GET             11,404 ops/sec | p50: 0.07ms | p99: 0.32ms | MAX: 7.92ms
  QUEUE PUSH            10,623 ops/sec | p50: 0.07ms | p99: 0.37ms | MAX: 38.57ms
  STREAM PUBLISH         9,572 ops/sec | p50: 0.07ms | p99: 0.39ms | MAX: 35.14ms
  PUBSUB PUBLISH        11,296 ops/sec | p50: 0.06ms | p99: 0.36ms | MAX: 31.67ms

UTILS:
  run_concurrent     8,796,718 ops/sec
"""
from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from nexo import NexoClient
from tests.utils.benchmark_probe import BenchmarkProbe
from nexo.utils.concurrent import run_concurrent


@pytest.mark.asyncio
class TestStressThroughput:
    async def test_store_set_concurrent(self, nexo: NexoClient):
        TOTAL = 50_000
        WORKERS = 50
        OPS_PER_WORKER = TOTAL // WORKERS

        probe = BenchmarkProbe("STORE SET", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            for i in range(OPS_PER_WORKER):
                t0 = time.perf_counter()
                await nexo.store.map.set(f"bench-{worker_id}-{i}", f"value-{i}")
                probe.record((time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        stats = probe.print_result()
        assert stats["throughput"] > 30_000

    async def test_store_get_concurrent(self, nexo: NexoClient):
        TOTAL = 50_000
        WORKERS = 50
        OPS_PER_WORKER = TOTAL // WORKERS

        for w in range(WORKERS):
            for i in range(OPS_PER_WORKER):
                await nexo.store.map.set(f"get-{w}-{i}", f"value-{i}")

        probe = BenchmarkProbe("STORE GET", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            for i in range(OPS_PER_WORKER):
                t0 = time.perf_counter()
                await nexo.store.map.get(f"get-{worker_id}-{i}")
                probe.record((time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        probe.print_result()

    async def test_queue_push_concurrent(self, nexo: NexoClient):
        q_name = f"bench-queue-push-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        TOTAL = 50_000
        WORKERS = 50
        OPS_PER_WORKER = TOTAL // WORKERS
        payload = {"op": "job", "data": "x", "t": time.time()}

        probe = BenchmarkProbe("QUEUE PUSH", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            for i in range(OPS_PER_WORKER):
                t0 = time.perf_counter()
                await q.push(payload)
                probe.record((time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        probe.print_result()
        await q.delete()

    async def test_queue_push_batch_concurrent(self, nexo: NexoClient):
        q_name = f"bench-queue-batch-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        TOTAL = 50_000
        WORKERS = 50
        BATCH_SIZE = 100
        BATCHES_PER_WORKER = TOTAL // WORKERS // BATCH_SIZE
        payload = {"op": "job", "data": "x", "t": time.time()}

        probe = BenchmarkProbe("QUEUE PUSH BATCH", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            batch = [{"data": payload} for _ in range(BATCH_SIZE)]
            for i in range(BATCHES_PER_WORKER):
                t0 = time.perf_counter()
                await q.push_batch(batch)
                probe.record_batch(BATCH_SIZE, (time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        probe.print_result()
        await q.delete()

    async def test_stream_publish_concurrent(self, nexo: NexoClient):
        topic = f"bench-stream-pub-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        TOTAL = 50_000
        WORKERS = 50
        OPS_PER_WORKER = TOTAL // WORKERS
        payload = {"op": "event", "data": "x", "t": time.time()}

        probe = BenchmarkProbe("STREAM PUBLISH", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            for i in range(OPS_PER_WORKER):
                t0 = time.perf_counter()
                await nexo.stream(topic).publish(payload)
                probe.record((time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        probe.print_result()
        await nexo.stream(topic).delete()

    async def test_stream_publish_batch_concurrent(self, nexo: NexoClient):
        topic = f"bench-stream-batch-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        TOTAL = 50_000
        WORKERS = 50
        BATCH_SIZE = 100
        BATCHES_PER_WORKER = TOTAL // WORKERS // BATCH_SIZE
        payload = {"op": "event", "data": "x", "t": time.time()}

        probe = BenchmarkProbe("STREAM PUBLISH BATCH", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            batch = [{"data": payload} for _ in range(BATCH_SIZE)]
            for i in range(BATCHES_PER_WORKER):
                t0 = time.perf_counter()
                await nexo.stream(topic).publish_batch(batch)
                probe.record_batch(BATCH_SIZE, (time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        probe.print_result()
        await nexo.stream(topic).delete()

    async def test_pubsub_publish_concurrent(self, nexo: NexoClient):
        topic_name = f"bench/pubsub-pub-{uuid.uuid4()}"
        topic = nexo.pubsub(topic_name)
        payload = {"op": "ping", "data": "x", "t": time.time()}

        TOTAL = 50_000
        WORKERS = 50
        OPS_PER_WORKER = TOTAL // WORKERS

        probe = BenchmarkProbe("PUBSUB PUBLISH", TOTAL)
        probe.start_timer()

        async def worker(worker_id: int):
            for i in range(OPS_PER_WORKER):
                t0 = time.perf_counter()
                await topic.publish(payload)
                probe.record((time.perf_counter() - t0) * 1000)

        await asyncio.gather(*[worker(i) for i in range(WORKERS)])
        probe.print_result()

    async def test_queue_consume_ack_throughput(self, nexo: NexoClient):
        from tests.utils.wait_for import wait_for

        q_name = f"bench-queue-consume-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        TOTAL = 50_000
        payload = {"op": "job", "data": "x", "t": time.time()}

        for i in range(TOTAL):
            await q.push(payload)

        probe = BenchmarkProbe("QUEUE CONSUME+ACK", TOTAL)
        consumed: list[int] = [0]
        probe.start_timer()

        def cb(_):
            consumed[0] += 1

        sub = await q.subscribe(
            cb,
            {"batch_size": 50, "wait_ms": 100, "concurrency": 10},
        )

        await wait_for(lambda: consumed[0] >= TOTAL, timeout=60.0)
        await sub.stop()
        probe.print_result()
        await q.delete()

    async def test_stream_subscribe_ack_throughput(self, nexo: NexoClient):
        topic = f"bench-stream-sub-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        TOTAL = 50_000
        payload = {"op": "event", "data": "x", "t": time.time()}

        for i in range(TOTAL):
            await nexo.stream(topic).publish(payload)

        probe = BenchmarkProbe("STREAM SUBSCRIBE+ACK", TOTAL)
        consumed: list[int] = [0]
        probe.start_timer()

        sub = await nexo.stream(topic).subscribe(
            "bench-group",
            lambda _data, _meta: consumed.__setitem__(0, consumed[0] + 1),
            {"batch_size": 100, "wait_ms": 100, "concurrency": 10},
        )

        from tests.utils.wait_for import wait_for
        await wait_for(lambda: consumed[0] >= TOTAL, timeout=60.0)
        await sub.stop()
        probe.print_result()
        await nexo.stream(topic).delete()


@pytest.mark.asyncio
class TestStressLatency:
    async def test_store_set_sequential(self, nexo: NexoClient):
        ITERATIONS = 100_000
        probe = BenchmarkProbe("STORE SET LATENCY", ITERATIONS)
        probe.start_timer()

        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            await nexo.store.map.set(f"latency-{i}", "v")
            probe.record((time.perf_counter() - t0) * 1000)

        probe.print_result()

    async def test_store_get_sequential(self, nexo: NexoClient):
        ITERATIONS = 100_000

        for i in range(ITERATIONS):
            await nexo.store.map.set(f"lat-get-{i}", f"val-{i}")

        probe = BenchmarkProbe("STORE GET LATENCY", ITERATIONS)
        probe.start_timer()

        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            await nexo.store.map.get(f"lat-get-{i}")
            probe.record((time.perf_counter() - t0) * 1000)

        probe.print_result()

    async def test_queue_push_sequential(self, nexo: NexoClient):
        q_name = f"bench-queue-lat-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        ITERATIONS = 100_000
        payload = {"op": "job", "data": "x", "t": time.time()}
        probe = BenchmarkProbe("QUEUE PUSH LATENCY", ITERATIONS)
        probe.start_timer()

        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            await q.push(payload)
            probe.record((time.perf_counter() - t0) * 1000)

        probe.print_result()
        await q.delete()

    async def test_stream_publish_sequential(self, nexo: NexoClient):
        topic = f"bench-stream-lat-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        ITERATIONS = 100_000
        payload = {"op": "event", "data": "x", "t": time.time()}
        probe = BenchmarkProbe("STREAM PUBLISH LATENCY", ITERATIONS)
        probe.start_timer()

        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            await nexo.stream(topic).publish(payload)
            probe.record((time.perf_counter() - t0) * 1000)

        probe.print_result()
        await nexo.stream(topic).delete()

    async def test_pubsub_publish_sequential(self, nexo: NexoClient):
        topic_name = f"bench/pubsub-lat-{uuid.uuid4()}"
        topic = nexo.pubsub(topic_name)
        payload = {"op": "ping", "data": "x", "t": time.time()}

        ITERATIONS = 100_000
        probe = BenchmarkProbe("PUBSUB PUBLISH LATENCY", ITERATIONS)
        probe.start_timer()

        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            await topic.publish(payload)
            probe.record((time.perf_counter() - t0) * 1000)

        probe.print_result()


@pytest.mark.asyncio
class TestStressUtils:
    async def test_run_concurrent_correctness(self):
        items = list(range(1000))
        processed: list[int] = []
        max_concurrent: list[int] = [0]
        current: list[int] = [0]

        async def fn(item: int):
            current[0] += 1
            max_concurrent[0] = max(max_concurrent[0], current[0])
            await asyncio.sleep(0)
            processed.append(item)
            current[0] -= 1

        await run_concurrent(items, 10, fn)

        assert len(processed) == 1000
        assert len(set(processed)) == 1000
        assert max_concurrent[0] <= 10

    async def test_run_concurrent_performance(self):
        items = list(range(100_000))
        probe = BenchmarkProbe("run_concurrent", len(items))
        probe.start_timer()

        async def fn(_: int):
            probe.record(0)

        await run_concurrent(items, 10, fn)
        probe.print_result()
