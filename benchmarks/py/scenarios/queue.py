from __future__ import annotations

import asyncio
import sys
import time
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, Message
from nexo import NexoClient

from report import Meter, embedded_time, env, make_payload, measure, now_ms, timed_payload

Q = f"bench.q.{int(time.time())}"


async def run(spec: dict[str, Any]) -> list[dict[str, Any]]:
    nexo = await NexoClient.connect(
        host=env("NEXO_HOST", "127.0.0.1"),
        port=int(env("NEXO_PORT", "7654")),
    )
    conn = await aio_pika.connect_robust(env("RABBITMQ_URL", "amqp://127.0.0.1:5672"))
    # Two durability tiers: nexo push confirms after the shared-WAL
    # transaction commits (synchronous=NORMAL), so it sits between rabbit-d
    # (disk + confirm) and rabbit-v (fire-and-forget). Report both.
    ch_d = await conn.channel(publisher_confirms=True)
    ch_v = await conn.channel()

    value = make_payload(spec["payload_bytes"])
    rows: list[dict[str, Any]] = []

    async def nexo_push(w: dict):
        name = f"{Q}.np.{w['id']}"
        await nexo.queue.create(name)
        q = await nexo.queue.get(name)
        m = await measure(w, w["ops"], lambda _: q.push(value))
        await nexo.queue.delete(name)
        return m.to_row(w["id"], "nexo", w["ops"], w.get("hot", False))

    async def nexo_consume(w: dict):
        name = f"{Q}.nc.{w['id']}"
        await nexo.queue.create(name)
        q = await nexo.queue.get(name)
        msgs = w["msgs"]
        chunk = [{"data": value}] * 500
        for _ in range(0, w.get("prefill", msgs), 500):
            await q.push_batch(chunk)

        meter = Meter()
        got = 0

        async def cb(_data: Any) -> None:
            nonlocal got
            got += 1

        subs = [
            await q.subscribe(
                cb,
                batch_size=w.get("batch_size", 50),
                wait_ms=w.get("wait_ms", 200),
                concurrency=1,
            )
            for _ in range(w.get("consumers", 1))
        ]
        t0 = now_ms()
        while got < msgs:
            if now_ms() - t0 > 120_000:
                raise TimeoutError("nexo consume drain timeout")
            await asyncio.sleep(0.005)
        meter.stop()
        for s in subs:
            await s.stop()
        await nexo.queue.delete(name)
        return meter.to_row(w["id"], "nexo", msgs, w.get("hot", False),
                          "drain pre-filled queue (consume+ack)")

    async def nexo_pipeline(w: dict):
        name = f"{Q}.nx.{w['id']}"
        await nexo.queue.create(name)
        q = await nexo.queue.get(name)
        msgs = w["msgs"]

        meter = Meter()
        got = 0
        done = asyncio.Event()

        async def cb(data: Any) -> None:
            nonlocal got
            meter.record_raw(now_ms() - embedded_time(data))
            got += 1
            if got == msgs:
                done.set()

        subs = [
            await q.subscribe(
                cb,
                batch_size=w.get("batch_size", 50),
                wait_ms=w.get("wait_ms", 200),
                concurrency=1,
            )
            for _ in range(w.get("consumers", 1))
        ]
        producers = w.get("producers", 1)
        per = -(-msgs // producers)

        async def producer(k: int) -> None:
            for i in range(per):
                if k * per + i >= msgs:
                    break
                await q.push(timed_payload(now_ms(), spec["payload_bytes"]))

        await asyncio.gather(*(producer(k) for k in range(producers)))
        await done.wait()
        meter.stop()
        for s in subs:
            await s.stop()
        await nexo.queue.delete(name)
        return meter.to_row(w["id"], "nexo", msgs, w.get("hot", False),
                          "e2e push->delivered, latency=enqueue->delivery")

    async def rabbit_push(w: dict, d: bool):
        ch = ch_d if d else ch_v
        name = f"{Q}.rp.{w['id']}.{'d' if d else 'v'}"
        await ch.declare_queue(name, durable=d, exclusive=not d)
        body = value.encode()
        mode = DeliveryMode.PERSISTENT if d else DeliveryMode.NOT_PERSISTENT
        msg = Message(body=body, delivery_mode=mode)
        m = await measure(w, w["ops"], lambda _: ch.default_exchange.publish(msg, routing_key=name))
        await ch.queue_delete(name)
        return m.to_row(
            w["id"], "rabbit-d" if d else "rabbit-v", w["ops"], w.get("hot", False),
            "persistent msg + confirm" if d else "non-persistent, no confirm",
        )

    async def rabbit_consume(w: dict, d: bool):
        ch = ch_d if d else ch_v
        name = f"{Q}.rc.{w['id']}.{'d' if d else 'v'}"
        queue = await ch.declare_queue(name, durable=d, exclusive=not d)
        msgs = w["msgs"]
        body = value.encode()
        mode = DeliveryMode.PERSISTENT if d else DeliveryMode.NOT_PERSISTENT
        total = w.get("prefill", msgs)
        for start in range(0, total, 200):
            n = min(200, total - start)
            await asyncio.gather(*(
                ch.default_exchange.publish(
                    Message(body=body, delivery_mode=mode),
                    routing_key=name,
                )
                for _ in range(n)
            ))

        if d:
            await ch.set_qos(prefetch_count=w.get("prefetch", 50))
        meter = Meter()
        got = 0
        done = asyncio.Event()

        async def on_msg(incoming: aio_pika.IncomingMessage) -> None:
            nonlocal got
            if d:
                async with incoming.process():
                    pass
            got += 1
            if got == msgs:
                done.set()

        tags = [
            await queue.consume(on_msg, no_ack=not d)
            for _ in range(w.get("consumers", 1))
        ]
        await done.wait()
        meter.stop()
        for t in tags:
            await queue.cancel(t)
        await ch.queue_delete(name)
        return meter.to_row(
            w["id"], "rabbit-d" if d else "rabbit-v", msgs, w.get("hot", False),
            "drain pre-filled (deliver+ack)" if d else "drain pre-filled (deliver, auto-ack)",
        )

    async def rabbit_pipeline(w: dict, d: bool):
        ch = ch_d if d else ch_v
        name = f"{Q}.rx.{w['id']}.{'d' if d else 'v'}"
        queue = await ch.declare_queue(name, durable=d, exclusive=not d)
        msgs = w["msgs"]
        mode = DeliveryMode.PERSISTENT if d else DeliveryMode.NOT_PERSISTENT

        if d:
            await ch.set_qos(prefetch_count=w.get("prefetch", 50))
        meter = Meter()
        got = 0
        done = asyncio.Event()

        async def on_msg(incoming: aio_pika.IncomingMessage) -> None:
            nonlocal got
            body = incoming.body.decode()
            if d:
                async with incoming.process():
                    pass
            meter.record_raw(now_ms() - embedded_time(body))
            got += 1
            if got == msgs:
                done.set()

        tags = [
            await queue.consume(on_msg, no_ack=not d)
            for _ in range(w.get("consumers", 1))
        ]
        producers = w.get("producers", 1)
        per = -(-msgs // producers)

        async def producer(k: int) -> None:
            for i in range(per):
                if k * per + i >= msgs:
                    break
                body = timed_payload(now_ms(), spec["payload_bytes"]).encode()
                await ch.default_exchange.publish(
                    Message(body=body, delivery_mode=mode),
                    routing_key=name,
                )

        await asyncio.gather(*(producer(k) for k in range(producers)))
        await done.wait()
        meter.stop()
        for t in tags:
            await queue.cancel(t)
        await ch.queue_delete(name)
        return meter.to_row(
            w["id"], "rabbit-d" if d else "rabbit-v", msgs, w.get("hot", False),
            "e2e push->delivered, persistent+confirm" if d
            else "e2e push->delivered, non-persistent, no confirm",
        )

    impls = {
        "nexo": {"push": nexo_push, "consume": nexo_consume, "pipeline": nexo_pipeline},
        "rabbit-d": {
            "push": lambda w: rabbit_push(w, True),
            "consume": lambda w: rabbit_consume(w, True),
            "pipeline": lambda w: rabbit_pipeline(w, True),
        },
        "rabbit-v": {
            "push": lambda w: rabbit_push(w, False),
            "consume": lambda w: rabbit_consume(w, False),
            "pipeline": lambda w: rabbit_pipeline(w, False),
        },
    }

    for w in spec["workloads"]:
        for system in ("nexo", "rabbit-d", "rabbit-v"):
            impl = impls.get(system, {}).get(w["op"])
            if impl is None:
                raise ValueError(f"unknown op '{w['op']}' for {system}")
            print(f"  · {w['id']} on {system}...", file=sys.stderr)
            rows.append(await impl(w))

    nexo.disconnect()
    await conn.close()
    return rows
