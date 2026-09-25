from __future__ import annotations

import asyncio
import sys
import time
from typing import Any

import nats
import nats.errors
from nats.js.api import StreamConfig
from nexo import NexoClient

from report import Meter, embedded_time, env, make_payload, measure, now_ms, timed_payload

RUN = str(int(time.time() * 1000))
STREAM = f"BENCH_{RUN}"
SUBJECTS = f"bench.stream.{RUN}.*"


def subj(w: dict) -> str:
    return f"bench.stream.{RUN}.{w['id']}"


async def run(spec: dict[str, Any]) -> list[dict[str, Any]]:
    nexo = await NexoClient.connect(
        host=env("NEXO_HOST", "127.0.0.1"),
        port=int(env("NEXO_PORT", "7654")),
    )

    nc = await nats.connect(env("NATS_URL", "nats://127.0.0.1:4222"))
    js = nc.jetstream()
    await js.add_stream(StreamConfig(name=STREAM, subjects=[SUBJECTS]))

    value = make_payload(spec["payload_bytes"])
    rows: list[dict[str, Any]] = []

    # ---- nexo ----
    async def nexo_publish(w: dict):
        name = f"bench-{RUN}-{w['id']}"
        await nexo.stream.create(name)
        s = await nexo.stream.get(name)
        m = await measure(w, w["ops"], lambda _: s.publish(value))
        await nexo.stream.delete(name)
        return m.to_row(w["id"], "nexo", w["ops"], w.get("hot", False))

    async def nexo_publish_batch(w: dict):
        name = f"bench-{RUN}-{w['id']}"
        await nexo.stream.create(name)
        s = await nexo.stream.get(name)
        batch = w.get("batch", 100)
        items = [{"data": value}] * batch
        calls = -(-w["ops"] // batch)
        m = await measure(w, calls, lambda _: s.publish_batch(items))
        await nexo.stream.delete(name)
        return m.to_row(w["id"], "nexo", w["ops"], w.get("hot", False),
                        f"latency per {batch}-msg call")

    async def nexo_consume(w: dict):
        name = f"bench-{RUN}-{w['id']}"
        await nexo.stream.create(name)
        s = await nexo.stream.get(name)
        msgs = w["msgs"]
        items = [{"data": value}] * 100
        for _ in range(msgs // 100):
            await s.publish_batch(items)

        meter = Meter()
        got = 0

        async def cb(_data: Any) -> None:
            nonlocal got
            got += 1

        sub = await s.group(f"drain-{w['id']}").subscribe(
            cb,
            batch_size=w.get("batch_size", 500),
            wait_ms=w.get("wait_ms", 100),
            concurrency=1,
        )
        t0 = now_ms()
        while got < msgs:
            if now_ms() - t0 > 180_000:
                raise TimeoutError("nexo stream drain timeout")
            await asyncio.sleep(0.005)
        meter.stop()
        await sub.stop()
        await nexo.stream.delete(name)
        return meter.to_row(w["id"], "nexo", msgs, w.get("hot", False),
                          "group read of pre-filled stream (fetch+ack)")

    async def nexo_pipeline(w: dict):
        name = f"bench-{RUN}-{w['id']}"
        await nexo.stream.create(name)
        s = await nexo.stream.get(name)
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
            await s.group(f"pipe-{w['id']}-{c}").subscribe(
                cb,
                batch_size=w.get("batch_size", 500),
                wait_ms=w.get("wait_ms", 100),
                concurrency=1,
            )
            for c in range(w.get("consumers", 1))
        ]
        producers = w.get("producers", 1)
        per = -(-msgs // producers)

        async def producer(k: int) -> None:
            for i in range(per):
                if k * per + i >= msgs:
                    break
                await s.publish(timed_payload(now_ms(), spec["payload_bytes"]))

        await asyncio.gather(*(producer(k) for k in range(producers)))
        await done.wait()
        meter.stop()
        for sub in subs:
            await sub.stop()
        await nexo.stream.delete(name)
        return meter.to_row(w["id"], "nexo", msgs, w.get("hot", False),
                          "e2e publish->delivered via consumer group")

    # ---- jetstream ----
    async def js_publish(w: dict):
        subject = subj(w)
        m = await measure(w, w["ops"], lambda _: js.publish(subject, value.encode()))
        return m.to_row(w["id"], "jetstream", w["ops"], w.get("hot", False))

    async def js_publish_batch(w: dict):
        subject = subj(w)
        batch = w.get("batch", 100)
        calls = -(-w["ops"] // batch)

        async def call(_: int):
            await asyncio.gather(*(js.publish(subject, value.encode()) for _ in range(batch)))

        m = await measure(w, calls, call)
        return m.to_row(w["id"], "jetstream", w["ops"], w.get("hot", False),
                        f"latency per {batch}-msg call (parallel pub)")

    async def js_consume(w: dict):
        subject = subj(w)
        msgs = w["msgs"]
        for _ in range(msgs // 100):
            await asyncio.gather(*(js.publish(subject, value.encode()) for _ in range(100)))

        psub = await js.pull_subscribe(subject, f"drain-{w['id']}", stream=STREAM)
        meter = Meter()
        got = 0
        while got < msgs:
            try:
                batch = await psub.fetch(
                    min(w.get("batch_size", 500), msgs - got), timeout=5
                )
            except nats.errors.TimeoutError:
                continue
            for msg in batch:
                await msg.ack()
                got += 1
        meter.stop()
        return meter.to_row(w["id"], "jetstream", msgs, w.get("hot", False),
                          "consumer read of pre-filled stream (fetch+ack)")

    async def js_pipeline(w: dict):
        subject = subj(w)
        msgs = w["msgs"]
        meter = Meter()
        got = 0
        done = asyncio.Event()

        psub = await js.pull_subscribe(subject, f"pipe-{w['id']}", stream=STREAM)

        async def drain() -> None:
            nonlocal got
            while got < msgs:
                try:
                    batch = await psub.fetch(w.get("batch_size", 500), timeout=2)
                except nats.errors.TimeoutError:
                    continue
                for msg in batch:
                    await msg.ack()
                    meter.record_raw(now_ms() - embedded_time(msg.data.decode()))
                    got += 1
                    if got == msgs:
                        done.set()

        drain_task = asyncio.create_task(drain())
        producers = w.get("producers", 1)
        per = -(-msgs // producers)

        async def producer(k: int) -> None:
            for i in range(per):
                if k * per + i >= msgs:
                    break
                await js.publish(subject, timed_payload(now_ms(), spec["payload_bytes"]).encode())

        await asyncio.gather(*(producer(k) for k in range(producers)))
        await done.wait()
        meter.stop()
        drain_task.cancel()
        return meter.to_row(w["id"], "jetstream", msgs, w.get("hot", False),
                          "e2e publish->delivered via durable pull consumer")

    impls = {
        "nexo": {
            "publish": nexo_publish,
            "publish_batch": nexo_publish_batch,
            "consume": nexo_consume,
            "pipeline": nexo_pipeline,
        },
        "jetstream": {
            "publish": js_publish,
            "publish_batch": js_publish_batch,
            "consume": js_consume,
            "pipeline": js_pipeline,
        },
    }

    for w in spec["workloads"]:
        for system in ("nexo", spec["reference"]):
            impl = impls.get(system, {}).get(w["op"])
            if impl is None:
                raise ValueError(f"unknown op '{w['op']}' for {system}")
            print(f"  · {w['id']} on {system}...", file=sys.stderr)
            rows.append(await impl(w))

    nexo.disconnect()
    try:
        await js.delete_stream(STREAM)
    except Exception:
        pass
    await nc.drain()
    return rows
