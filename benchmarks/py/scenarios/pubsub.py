from __future__ import annotations

import asyncio
import logging
import sys
import time
from typing import Any, Callable

import aiomqtt
from nexo import NexoClient

logging.getLogger("mqtt").setLevel(logging.ERROR)

from report import Meter, embedded_time, env, make_payload, measure, now_ms, timed_payload

RUN = str(int(time.time() * 1000))
TOPIC = f"bench/{RUN}/metric"
MQTT_HOST = env("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(env("MQTT_PORT", "1883"))


def pattern_for(w: dict) -> str:
    p = w.get("pattern")
    return f"bench/{RUN}/" + "/".join(p.split("/")[1:]) if p else TOPIC


def topic_for(w: dict) -> str:
    t = w.get("topic")
    return f"bench/{RUN}/" + "/".join(t.split("/")[1:]) if t else TOPIC


class MqttSub:
    def __init__(self, pattern: str, on_msg: Callable[[str], None]) -> None:
        self._client = aiomqtt.Client(MQTT_HOST, port=MQTT_PORT)
        self._pattern = pattern
        self._on_msg = on_msg
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await self._client.__aenter__()
        await self._client.subscribe(self._pattern, qos=0)
        self._task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        async for msg in self._client.messages:
            self._on_msg(msg.payload.decode())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        await self._client.__aexit__(None, None, None)


async def run(spec: dict[str, Any]) -> list[dict[str, Any]]:
    pub = await NexoClient.connect(
        host=env("NEXO_HOST", "127.0.0.1"),
        port=int(env("NEXO_PORT", "7654")),
    )

    value = make_payload(spec["payload_bytes"])
    rows: list[dict[str, Any]] = []

    async def nexo_subs(w: dict, on_msg: Callable[[Any], None]):
        clients = []
        for _ in range(w.get("subscribers", 1)):
            c = await NexoClient.connect(
                host=env("NEXO_HOST", "127.0.0.1"),
                port=int(env("NEXO_PORT", "7654")),
            )
            if w.get("pattern"):
                sub = await c.pubsub.pattern(pattern_for(w)).subscribe(lambda d: on_msg(d))
            else:
                sub = await c.pubsub.topic(TOPIC).subscribe(lambda d: on_msg(d))
            clients.append((c, sub))
        return clients

    async def nexo_teardown(subs) -> None:
        for c, sub in subs:
            try:
                await sub.stop()
            except Exception:
                pass
            c.disconnect()

    async def nexo_publish(w: dict):
        delivered = 0

        def _inc(_d: Any) -> None:
            nonlocal delivered
            delivered += 1

        subs = await nexo_subs(w, _inc)
        topic = pub.pubsub.topic(topic_for(w))
        m = await measure(w, w["ops"], lambda _: topic.publish(value))
        t0 = now_ms()
        while delivered < w["ops"] * w.get("subscribers", 1) and now_ms() - t0 < 30_000:
            await asyncio.sleep(0.005)
        await nexo_teardown(subs)
        return m.to_row(w["id"], "nexo", w["ops"], w.get("hot", False),
                        f"delivered={delivered}")

    async def nexo_fanout(w: dict):
        delivered = 0
        def _inc():
            nonlocal delivered
            delivered += 1
        subs = await nexo_subs(w, lambda _d: _inc())
        expected = w["ops"] * w.get("subscribers", 1)
        topic = pub.pubsub.topic(TOPIC)
        m = await measure(w, w["ops"], lambda _: topic.publish(value))
        t0 = now_ms()
        while delivered < expected and now_ms() - t0 < 60_000:
            await asyncio.sleep(0.005)
        await nexo_teardown(subs)
        return m.to_row(w["id"], "nexo", w["ops"], w.get("hot", False),
                        f"1->{w.get('subscribers')} delivered={delivered}")

    async def nexo_latency(w: dict):
        meter = Meter()
        pending: list[asyncio.Future] = []

        def on_msg(data: Any) -> None:
            meter.record_raw(now_ms() - embedded_time(data))
            if pending and not pending[0].done():
                pending.pop(0).set_result(None)

        subs = await nexo_subs(w, on_msg)
        topic = pub.pubsub.topic(TOPIC)
        for _ in range(w["ops"]):
            fut = asyncio.get_running_loop().create_future()
            pending.append(fut)
            await topic.publish(timed_payload(now_ms(), spec["payload_bytes"]))
            await fut
        meter.stop()
        await nexo_teardown(subs)
        return meter.to_row(w["id"], "nexo", w["ops"], w.get("hot", False),
                          "pub->deliver latency")

    async def mqtt_publish(w: dict):
        delivered = 0
        def _inc():
            nonlocal delivered
            delivered += 1
        subs = [MqttSub(pattern_for(w), lambda _d: _inc()) for _ in range(w.get("subscribers", 1))]
        for s in subs:
            await s.start()
        pubc = aiomqtt.Client(MQTT_HOST, port=MQTT_PORT, max_inflight_messages=65_535)
        await pubc.__aenter__()
        topic = topic_for(w)
        m = await measure(w, w["ops"], lambda _: pubc.publish(topic, value, qos=1))
        t0 = now_ms()
        while delivered < w["ops"] * w.get("subscribers", 1) and now_ms() - t0 < 30_000:
            await asyncio.sleep(0.005)
        await pubc.__aexit__(None, None, None)
        for s in subs:
            await s.stop()
        return m.to_row(w["id"], "mqtt", w["ops"], w.get("hot", False),
                        f"delivered={delivered}, qos1 pub")

    async def mqtt_fanout(w: dict):
        delivered = 0
        def _inc():
            nonlocal delivered
            delivered += 1
        subs = [MqttSub(TOPIC, lambda _d: _inc()) for _ in range(w.get("subscribers", 1))]
        for s in subs:
            await s.start()
        pubc = aiomqtt.Client(MQTT_HOST, port=MQTT_PORT, max_inflight_messages=65_535)
        await pubc.__aenter__()
        expected = w["ops"] * w.get("subscribers", 1)
        m = await measure(w, w["ops"], lambda _: pubc.publish(TOPIC, value, qos=1))
        t0 = now_ms()
        while delivered < expected and now_ms() - t0 < 60_000:
            await asyncio.sleep(0.005)
        await pubc.__aexit__(None, None, None)
        for s in subs:
            await s.stop()
        return m.to_row(w["id"], "mqtt", w["ops"], w.get("hot", False),
                        f"1->{w.get('subscribers')} delivered={delivered}, qos1 pub")

    async def mqtt_latency(w: dict):
        meter = Meter()
        pending: list[asyncio.Future] = []

        def on_msg(data: str) -> None:
            meter.record_raw(now_ms() - embedded_time(data))
            if pending and not pending[0].done():
                pending.pop(0).set_result(None)

        sub = MqttSub(TOPIC, on_msg)
        await sub.start()
        pubc = aiomqtt.Client(MQTT_HOST, port=MQTT_PORT, max_inflight_messages=65_535)
        await pubc.__aenter__()
        for _ in range(w["ops"]):
            fut = asyncio.get_running_loop().create_future()
            pending.append(fut)
            await pubc.publish(TOPIC, timed_payload(now_ms(), spec["payload_bytes"]), qos=1)
            await fut
        meter.stop()
        await pubc.__aexit__(None, None, None)
        await sub.stop()
        return meter.to_row(w["id"], "mqtt", w["ops"], w.get("hot", False),
                          "pub->deliver latency, qos1 pub")

    impls = {
        "nexo": {"publish": nexo_publish, "fanout": nexo_fanout, "latency": nexo_latency},
        "mqtt": {"publish": mqtt_publish, "fanout": mqtt_fanout, "latency": mqtt_latency},
    }

    for w in spec["workloads"]:
        for system in ("nexo", spec["reference"]):
            impl = impls.get(system, {}).get(w["op"])
            if impl is None:
                raise ValueError(f"unknown op '{w['op']}' for {system}")
            print(f"  · {w['id']} on {system}...", file=sys.stderr)
            rows.append(await impl(w))

    pub.disconnect()
    return rows
