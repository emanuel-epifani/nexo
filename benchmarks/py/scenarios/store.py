from __future__ import annotations

import sys
import time
from typing import Any

import redis.asyncio as aioredis
from nexo import NexoClient

from report import env, load_spec, make_payload, measure


def kv_impl(system: str, set_fn, get_fn):
    async def set_(w: dict, run_id: str, value: str):
        m = await measure(w, w["ops"], lambda i: set_fn(f"b:{run_id}:s:{i}", value))
        return m.to_row(w["id"], system, w["ops"], w.get("hot", False))

    async def get_(w: dict, run_id: str, value: str):
        pre = w.get("prefill", w["ops"])
        for i in range(pre):
            await set_fn(f"b:{run_id}:g:{i}", value)
        m = await measure(w, w["ops"], lambda i: get_fn(f"b:{run_id}:g:{i % pre}"))
        return m.to_row(w["id"], system, w["ops"], w.get("hot", False))

    async def mix(w: dict, run_id: str, value: str):
        pre = w.get("prefill", 1000)
        for i in range(pre):
            await set_fn(f"b:{run_id}:m:{i}", value)
        write_every = round(1 / (1 - w.get("read_ratio", 0.8)))

        async def op(i: int):
            if i % write_every == 0:
                await set_fn(f"b:{run_id}:m:{i % pre}", value)
            else:
                await get_fn(f"b:{run_id}:m:{i % pre}")

        m = await measure(w, w["ops"], op)
        return m.to_row(w["id"], system, w["ops"], w.get("hot", False),
                      f"read_ratio={w.get('read_ratio')}")

    return {"set": set_, "get": get_, "mix": mix}


async def run(spec: dict[str, Any]) -> list[dict[str, Any]]:
    nexo = await NexoClient.connect(
        host=env("NEXO_HOST", "127.0.0.1"),
        port=int(env("NEXO_PORT", "7654")),
    )
    redis = aioredis.Redis.from_url(env("REDIS_URL", "redis://127.0.0.1:6379"))
    await redis.ping()

    value = make_payload(spec["payload_bytes"])
    run_id = str(int(time.time() * 1000))
    impls = {
        "nexo": kv_impl(
            "nexo",
            lambda k, v: nexo.store.map.set(k, v),
            lambda k: nexo.store.map.get(k),
        ),
        "redis": kv_impl(
            "redis",
            lambda k, v: redis.set(k, v),
            lambda k: redis.get(k),
        ),
    }

    rows: list[dict[str, Any]] = []
    for w in spec["workloads"]:
        for system in ("nexo", spec["reference"]):
            impl = impls[system].get(w["op"])
            if impl is None:
                raise ValueError(f"unknown op '{w['op']}' for {system}")
            print(f"  · {w['id']} on {system}...", file=sys.stderr)
            rows.append(await impl(w, run_id, value))

    nexo.disconnect()
    await redis.aclose()
    return rows
