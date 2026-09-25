from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

BENCH_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = BENCH_ROOT / "scenarios"
RESULTS_DIR = BENCH_ROOT / "results"


def env(key: str, fallback: str) -> str:
    return os.environ.get(key, fallback)


def load_spec(name: str) -> dict[str, Any]:
    return json.loads((SCENARIOS_DIR / f"{name}.json").read_text())


def make_payload(nbytes: int) -> str:
    return "x" * max(1, nbytes)


def timed_payload(now_ms: float, nbytes: int) -> str:
    head = f"{now_ms}|"
    return head + "x" * max(0, nbytes - len(head))


def embedded_time(data: Any) -> float:
    s = data if isinstance(data, str) else str(data)
    sep = s.find("|")
    return float(s[:sep]) if sep > 0 else float("nan")


def now_ms() -> float:
    return time.perf_counter() * 1000


class Meter:
    def __init__(self) -> None:
        self.latencies: list[float] = []
        self.secs = 0.0
        self._t0 = now_ms()

    def record(self, start_ms: float) -> None:
        self.latencies.append(now_ms() - start_ms)

    def record_raw(self, ms: float) -> None:
        if ms == ms:
            self.latencies.append(ms)

    def stop(self) -> None:
        self.secs = (now_ms() - self._t0) / 1000

    def to_row(self, workload: str, system: str, ops: int,
               hot: bool = False, note: str = "") -> dict[str, Any]:
        ls = sorted(self.latencies)
        n = len(ls)

        def at(p: int) -> float:
            return ls[min(n * p // 100, n - 1)] if n else 0.0

        return {
            "workload": workload, "system": system, "ops": ops, "secs": self.secs,
            "avg": sum(ls) / n if n else 0.0,
            "p50": at(50), "p95": at(95), "p99": at(99),
            "max": ls[-1] if n else 0.0, "hot": hot, "note": note,
        }


async def measure(w: dict[str, Any], ops: int,
                  fn: Callable[[int], Awaitable[Any]],
                  workers: int | None = None) -> Meter:
    """Run `ops` iterations split across `workers` sequential loops."""
    meter = Meter()
    n_workers = workers or (1 if w.get("mode") == "sequential" else w.get("workers", 10))
    per = -(-ops // n_workers)

    async def worker(k: int) -> None:
        for i in range(per):
            idx = k * per + i
            if idx >= ops:
                break
            s = now_ms()
            await fn(idx)
            meter.record(s)

    await asyncio.gather(*(worker(k) for k in range(n_workers)))
    meter.stop()
    return meter


def _fmt(ms: float) -> str:
    return f"{ms:.0f}" if ms >= 100 else f"{ms:.3f}"


def print_results(spec: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    line = "=" * 96
    print(f"\n{line}\n  {spec['title']}\n  hot path: {spec['hot_path']}\n{line}")
    print(f"  {'workload':<20} {'system':<9} {'ops':>8} {'ops/sec':>10} "
          f"{'avg ms':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}  note")
    for r in rows:
        wl = ("*" if r["hot"] else " ") + r["workload"]
        print(f"  {wl:<20} {r['system']:<9} {r['ops']:>8} {r['ops'] / r['secs']:>10.0f} "
              f"{_fmt(r['avg']):>8} {_fmt(r['p50']):>8} {_fmt(r['p95']):>8} "
              f"{_fmt(r['p99']):>8} {_fmt(r['max']):>8}  {r['note']}")
    print(f"{line}\n  * = hot-path workload\n")


def save_results(spec: dict[str, Any], rows: list[dict[str, Any]], lang: str) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dur = spec["durability"]
    lines = [
        f"# {spec['title']}", "",
        f"_run: {datetime.now(timezone.utc).isoformat()} · harness: {lang}_", "",
        f"**hot path**: {spec['hot_path']}", "",
        f"**durability** — nexo: {dur['nexo']} · {spec['reference']}: {dur['reference']}", "",
        "| workload | system | ops | ops/sec | avg ms | p50 | p95 | p99 | max | note |",
        "|---|---|---|---|---|---|---|---|---|---|",
        *[f"| {'**' + r['workload'] + '**' if r['hot'] else r['workload']} | {r['system']} | "
          f"{r['ops']} | {r['ops'] / r['secs']:.0f} | {_fmt(r['avg'])} | {_fmt(r['p50'])} | "
          f"{_fmt(r['p95'])} | {_fmt(r['p99'])} | {_fmt(r['max'])} | {r['note']} |"
          for r in rows],
        "",
    ]
    out = RESULTS_DIR / f"{spec['id']}.{lang}.md"
    out.write_text("\n".join(lines))
    return out
