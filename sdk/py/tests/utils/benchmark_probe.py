from __future__ import annotations

import time
from typing import Optional


class BenchmarkProbe:
    def __init__(self, name: str, total_ops: int) -> None:
        self._name = name
        self._total_ops = total_ops
        self._latencies: list[float] = [0.0] * total_ops
        self._current_index = 0
        self._start: float = 0.0

    def start_timer(self) -> None:
        self._start = time.perf_counter()

    def record(self, val: float) -> None:
        if self._current_index < self._total_ops:
            self._latencies[self._current_index] = val
            self._current_index += 1

    def record_batch(self, count: int, total_ms: float) -> None:
        per_message = total_ms / count
        remaining = self._total_ops - self._current_index
        to_record = min(count, remaining)
        for _ in range(to_record):
            self._latencies[self._current_index] = per_message
            self._current_index += 1

    def print_result(self) -> dict[str, float]:
        duration_sec = time.perf_counter() - self._start
        throughput = int(self._total_ops / duration_sec) if duration_sec > 0 else 0

        valid = sorted(self._latencies[: self._current_index])
        count = len(valid)

        p50 = valid[int(count * 0.50)] if count > 0 else 0.0
        p99 = valid[int(count * 0.99)] if count > 0 else 0.0
        max_lat = valid[count - 1] if count > 0 else 0.0

        print(f"\n\033[36m[{self._name}]\033[0m")
        print(f" Throughput:  \033[32m{throughput:,} ops/sec\033[0m")
        if count > 0:
            color_max = "\033[31m" if max_lat > 100 else "\033[33m"
            print(
                f" Latency:     p50: {p50:.2f}ms | p99: {p99:.2f}ms | "
                f"{color_max}MAX: {max_lat:.2f}ms\033[0m (samples: {count})"
            )
        else:
            print(" Latency:     (no samples recorded)")

        return {"throughput": throughput, "p99": p99, "max": max_lat}
