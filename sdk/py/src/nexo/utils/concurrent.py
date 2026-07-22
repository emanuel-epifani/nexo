from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


async def run_concurrent(
    items: list[T],
    concurrency: int,
    fn: Callable[[T], Awaitable[None]],
) -> None:
    if not items:
        return
    if concurrency <= 1:
        for item in items:
            await fn(item)
        return

    index = 0
    total = len(items)

    async def worker() -> None:
        nonlocal index
        while index < total:
            current = index
            index += 1
            await fn(items[current])

    num_workers = min(concurrency, total)
    tasks = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await asyncio.gather(*tasks)
