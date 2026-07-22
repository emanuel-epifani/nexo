from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine


async def wait_for(
    assertion_or_condition: Callable[[], Any],
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    start = time.monotonic()
    last_error: Exception | None = None

    while True:
        try:
            result = assertion_or_condition()
            if asyncio.iscoroutine(result):
                result = await result
            if result is False:
                raise AssertionError("Condition returned false")
            return
        except Exception as e:
            last_error = e
            if time.monotonic() - start > timeout:
                if last_error is not None:
                    raise last_error
                raise
            await asyncio.sleep(interval)
