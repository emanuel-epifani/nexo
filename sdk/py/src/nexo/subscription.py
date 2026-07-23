from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Generic, Optional, TypeVar

T = TypeVar("T")


class Subscription(Generic[T]):
    """Uniform subscription handle returned by all brokers.

    Usage:
        sub = await nexo.stream("s").subscribe("g", callback)
        await sub.stop()
    """

    def __init__(
        self,
        stop_fn: Callable[[], Awaitable[None]],
        active_fn: Callable[[], bool] | None = None,
    ) -> None:
        self._stop_fn = stop_fn
        self._active_fn = active_fn
        self._stopped = False

    @property
    def active(self) -> bool:
        if self._stopped:
            return False
        if self._active_fn is not None:
            return self._active_fn()
        return not self._stopped

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        await self._stop_fn()
