from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Generic, TypeVar

T = TypeVar("T")


class Subscription(Generic[T]):
    """Uniform observable subscription handle returned by all brokers.

    Usage:
        stream = await nexo.stream.get("s")
        sub = await stream.group("g").subscribe(callback)
        await sub.stop()
        await sub.wait_closed()
    """

    def __init__(
        self,
        stop_fn: Callable[[], Awaitable[None]],
        active_fn: Callable[[], bool],
        completion: asyncio.Future[None],
        error_fn: Callable[[], BaseException | None] | None = None,
    ) -> None:
        self._stop_fn = stop_fn
        self._active_fn = active_fn
        self._completion = completion
        self._error_fn = error_fn
        self._stop_requested = False
        self._stop_task: asyncio.Task[None] | None = None

    @property
    def active(self) -> bool:
        return not self._stop_requested and self._active_fn()

    @property
    def completion(self) -> asyncio.Future[None]:
        return self._completion

    @property
    def error(self) -> BaseException | None:
        if self._error_fn is not None:
            return self._error_fn()
        if self._completion.done() and not self._completion.cancelled():
            return self._completion.exception()
        return None

    async def stop(self) -> None:
        if self._stop_task is None:
            self._stop_requested = True
            self._stop_task = asyncio.create_task(self._run_stop())
        await asyncio.shield(self._stop_task)

    async def _run_stop(self) -> None:
        await self._stop_fn()

    async def wait_closed(self) -> None:
        await asyncio.shield(self._completion)
