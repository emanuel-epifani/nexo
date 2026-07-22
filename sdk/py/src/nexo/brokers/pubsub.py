from __future__ import annotations

import asyncio
from typing import Any, Callable, Generic, TypeVar, TypedDict

from ..connection import NexoConnection
from ..utils.logger import Logger


T = TypeVar("T")
PubSubHandler = Callable[[T], Any]


class PubSubOpcode:
    PUB = 0x21
    SUB = 0x22
    UNSUB = 0x23


class PublishOptions(TypedDict, total=False):
    retain: bool
    ttl: int


Handler = Callable[..., Any]


class NexoTopic(Generic[T]):
    def __init__(self, broker: "NexoPubSub", name: str) -> None:
        self._broker = broker
        self.name = name

    async def publish(self, data: T, options: PublishOptions | None = None) -> None:
        await self._broker.publish(self.name, data, options)

    async def clear(self) -> None:
        await self._broker.clear(self.name)

    async def subscribe(self, cb: PubSubHandler[T]) -> None:
        await self._broker.subscribe(self.name, cb)

    async def unsubscribe(self) -> None:
        await self._broker.unsubscribe(self.name)


class _Subscription:
    __slots__ = ("handler", "queue", "task")

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.task: asyncio.Task[None] | None = None


class NexoPubSub:
    def __init__(self, conn: NexoConnection, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger
        self._exact: dict[str, _Subscription] = {}
        self._wild: dict[str, tuple[list[str], _Subscription]] = {}

        conn.on_push = self._enqueue

        async def on_reconnect():
            topics = list(self._exact.keys()) + list(self._wild.keys())
            if not topics:
                return
            self._logger.info(f"[PubSub] Restoring {len(topics)} subscription(s)...")
            results = await asyncio.gather(
                *[self._conn.send(PubSubOpcode.SUB, lambda w, t=t: w.string(t)) for t in topics],
                return_exceptions=True,
            )
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    self._logger.error(f"[PubSub] Failed to resubscribe to {topics[i]}", r)

        conn.on_reconnect = on_reconnect

    async def publish(
        self, topic: str, data: Any, options: PublishOptions | None = None
    ) -> None:
        opts = options or {}
        retain = opts.get("retain", False)
        ttl = opts.get("ttl")
        if ttl is not None and (ttl < 0 or not isinstance(ttl, int)):
            raise ValueError(f"[PubSub] Invalid ttl: {ttl}")
        has_ttl = ttl is not None
        flags = (0x01 if retain else 0x00) | (0x02 if has_ttl else 0x00)

        def build(w):
            w.string(topic).u8(flags)
            if has_ttl:
                w.u32(ttl)
            w.any(data)

        await self._conn.send(PubSubOpcode.PUB, build)

    async def clear(self, topic: str) -> None:
        def build(w):
            w.string(topic).u8(0x04).any(b"")

        await self._conn.send(PubSubOpcode.PUB, build)

    async def subscribe(self, topic: str, callback: Handler) -> None:
        if topic in self._exact or topic in self._wild:
            raise ValueError(
                f'[PubSub] Already subscribed to "{topic}". Call unsubscribe() first.'
            )

        sub = _Subscription(callback)
        is_wild = self._is_wildcard(topic)
        if is_wild:
            self._wild[topic] = (topic.split("/"), sub)
        else:
            self._exact[topic] = sub

        try:
            await self._conn.send(PubSubOpcode.SUB, lambda w: w.string(topic))
        except Exception:
            if is_wild:
                self._wild.pop(topic, None)
            else:
                self._exact.pop(topic, None)
            raise

        sub.task = asyncio.create_task(self._consume(sub))

    async def unsubscribe(self, topic: str) -> None:
        sub = self._exact.pop(topic, None) or self._wild.pop(topic, None)
        if sub is None:
            return
        if isinstance(sub, tuple):
            sub = sub[1]
        await self._conn.send(PubSubOpcode.UNSUB, lambda w: w.string(topic))
        if sub.task is not None:
            sub.task.cancel()
            try:
                await sub.task
            except asyncio.CancelledError:
                pass

    def _enqueue(self, topic: str, data: Any) -> None:
        sub = self._exact.get(topic)
        if sub is not None:
            sub.queue.put_nowait(data)

        if not self._wild:
            return

        t_parts = topic.split("/")
        for parts, sub in self._wild.values():
            if self._matches_parts(parts, t_parts):
                sub.queue.put_nowait(data)

    async def _consume(self, sub: _Subscription) -> None:
        while True:
            data = await sub.queue.get()
            try:
                result = sub.handler(data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                self._logger.error(f"[PubSub] handler error: {e}")

    @staticmethod
    def _is_wildcard(topic: str) -> bool:
        return "+" in topic or "#" in topic

    @staticmethod
    def _matches_parts(p_parts: list[str], t_parts: list[str]) -> bool:
        for i, p in enumerate(p_parts):
            if p == "#":
                return True
            if i >= len(t_parts) or (p != "+" and p != t_parts[i]):
                return False
        return len(p_parts) == len(t_parts)
