from __future__ import annotations

import asyncio
from typing import Any, Callable, TypedDict

from ..connection import NexoConnection
from ..utils.logger import Logger


class PubSubOpcode:
    PUB = 0x21
    SUB = 0x22
    UNSUB = 0x23


class PublishOptions(TypedDict, total=False):
    retain: bool
    ttl: int


Handler = Callable[[Any], None]


class NexoTopic:
    def __init__(self, broker: "NexoPubSub", name: str) -> None:
        self._broker = broker
        self.name = name

    async def publish(self, data: Any, options: PublishOptions | None = None) -> None:
        await self._broker.publish(self.name, data, options)

    async def clear(self) -> None:
        await self._broker.clear(self.name)

    async def subscribe(self, cb: Handler) -> None:
        await self._broker.subscribe(self.name, cb)

    async def unsubscribe(self) -> None:
        await self._broker.unsubscribe(self.name)


class NexoPubSub:
    def __init__(self, conn: NexoConnection, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger
        self._exact: dict[str, Handler] = {}
        self._wild: dict[str, tuple[list[str], Handler]] = {}

        conn.on_push = self._dispatch

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

        is_wild = self._is_wildcard(topic)
        if is_wild:
            self._wild[topic] = (topic.split("/"), callback)
        else:
            self._exact[topic] = callback

        try:
            await self._conn.send(PubSubOpcode.SUB, lambda w: w.string(topic))
        except Exception:
            if is_wild:
                self._wild.pop(topic, None)
            else:
                self._exact.pop(topic, None)
            raise

    async def unsubscribe(self, topic: str) -> None:
        if topic not in self._exact and topic not in self._wild:
            return
        await self._conn.send(PubSubOpcode.UNSUB, lambda w: w.string(topic))
        self._exact.pop(topic, None)
        self._wild.pop(topic, None)

    def _dispatch(self, topic: str, data: Any) -> None:
        exact_cb = self._exact.get(topic)
        if exact_cb is not None:
            try:
                exact_cb(data)
            except Exception as e:
                self._logger.error(f"[PubSub] handler error: {e}")

        if not self._wild:
            return

        t_parts = topic.split("/")
        for parts, cb in self._wild.values():
            if self._matches_parts(parts, t_parts):
                try:
                    cb(data)
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
