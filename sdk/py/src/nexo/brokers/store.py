from __future__ import annotations

from typing import Any, TypeVar, TypedDict

from ..connection import NexoConnection
from ..protocol import ResponseStatus


T = TypeVar("T")


class StoreOpcode:
    MAP_SET = 0x02
    MAP_GET = 0x03
    MAP_DEL = 0x04
    MAP_INCR = 0x05


class MapSetOptions(TypedDict, total=False):
    ttl: int


class NexoMap:
    def __init__(self, conn: NexoConnection) -> None:
        self._conn = conn

    async def set(
        self, key: str, value: T, options: MapSetOptions | None = None
    ) -> None:
        opts = options or {}
        ttl = opts.get("ttl")
        has_ttl = ttl is not None
        flags = 0x01 if has_ttl else 0x00

        def build(w):
            w.string(key).u8(flags)
            if has_ttl:
                w.u64(ttl)
            w.any(value)

        await self._conn.send(StoreOpcode.MAP_SET, build)

    async def get(self, key: str) -> T | None:
        status, cursor = await self._conn.send(
            StoreOpcode.MAP_GET, lambda w: w.string(key)
        )
        if status == ResponseStatus.NULL:
            return None
        return cursor.decode_any()

    async def delete(self, key: str) -> None:
        await self._conn.send(StoreOpcode.MAP_DEL, lambda w: w.string(key))

    async def incr(self, key: str, delta: int = 1) -> int:
        status, cursor = await self._conn.send(
            StoreOpcode.MAP_INCR, lambda w: w.string(key).i64(delta)
        )
        if status == ResponseStatus.DATA:
            return cursor.decode_any()
        raise Exception(cursor.read_string())


class NexoStore:
    def __init__(self, conn: NexoConnection) -> None:
        self.map = NexoMap(conn)
