from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Generic, Optional, TypeVar, TypedDict, Union

from ..config import DEFAULT_CONFIG
from ..connection import NexoConnection
from ..errors import ConnectionClosedError, NotConnectedError
from ..utils.concurrent import run_concurrent
from ..utils.logger import Logger


T = TypeVar("T")


class StreamMessageMeta(TypedDict):
    seq: int
    key: bytes | None


StreamHandler = Callable[[T, StreamMessageMeta], Any] | Callable[[T], Any]


FETCH_TIMEOUT_MARGIN_MS = 5000


class StreamOpcode:
    S_CREATE = 0x30
    S_PUB = 0x31
    S_FETCH = 0x32
    S_JOIN = 0x33
    S_ACK = 0x34
    S_EXISTS = 0x35
    S_DELETE = 0x36
    S_SEEK = 0x38
    S_LEAVE = 0x39
    S_PEEK_DLT = 0x3A
    S_MOVE_TO_STREAM = 0x3B
    S_DELETE_DLT = 0x3C
    S_PURGE_DLT = 0x3D


class RetentionOptions(TypedDict, total=False):
    max_age_ms: int
    max_bytes: int


class StreamCreateOptions(TypedDict, total=False):
    retention: RetentionOptions


class StreamSubscribeOptions(TypedDict, total=False):
    batch_size: int
    wait_ms: int
    concurrency: int


def _callback_accepts_meta(fn: Callable[..., Any]) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return True
    positional = 0
    for p in sig.parameters.values():
        if p.kind == p.VAR_POSITIONAL:
            return True
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            positional += 1
    return positional >= 2


def _is_recoverable_membership_error(e: Exception) -> bool:
    msg = str(e)
    return "FENCED" in msg or "NOT_MEMBER" in msg


async def _sleep(ms: int) -> None:
    await asyncio.sleep(ms / 1000.0)


class StreamSubscription(Generic[T]):
    def __init__(
        self,
        conn: NexoConnection,
        stream_name: str,
        group: str,
        logger: Logger,
        callback: Callable[..., Any],
        batch_size: int,
        wait_ms: int,
        concurrency: int,
    ) -> None:
        self._conn = conn
        self._stream_name = stream_name
        self._group = group
        self._logger = logger
        self._callback = callback
        self._callback_wants_meta = _callback_accepts_meta(callback)
        self._batch_size = batch_size
        self._wait_ms = wait_ms
        self._concurrency = concurrency

        self._active = False
        self._loop_task: Optional[asyncio.Task] = None
        self._consumer_id: Optional[str] = None
        self._generation: int = 0

    async def start(self) -> None:
        self._active = True
        await self._join()
        self._loop_task = asyncio.create_task(self._loop())
        self._loop_task.add_done_callback(
            lambda t: self._logger.error(
                f"[{self._stream_name}:{self._group}] Consumer crashed",
                t.exception(),
            )
            if t.exception()
            else None
        )

    async def stop(self) -> None:
        self._active = False
        if self._consumer_id is not None:
            try:
                await self._conn.send(
                    StreamOpcode.S_LEAVE,
                    lambda w: w.string(self._stream_name)
                    .string(self._group)
                    .string(self._consumer_id)  # type: ignore[arg-type]
                    .u64(self._generation),
                )
            except Exception:
                pass
        if self._loop_task is not None:
            await self._loop_task

    async def _join(self) -> None:
        if not self._conn.is_connected:
            raise NotConnectedError()
        status, cursor = await self._conn.send(
            StreamOpcode.S_JOIN,
            lambda w: w.string(self._stream_name).string(self._group),
        )
        cursor.read_u64()  # ack_floor (unused)
        self._generation = cursor.read_u64()
        self._consumer_id = cursor.read_string()

    async def _loop(self) -> None:
        while self._active:
            try:
                if self._consumer_id is None:
                    await self._join()
                await self._poll_once()
            except Exception as e:
                if not self._active:
                    break
                self._consumer_id = None

                if _is_recoverable_membership_error(e):
                    continue

                if not self._conn.is_connected or isinstance(e, ConnectionClosedError):
                    await _sleep(DEFAULT_CONFIG.connection.backoff_short_ms)
                    continue

                self._logger.error(
                    f"[{self._stream_name}:{self._group}] Error. "
                    f"Retrying in {DEFAULT_CONFIG.connection.backoff_long_ms}ms... {e}"
                )
                await _sleep(DEFAULT_CONFIG.connection.backoff_long_ms)

    async def _poll_once(self) -> None:
        consumer_id = self._consumer_id
        assert consumer_id is not None
        generation = self._generation

        status, cursor = await self._conn.send(
            StreamOpcode.S_FETCH,
            lambda w: w.string(self._stream_name)
            .string(self._group)
            .string(consumer_id)
            .u64(generation)
            .u32(self._batch_size)
            .u32(self._wait_ms),
            timeout_ms=self._wait_ms + FETCH_TIMEOUT_MARGIN_MS,
        )

        count = cursor.read_u32()
        if count == 0:
            return

        batch: list[dict[str, Any]] = []
        for _ in range(count):
            seq = cursor.read_u64()
            cursor.read_u64()  # skip timestamp
            key_len = cursor.read_u16()
            key = cursor.read_buffer(key_len) if key_len > 0 else None
            payload_len = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_len)
            batch.append({"seq": seq, "key": key, "data": data})

        async def process(msg):
            if not self._active:
                return
            try:
                if self._callback_wants_meta:
                    result = self._callback(msg["data"], {"seq": msg["seq"], "key": msg["key"]})
                else:
                    result = self._callback(msg["data"])
                if asyncio.iscoroutine(result):
                    await result
                self._conn.send_fire_and_forget(
                    StreamOpcode.S_ACK,
                    lambda w: w.string(self._stream_name)
                    .string(self._group)
                    .string(consumer_id)
                    .u64(generation)
                    .u64(msg["seq"]),
                )
            except Exception as err:
                self._logger.error(
                    f"[{self._stream_name}:{self._group}] "
                    f"Processing error at seq={msg['seq']}. "
                    f"Waiting for timeout-based retry. {err}"
                )

        await run_concurrent(batch, self._concurrency, process)


class NexoStream(Generic[T]):
    def __init__(self, conn: NexoConnection, name: str, logger: Logger) -> None:
        self._conn = conn
        self.name = name
        self._logger = logger

    async def create(
        self, options: StreamCreateOptions | None = None
    ) -> NexoStream[T]:
        opts = options or {}
        retention = opts.get("retention") or {}
        max_age = retention.get("max_age_ms")
        max_bytes = retention.get("max_bytes")
        has_max_age = max_age is not None
        has_max_bytes = max_bytes is not None
        flags = (0x01 if has_max_age else 0x00) | (0x02 if has_max_bytes else 0x00)

        def build(w):
            w.string(self.name).u8(flags)
            if has_max_age:
                w.u64(max_age)
            if has_max_bytes:
                w.u64(max_bytes)

        await self._conn.send(StreamOpcode.S_CREATE, build)
        return self

    async def exists(self) -> bool:
        try:
            status, cursor = await self._conn.send(
                StreamOpcode.S_EXISTS, lambda w: w.string(self.name)
            )
            return cursor.read_u8() == 1
        except Exception:
            return False

    async def delete(self) -> None:
        await self._conn.send(StreamOpcode.S_DELETE, lambda w: w.string(self.name))

    async def publish(
        self,
        data: T,
        options: Optional[dict[str, Union[str, bytes]]] = None,
    ) -> int:
        opts = options or {}
        key = opts.get("key")

        def build(w):
            w.string(self.name).u32(1)
            if key is None:
                w.u16(0)
            else:
                key_bytes = key.encode("utf-8") if isinstance(key, str) else bytes(key)
                w.u16(len(key_bytes))
                w.raw_bytes(key_bytes)
            w.any_with_len(data)

        status, cursor = await self._conn.send(StreamOpcode.S_PUB, build)
        count = cursor.read_u32()
        return cursor.read_u64() if count > 0 else 0

    async def publish_batch(
        self, items: list[dict[str, Any]]
    ) -> list[int]:
        if not items:
            return []

        def build(w):
            w.string(self.name).u32(len(items))
            for item in items:
                key = item.get("key")
                if key is None:
                    w.u16(0)
                else:
                    key_bytes = key.encode("utf-8") if isinstance(key, str) else bytes(key)
                    w.u16(len(key_bytes))
                    w.raw_bytes(key_bytes)
                w.any_with_len(item["data"])

        status, cursor = await self._conn.send(StreamOpcode.S_PUB, build)
        count = cursor.read_u32()
        seqs: list[int] = []
        for _ in range(count):
            seqs.append(cursor.read_u64())
        return seqs

    async def subscribe(
        self,
        group: str,
        callback: StreamHandler[T],
        options: StreamSubscribeOptions | None = None,
    ) -> dict[str, Callable[[], "asyncio.Future[None]"]]:
        if not group:
            raise ValueError("Consumer Group is required for subscription")

        opts = options or {}
        batch_size = opts.get("batch_size")
        if batch_size is None:
            batch_size = DEFAULT_CONFIG.stream.batch_size
        wait_ms = opts.get("wait_ms")
        if wait_ms is None:
            wait_ms = DEFAULT_CONFIG.stream.wait_ms
        concurrency = opts.get("concurrency")
        if concurrency is None:
            concurrency = DEFAULT_CONFIG.stream.concurrency
        concurrency = max(1, concurrency)

        sub: StreamSubscription[T] = StreamSubscription(
            self._conn,
            self.name,
            group,
            self._logger,
            callback,
            batch_size,
            wait_ms,
            concurrency,
        )
        await sub.start()

        def stop():
            return asyncio.create_task(sub.stop())

        return {"stop": stop}

    async def seek(self, group: str, target: str) -> None:
        await self._conn.send(
            StreamOpcode.S_SEEK,
            lambda w: w.string(self.name)
            .string(group)
            .u8(0 if target == "beginning" else 1),
        )

    async def peek_dlt(
        self, group: str, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        status, cursor = await self._conn.send(
            StreamOpcode.S_PEEK_DLT,
            lambda w: w.string(self.name).string(group).u32(limit).u32(offset),
        )
        count = cursor.read_u32()
        entries: list[dict[str, Any]] = []
        for _ in range(count):
            seq = cursor.read_u64()
            reason = cursor.read_string()
            attempts = cursor.read_u32()
            key_len = cursor.read_u16()
            key = cursor.read_buffer(key_len) if key_len > 0 else None
            entries.append({"seq": seq, "reason": reason, "attempts": attempts, "key": key})
        return entries

    async def move_to_stream(self, group: str, seq: int) -> None:
        await self._conn.send(
            StreamOpcode.S_MOVE_TO_STREAM,
            lambda w: w.string(self.name).string(group).u64(seq),
        )

    async def delete_dlt(self, group: str, seq: int) -> None:
        await self._conn.send(
            StreamOpcode.S_DELETE_DLT,
            lambda w: w.string(self.name).string(group).u64(seq),
        )

    async def purge_dlt(self, group: str) -> int:
        status, cursor = await self._conn.send(
            StreamOpcode.S_PURGE_DLT,
            lambda w: w.string(self.name).string(group),
        )
        return cursor.read_u32()
