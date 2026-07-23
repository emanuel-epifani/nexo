from __future__ import annotations

import asyncio
from typing import Any, Callable, Generic, TypeVar, TypedDict

from ..config import DEFAULT_CONFIG
from ..connection import NexoConnection
from ..errors import ConnectionClosedError, NotConnectedError, RequestTimeoutError
from ..utils.concurrent import run_concurrent
from ..utils.logger import Logger


T = TypeVar("T")
QueueHandler = Callable[[T], Any]


class QueueOpcode:
    Q_CREATE = 0x10
    Q_PUSH = 0x11
    Q_CONSUME = 0x12
    Q_ACK = 0x13
    Q_EXISTS = 0x14
    Q_DELETE = 0x15
    Q_PEEK_DLQ = 0x16
    Q_MOVE_TO_QUEUE = 0x17
    Q_DELETE_DLQ = 0x18
    Q_PURGE_DLQ = 0x19
    Q_NACK = 0x1A


CONSUME_TIMEOUT_MARGIN_MS = 5000


class QueueConfig(TypedDict, total=False):
    visibility_timeout_ms: int
    max_retries: int


class QueueSubscribeOptions(TypedDict, total=False):
    batch_size: int
    wait_ms: int
    concurrency: int


class QueuePushOptions(TypedDict, total=False):
    priority: int


class QueueCommands:
    @staticmethod
    async def create(
        conn: NexoConnection, name: str, config: QueueConfig
    ) -> None:
        vto = config.get("visibility_timeout_ms")
        retries = config.get("max_retries")
        has_vto = vto is not None
        has_retries = retries is not None
        flags = (0x01 if has_vto else 0x00) | (0x02 if has_retries else 0x00)

        def build(w):
            w.string(name).u8(flags)
            if has_vto:
                w.u64(vto)
            if has_retries:
                w.u32(retries)

        await conn.send(QueueOpcode.Q_CREATE, build)

    @staticmethod
    async def exists(conn: NexoConnection, name: str) -> bool:
        try:
            status, cursor = await conn.send(
                QueueOpcode.Q_EXISTS, lambda w: w.string(name)
            )
            return cursor.read_u8() == 1
        except Exception:
            return False

    @staticmethod
    async def delete(conn: NexoConnection, name: str) -> None:
        await conn.send(QueueOpcode.Q_DELETE, lambda w: w.string(name))

    @staticmethod
    async def push(
        conn: NexoConnection,
        name: str,
        data: Any,
        options: QueuePushOptions | None = None,
    ) -> None:
        opts = options or {}
        priority = opts.get("priority")
        has_priority = priority is not None
        flags = 0x01 if has_priority else 0x00

        def build(w):
            w.string(name).u32(1).u8(flags)
            if has_priority:
                w.u8(priority)
            w.any_with_len(data)

        await conn.send(QueueOpcode.Q_PUSH, build)

    @staticmethod
    async def push_batch(
        conn: NexoConnection,
        name: str,
        items: list[dict[str, Any]],
    ) -> None:
        def build(w):
            w.string(name).u32(len(items))
            for item in items:
                opts = item.get("options") or {}
                priority = opts.get("priority")
                has_priority = priority is not None
                flags = 0x01 if has_priority else 0x00
                w.u8(flags)
                if has_priority:
                    w.u8(priority)
                w.any_with_len(item["data"])

        await conn.send(QueueOpcode.Q_PUSH, build)

    @staticmethod
    async def consume(
        conn: NexoConnection,
        name: str,
        batch_size: int,
        wait_ms: int,
    ) -> list[dict[str, Any]]:
        status, cursor = await conn.send(
            QueueOpcode.Q_CONSUME,
            lambda w: w.string(name).u32(batch_size).u32(wait_ms),
            timeout_ms=wait_ms + CONSUME_TIMEOUT_MARGIN_MS,
        )
        count = cursor.read_u32()
        if count == 0:
            return []
        messages: list[dict[str, Any]] = []
        for _ in range(count):
            id_hex = cursor.read_uuid()
            payload_len = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_len)
            messages.append({"id": id_hex, "data": data})
        return messages

    @staticmethod
    def ack(conn: NexoConnection, name: str, id: str) -> None:
        conn.send_fire_and_forget(
            QueueOpcode.Q_ACK, lambda w: w.uuid(id).string(name)
        )

    @staticmethod
    def nack(conn: NexoConnection, name: str, id: str, reason: str) -> None:
        conn.send_fire_and_forget(
            QueueOpcode.Q_NACK,
            lambda w: w.uuid(id).string(name).string(reason),
        )

    @staticmethod
    async def peek_dlq(
        conn: NexoConnection, name: str, limit: int, offset: int
    ) -> dict[str, Any]:
        status, cursor = await conn.send(
            QueueOpcode.Q_PEEK_DLQ,
            lambda w: w.string(name).u32(limit).u32(offset),
        )
        total = cursor.read_u32()
        count = cursor.read_u32()
        items: list[dict[str, Any]] = []
        for _ in range(count):
            id_hex = cursor.read_uuid()
            payload_len = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_len)
            attempts = cursor.read_u32()
            failure_reason = cursor.read_string()
            items.append(
                {
                    "id": id_hex,
                    "data": data,
                    "attempts": attempts,
                    "failure_reason": failure_reason,
                }
            )
        return {"total": total, "items": items}

    @staticmethod
    async def move_to_queue(
        conn: NexoConnection, name: str, message_id: str
    ) -> bool:
        status, cursor = await conn.send(
            QueueOpcode.Q_MOVE_TO_QUEUE,
            lambda w: w.string(name).uuid(message_id),
        )
        return cursor.read_u8() == 1

    @staticmethod
    async def delete_dlq(
        conn: NexoConnection, name: str, message_id: str
    ) -> bool:
        status, cursor = await conn.send(
            QueueOpcode.Q_DELETE_DLQ,
            lambda w: w.string(name).uuid(message_id),
        )
        return cursor.read_u8() == 1

    @staticmethod
    async def purge_dlq(conn: NexoConnection, name: str) -> int:
        status, cursor = await conn.send(
            QueueOpcode.Q_PURGE_DLQ, lambda w: w.string(name)
        )
        return cursor.read_u32()


class NexoDLQ:
    def __init__(self, conn: NexoConnection, queue_name: str, logger: Logger) -> None:
        self._conn = conn
        self._queue_name = queue_name
        self._logger = logger

    async def peek(
        self,
        limit: int = DEFAULT_CONFIG.queue.peek_limit,
        offset: int = DEFAULT_CONFIG.queue.peek_offset,
    ) -> dict[str, Any]:
        self._logger.debug(
            f"[DLQ:{self._queue_name}] Peeking {limit} messages at offset {offset}"
        )
        return await QueueCommands.peek_dlq(self._conn, self._queue_name, limit, offset)

    async def move_to_queue(self, message_id: str) -> bool:
        self._logger.debug(
            f"[DLQ:{self._queue_name}] Moving message {message_id} to main queue"
        )
        return await QueueCommands.move_to_queue(self._conn, self._queue_name, message_id)

    async def delete(self, message_id: str) -> bool:
        self._logger.debug(
            f"[DLQ:{self._queue_name}] Deleting message {message_id}"
        )
        return await QueueCommands.delete_dlq(self._conn, self._queue_name, message_id)

    async def purge(self) -> int:
        self._logger.debug(f"[DLQ:{self._queue_name}] Purging all messages")
        return await QueueCommands.purge_dlq(self._conn, self._queue_name)


class NexoQueue(Generic[T]):
    def __init__(self, conn: NexoConnection, name: str, logger: Logger) -> None:
        self._conn = conn
        self.name = name
        self._logger = logger
        self._dlq = NexoDLQ(conn, name, logger)

    @property
    def dlq(self) -> NexoDLQ:
        return self._dlq

    async def create(self, config: QueueConfig | None = None) -> NexoQueue[T]:
        await QueueCommands.create(self._conn, self.name, config or {})
        return self

    async def exists(self) -> bool:
        return await QueueCommands.exists(self._conn, self.name)

    async def delete(self) -> None:
        await QueueCommands.delete(self._conn, self.name)

    async def push(self, data: T, options: QueuePushOptions | None = None) -> None:
        await QueueCommands.push(self._conn, self.name, data, options)

    async def push_batch(self, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        await QueueCommands.push_batch(self._conn, self.name, items)

    async def subscribe(
        self,
        callback: QueueHandler[T],
        options: QueueSubscribeOptions | None = None,
    ) -> dict[str, Callable[[], None]]:
        opts = options or {}
        batch_size = opts.get("batch_size")
        if batch_size is None:
            batch_size = DEFAULT_CONFIG.queue.batch_size
        wait_ms = opts.get("wait_ms")
        if wait_ms is None:
            wait_ms = DEFAULT_CONFIG.queue.wait_ms
        concurrency = opts.get("concurrency")
        if concurrency is None:
            concurrency = DEFAULT_CONFIG.queue.concurrency

        if batch_size < 1:
            raise ValueError(f"batchSize must be >= 1, got {batch_size}")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")

        active = True

        async def loop():
            nonlocal active
            try:
                while active:
                    if not self._conn.is_connected:
                        if not active:
                            break
                        await asyncio.sleep(
                            DEFAULT_CONFIG.connection.backoff_short_ms / 1000.0
                        )
                        continue

                    try:
                        if not self._conn.is_connected:
                            continue

                        messages = await QueueCommands.consume(
                            self._conn, self.name, batch_size, wait_ms
                        )

                        if not messages:
                            continue

                        async def process_msg(msg):
                            if not active:
                                return
                            try:
                                result = callback(msg["data"])
                                if asyncio.iscoroutine(result):
                                    await result
                                QueueCommands.ack(self._conn, self.name, msg["id"])
                            except Exception as e:
                                if not self._conn.is_connected:
                                    return
                                reason = str(e)
                                self._logger.error(
                                    f"[Queue:{self.name}] Consumer error, sending NACK. Reason: {reason}"
                                )
                                QueueCommands.nack(self._conn, self.name, msg["id"], reason)

                        await run_concurrent(messages, concurrency, process_msg)

                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        if not active:
                            break
                        if (
                            not self._conn.is_connected
                            or isinstance(e, (ConnectionClosedError, RequestTimeoutError))
                        ):
                            if not active:
                                break
                            await asyncio.sleep(
                                DEFAULT_CONFIG.connection.backoff_short_ms / 1000.0
                            )
                            continue
                        self._logger.error(f"[Queue:{self.name}] Consumer stopping: {e}")
                        break
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(loop())
        task.add_done_callback(
            lambda t: self._logger.error(
                f"[CRITICAL] Queue loop crashed for {self.name}", t.exception()
            )
            if not t.cancelled() and t.exception()
            else None
        )

        def stop():
            nonlocal active
            active = False

        return {"stop": stop}
