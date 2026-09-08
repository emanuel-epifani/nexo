from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, TypedDict

from . import ProvisionOutcome, ProvisionResult
from ..config import DEFAULT_CONFIG
from ..errors import (
    ConnectionClosedError,
    NotConnectedError,
    ProtocolError,
    RequestCancelledError,
    RequestTimeoutError,
)
from ..protocol.generated import (
    FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES,
    FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT,
    FLAG_QUEUE_Q_PUSH_HAS_PRIORITY,
    QUEUE_MAX_PUSH_ITEMS,
    ProvisionStatus,
    QueueOpcode,
)
from ..subscription import Subscription
from ..transport.tcp.connection import NexoConnection
from ..utils.concurrent import run_concurrent
from ..utils.logger import Logger


T = TypeVar("T")
QueueHandler = Callable[[T], Any] | Callable[[T, "QueueMessageMeta"], Any]


CONSUME_TIMEOUT_MARGIN_MS = 5000


@dataclass(frozen=True)
class QueueConfig:
    visibility_timeout_ms: int
    max_deliveries: int


@dataclass(frozen=True)
class QueueDefinition:
    name: str
    config: QueueConfig


@dataclass(frozen=True)
class QueuePushItem(Generic[T]):
    data: T
    priority: int | None = None


class QueueMessageMeta(TypedDict):
    id: str


def _callback_accepts_meta(callback: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return True
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.kind == parameter.VAR_POSITIONAL:
            return True
        if parameter.kind in (
            parameter.POSITIONAL_ONLY,
            parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 2


def _read_definition(cursor: Any) -> QueueDefinition:
    return QueueDefinition(
        name=cursor.read_string(),
        config=QueueConfig(
            visibility_timeout_ms=cursor.read_u64(),
            max_deliveries=cursor.read_u32(),
        ),
    )


def _read_provision_result(cursor: Any) -> ProvisionResult[QueueDefinition]:
    try:
        status = ProvisionStatus(cursor.read_u8())
    except ValueError as error:
        raise ProtocolError("Unknown queue provision status") from error
    outcome = (
        ProvisionOutcome.CREATED
        if status == ProvisionStatus.CREATED
        else ProvisionOutcome.UNCHANGED
    )
    return ProvisionResult(status=outcome, definition=_read_definition(cursor))


def _validate_priority(priority: int | None) -> None:
    if priority is not None and (
        not isinstance(priority, int)
        or isinstance(priority, bool)
        or priority < 0
        or priority > 0xFF
    ):
        raise ValueError("priority must be an integer between 0 and 255")


class QueueCommands:
    @staticmethod
    async def create(
        conn: NexoConnection,
        name: str,
        *,
        visibility_timeout_ms: int | None,
        max_deliveries: int | None,
    ) -> ProvisionResult[QueueDefinition]:
        has_visibility_timeout = visibility_timeout_ms is not None
        has_max_deliveries = max_deliveries is not None
        flags = (
            FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT
            if has_visibility_timeout
            else 0x00
        ) | (
            FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES
            if has_max_deliveries
            else 0x00
        )

        def build(writer: Any) -> None:
            writer.string(name).u8(flags)
            if has_visibility_timeout:
                writer.u64(visibility_timeout_ms)
            if has_max_deliveries:
                writer.u32(max_deliveries)

        _, cursor = await conn.send(QueueOpcode.Q_CREATE, build)
        return _read_provision_result(cursor)

    @staticmethod
    async def describe(conn: NexoConnection, name: str) -> QueueDefinition:
        _, cursor = await conn.send(
            QueueOpcode.Q_DESCRIBE,
            lambda writer: writer.string(name),
        )
        return _read_definition(cursor)

    @staticmethod
    async def exists(conn: NexoConnection, name: str) -> bool:
        _, cursor = await conn.send(
            QueueOpcode.Q_EXISTS,
            lambda writer: writer.string(name),
        )
        return cursor.read_u8() == 1

    @staticmethod
    async def delete(conn: NexoConnection, name: str) -> None:
        await conn.send(QueueOpcode.Q_DELETE, lambda writer: writer.string(name))

    @staticmethod
    async def push(
        conn: NexoConnection,
        name: str,
        data: Any,
        *,
        priority: int | None,
    ) -> None:
        has_priority = priority is not None
        flags = FLAG_QUEUE_Q_PUSH_HAS_PRIORITY if has_priority else 0x00

        def build(writer: Any) -> None:
            writer.string(name).u32(1).u8(flags)
            if has_priority:
                writer.u8(priority)
            writer.any_with_len(data)

        await conn.send(QueueOpcode.Q_PUSH, build)

    @staticmethod
    async def push_batch(
        conn: NexoConnection,
        name: str,
        items: list[QueuePushItem[Any]],
    ) -> None:
        def build(writer: Any) -> None:
            writer.string(name).u32(len(items))
            for item in items:
                has_priority = item.priority is not None
                flags = FLAG_QUEUE_Q_PUSH_HAS_PRIORITY if has_priority else 0x00
                writer.u8(flags)
                if has_priority:
                    writer.u8(item.priority)
                writer.any_with_len(item.data)

        await conn.send(QueueOpcode.Q_PUSH, build)

    @staticmethod
    async def consume(
        conn: NexoConnection,
        name: str,
        batch_size: int,
        wait_ms: int,
    ) -> list[dict[str, Any]]:
        _, cursor = await conn.send(
            QueueOpcode.Q_CONSUME,
            lambda writer: writer.string(name).u32(batch_size).u32(wait_ms),
            timeout_ms=wait_ms + CONSUME_TIMEOUT_MARGIN_MS,
        )
        count = cursor.read_u32()
        messages: list[dict[str, Any]] = []
        for _ in range(count):
            message_id = cursor.read_uuid()
            delivery_token = cursor.read_u64()
            payload_len = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_len)
            messages.append(
                {
                    "id": message_id,
                    "delivery_token": delivery_token,
                    "data": data,
                }
            )
        return messages

    @staticmethod
    def ack(
        conn: NexoConnection,
        name: str,
        message_id: str,
        delivery_token: int,
    ) -> None:
        conn.send_fire_and_forget(
            QueueOpcode.Q_ACK,
            lambda writer: writer.uuid(message_id)
            .u64(delivery_token)
            .string(name),
        )

    @staticmethod
    def nack(
        conn: NexoConnection,
        name: str,
        message_id: str,
        delivery_token: int,
        reason: str,
    ) -> None:
        conn.send_fire_and_forget(
            QueueOpcode.Q_NACK,
            lambda writer: writer.uuid(message_id)
            .u64(delivery_token)
            .string(name)
            .string(reason),
        )

    @staticmethod
    async def peek_dlq(
        conn: NexoConnection,
        name: str,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        _, cursor = await conn.send(
            QueueOpcode.Q_PEEK_DLQ,
            lambda writer: writer.string(name).u32(limit).u32(offset),
        )
        total = cursor.read_u32()
        count = cursor.read_u32()
        items: list[dict[str, Any]] = []
        for _ in range(count):
            message_id = cursor.read_uuid()
            payload_len = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_len)
            attempts = cursor.read_u32()
            failure_reason = cursor.read_string()
            items.append(
                {
                    "id": message_id,
                    "data": data,
                    "attempts": attempts,
                    "failure_reason": failure_reason,
                }
            )
        return {"total": total, "items": items}

    @staticmethod
    async def move_to_queue(
        conn: NexoConnection,
        name: str,
        message_id: str,
    ) -> bool:
        _, cursor = await conn.send(
            QueueOpcode.Q_MOVE_TO_QUEUE,
            lambda writer: writer.string(name).uuid(message_id),
        )
        return cursor.read_u8() == 1

    @staticmethod
    async def delete_dlq(
        conn: NexoConnection,
        name: str,
        message_id: str,
    ) -> bool:
        _, cursor = await conn.send(
            QueueOpcode.Q_DELETE_DLQ,
            lambda writer: writer.string(name).uuid(message_id),
        )
        return cursor.read_u8() == 1

    @staticmethod
    async def purge_dlq(conn: NexoConnection, name: str) -> int:
        _, cursor = await conn.send(
            QueueOpcode.Q_PURGE_DLQ,
            lambda writer: writer.string(name),
        )
        return cursor.read_u32()


class NexoDLQ:
    def __init__(self, conn: NexoConnection, queue_name: str, logger: Logger) -> None:
        self._conn = conn
        self._queue_name = queue_name
        self._logger = logger

    async def peek(
        self,
        *,
        limit: int = DEFAULT_CONFIG.queue.peek_limit,
        offset: int = DEFAULT_CONFIG.queue.peek_offset,
    ) -> dict[str, Any]:
        self._logger.debug(
            f"[DLQ:{self._queue_name}] Peeking {limit} messages at offset {offset}"
        )
        return await QueueCommands.peek_dlq(
            self._conn,
            self._queue_name,
            limit,
            offset,
        )

    async def replay(self, message_id: str) -> bool:
        self._logger.debug(
            f"[DLQ:{self._queue_name}] Moving message {message_id} to main queue"
        )
        return await QueueCommands.move_to_queue(
            self._conn,
            self._queue_name,
            message_id,
        )

    async def delete(self, message_id: str) -> bool:
        self._logger.debug(
            f"[DLQ:{self._queue_name}] Deleting message {message_id}"
        )
        return await QueueCommands.delete_dlq(
            self._conn,
            self._queue_name,
            message_id,
        )

    async def purge(self) -> int:
        self._logger.debug(f"[DLQ:{self._queue_name}] Purging all messages")
        return await QueueCommands.purge_dlq(self._conn, self._queue_name)


class QueueSubscription(Generic[T]):
    def __init__(
        self,
        conn: NexoConnection,
        queue_name: str,
        logger: Logger,
        callback: QueueHandler[T],
        batch_size: int,
        wait_ms: int,
        concurrency: int,
        stop_timeout_ms: int,
    ) -> None:
        self._conn = conn
        self._queue_name = queue_name
        self._logger = logger
        self._callback: Callable[..., Any] = callback
        self._callback_wants_meta = _callback_accepts_meta(callback)
        self._batch_size = batch_size
        self._wait_ms = wait_ms
        self._concurrency = concurrency
        self._stop_timeout_ms = stop_timeout_ms
        self._active = False
        self._loop_task: asyncio.Task[None] | None = None
        self._consume_task: asyncio.Task[list[dict[str, Any]]] | None = None
        self._terminal_error: BaseException | None = None

    @property
    def completion(self) -> asyncio.Task[None]:
        assert self._loop_task is not None
        return self._loop_task

    @property
    def error(self) -> BaseException | None:
        return self._terminal_error

    def start(self) -> None:
        self._active = True
        self._loop_task = asyncio.create_task(self._loop())
        self._loop_task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        self._active = False
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._logger.error(
                f"[CRITICAL] Queue loop crashed for {self._queue_name}",
                error,
            )

    async def stop(self) -> None:
        self._active = False
        if self._consume_task is not None and not self._consume_task.done():
            self._consume_task.cancel()
        if self._loop_task is None:
            return
        try:
            await asyncio.wait_for(
                asyncio.shield(self._loop_task),
                timeout=self._stop_timeout_ms / 1000.0,
            )
        except asyncio.TimeoutError as error:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            raise TimeoutError(
                f"Queue subscription stop timed out after {self._stop_timeout_ms}ms"
            ) from error

    async def _loop(self) -> None:
        try:
            while self._active:
                if not self._conn.is_connected:
                    await asyncio.sleep(
                        DEFAULT_CONFIG.connection.backoff_short_ms / 1000.0
                    )
                    continue
                try:
                    self._consume_task = asyncio.create_task(
                        QueueCommands.consume(
                            self._conn,
                            self._queue_name,
                            self._batch_size,
                            self._wait_ms,
                        )
                    )
                    messages = await self._consume_task
                    self._consume_task = None
                    if not messages:
                        continue

                    async def process_message(message: dict[str, Any]) -> None:
                        if not self._active:
                            return
                        try:
                            if self._callback_wants_meta:
                                result = self._callback(
                                    message["data"],
                                    {"id": message["id"]},
                                )
                            else:
                                result = self._callback(message["data"])
                            if asyncio.iscoroutine(result):
                                await result
                            QueueCommands.ack(
                                self._conn,
                                self._queue_name,
                                message["id"],
                                message["delivery_token"],
                            )
                        except Exception as error:
                            if not self._conn.is_connected:
                                return
                            reason = str(error)
                            self._logger.error(
                                f"[Queue:{self._queue_name}] Consumer error, "
                                f"sending NACK. Reason: {reason}"
                            )
                            QueueCommands.nack(
                                self._conn,
                                self._queue_name,
                                message["id"],
                                message["delivery_token"],
                                reason,
                            )

                    await run_concurrent(
                        messages,
                        self._concurrency,
                        process_message,
                    )
                except asyncio.CancelledError:
                    if not self._active:
                        break
                    raise
                except (
                    ConnectionClosedError,
                    NotConnectedError,
                    RequestCancelledError,
                    RequestTimeoutError,
                ):
                    if not self._active:
                        break
                    await asyncio.sleep(
                        DEFAULT_CONFIG.connection.backoff_short_ms / 1000.0
                    )
                except Exception as error:
                    self._terminal_error = error
                    self._logger.error(
                        f"[Queue:{self._queue_name}] Consumer stopping: {error}"
                    )
                    break
        finally:
            self._active = False
            self._consume_task = None


class NexoQueue(Generic[T]):
    def __init__(self, conn: NexoConnection, name: str, logger: Logger) -> None:
        self._conn = conn
        self.name = name
        self._logger = logger
        self._dlq = NexoDLQ(conn, name, logger)

    @property
    def dlq(self) -> NexoDLQ:
        return self._dlq

    async def push(self, data: T, *, priority: int | None = None) -> None:
        _validate_priority(priority)
        await QueueCommands.push(
            self._conn,
            self.name,
            data,
            priority=priority,
        )

    async def push_batch(
        self,
        items: Sequence[QueuePushItem[T] | Mapping[str, Any]],
    ) -> None:
        if not items:
            return
        if len(items) > QUEUE_MAX_PUSH_ITEMS:
            raise ValueError(
                f"Push batch too large: {len(items)} items "
                f"(max: {QUEUE_MAX_PUSH_ITEMS})"
            )
        normalized: list[QueuePushItem[Any]] = []
        for item in items:
            if isinstance(item, QueuePushItem):
                normalized_item = item
            elif isinstance(item, Mapping):
                if "options" in item:
                    raise TypeError("Queue batch options must be direct item fields")
                if "data" not in item:
                    raise ValueError("Queue batch item requires data")
                normalized_item = QueuePushItem(
                    data=item["data"],
                    priority=item.get("priority"),
                )
            else:
                raise TypeError("Queue batch items must be QueuePushItem or mapping")
            _validate_priority(normalized_item.priority)
            normalized.append(normalized_item)
        await QueueCommands.push_batch(self._conn, self.name, normalized)

    async def subscribe(
        self,
        callback: QueueHandler[T],
        *,
        batch_size: int = DEFAULT_CONFIG.queue.batch_size,
        wait_ms: int = DEFAULT_CONFIG.queue.wait_ms,
        concurrency: int = DEFAULT_CONFIG.queue.concurrency,
    ) -> Subscription[T]:
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if not isinstance(wait_ms, int) or isinstance(wait_ms, bool) or wait_ms < 1:
            raise ValueError(f"wait_ms must be >= 1, got {wait_ms}")
        if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")

        await QueueCommands.describe(self._conn, self.name)
        consumer = QueueSubscription[T](
            self._conn,
            self.name,
            self._logger,
            callback,
            batch_size,
            wait_ms,
            concurrency,
            DEFAULT_CONFIG.queue.stop_timeout_ms,
        )
        consumer.start()
        return Subscription(
            stop_fn=consumer.stop,
            active_fn=lambda: consumer._active,
            completion=consumer.completion,
            error_fn=lambda: consumer.error,
        )


class NexoQueueFacade:
    def __init__(self, conn: NexoConnection, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger

    async def create(
        self,
        name: str,
        *,
        visibility_timeout_ms: int | None = None,
        max_deliveries: int | None = None,
    ) -> ProvisionResult[QueueDefinition]:
        return await QueueCommands.create(
            self._conn,
            name,
            visibility_timeout_ms=visibility_timeout_ms,
            max_deliveries=max_deliveries,
        )

    async def describe(self, name: str) -> QueueDefinition:
        return await QueueCommands.describe(self._conn, name)

    async def get(self, name: str) -> NexoQueue[Any]:
        await self.describe(name)
        return NexoQueue(self._conn, name, self._logger)

    async def exists(self, name: str) -> bool:
        return await QueueCommands.exists(self._conn, name)

    async def delete(self, name: str) -> None:
        await QueueCommands.delete(self._conn, name)
