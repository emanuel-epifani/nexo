from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Generic, Literal, TypeVar, TypedDict

from . import ProvisionOutcome, ProvisionResult
from ..config import DEFAULT_CONFIG
from ..errors import ConnectionClosedError, NexoError, NotConnectedError, ProtocolError
from ..protocol.generated import (
    ErrorCode,
    FLAG_STREAM_S_CREATE_HAS_MAX_AGE,
    FLAG_STREAM_S_CREATE_HAS_MAX_BYTES,
    STREAM_MAX_FETCH_BATCH_SIZE,
    STREAM_MAX_KEY_BYTES,
    STREAM_MAX_PUBLISH_BATCH,
    ProvisionStatus,
    StreamOpcode,
)
from ..subscription import Subscription
from ..transport.tcp.connection import NexoConnection
from ..utils.concurrent import run_concurrent
from ..utils.logger import Logger


T = TypeVar("T")


class StreamMessageMeta(TypedDict):
    seq: int
    key: bytes | None


StreamHandler = Callable[[T, StreamMessageMeta], Any] | Callable[[T], Any]


FETCH_TIMEOUT_MARGIN_MS = 5000


@dataclass(frozen=True)
class StreamRetention:
    max_age_ms: int | None
    max_bytes: int | None


@dataclass(frozen=True)
class StreamConfig:
    retention: StreamRetention
    max_segment_size: int
    max_ack_pending: int
    ack_wait_ms: int
    max_deliveries: int


@dataclass(frozen=True)
class StreamDefinition:
    name: str
    config: StreamConfig


@dataclass(frozen=True)
class StreamPublishItem(Generic[T]):
    data: T
    key: str | bytes | None = None


class DLTEntry(TypedDict):
    seq: int
    reason: str
    attempts: int
    key: bytes | None


def _write_stream_key(writer: Any, key: str | bytes | None) -> None:
    if key is None:
        writer.u16(0)
        return
    key_bytes = key.encode("utf-8") if isinstance(key, str) else bytes(key)
    if not key_bytes:
        raise ValueError("Stream key must not be empty")
    if len(key_bytes) > STREAM_MAX_KEY_BYTES:
        raise ValueError(f"Stream key exceeds {STREAM_MAX_KEY_BYTES} bytes")
    writer.u16(len(key_bytes)).raw_bytes(key_bytes)


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


def _read_definition(cursor: Any) -> StreamDefinition:
    name = cursor.read_string()
    flags = cursor.read_u8()
    max_age_ms = (
        cursor.read_u64()
        if flags & FLAG_STREAM_S_CREATE_HAS_MAX_AGE
        else None
    )
    max_bytes = (
        cursor.read_u64()
        if flags & FLAG_STREAM_S_CREATE_HAS_MAX_BYTES
        else None
    )
    return StreamDefinition(
        name=name,
        config=StreamConfig(
            retention=StreamRetention(
                max_age_ms=max_age_ms,
                max_bytes=max_bytes,
            ),
            max_segment_size=cursor.read_u64(),
            max_ack_pending=cursor.read_u64(),
            ack_wait_ms=cursor.read_u64(),
            max_deliveries=cursor.read_u32(),
        ),
    )


def _read_provision_result(cursor: Any) -> ProvisionResult[StreamDefinition]:
    try:
        status = ProvisionStatus(cursor.read_u8())
    except ValueError as error:
        raise ProtocolError("Unknown stream provision status") from error
    outcome = (
        ProvisionOutcome.CREATED
        if status == ProvisionStatus.CREATED
        else ProvisionOutcome.UNCHANGED
    )
    return ProvisionResult(status=outcome, definition=_read_definition(cursor))


class StreamAckError(Exception):
    def __init__(self, errors: list[Exception]) -> None:
        super().__init__(f"{len(errors)} stream ACK request(s) failed")
        self.errors = errors


def _is_recoverable_membership_error(error: Exception) -> bool:
    if isinstance(error, StreamAckError):
        return any(
            _is_recoverable_membership_error(nested_error)
            for nested_error in error.errors
        )
    return isinstance(error, NexoError) and error.code in {
        ErrorCode.FENCED,
        ErrorCode.NOT_MEMBER,
    }


async def _sleep(milliseconds: int) -> None:
    await asyncio.sleep(milliseconds / 1000.0)


class StreamCommands:
    @staticmethod
    async def create(
        conn: NexoConnection,
        name: str,
        *,
        max_age_ms: int | None,
        max_bytes: int | None,
    ) -> ProvisionResult[StreamDefinition]:
        has_max_age = max_age_ms is not None
        has_max_bytes = max_bytes is not None
        flags = (
            FLAG_STREAM_S_CREATE_HAS_MAX_AGE if has_max_age else 0x00
        ) | (
            FLAG_STREAM_S_CREATE_HAS_MAX_BYTES if has_max_bytes else 0x00
        )

        def build(writer: Any) -> None:
            writer.string(name).u8(flags)
            if has_max_age:
                writer.u64(max_age_ms)
            if has_max_bytes:
                writer.u64(max_bytes)

        _, cursor = await conn.send(StreamOpcode.S_CREATE, build)
        return _read_provision_result(cursor)

    @staticmethod
    async def describe(conn: NexoConnection, name: str) -> StreamDefinition:
        _, cursor = await conn.send(
            StreamOpcode.S_DESCRIBE,
            lambda writer: writer.string(name),
        )
        return _read_definition(cursor)

    @staticmethod
    async def exists(conn: NexoConnection, name: str) -> bool:
        _, cursor = await conn.send(
            StreamOpcode.S_EXISTS,
            lambda writer: writer.string(name),
        )
        return cursor.read_u8() == 1

    @staticmethod
    async def delete(conn: NexoConnection, name: str) -> None:
        await conn.send(StreamOpcode.S_DELETE, lambda writer: writer.string(name))

    @staticmethod
    async def publish(
        conn: NexoConnection,
        name: str,
        data: Any,
        *,
        key: str | bytes | None,
    ) -> int:
        def build(writer: Any) -> None:
            writer.string(name).u32(1)
            _write_stream_key(writer, key)
            writer.any_with_len(data)

        _, cursor = await conn.send(StreamOpcode.S_PUB, build)
        count = cursor.read_u32()
        return cursor.read_u64() if count > 0 else 0

    @staticmethod
    async def publish_batch(
        conn: NexoConnection,
        name: str,
        items: list[StreamPublishItem[Any]],
    ) -> list[int]:
        def build(writer: Any) -> None:
            writer.string(name).u32(len(items))
            for item in items:
                _write_stream_key(writer, item.key)
                writer.any_with_len(item.data)

        _, cursor = await conn.send(StreamOpcode.S_PUB, build)
        count = cursor.read_u32()
        return [cursor.read_u64() for _ in range(count)]

    @staticmethod
    async def seek(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        target: Literal["beginning", "end"],
    ) -> None:
        await conn.send(
            StreamOpcode.S_SEEK,
            lambda writer: writer.string(stream_name)
            .string(group_name)
            .u8(0 if target == "beginning" else 1),
        )

    @staticmethod
    async def peek_dlt(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        limit: int,
        offset: int,
    ) -> list[DLTEntry]:
        _, cursor = await conn.send(
            StreamOpcode.S_PEEK_DLT,
            lambda writer: writer.string(stream_name)
            .string(group_name)
            .u32(limit)
            .u32(offset),
        )
        count = cursor.read_u32()
        entries: list[DLTEntry] = []
        for _ in range(count):
            seq = cursor.read_u64()
            reason = cursor.read_string()
            attempts = cursor.read_u32()
            key_length = cursor.read_u16()
            key = cursor.read_buffer(key_length) if key_length > 0 else None
            entries.append(
                {
                    "seq": seq,
                    "reason": reason,
                    "attempts": attempts,
                    "key": key,
                }
            )
        return entries

    @staticmethod
    async def move_to_stream(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        seq: int,
    ) -> None:
        await conn.send(
            StreamOpcode.S_MOVE_TO_STREAM,
            lambda writer: writer.string(stream_name).string(group_name).u64(seq),
        )

    @staticmethod
    async def delete_dlt(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        seq: int,
    ) -> None:
        await conn.send(
            StreamOpcode.S_DELETE_DLT,
            lambda writer: writer.string(stream_name).string(group_name).u64(seq),
        )

    @staticmethod
    async def purge_dlt(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
    ) -> int:
        _, cursor = await conn.send(
            StreamOpcode.S_PURGE_DLT,
            lambda writer: writer.string(stream_name).string(group_name),
        )
        return cursor.read_u32()


class StreamSubscription(Generic[T]):
    def __init__(
        self,
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        logger: Logger,
        callback: StreamHandler[T],
        batch_size: int,
        wait_ms: int,
        concurrency: int,
        stop_timeout_ms: int,
    ) -> None:
        self._conn = conn
        self._stream_name = stream_name
        self._group_name = group_name
        self._logger = logger
        self._callback: Callable[..., Any] = callback
        self._callback_wants_meta = _callback_accepts_meta(callback)
        self._batch_size = batch_size
        self._wait_ms = wait_ms
        self._concurrency = concurrency
        self._stop_timeout_ms = stop_timeout_ms
        self._active = False
        self._loop_task: asyncio.Task[None] | None = None
        self._consumer_id: str | None = None
        self._generation = 0
        self._phase = "idle"
        self._left_to_cancel_fetch = False

    @property
    def completion(self) -> asyncio.Task[None]:
        assert self._loop_task is not None
        return self._loop_task

    async def start(self) -> None:
        self._active = True
        try:
            await self._join()
        except Exception:
            self._active = False
            raise
        self._loop_task = asyncio.create_task(self._loop())
        self._loop_task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        self._active = False
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._logger.error(
                f"[{self._stream_name}:{self._group_name}] Consumer crashed",
                error,
            )

    async def stop(self) -> None:
        self._active = False
        left_while_fetching = self._phase == "fetching"
        if left_while_fetching:
            self._left_to_cancel_fetch = True
            await self._leave()
        try:
            if self._loop_task is not None:
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
                        "Stream subscription stop timed out after "
                        f"{self._stop_timeout_ms}ms"
                    ) from error
        finally:
            if not left_while_fetching:
                await self._leave()

    async def _leave(self) -> None:
        consumer_id = self._consumer_id
        if consumer_id is None:
            return
        try:
            await self._conn.send(
                StreamOpcode.S_LEAVE,
                lambda writer: writer.string(self._stream_name)
                .string(self._group_name)
                .string(consumer_id)
                .u64(self._generation),
            )
        except Exception:
            pass

    async def _join(self) -> None:
        if not self._conn.is_connected:
            raise NotConnectedError()
        _, cursor = await self._conn.send(
            StreamOpcode.S_JOIN,
            lambda writer: writer.string(self._stream_name).string(
                self._group_name
            ),
        )
        cursor.read_u64()  # ack_floor (unused)
        self._generation = cursor.read_u64()
        self._consumer_id = cursor.read_string()

    async def _loop(self) -> None:
        try:
            while self._active:
                try:
                    if self._consumer_id is None:
                        await self._join()
                    await self._poll_once()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if not self._active:
                        if self._left_to_cancel_fetch and _is_recoverable_membership_error(
                            error
                        ):
                            break
                        raise
                    self._consumer_id = None
                    if _is_recoverable_membership_error(error):
                        continue
                    if not self._conn.is_connected or isinstance(
                        error,
                        (ConnectionClosedError, NotConnectedError),
                    ):
                        await _sleep(DEFAULT_CONFIG.connection.backoff_short_ms)
                        continue
                    if isinstance(error, NexoError):
                        raise
                    self._logger.error(
                        f"[{self._stream_name}:{self._group_name}] Error. "
                        "Retrying in "
                        f"{DEFAULT_CONFIG.connection.backoff_long_ms}ms... {error}"
                    )
                    await _sleep(DEFAULT_CONFIG.connection.backoff_long_ms)
        except asyncio.CancelledError:
            pass
        finally:
            self._active = False

    async def _poll_once(self) -> None:
        consumer_id = self._consumer_id
        assert consumer_id is not None
        generation = self._generation
        self._phase = "fetching"
        try:
            _, cursor = await self._conn.send(
                StreamOpcode.S_FETCH,
                lambda writer: writer.string(self._stream_name)
                .string(self._group_name)
                .string(consumer_id)
                .u64(generation)
                .u32(self._batch_size)
                .u32(self._wait_ms),
                timeout_ms=self._wait_ms + FETCH_TIMEOUT_MARGIN_MS,
            )
        finally:
            if self._phase == "fetching":
                self._phase = "idle"

        count = cursor.read_u32()
        if count == 0:
            return
        batch: list[dict[str, Any]] = []
        for _ in range(count):
            seq = cursor.read_u64()
            cursor.read_u64()  # skip timestamp
            key_length = cursor.read_u16()
            key = cursor.read_buffer(key_length) if key_length > 0 else None
            payload_length = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_length)
            batch.append({"seq": seq, "key": key, "data": data})

        ack_errors: list[Exception] = []

        async def process(message: dict[str, Any]) -> None:
            if not self._active or ack_errors:
                return
            try:
                if self._callback_wants_meta:
                    result = self._callback(
                        message["data"],
                        {"seq": message["seq"], "key": message["key"]},
                    )
                else:
                    result = self._callback(message["data"])
                if asyncio.iscoroutine(result):
                    await result
            except Exception as error:
                self._logger.error(
                    f"[{self._stream_name}:{self._group_name}] "
                    f"Processing error at seq={message['seq']}. "
                    f"Waiting for timeout-based retry. {error}"
                )
                return

            try:
                await self._conn.send(
                    StreamOpcode.S_ACK,
                    lambda writer: writer.string(self._stream_name)
                    .string(self._group_name)
                    .string(consumer_id)
                    .u64(generation)
                    .u64(message["seq"]),
                )
            except Exception as error:
                ack_errors.append(error)
                self._logger.error(
                    f"[{self._stream_name}:{self._group_name}] "
                    f"ACK failed at seq={message['seq']}. {error}"
                )

        self._phase = "processing"
        try:
            await run_concurrent(batch, self._concurrency, process)
            if ack_errors:
                raise StreamAckError(ack_errors) from ack_errors[0]
        finally:
            self._phase = "idle"


class NexoStreamDLT:
    def __init__(
        self,
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
    ) -> None:
        self._conn = conn
        self._stream_name = stream_name
        self._group_name = group_name

    async def peek(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[DLTEntry]:
        return await StreamCommands.peek_dlt(
            self._conn,
            self._stream_name,
            self._group_name,
            limit,
            offset,
        )

    async def replay(self, seq: int) -> None:
        await StreamCommands.move_to_stream(
            self._conn,
            self._stream_name,
            self._group_name,
            seq,
        )

    async def delete(self, seq: int) -> None:
        await StreamCommands.delete_dlt(
            self._conn,
            self._stream_name,
            self._group_name,
            seq,
        )

    async def purge(self) -> int:
        return await StreamCommands.purge_dlt(
            self._conn,
            self._stream_name,
            self._group_name,
        )


class NexoStreamGroup(Generic[T]):
    def __init__(
        self,
        conn: NexoConnection,
        stream_name: str,
        name: str,
        logger: Logger,
    ) -> None:
        if not name:
            raise ValueError("Consumer group is required")
        self._conn = conn
        self._stream_name = stream_name
        self.name = name
        self._logger = logger
        self._dlt = NexoStreamDLT(conn, stream_name, name)

    @property
    def dlt(self) -> NexoStreamDLT:
        return self._dlt

    async def subscribe(
        self,
        callback: StreamHandler[T],
        *,
        batch_size: int = DEFAULT_CONFIG.stream.batch_size,
        wait_ms: int = DEFAULT_CONFIG.stream.wait_ms,
        concurrency: int = DEFAULT_CONFIG.stream.concurrency,
        stop_timeout_ms: int = DEFAULT_CONFIG.stream.stop_timeout_ms,
    ) -> Subscription[T]:
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size < 1
            or batch_size > STREAM_MAX_FETCH_BATCH_SIZE
        ):
            raise ValueError(
                "batch_size must be an integer between 1 and "
                f"{STREAM_MAX_FETCH_BATCH_SIZE}"
            )
        if not isinstance(wait_ms, int) or isinstance(wait_ms, bool) or wait_ms < 1:
            raise ValueError("wait_ms must be a positive integer")
        if (
            not isinstance(concurrency, int)
            or isinstance(concurrency, bool)
            or concurrency < 1
        ):
            raise ValueError("concurrency must be a positive integer")
        if (
            not isinstance(stop_timeout_ms, int)
            or isinstance(stop_timeout_ms, bool)
            or stop_timeout_ms < 1
        ):
            raise ValueError("stop_timeout_ms must be a positive integer")

        consumer = StreamSubscription[T](
            self._conn,
            self._stream_name,
            self.name,
            self._logger,
            callback,
            batch_size,
            wait_ms,
            concurrency,
            stop_timeout_ms,
        )
        await consumer.start()
        return Subscription(
            stop_fn=consumer.stop,
            active_fn=lambda: consumer._active,
            completion=consumer.completion,
        )

    async def seek(self, target: Literal["beginning", "end"]) -> None:
        if target not in ("beginning", "end"):
            raise ValueError(f"Invalid seek target: {target!r}")
        await StreamCommands.seek(
            self._conn,
            self._stream_name,
            self.name,
            target,
        )


class NexoStream(Generic[T]):
    def __init__(self, conn: NexoConnection, name: str, logger: Logger) -> None:
        self._conn = conn
        self.name = name
        self._logger = logger

    async def publish(
        self,
        data: T,
        *,
        key: str | bytes | None = None,
    ) -> int:
        return await StreamCommands.publish(
            self._conn,
            self.name,
            data,
            key=key,
        )

    async def publish_batch(
        self,
        items: Sequence[StreamPublishItem[T] | Mapping[str, Any]],
    ) -> list[int]:
        if not items:
            return []
        if len(items) > STREAM_MAX_PUBLISH_BATCH:
            raise ValueError(
                f"Publish batch too large: {len(items)} items "
                f"(max: {STREAM_MAX_PUBLISH_BATCH})"
            )
        normalized: list[StreamPublishItem[Any]] = []
        for item in items:
            if isinstance(item, StreamPublishItem):
                normalized_item = item
            elif isinstance(item, Mapping):
                if "options" in item:
                    raise TypeError("Stream batch options must be direct item fields")
                if "data" not in item:
                    raise ValueError("Stream batch item requires data")
                normalized_item = StreamPublishItem(
                    data=item["data"],
                    key=item.get("key"),
                )
            else:
                raise TypeError("Stream batch items must be StreamPublishItem or mapping")
            if normalized_item.key is not None and not isinstance(
                normalized_item.key,
                (str, bytes),
            ):
                raise TypeError("Stream key must be str, bytes, or None")
            normalized.append(normalized_item)
        return await StreamCommands.publish_batch(self._conn, self.name, normalized)

    def group(self, name: str) -> NexoStreamGroup[T]:
        return NexoStreamGroup(self._conn, self.name, name, self._logger)


class NexoStreamFacade:
    def __init__(self, conn: NexoConnection, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger

    async def create(
        self,
        name: str,
        *,
        max_age_ms: int | None = None,
        max_bytes: int | None = None,
    ) -> ProvisionResult[StreamDefinition]:
        return await StreamCommands.create(
            self._conn,
            name,
            max_age_ms=max_age_ms,
            max_bytes=max_bytes,
        )

    async def describe(self, name: str) -> StreamDefinition:
        return await StreamCommands.describe(self._conn, name)

    async def get(self, name: str) -> NexoStream[Any]:
        await self.describe(name)
        return NexoStream(self._conn, name, self._logger)

    async def exists(self, name: str) -> bool:
        return await StreamCommands.exists(self._conn, name)

    async def delete(self, name: str) -> None:
        await StreamCommands.delete(self._conn, name)
