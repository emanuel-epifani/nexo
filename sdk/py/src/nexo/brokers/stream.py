from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Generic, Literal, TypeVar, TypedDict

from . import ProvisionOutcome, ProvisionResult
from ..config import DEFAULT_CONFIG
from ..errors import (
    ConnectionClosedError,
    NexoError,
    NotConnectedError,
    ProtocolError,
    RequestCancelledError,
    RequestTimeoutError,
)
from ..protocol.generated import (
    ErrorCode,
    FLAG_STREAM_S_CREATE_HAS_MAX_AGE,
    FLAG_STREAM_S_CREATE_HAS_MAX_BYTES,
    FLAG_STREAM_S_UPDATE_BATCH_RELEASE,
    FLAG_STREAM_S_UPDATE_BATCH_RETRY,
    STREAM_MAX_FETCH_BATCH_SIZE,
    STREAM_MAX_KEY_BATCH_SIZE,
    STREAM_MAX_KEY_BYTES,
    STREAM_MAX_PUBLISH_BATCH,
    ProvisionStatus,
    StreamOpcode,
)
from ..subscription import Subscription
from ..transport.tcp.connection import NexoConnection
from ..utils.logger import Logger


T = TypeVar("T")


class StreamMessageMeta(TypedDict):
    seq: int
    key: bytes | None


StreamHandler = Callable[[T, StreamMessageMeta], Any] | Callable[[T], Any]


FETCH_TIMEOUT_MARGIN_MS = 5000
CANCELLATION_SETTLE_MS = 100
FETCH_MAX_BYTES = 10 * 1024 * 1024
FETCH_PAGE_MIN_BYTES = 54
ACTIVE_FETCH_WAIT_MS = 5
UPDATE_FRAME_MAX = STREAM_MAX_FETCH_BATCH_SIZE


@dataclass(frozen=True)
class StreamRetention:
    max_age_ms: int | None
    max_bytes: int | None


@dataclass(frozen=True)
class StreamConfig:
    retention: StreamRetention
    max_ack_pending: int
    max_ack_pending_bytes: int
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


class DLSEntry(TypedDict):
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
            max_ack_pending=cursor.read_u64(),
            max_ack_pending_bytes=cursor.read_u64(),
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


def _is_recoverable_membership_error(error: Exception) -> bool:
    return isinstance(error, NexoError) and error.code in {
        ErrorCode.FENCED,
        ErrorCode.NOT_MEMBER,
    }


_MIN_BATCH_WIRE_BYTES = 42
_MIN_EVENT_WIRE_BYTES = 20


def _malformed_fetch_page(reason: str) -> ProtocolError:
    return ProtocolError(f"Malformed fetch page: {reason}")


def _decode_fetch_page(
    cursor: Any,
    max_messages: int,
    max_key_messages: int,
    max_key_batches: int,
    max_bytes: int,
) -> tuple[int, list[dict[str, Any]]]:
    if len(cursor.buf) > max_bytes:
        raise _malformed_fetch_page("response exceeds the requested byte budget")
    lease_ms = cursor.read_u64()
    if lease_ms <= 0 or lease_ms > 0x7FFFFFFF:
        raise _malformed_fetch_page("lease_ms is not a valid timer duration")
    batch_count = cursor.read_u32()
    remaining = len(cursor.buf) - cursor.offset
    if batch_count > max_messages or batch_count > remaining // _MIN_BATCH_WIRE_BYTES:
        raise _malformed_fetch_page("batch count exceeds the payload")
    seen_receipts: set[str] = set()
    seen_keys: set[bytes] = set()
    total_messages = 0
    keyed_count = 0
    batches: list[dict[str, Any]] = []
    for _ in range(batch_count):
        batch_start = cursor.offset
        receipt = cursor.read_uuid()
        if receipt in seen_receipts:
            raise _malformed_fetch_page("duplicate receipt")
        seen_receipts.add(receipt)
        key_length = cursor.read_u16()
        key = cursor.read_buffer(key_length) if key_length > 0 else None
        count = cursor.read_u32()
        if count == 0:
            raise _malformed_fetch_page("empty batch")
        if key is None:
            if count != 1:
                raise _malformed_fetch_page("keyless batch must be a singleton")
        else:
            keyed_count += 1
            if keyed_count > max_key_batches:
                raise _malformed_fetch_page("more keyed batches than requested")
            if count > max_key_messages:
                raise _malformed_fetch_page("keyed batch exceeds max_key_messages")
            if key in seen_keys:
                raise _malformed_fetch_page("same key leased twice in one page")
            seen_keys.add(key)
        total_messages += count
        if total_messages > max_messages:
            raise _malformed_fetch_page("more messages than requested")
        remaining = len(cursor.buf) - cursor.offset
        if count > remaining // _MIN_EVENT_WIRE_BYTES:
            raise _malformed_fetch_page("message count exceeds the payload")
        messages: list[dict[str, Any]] = []
        prev_seq = -1
        for _ in range(count):
            seq = cursor.read_u64()
            cursor.read_u64()  # skip timestamp
            payload_length = cursor.read_u32()
            data = cursor.decode_any_from_buffer(payload_length)
            if key is not None and seq <= prev_seq:
                raise _malformed_fetch_page("keyed batch out of order")
            prev_seq = seq
            messages.append({"seq": seq, "data": data})
        batches.append(
            {
                "receipt": receipt,
                "key": key,
                "messages": messages,
                "wire_bytes": cursor.offset - batch_start,
            }
        )
    if cursor.offset != len(cursor.buf):
        raise _malformed_fetch_page("trailing bytes")
    return lease_ms, batches


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
    async def peek_dls(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        limit: int,
        offset: int,
    ) -> list[DLSEntry]:
        _, cursor = await conn.send(
            StreamOpcode.S_PEEK_DLS,
            lambda writer: writer.string(stream_name)
            .string(group_name)
            .u32(limit)
            .u32(offset),
        )
        count = cursor.read_u32()
        entries: list[DLSEntry] = []
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
    async def delete_dls(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        seq: int,
    ) -> None:
        await conn.send(
            StreamOpcode.S_DELETE_DLS,
            lambda writer: writer.string(stream_name).string(group_name).u64(seq),
        )

    @staticmethod
    async def purge_dls(
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
    ) -> int:
        _, cursor = await conn.send(
            StreamOpcode.S_PURGE_DLS,
            lambda writer: writer.string(stream_name).string(group_name),
        )
        return cursor.read_u32()


@dataclass
class _ConsumerSession:
    epoch: str
    consumer_id: str
    valid: bool = True


@dataclass
class _LeasedBatch(Generic[T]):
    session: _ConsumerSession
    receipt: str
    key: bytes | None
    messages: list[dict[str, Any]]
    wire_bytes: int
    lease_ms: int
    completed: int = 0
    terminal: bool = False
    dead: bool = False
    running: bool = False
    settled: bool = False
    pending_calls: int = 0
    renewing: bool = False
    current_task: asyncio.Task[Any] | None = None
    current_cancel: Callable[[str], None] | None = None


@dataclass
class _QueuedUpdate(Generic[T]):
    completed: int
    flags: int
    terminal: bool
    batch: _LeasedBatch[T]


@dataclass
class _FetchRequest:
    max_messages: int
    max_key_messages: int
    max_key_batches: int
    max_bytes: int
    wait_ms: int


class StreamSubscription(Generic[T]):
    def __init__(
        self,
        conn: NexoConnection,
        stream_name: str,
        group_name: str,
        logger: Logger,
        callback: StreamHandler[T],
        batch_size: int,
        key_batch_size: int,
        wait_ms: int,
        concurrency: int,
        stop_timeout_ms: int,
        callback_timeout_ms: int,
        max_buffered_messages: int,
        max_buffered_bytes: int,
    ) -> None:
        self._conn = conn
        self._stream_name = stream_name
        self._group_name = group_name
        self._logger = logger
        self._callback: Callable[..., Any] = callback
        self._callback_wants_meta = _callback_accepts_meta(callback)
        self._batch_size = batch_size
        self._key_batch_size = key_batch_size
        self._wait_ms = wait_ms
        self._concurrency = concurrency
        self._stop_timeout_ms = stop_timeout_ms
        self._callback_timeout_ms = callback_timeout_ms
        self._max_buffered_messages = max_buffered_messages
        self._max_buffered_bytes = max_buffered_bytes
        self._active = False
        self._loop_task: asyncio.Task[None] | None = None
        self._renew_task: asyncio.Task[None] | None = None
        self._session: _ConsumerSession | None = None
        self._joining = False
        self._stopping = False
        self._stopped = False
        self._fatal_error: BaseException | None = None
        self._teardown_done: asyncio.Task[None] | None = None
        self._batches: dict[str, _LeasedBatch[T]] = {}
        self._owned_keys: dict[bytes, _LeasedBatch[T]] = {}
        self._executing_keys: dict[bytes, _LeasedBatch[T]] = {}
        self._blocked_keys: dict[bytes, _LeasedBatch[T]] = {}
        self._ready: list[_LeasedBatch[T] | None] = []
        self._ready_head = 0
        self._buffered_messages = 0
        self._buffered_bytes = 0
        self._reserved_messages = 0
        self._reserved_bytes = 0
        self._reserved_key_slots = 0
        self._running_workers = 0
        self._retired_invocations = 0
        self._calls_abandoned = False
        self._capacity_task: asyncio.Task[None] | None = None
        self._cleanup_session: _ConsumerSession | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._join_task: asyncio.Task[None] | None = None
        self._update_queue: dict[str, _QueuedUpdate[T]] = {}
        self._update_inflight = False
        self._inflight_fetch: asyncio.Task[None] | None = None
        self._renewable_count = 0
        self._renew_interval_s: float | None = None
        self._wake = asyncio.Event()
        self._drive_scheduled = False

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
        self._loop_task = asyncio.create_task(self._supervise())
        self._loop_task.add_done_callback(self._on_done)
        self._poke()

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
        if self._stopping:
            return
        self._stopping = True
        self._active = False
        self._poke()
        for batch in list(self._batches.values()):
            if (
                not batch.running
                and not batch.terminal
                and not batch.dead
                and not batch.settled
                and batch.session.valid
            ):
                self._enqueue_update(
                    batch, FLAG_STREAM_S_UPDATE_BATCH_RELEASE, terminal=True
                )
        drained_in_grace = await self._wait_drained_until(
            time.monotonic() + self._stop_timeout_ms / 1000.0
        )
        if not drained_in_grace:
            for batch in list(self._batches.values()):
                if batch.dead or batch.settled or batch.terminal:
                    continue
                if not batch.session.valid:
                    batch.dead = True
                    self._maybe_release(batch)
                    continue
                if batch.running and batch.pending_calls > 0:
                    self._calls_abandoned = True
                    self._abort_call(batch)
                    self._enqueue_update(
                        batch, FLAG_STREAM_S_UPDATE_BATCH_RETRY, terminal=True
                    )
                else:
                    self._enqueue_update(
                        batch, FLAG_STREAM_S_UPDATE_BATCH_RELEASE, terminal=True
                    )
            await self._wait_drained_until(
                time.monotonic() + FETCH_TIMEOUT_MARGIN_MS / 1000.0
            )
        self._stopped = True
        self._poke()
        await self._teardown()
        if not drained_in_grace:
            raise TimeoutError(
                "Stream subscription stop timed out after "
                f"{self._stop_timeout_ms}ms"
            )
        if self._loop_task is not None:
            error = self._fatal_error
            if error is not None:
                raise error

    async def _supervise(self) -> None:
        while self._fatal_error is None and not self._stopped:
            await self._wake.wait()
        try:
            await self._teardown()
        except Exception as error:
            self._logger.error(
                f"[{self._stream_name}:{self._group_name}] "
                f"Stream teardown failed {error}"
            )
            if self._fatal_error is None:
                self._fatal_error = error
        if self._fatal_error is not None:
            raise self._fatal_error

    def _fail(self, error: BaseException) -> None:
        if self._fatal_error is not None:
            return
        self._fatal_error = error
        self._active = False
        self._calls_abandoned = True
        if self._capacity_task is not None:
            self._capacity_task.cancel()
            self._capacity_task = None
        session = self._session
        if session is not None:
            self._invalidate_session(session)
        for batch in list(self._batches.values()):
            if not batch.dead:
                batch.dead = True
                self._abort_call(batch)
                self._maybe_release(batch)
        self._update_queue.clear()
        self._poke()

    def _poke(self) -> None:
        self._wake.set()
        self._wake = asyncio.Event()
        if self._drive_scheduled:
            return
        self._drive_scheduled = True
        asyncio.get_running_loop().call_soon(self._drive_once)

    def _drive_once(self) -> None:
        self._drive_scheduled = False
        self._drive()

    async def _nap_or_sleep(self, milliseconds: int) -> None:
        event = self._wake
        try:
            await asyncio.wait_for(event.wait(), timeout=milliseconds / 1000.0)
        except asyncio.TimeoutError:
            pass

    def _assess_capacity_soon(self) -> None:
        if (
            self._capacity_task is not None
            or self._retired_invocations < self._concurrency
            or self._fatal_error is not None
            or self._stopped
        ):
            return

        async def assess() -> None:
            await asyncio.sleep(CANCELLATION_SETTLE_MS / 1000.0)
            if (
                self._retired_invocations >= self._concurrency
                and self._fatal_error is None
                and not self._stopped
            ):
                self._fail(
                    RuntimeError(
                        "Stream callbacks did not settle after cancellation; "
                        "callback capacity exhausted"
                    )
                )

        self._capacity_task = asyncio.ensure_future(assess())
        self._capacity_task.add_done_callback(self._capacity_done)

    def _capacity_done(self, task: asyncio.Task[None]) -> None:
        if task is self._capacity_task:
            self._capacity_task = None
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._fail(error)

    def _take_ready_batch(self) -> _LeasedBatch[T] | None:
        while self._ready_head < len(self._ready):
            batch = self._ready[self._ready_head]
            self._ready[self._ready_head] = None
            self._ready_head += 1
            if batch is None:
                continue
            if (
                batch.dead
                or batch.terminal
                or batch.settled
                or not batch.session.valid
                or batch.running
            ):
                self._maybe_release(batch)
                continue
            if batch.key is not None:
                if batch.key in self._executing_keys:
                    self._blocked_keys[batch.key] = batch
                    continue
                self._executing_keys[batch.key] = batch
            return batch
        return None

    def _drive(self) -> None:
        if self._active and not self._stopping and self._fatal_error is None:
            self._ensure_session()
            while self._running_workers < self._concurrency:
                batch = self._take_ready_batch()
                if batch is None:
                    break
                self._running_workers += 1
                worker = asyncio.ensure_future(self._run_worker(batch))
                worker.add_done_callback(self._runner_done)
            if (
                self._ready_head > 64
                and self._ready_head * 2 >= len(self._ready)
            ):
                del self._ready[: self._ready_head]
                self._ready_head = 0
            self._maybe_fetch()
        self._kick_updates()

    def _runner_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._fail(error)

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
        epoch = cursor.read_uuid()
        consumer_id = cursor.read_string()
        session = _ConsumerSession(epoch=epoch, consumer_id=consumer_id)
        if (
            not self._active
            or self._stopping
            or self._fatal_error is not None
            or self._session is not None
        ):
            session.valid = False
            self._capture_cleanup(session)
            return
        self._session = session

    def _ensure_session(self) -> None:
        if (
            self._session is not None
            or self._joining
            or not self._active
            or self._stopping
            or self._fatal_error is not None
        ):
            return
        self._joining = True

        async def joiner() -> None:
            try:
                while (
                    self._active
                    and not self._stopping
                    and self._fatal_error is None
                    and self._session is None
                ):
                    if not self._conn.is_connected:
                        await self._nap_or_sleep(
                            DEFAULT_CONFIG.connection.backoff_short_ms
                        )
                        continue
                    cleanup = self._cleanup_task
                    if cleanup is not None:
                        try:
                            await cleanup
                        except Exception:
                            pass
                        if self._cleanup_task is cleanup:
                            self._cleanup_task = None
                    try:
                        await self._join()
                        break
                    except Exception as error:
                        if _is_recoverable_membership_error(error):
                            continue
                        if not self._conn.is_connected or isinstance(
                            error, (ConnectionClosedError, NotConnectedError)
                        ):
                            await self._nap_or_sleep(
                                DEFAULT_CONFIG.connection.backoff_short_ms
                            )
                            continue
                        if isinstance(error, NexoError):
                            self._fail(error)
                            return
                        self._logger.error(
                            f"[{self._stream_name}:{self._group_name}] "
                            "Join failed. Retrying in "
                            f"{DEFAULT_CONFIG.connection.backoff_long_ms}ms... "
                            f"{error}"
                        )
                        await self._nap_or_sleep(
                            DEFAULT_CONFIG.connection.backoff_long_ms
                        )
            finally:
                self._joining = False
                self._poke()

        self._join_task = asyncio.ensure_future(joiner())
        self._join_task.add_done_callback(self._joiner_done)

    def _joiner_done(self, task: asyncio.Task[None]) -> None:
        if self._join_task is task:
            self._join_task = None
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._fail(error)

    def _capture_cleanup(self, session: _ConsumerSession) -> None:
        if self._cleanup_session is session:
            return
        self._cleanup_session = session
        if not self._conn.is_connected:
            return

        async def chain(prior: asyncio.Task[None] | None) -> None:
            prior_error: BaseException | None = None
            if prior is not None:
                try:
                    await prior
                except Exception as error:
                    prior_error = error
            await self._leave_member(session)
            if prior_error is not None:
                raise prior_error

        task = asyncio.ensure_future(chain(self._cleanup_task))
        self._cleanup_task = task

        def _observe(t: asyncio.Task[None]) -> None:
            if not t.cancelled():
                t.exception()

        task.add_done_callback(_observe)

    def _invalidate_session(self, session: _ConsumerSession) -> None:
        if not session.valid:
            return
        session.valid = False
        if self._session is session:
            self._session = None
        self._capture_cleanup(session)
        for batch in list(self._batches.values()):
            if batch.session is session and not batch.dead:
                batch.dead = True
                self._abort_call(batch)
                self._maybe_release(batch)
        for receipt in [
            receipt
            for receipt, update in self._update_queue.items()
            if update.batch.session is session
        ]:
            del self._update_queue[receipt]
        self._poke()

    def _on_rpc_error(self, session: _ConsumerSession, error: Exception) -> None:
        if not session.valid or self._session is not session:
            return
        if self._stopping or not self._active:
            self._fail(error)
            return
        if _is_recoverable_membership_error(error):
            self._invalidate_session(session)
            return
        if not self._conn.is_connected or isinstance(
            error, (ConnectionClosedError, NotConnectedError)
        ):
            self._invalidate_session(session)
            return
        if isinstance(error, (RequestTimeoutError, RequestCancelledError)):
            self._logger.error(
                f"[{self._stream_name}:{self._group_name}] "
                f"Stream request failed. Rejoining. {error}"
            )
            self._invalidate_session(session)
            return
        if isinstance(error, NexoError):
            self._fail(error)
            return
        self._logger.error(
            f"[{self._stream_name}:{self._group_name}] "
            f"Stream request failed. Rejoining. {error}"
        )
        self._invalidate_session(session)

    async def _leave_member(self, session: _ConsumerSession) -> None:
        if not self._conn.is_connected:
            return
        try:
            await asyncio.wait_for(
                self._conn.send(
                    StreamOpcode.S_LEAVE,
                    lambda writer: writer.string(self._stream_name)
                    .string(self._group_name)
                    .string(session.consumer_id)
                    .uuid(session.epoch),
                    timeout_ms=FETCH_TIMEOUT_MARGIN_MS,
                ),
                timeout=FETCH_TIMEOUT_MARGIN_MS / 1000.0,
            )
        except asyncio.TimeoutError:
            raise TimeoutError("Stream member leave timed out") from None
        except Exception:
            pass

    def _abort_call(self, batch: _LeasedBatch[T]) -> None:
        if batch.current_cancel is not None:
            batch.current_cancel("aborted")

    def _maybe_fetch(self) -> None:
        session = self._session
        if (
            session is None
            or not session.valid
            or self._inflight_fetch is not None
        ):
            return
        free_messages = (
            self._max_buffered_messages
            - self._buffered_messages
            - self._reserved_messages
        )
        free_bytes = (
            self._max_buffered_bytes - self._buffered_bytes - self._reserved_bytes
        )
        free_key_slots = (
            self._concurrency - len(self._owned_keys) - self._reserved_key_slots
        )
        if (
            free_messages <= 0
            or free_bytes < FETCH_PAGE_MIN_BYTES
            or free_key_slots <= 0
        ):
            return
        ready_empty = self._ready_head >= len(self._ready)
        if free_messages < self._batch_size and not (
            ready_empty
            and self._running_workers < self._concurrency
        ):
            return
        max_messages = min(self._batch_size, free_messages)
        max_bytes = min(FETCH_MAX_BYTES, free_bytes)
        max_key_batches = min(free_key_slots, max_messages)
        max_key_messages = min(
            self._key_batch_size, max(1, self._max_buffered_messages // 2)
        )
        work_active = len(self._batches) > 0 or self._running_workers > 0
        wait_ms = (
            min(self._wait_ms, ACTIVE_FETCH_WAIT_MS)
            if work_active
            else self._wait_ms
        )
        request = _FetchRequest(
            max_messages=max_messages,
            max_key_messages=max_key_messages,
            max_key_batches=max_key_batches,
            max_bytes=max_bytes,
            wait_ms=wait_ms,
        )
        self._reserved_messages += max_messages
        self._reserved_bytes += max_bytes
        self._reserved_key_slots += max_key_batches
        self._inflight_fetch = asyncio.ensure_future(
            self._fetch(session, request)
        )
        self._inflight_fetch.add_done_callback(self._fetch_done)

    def _fetch_done(self, task: asyncio.Task[None]) -> None:
        if self._inflight_fetch is task:
            self._inflight_fetch = None
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as error:
            self._fail(error)
            return
        self._poke()

    async def _fetch(self, session: _ConsumerSession, request: _FetchRequest) -> None:
        try:
            _, cursor = await self._conn.send(
                StreamOpcode.S_FETCH,
                lambda writer: writer.string(self._stream_name)
                .string(self._group_name)
                .string(session.consumer_id)
                .uuid(session.epoch)
                .u32(request.max_messages)
                .u32(request.max_key_messages)
                .u32(request.max_key_batches)
                .u32(request.max_bytes)
                .u32(request.wait_ms),
                timeout_ms=request.wait_ms + FETCH_TIMEOUT_MARGIN_MS,
            )
        except asyncio.CancelledError:
            self._unreserve(request)
            raise
        except Exception as error:
            self._unreserve(request)
            self._on_rpc_error(session, error)
            return
        self._unreserve(request)
        if session is not self._session or not session.valid:
            return
        try:
            lease_ms, decoded = _decode_fetch_page(
                cursor,
                max_messages=request.max_messages,
                max_key_messages=request.max_key_messages,
                max_key_batches=request.max_key_batches,
                max_bytes=request.max_bytes,
            )
        except Exception as error:
            self._fail(error)
            return
        self._admit(session, lease_ms, decoded)

    def _unreserve(self, request: _FetchRequest) -> None:
        self._reserved_messages -= request.max_messages
        self._reserved_bytes -= request.max_bytes
        self._reserved_key_slots -= request.max_key_batches

    def _admit(
        self,
        session: _ConsumerSession,
        lease_ms: int,
        decoded: list[dict[str, Any]],
    ) -> None:
        for item in decoded:
            key = item["key"]
            previous = self._owned_keys.get(key) if key is not None else None
            if item["receipt"] in self._batches:
                self._fail(
                    ProtocolError("Duplicate batch receipt across pages")
                )
                return
            if previous is not None:
                previous.dead = True
                previous.settled = True
                self._update_queue.pop(previous.receipt, None)
                self._abort_call(previous)
                self._maybe_release(previous)
            batch: _LeasedBatch[T] = _LeasedBatch(
                session=session,
                receipt=item["receipt"],
                key=key,
                messages=item["messages"],
                wire_bytes=item["wire_bytes"],
                lease_ms=lease_ms,
            )
            self._batches[batch.receipt] = batch
            if key is not None:
                self._owned_keys[key] = batch
            self._buffered_messages += len(batch.messages)
            self._buffered_bytes += batch.wire_bytes
            if self._stopping or self._fatal_error is not None:
                self._enqueue_update(
                    batch, FLAG_STREAM_S_UPDATE_BATCH_RELEASE, terminal=True
                )
            else:
                self._track_renewal(batch)
                self._ready.append(batch)
        self._poke()

    def _enqueue_update(
        self,
        batch: _LeasedBatch[T],
        flags: int,
        *,
        terminal: bool,
    ) -> None:
        if batch.dead or batch.settled:
            return
        if terminal:
            batch.terminal = True
            self._stop_renewing(batch)
        if not batch.session.valid:
            batch.dead = True
            self._maybe_release(batch)
            return
        existing = self._update_queue.get(batch.receipt)
        if existing is not None and existing.terminal and not terminal:
            return
        self._update_queue[batch.receipt] = _QueuedUpdate(
            completed=batch.completed,
            flags=flags,
            terminal=terminal,
            batch=batch,
        )
        self._kick_updates()

    def _kick_updates(self) -> None:
        if self._update_inflight or not self._update_queue:
            return
        session = self._session
        if session is None or not session.valid:
            return
        self._update_inflight = True
        task = asyncio.ensure_future(self._flush_updates(session))

        def done(t: asyncio.Task[None]) -> None:
            self._update_inflight = False
            try:
                t.result()
            except asyncio.CancelledError:
                pass
            except Exception as error:
                self._fail(error)
                return
            self._poke()

        task.add_done_callback(done)

    async def _flush_updates(self, session: _ConsumerSession) -> None:
        items = [
            (receipt, update)
            for receipt, update in self._update_queue.items()
            if update.batch.session is session
        ][:UPDATE_FRAME_MAX]
        if not items:
            return
        sent = {receipt for receipt, _ in items}
        for receipt, update in items:
            if self._update_queue.get(receipt) is update:
                del self._update_queue[receipt]

        def write_updates(writer: Any) -> None:
            writer.string(self._stream_name).string(
                self._group_name
            ).string(session.consumer_id).uuid(session.epoch).u32(len(items))
            for receipt, update in items:
                writer.uuid(receipt).u32(update.completed).u8(update.flags)

        try:
            _, cursor = await self._conn.send(
                StreamOpcode.S_UPDATE_BATCHES, write_updates
            )
        except Exception as error:
            self._on_rpc_error(session, error)
            return
        if not session.valid or self._session is not session:
            return
        try:
            count = cursor.read_u32()
            if count > len(sent):
                raise ProtocolError("more failures than sent updates")
            failures: dict[str, int] = {}
            for _ in range(count):
                receipt = cursor.read_uuid()
                failure_code = cursor.read_u8()
                if receipt in failures:
                    raise ProtocolError("duplicate outcome for a batch receipt")
                failures[receipt] = failure_code
            if cursor.offset != len(cursor.buf):
                raise ProtocolError("trailing bytes in batch outcome response")
            for receipt in failures:
                if receipt not in sent:
                    raise ProtocolError("outcome for an unknown receipt")
        except Exception as error:
            self._fail(
                error
                if isinstance(error, ProtocolError)
                else ProtocolError("Malformed batch outcome response")
            )
            return
        for receipt, update in items:
            code = failures.get(receipt)
            batch = update.batch
            if code is None:
                if update.terminal:
                    batch.settled = True
            elif code == ErrorCode.FENCED:
                batch.dead = True
                batch.settled = True
                self._update_queue.pop(receipt, None)
                self._abort_call(batch)
                self._logger.error(
                    f"[{self._stream_name}:{self._group_name}] "
                    f"Batch update fenced for receipt={receipt}."
                )
            else:
                self._fail(
                    ProtocolError(
                        f"Batch update rejected (code {code}) "
                        f"for receipt={receipt}"
                    )
                )
                return
            self._maybe_release(batch)
        self._poke()

    def _track_renewal(self, batch: _LeasedBatch[T]) -> None:
        if batch.renewing:
            return
        batch.renewing = True
        self._renewable_count += 1
        interval = max(1, batch.lease_ms // 3) / 1000.0
        if self._renew_interval_s is None or interval < self._renew_interval_s:
            self._renew_interval_s = interval
            task = self._renew_task
            if task is not None:
                self._renew_task = None
                task.cancel()
        if self._renew_task is None or self._renew_task.done():
            self._renew_task = asyncio.ensure_future(self._renew_loop())
            self._renew_task.add_done_callback(self._renew_done)

    def _stop_renewing(self, batch: _LeasedBatch[T]) -> None:
        if not batch.renewing:
            return
        batch.renewing = False
        self._renewable_count -= 1
        if self._renewable_count == 0:
            task = self._renew_task
            self._renew_task = None
            if task is not None:
                task.cancel()

    def _renew_done(self, task: asyncio.Task[None]) -> None:
        if task is self._renew_task:
            self._renew_task = None
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._fail(error)

    async def _renew_loop(self) -> None:
        while self._renewable_count > 0:
            await asyncio.sleep(self._renew_interval_s or 0.001)
            if self._stopped or self._fatal_error is not None:
                return
            for batch in list(self._batches.values()):
                if (
                    batch.renewing
                    and not batch.terminal
                    and not batch.dead
                    and not batch.settled
                    and batch.session.valid
                ):
                    self._enqueue_update(batch, 0, terminal=False)

    async def _run_worker(self, first_batch: _LeasedBatch[T]) -> None:
        batch = first_batch
        since_yield = 0
        try:
            while True:
                batch.running = True
                proceed = True
                try:
                    while batch.completed < len(batch.messages):
                        if (
                            self._stopping
                            or self._fatal_error is not None
                            or batch.dead
                            or batch.terminal
                            or not batch.session.valid
                        ):
                            proceed = False
                            break
                        message = batch.messages[batch.completed]
                        outcome = await self._invoke_callback(batch, message)
                        if outcome != "ok":
                            if (
                                outcome != "aborted"
                                and not batch.dead
                                and batch.session.valid
                                and not batch.terminal
                            ):
                                self._enqueue_update(
                                    batch,
                                    FLAG_STREAM_S_UPDATE_BATCH_RETRY,
                                    terminal=True,
                                )
                            proceed = False
                            break
                        if (
                            batch.dead
                            or batch.terminal
                            or not batch.session.valid
                            or self._fatal_error is not None
                            or self._calls_abandoned
                        ):
                            proceed = False
                            break
                        batch.completed += 1
                        since_yield += 1
                        if since_yield >= 64:
                            since_yield = 0
                            await asyncio.sleep(0)
                finally:
                    batch.running = False
                    self._finish_batch(batch)
                if (
                    not proceed
                    or self._stopping
                    or self._fatal_error is not None
                ):
                    return
                next_batch = self._take_ready_batch()
                if next_batch is None:
                    return
                batch = next_batch
        finally:
            self._running_workers -= 1
            self._poke()

    async def _invoke_callback(
        self,
        batch: _LeasedBatch[T],
        message: dict[str, Any],
    ) -> str:
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None
        batch.current_task = task
        batch.pending_calls += 1
        settled = False
        retired = False
        reason: str | None = None

        def cancel(why: str) -> None:
            nonlocal retired, reason
            if settled or reason is not None:
                return
            reason = why
            retired = True
            self._retired_invocations += 1
            if (
                why == "timeout"
                and batch.session.valid
                and not batch.dead
                and not batch.terminal
            ):
                self._enqueue_update(
                    batch, FLAG_STREAM_S_UPDATE_BATCH_RETRY, terminal=True
                )
                self._logger.error(
                    f"[{self._stream_name}:{self._group_name}] "
                    f"Callback timed out after {self._callback_timeout_ms}ms "
                    f"at seq={message['seq']}."
                )
            task.cancel()
            self._assess_capacity_soon()
            self._poke()

        batch.current_cancel = cancel
        timer = loop.call_later(
            self._callback_timeout_ms / 1000.0, cancel, "timeout"
        )
        try:
            if self._callback_wants_meta:
                result = self._callback(
                    message["data"], {"seq": message["seq"], "key": batch.key}
                )
            else:
                result = self._callback(message["data"])
            if inspect.isawaitable(result):
                await result
            return reason or "ok"
        except asyncio.CancelledError:
            if reason is not None:
                return reason
            self._log_processing_error(message["seq"])
            return "failed"
        except Exception:
            if reason is None:
                self._log_processing_error(message["seq"])
            return reason or "failed"
        finally:
            settled = True
            timer.cancel()
            batch.pending_calls -= 1
            if retired:
                self._retired_invocations -= 1
            if batch.current_cancel is cancel:
                batch.current_cancel = None
                batch.current_task = None
            if (
                self._retired_invocations < self._concurrency
                and self._capacity_task is not None
            ):
                self._capacity_task.cancel()
                self._capacity_task = None
            self._maybe_release(batch)
            self._poke()

    def _log_processing_error(self, seq: int) -> None:
        self._logger.error(
            f"[{self._stream_name}:{self._group_name}] "
            f"Processing error at seq={seq}. "
            "Batch retries after the lease wait."
        )

    def _finish_batch(self, batch: _LeasedBatch[T]) -> None:
        if batch.terminal or batch.dead:
            self._maybe_release(batch)
            return
        if not batch.session.valid or self._fatal_error is not None:
            batch.dead = True
            self._maybe_release(batch)
            return
        flags = (
            0
            if batch.completed == len(batch.messages)
            else FLAG_STREAM_S_UPDATE_BATCH_RELEASE
        )
        self._enqueue_update(batch, flags, terminal=True)
        self._maybe_release(batch)

    def _release_execution(self, batch: _LeasedBatch[T]) -> None:
        if (
            batch.key is None
            or batch.running
            or batch.pending_calls > 0
            or self._executing_keys.get(batch.key) is not batch
        ):
            return
        del self._executing_keys[batch.key]
        waiting = self._blocked_keys.get(batch.key)
        if waiting is not None:
            del self._blocked_keys[batch.key]
            if (
                not waiting.dead
                and not waiting.terminal
                and not waiting.settled
                and waiting.session.valid
            ):
                self._ready.append(waiting)

    def _maybe_release(self, batch: _LeasedBatch[T]) -> None:
        self._release_execution(batch)
        if not batch.settled and not batch.dead:
            if batch.terminal:
                self._stop_renewing(batch)
            return
        self._stop_renewing(batch)
        if batch.key is not None and self._blocked_keys.get(batch.key) is batch:
            del self._blocked_keys[batch.key]
        if batch.key is not None and self._owned_keys.get(batch.key) is batch:
            del self._owned_keys[batch.key]
        if batch.running or batch.pending_calls > 0:
            return
        if self._batches.pop(batch.receipt, None) is None:
            return
        self._buffered_messages -= len(batch.messages)
        self._buffered_bytes -= batch.wire_bytes
        batch.messages.clear()

    def _drained(self) -> bool:
        if (
            not self._calls_abandoned
            and self._running_workers > self._retired_invocations
        ):
            return False
        if self._update_inflight or self._update_queue:
            return False
        for batch in self._batches.values():
            if not batch.settled and not batch.dead and batch.session.valid:
                return False
        return True

    async def _wait_drained_until(self, deadline: float) -> bool:
        while not self._drained():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return self._drained()
            event = self._wake
            try:
                await asyncio.wait_for(event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return self._drained()
        return True

    def _teardown(self) -> asyncio.Task[None]:
        if self._teardown_done is None:
            self._teardown_done = asyncio.ensure_future(self._do_teardown())
        return self._teardown_done

    async def _do_teardown(self) -> None:
        if self._renew_task is not None:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        if self._capacity_task is not None:
            self._capacity_task.cancel()
            self._capacity_task = None
        session = self._session
        if session is not None:
            self._invalidate_session(session)
        joiner = self._join_task
        if joiner is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(joiner),
                    timeout=FETCH_TIMEOUT_MARGIN_MS / 1000.0,
                )
            except asyncio.TimeoutError:
                raise TimeoutError(
                    "Stream join did not settle during teardown"
                ) from None
            except Exception:
                pass
        if self._cleanup_task is not None:
            await self._cleanup_task
        pending = self._inflight_fetch
        if pending is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(pending),
                    timeout=FETCH_TIMEOUT_MARGIN_MS / 1000.0,
                )
            except asyncio.TimeoutError:
                raise TimeoutError(
                    "Stream fetch did not settle during teardown"
                ) from None
            except Exception:
                pass
            except asyncio.CancelledError:
                pass


class NexoStreamDLS:
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
    ) -> list[DLSEntry]:
        return await StreamCommands.peek_dls(
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
        await StreamCommands.delete_dls(
            self._conn,
            self._stream_name,
            self._group_name,
            seq,
        )

    async def purge(self) -> int:
        return await StreamCommands.purge_dls(
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
        self._dls = NexoStreamDLS(conn, stream_name, name)

    @property
    def dls(self) -> NexoStreamDLS:
        return self._dls

    async def subscribe(
        self,
        callback: StreamHandler[T],
        *,
        batch_size: int = DEFAULT_CONFIG.stream.batch_size,
        key_batch_size: int = DEFAULT_CONFIG.stream.key_batch_size,
        wait_ms: int = DEFAULT_CONFIG.stream.wait_ms,
        concurrency: int = DEFAULT_CONFIG.stream.concurrency,
        stop_timeout_ms: int = DEFAULT_CONFIG.stream.stop_timeout_ms,
        callback_timeout_ms: int = DEFAULT_CONFIG.stream.callback_timeout_ms,
        max_buffered_messages: int | None = None,
        max_buffered_bytes: int = DEFAULT_CONFIG.stream.max_buffered_bytes,
    ) -> Subscription[T]:
        resolved_buffered_messages = (
            2 * batch_size if max_buffered_messages is None else max_buffered_messages
        )

        def _positive_timer(value: Any) -> bool:
            return (
                isinstance(value, int)
                and not isinstance(value, bool)
                and 1 <= value <= 0x7FFFFFFF
            )

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
        if (
            not isinstance(key_batch_size, int)
            or isinstance(key_batch_size, bool)
            or key_batch_size < 1
            or key_batch_size > STREAM_MAX_KEY_BATCH_SIZE
        ):
            raise ValueError(
                "key_batch_size must be an integer between 1 and "
                f"{STREAM_MAX_KEY_BATCH_SIZE}"
            )
        if not _positive_timer(wait_ms):
            raise ValueError("wait_ms must be a positive integer")
        if (
            not isinstance(concurrency, int)
            or isinstance(concurrency, bool)
            or concurrency < 1
            or concurrency > STREAM_MAX_FETCH_BATCH_SIZE
        ):
            raise ValueError(
                "concurrency must be an integer between 1 and "
                f"{STREAM_MAX_FETCH_BATCH_SIZE}"
            )
        if not _positive_timer(stop_timeout_ms):
            raise ValueError("stop_timeout_ms must be a positive integer")
        if not _positive_timer(callback_timeout_ms):
            raise ValueError("callback_timeout_ms must be a positive integer")
        if (
            not isinstance(resolved_buffered_messages, int)
            or isinstance(resolved_buffered_messages, bool)
            or resolved_buffered_messages < 1
            or resolved_buffered_messages > 2 * STREAM_MAX_FETCH_BATCH_SIZE
        ):
            raise ValueError(
                "max_buffered_messages must be an integer between 1 and "
                f"{2 * STREAM_MAX_FETCH_BATCH_SIZE}"
            )
        if (
            not isinstance(max_buffered_bytes, int)
            or isinstance(max_buffered_bytes, bool)
            or max_buffered_bytes < 54
            or max_buffered_bytes > 0xFFFFFFFF
        ):
            raise ValueError(
                "max_buffered_bytes must be an integer between 54 and 4294967295"
            )

        consumer = StreamSubscription[T](
            self._conn,
            self._stream_name,
            self.name,
            self._logger,
            callback,
            batch_size,
            key_batch_size,
            wait_ms,
            concurrency,
            stop_timeout_ms,
            callback_timeout_ms,
            resolved_buffered_messages,
            max_buffered_bytes,
        )
        await consumer.start()
        return Subscription(
            stop_fn=consumer.stop,
            active_fn=lambda: consumer._active,
            completion=consumer.completion,
            error_fn=lambda: consumer._fatal_error,
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
