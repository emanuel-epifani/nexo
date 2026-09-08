from __future__ import annotations

import asyncio
import json
import struct
import time
from typing import Any, Callable, Optional

from ...protocol.codec import FrameWriter, Cursor, BuildFn
from ...config import NexoConnectionConfig
from ...errors import (
    ConnectionClosedError,
    NexoError,
    NotConnectedError,
    ProtocolError,
    RequestTimeoutError,
    server_error,
)
from ...protocol.generated import (
    ErrorCode,
    FrameType,
    ResponseStatus,
    PROTOCOL_VERSION,
    HEADER_SIZE,
    HEADER_OFFSET_VERSION,
    HEADER_OFFSET_TYPE,
    HEADER_OFFSET_META,
    HEADER_OFFSET_ID,
    HEADER_OFFSET_PAYLOAD_LEN,
)
from ...utils.logger import Logger


def _decode_server_error(data: bytes) -> NexoError:
    if len(data) < 5:
        return ProtocolError("Malformed error response")
    code = data[0]
    message_length = struct.unpack_from(">I", data, 1)[0]
    message_end = 5 + message_length
    if message_end > len(data):
        return ProtocolError("Malformed error response")
    try:
        message = data[5:message_end].decode("utf-8")
    except UnicodeDecodeError:
        return ProtocolError("Malformed UTF-8 error message")
    details_data = data[message_end:]
    details: Any = None
    if details_data:
        try:
            details = json.loads(details_data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ProtocolError("Malformed JSON error details")
    return server_error(code, message, details=details)


class NexoConnection:
    def __init__(self, config: NexoConnectionConfig, logger: Logger) -> None:
        self._config = config
        self._host = config.host
        self._port = config.port
        self._logger = logger

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self.is_connected: bool = False

        self._next_id: int = 1
        self._pending: dict[int, asyncio.Future[tuple[int, bytes]]] = {}

        self._should_reconnect: bool = False
        self._is_reconnecting: bool = False

        self._sweep_task: Optional[asyncio.Task] = None
        self._read_task: Optional[asyncio.Task] = None

        self.on_push: Optional[Callable[[str, Any], None]] = None
        self.on_reconnect: Optional[Callable[[], Any]] = None

        self._writer_buf = FrameWriter()

    async def connect(self) -> None:
        self._should_reconnect = True
        self._start_sweep()
        await self._create_socket_and_connect()

    def _start_sweep(self) -> None:
        if self._sweep_task is not None:
            return
        self._sweep_task = asyncio.create_task(self._sweep_loop())

    def _stop_sweep(self) -> None:
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            self._sweep_task = None

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._config.sweep_interval_ms / 1000.0)
                now = time.monotonic()
                expired: list[int] = []
                for cid, fut in self._pending.items():
                    if fut.done():
                        expired.append(cid)
                        continue
                    # Check deadline via stored attribute
                    deadline = getattr(fut, "_nexo_deadline", None)
                    timeout_ms = getattr(fut, "_nexo_timeout_ms", None)
                    if deadline is not None and now > deadline:
                        expired.append(cid)
                        if not fut.done():
                            fut.set_exception(RequestTimeoutError(timeout_ms or 0))
                for cid in expired:
                    self._pending.pop(cid, None)
        except asyncio.CancelledError:
            pass

    async def _create_socket_and_connect(self) -> None:
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host, self._port
            )
        except OSError as e:
            raise ConnectionError(str(e)) from e

        self.is_connected = True

        # Start read loop
        self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                header = await self._reader.readexactly(HEADER_SIZE)
                version = header[HEADER_OFFSET_VERSION]
                if version != PROTOCOL_VERSION:
                    self._logger.error(
                        f"Unsupported protocol version: 0x{version:02x} "
                        f"(expected 0x{PROTOCOL_VERSION:02x})"
                    )
                    # Drain payload to stay aligned
                    payload_len = struct.unpack_from(
                        ">I", header, HEADER_OFFSET_PAYLOAD_LEN
                    )[0]
                    if payload_len:
                        await self._reader.readexactly(payload_len)
                    continue

                frame_type = header[HEADER_OFFSET_TYPE]
                meta = header[HEADER_OFFSET_META]
                corr_id = struct.unpack_from(">I", header, HEADER_OFFSET_ID)[0]
                payload_len = struct.unpack_from(
                    ">I", header, HEADER_OFFSET_PAYLOAD_LEN
                )[0]

                payload = await self._reader.readexactly(payload_len) if payload_len else b""

                if frame_type == FrameType.RESPONSE:
                    fut = self._pending.pop(corr_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result((meta, payload))
                elif frame_type == FrameType.PUSH_PUBSUB:
                    if self.on_push is not None:
                        cursor = Cursor(payload)
                        topic = cursor.read_string()
                        data = cursor.decode_any()
                        self.on_push(topic, data)
                else:
                    self._logger.warn(f"Unknown frame type: 0x{frame_type:02x}")

        except asyncio.IncompleteReadError:
            pass  # Connection closed
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._logger.error(f"Read loop error: {e}")
        finally:
            await self._handle_disconnect()

    async def _handle_disconnect(self) -> None:
        was_connected = self.is_connected
        self.is_connected = False

        if was_connected or self._is_reconnecting:
            self._logger.error("[Connection] SOCKET CLOSED.")

        # Reject all pending
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionClosedError())
        self._pending.clear()

        if self._should_reconnect and not self._is_reconnecting:
            asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        self._is_reconnecting = True
        self._logger.warn("Connection lost. Attempting to reconnect...")

        while self._should_reconnect and not self.is_connected:
            await asyncio.sleep(self._config.reconnect_delay_ms / 1000.0)
            try:
                await self._create_socket_and_connect()
                self._logger.info("Reconnected to Nexo Server")
                self._is_reconnecting = False
                if self.on_reconnect is not None:
                    result = self.on_reconnect()
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
                return
            except Exception:
                pass  # Retry silently

    async def send(
        self,
        opcode: int,
        build: Optional[BuildFn] = None,
        *,
        timeout_ms: Optional[int] = None,
    ) -> tuple[int, Cursor]:
        if not self.is_connected:
            raise NotConnectedError()

        corr_id = self._next_id
        self._next_id = (self._next_id + 1) & 0xFFFFFFFF or 1

        self._writer_buf.begin()
        if build is not None:
            build(self._writer_buf)
        packet = self._writer_buf.finish(corr_id, opcode)

        loop = asyncio.get_event_loop()
        fut: asyncio.Future[tuple[int, bytes]] = loop.create_future()
        timeout = timeout_ms if timeout_ms is not None else self._config.request_timeout_ms
        fut._nexo_deadline = time.monotonic() + timeout / 1000.0  # type: ignore[attr-defined]
        fut._nexo_timeout_ms = timeout  # type: ignore[attr-defined]

        self._pending[corr_id] = fut

        assert self._writer is not None
        self._writer.write(packet)
        try:
            await self._writer.drain()
        except Exception:
            self._pending.pop(corr_id, None)
            raise ConnectionClosedError()

        try:
            status, data = await asyncio.wait_for(fut, timeout=timeout / 1000.0)
        except asyncio.TimeoutError:
            self._pending.pop(corr_id, None)
            raise RequestTimeoutError(timeout)
        except asyncio.CancelledError:
            self._pending.pop(corr_id, None)
            raise
        except ConnectionClosedError:
            raise
        except Exception as e:
            self._pending.pop(corr_id, None)
            raise

        if status == ResponseStatus.ERR:
            error = _decode_server_error(data)
            if error.code not in {
                ErrorCode.FENCED,
                ErrorCode.NOT_MEMBER,
                ErrorCode.RESOURCE_NOT_FOUND,
            }:
                self._logger.error(f"<- ERROR 0x{opcode:02x} ({error})")
            raise error

        return status, Cursor(data)

    def send_fire_and_forget(
        self, opcode: int, build: Optional[BuildFn] = None
    ) -> None:
        if not self.is_connected or self._writer is None:
            return

        corr_id = self._next_id
        self._next_id = (self._next_id + 1) & 0xFFFFFFFF or 1

        self._writer_buf.begin()
        if build is not None:
            build(self._writer_buf)
        packet = self._writer_buf.finish(corr_id, opcode, FrameType.REQUEST_NO_RESPONSE)
        self._writer.write(packet)
        # Best-effort drain — don't block on fire-and-forget

    def disconnect(self) -> None:
        self._should_reconnect = False
        self._is_reconnecting = False
        self._stop_sweep()

        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionClosedError())
        self._pending.clear()

        if self._read_task is not None:
            self._read_task.cancel()
            self._read_task = None

        if self._writer is not None:
            self._writer.close()
            try:
                # Schedule close without blocking
                pass
            except Exception:
                pass

        self.is_connected = False
