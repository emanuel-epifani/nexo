from __future__ import annotations

import asyncio
import json
import socket
import struct
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
        self._pending: dict[
            int, tuple[asyncio.Future[tuple[int, bytes]], asyncio.TimerHandle]
        ] = {}

        self._should_reconnect: bool = False
        self._is_reconnecting: bool = False

        self._read_task: Optional[asyncio.Task] = None

        self.on_push: Optional[Callable[[str, Any], None]] = None
        self.on_reconnect: Optional[Callable[[], Any]] = None

        self._writer_buf = FrameWriter()

    async def connect(self) -> None:
        self._should_reconnect = True
        await self._create_socket_and_connect()

    async def _create_socket_and_connect(self) -> None:
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host, self._port
            )
        except OSError as e:
            raise ConnectionError(str(e)) from e

        self._tune_socket()
        self.is_connected = True

        # Start read loop
        self._read_task = asyncio.create_task(self._read_loop())

    def _tune_socket(self) -> None:
        # TCP tuning (parity with the TS SDK):
        # - TCP_NODELAY disables Nagle's algorithm, avoiding Nagle/delayed-ACK
        #   interactions that add up to ~40ms to sporadic small writes.
        # - SO_KEEPALIVE lets the kernel probe idle connections so dead sockets
        #   (NAT/LB idle timeouts) are detected in seconds instead of minutes.
        assert self._writer is not None
        sock = self._writer.get_extra_info("socket")
        if sock is None:
            return
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Idle delay before the first probe, mirroring the TS 30s setting:
            # TCP_KEEPIDLE on Linux, TCP_KEEPALIVE on macOS/BSD.
            for opt in ("TCP_KEEPIDLE", "TCP_KEEPALIVE"):
                constant = getattr(socket, opt, None)
                if constant is not None:
                    sock.setsockopt(socket.IPPROTO_TCP, constant, 30)
                    break
        except OSError:
            pass

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
                    entry = self._pending.pop(corr_id, None)
                    if entry is not None:
                        fut, timer = entry
                        timer.cancel()
                        if not fut.done():
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
        for fut, timer in self._pending.values():
            timer.cancel()
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

    def _expire_request(self, corr_id: int, timeout_ms: int) -> None:
        entry = self._pending.pop(corr_id, None)
        if entry is not None and not entry[0].done():
            entry[0].set_exception(RequestTimeoutError(timeout_ms))

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

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[int, bytes]] = loop.create_future()
        timeout = timeout_ms if timeout_ms is not None else self._config.request_timeout_ms
        timer = loop.call_later(timeout / 1000.0, self._expire_request, corr_id, timeout)
        self._pending[corr_id] = (fut, timer)

        assert self._writer is not None
        self._writer.write(packet)
        drain_task = asyncio.ensure_future(self._writer.drain())
        waiters: set[asyncio.Future[Any]] = {drain_task, fut}
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            if fut.done():
                status, data = fut.result()
            else:
                if drain_task.cancelled() or drain_task.exception() is not None:
                    raise ConnectionClosedError()
                status, data = await fut
        except asyncio.CancelledError:
            self._pending.pop(corr_id, None)
            raise
        except Exception:
            self._pending.pop(corr_id, None)
            raise
        finally:
            timer.cancel()
            if not drain_task.done():
                drain_task.cancel()

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

        for fut, timer in self._pending.values():
            timer.cancel()
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
