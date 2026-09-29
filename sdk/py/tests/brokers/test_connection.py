from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from nexo import NexoClient
from nexo.config import NexoConnectionConfig
from nexo.errors import RequestTimeoutError
from nexo.transport.tcp.connection import NexoConnection
from nexo.utils.logger import Logger


@pytest.mark.asyncio
class TestConnection:
    async def test_request_timeout(self):
        client = await NexoClient.connect()
        q_name = f"timeout-test-{uuid.uuid4()}"
        await client.queue.create(q_name)

        conn = client._conn  # access internal connection

        async def consume():
            await conn.send(
                0x12,
                lambda w: w.string(q_name).u32(1).u32(10000),
                timeout_ms=300,
            )

        start = time.monotonic()
        with pytest.raises(RequestTimeoutError, match="Request timeout after 300ms"):
            await consume()
        elapsed = time.monotonic() - start

        assert elapsed < 3.0
        client.disconnect()

    async def test_fire_and_forget_when_disconnected(self):
        client = await NexoClient.connect()
        q_name = f"fire-forget-{uuid.uuid4()}"
        await client.queue.create(q_name)

        queue = await client.queue.get(q_name)
        await queue.push("test-data")
        conn = client._conn
        _, cursor = await conn.send(0x12, lambda w: w.string(q_name).u32(1).u32(1000))
        msg_id = cursor.read_uuid()

        client.disconnect()
        assert conn.is_connected is False

        # Must not raise
        conn.send_fire_and_forget(0x13, lambda w: w.uuid(msg_id).string(q_name))

    async def test_timeout_covers_stalled_drain(self):
        class _StalledWriter:
            def write(self, data):
                pass

            async def drain(self):
                await asyncio.Future()

        conn = NexoConnection(NexoConnectionConfig(), Logger(level="OFF"))
        conn.is_connected = True
        conn._writer = _StalledWriter()

        start = time.monotonic()
        with pytest.raises(RequestTimeoutError, match="Request timeout after 300ms"):
            await asyncio.wait_for(
                conn.send(0x12, lambda w: w.u8(0), timeout_ms=300), timeout=2.0
            )
        elapsed = time.monotonic() - start

        assert elapsed < 1.5
        assert conn._pending == {}

    async def test_cancelled_send_cleans_pending_and_timer(self):
        class _InstantWriter:
            def write(self, data):
                pass

            async def drain(self):
                return None

        conn = NexoConnection(NexoConnectionConfig(), Logger(level="OFF"))
        conn.is_connected = True
        conn._writer = _InstantWriter()

        task = asyncio.create_task(
            conn.send(0x12, lambda w: w.u8(0), timeout_ms=60000)
        )
        await asyncio.sleep(0)
        assert len(conn._pending) == 1
        timer = next(iter(conn._pending.values()))[1]

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert conn._pending == {}
        assert timer.cancelled()
