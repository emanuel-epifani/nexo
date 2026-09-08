from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from nexo import NexoClient
from nexo.errors import RequestTimeoutError


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
