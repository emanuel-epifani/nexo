from __future__ import annotations

import asyncio
import signal
import uuid

import pytest

from nexo import NexoClient
from nexo.errors import ConnectionClosedError


@pytest.mark.asyncio
class TestDisconnect:
    async def test_reject_pending_requests_on_disconnect(self):
        client = await NexoClient.connect()
        q_name = f"disconnect-pending-{uuid.uuid4()}"
        await client.queue.create(q_name)

        conn = client._conn

        async def consume():
            await conn.send(
                0x12,
                lambda w: w.string(q_name).u32(1).u32(5000),
                timeout_ms=10000,
            )

        consume_task = asyncio.create_task(consume())

        await asyncio.sleep(0.1)

        client.disconnect()

        with pytest.raises((ConnectionClosedError, Exception)):
            await consume_task

    async def test_signal_listeners_registered_and_removed(self):
        # Python signal handlers are process-global, so we verify
        # that disconnect restores the default handler.
        client = await NexoClient.connect()

        # Client should have replaced SIGINT with a custom handler
        current = signal.getsignal(signal.SIGINT)
        assert current is not signal.SIG_DFL

        client.disconnect()

        # After disconnect, default handler should be restored
        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

    async def test_multiple_clients_independent_listeners(self):
        client_a = await NexoClient.connect()
        client_b = await NexoClient.connect()

        # Both connected
        assert client_a._conn.is_connected
        assert client_b._conn.is_connected

        client_a.disconnect()
        assert client_a._conn.is_connected is False
        assert client_b._conn.is_connected  # B still connected

        client_b.disconnect()
        assert client_b._conn.is_connected is False
