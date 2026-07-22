from __future__ import annotations

import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


@pytest.mark.asyncio
class TestCrossBroker:
    BINARY_PAYLOAD = b"\xde\xad\xbe\xef\x00\xff"

    async def test_store_binary_payload(self, nexo: NexoClient):
        key = f"bin-store-{uuid.uuid4()}"
        await nexo.store.map.set(key, self.BINARY_PAYLOAD)

        retrieved = await nexo.store.map.get(key)

        assert isinstance(retrieved, (bytes, bytearray))
        assert bytes(retrieved) == self.BINARY_PAYLOAD

    async def test_queue_binary_payload(self, nexo: NexoClient):
        q_name = f"bin-queue-{uuid.uuid4()}"
        q = await nexo.queue(q_name).create()

        await q.push(self.BINARY_PAYLOAD)

        received: list = []
        sub = await q.subscribe(lambda msg: received.append(msg))

        await wait_for(lambda: len(received) == 1)
        assert isinstance(received[0], (bytes, bytearray))
        assert bytes(received[0]) == self.BINARY_PAYLOAD
        sub["stop"]()

    async def test_pubsub_binary_payload(self, nexo: NexoClient):
        topic = f"bin-pubsub-{uuid.uuid4()}"
        received: list = []

        await nexo.pubsub(topic).subscribe(lambda msg: received.append(msg))
        await nexo.pubsub(topic).publish(self.BINARY_PAYLOAD)

        await wait_for(lambda: len(received) == 1)
        assert isinstance(received[0], (bytes, bytearray))
        assert bytes(received[0]) == self.BINARY_PAYLOAD

    async def test_stream_binary_payload(self, nexo: NexoClient):
        topic = f"bin-stream-{uuid.uuid4()}"
        await nexo.stream(topic).create()

        await nexo.stream(topic).publish(self.BINARY_PAYLOAD)

        received: list = []
        sub = await nexo.stream(topic).subscribe("g1", lambda msg: received.append(msg))

        await wait_for(lambda: len(received) == 1)
        assert isinstance(received[0], (bytes, bytearray))
        assert bytes(received[0]) == self.BINARY_PAYLOAD
        await sub["stop"]()

    async def test_json_special_chars_and_nested(self, nexo: NexoClient):
        complex_data = {
            "string": "Nexo Engine 🚀",
            "number": 42.5,
            "boolean": True,
            "nullValue": None,
            "nested": {
                "id": "abc-123",
                "meta": {"active": True, "deep": {"value": "ok"}},
            },
            "unicode": "こんにちは",
        }

        key = f"proto:complex:{uuid.uuid4()}"
        await nexo.store.map.set(key, complex_data)
        result = await nexo.store.map.get(key)

        assert result == complex_data

    async def test_distinguish_empty_string_and_null(self, nexo: NexoClient):
        key_empty = f"proto:empty:{uuid.uuid4()}"
        key_null = f"proto:null:{uuid.uuid4()}"

        await nexo.store.map.set(key_empty, "")
        await nexo.store.map.set(key_null, None)

        assert await nexo.store.map.get(key_empty) == ""
        assert await nexo.store.map.get(key_null) is None
