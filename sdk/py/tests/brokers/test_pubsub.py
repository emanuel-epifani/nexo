from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


@pytest.mark.asyncio
class TestPubSub:
    async def test_exact_match_and_ignore_noise(self, nexo: NexoClient):
        target = f"chat/room1-{uuid.uuid4()}"
        noise = f"chat/room2-{uuid.uuid4()}"
        received: list = []

        await nexo.pubsub(target).subscribe(lambda data: received.append(data))
        await nexo.pubsub(target).publish({"msg": "target"})
        await nexo.pubsub(noise).publish({"msg": "noise"})

        await wait_for(lambda: len(received) == 1)
        assert received[0]["msg"] == "target"
        await nexo.pubsub(target).unsubscribe()

    async def test_single_level_wildcard(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"home-{base_id}/+/temp"
        received: list = []

        await nexo.pubsub(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub(f"home-{base_id}/kitchen/temp").publish({"id": "match-1"})
        await nexo.pubsub(f"home-{base_id}/garage/temp").publish({"id": "match-2"})
        await nexo.pubsub(f"home-{base_id}/kitchen/light").publish({"id": "fail-suffix"})
        await nexo.pubsub(f"home-{base_id}/kitchen/cupboard/temp").publish({"id": "fail-deep"})
        await nexo.pubsub(f"office-{base_id}/kitchen/temp").publish({"id": "fail-prefix"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["match-1", "match-2"]
        await nexo.pubsub(pattern).unsubscribe()

    async def test_clear_retained(self, nexo: NexoClient):
        topic = f"clear-retained-{uuid.uuid4()}"

        await nexo.pubsub(topic).publish("dark", {"retain": True})

        received: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: received.append(data))
        await wait_for(lambda: len(received) == 1)
        assert received[0] == "dark"

        await nexo.pubsub(topic).clear()
        await nexo.pubsub(topic).unsubscribe()

        after_clear: list[str] = []
        await nexo.pubsub(topic).subscribe(lambda data: after_clear.append(data))
        await asyncio.sleep(0.2)
        assert after_clear == []

    async def test_reject_invalid_ttl(self, nexo: NexoClient):
        topic = f"ttl-invalid-{uuid.uuid4()}"

        with pytest.raises(ValueError, match="Invalid ttl"):
            await nexo.pubsub(topic).publish("x", {"ttl": -1})

        with pytest.raises(ValueError, match="Invalid ttl"):
            await nexo.pubsub(topic).publish("x", {"ttl": 1.5})

    async def test_multi_level_wildcard(self, nexo: NexoClient):
        base_id = uuid.uuid4()
        pattern = f"sensors-{base_id}/#"
        received: list = []

        await nexo.pubsub(pattern).subscribe(lambda data: received.append(data))

        await nexo.pubsub(f"sensors-{base_id}/main").publish({"id": "root"})
        await nexo.pubsub(f"sensors-{base_id}/a/b/c").publish({"id": "deep"})
        await nexo.pubsub(f"other-{base_id}/main").publish({"id": "fail-prefix"})

        await wait_for(lambda: len(received) == 2)
        ids = sorted(r["id"] for r in received)
        assert ids == ["deep", "root"]
        await nexo.pubsub(pattern).unsubscribe()
