from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient


@pytest.mark.asyncio
class TestStore:
    async def test_basic_crud(self, nexo: NexoClient):
        key = f"crud:{uuid.uuid4()}"
        value = "persistent_value"

        await nexo.store.map.set(key, value)
        result = await nexo.store.map.get(key)
        assert result == value

        await nexo.store.map.delete(key)
        assert await nexo.store.map.get(key) is None

    async def test_ttl_expiration(self, nexo: NexoClient):
        key = f"ttl:{uuid.uuid4()}"
        await nexo.store.map.set(key, "temp", {"ttl": 1})

        assert await nexo.store.map.get(key) == "temp"

        await asyncio.sleep(1.2)

        assert await nexo.store.map.get(key) is None

    async def test_ttl_zero_is_error(self, nexo: NexoClient):
        key = f"ttl0:{uuid.uuid4()}"
        with pytest.raises(Exception):
            await nexo.store.map.set(key, "val", {"ttl": 0})
        assert await nexo.store.map.get(key) is None

    async def test_no_ttl_is_persistent(self, nexo: NexoClient):
        key = f"persist:{uuid.uuid4()}"
        await nexo.store.map.set(key, "forever")

        await asyncio.sleep(0.2)
        assert await nexo.store.map.get(key) == "forever"

    # ── Edge cases ──────────────────────────────────────────────

    async def test_get_nonexistent_returns_none(self, nexo: NexoClient):
        key = f"missing:{uuid.uuid4()}"
        assert await nexo.store.map.get(key) is None

    async def test_del_nonexistent_is_idempotent(self, nexo: NexoClient):
        key = f"del-missing:{uuid.uuid4()}"
        await nexo.store.map.delete(key)
        assert await nexo.store.map.get(key) is None

    async def test_overwrite_existing_key(self, nexo: NexoClient):
        key = f"overwrite:{uuid.uuid4()}"
        await nexo.store.map.set(key, "first")
        assert await nexo.store.map.get(key) == "first"

        await nexo.store.map.set(key, "second")
        assert await nexo.store.map.get(key) == "second"

        await nexo.store.map.delete(key)

    async def test_large_value_1mb(self, nexo: NexoClient):
        key = f"large:{uuid.uuid4()}"
        large_value = "x" * (1024 * 1024)
        await nexo.store.map.set(key, large_value)
        result = await nexo.store.map.get(key)
        assert result == large_value
        await nexo.store.map.delete(key)
