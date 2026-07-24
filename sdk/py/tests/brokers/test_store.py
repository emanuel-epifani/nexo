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

    # ── INCR ───────────────────────────────────────────────────

    async def test_incr_new_key_from_zero(self, nexo: NexoClient):
        key = f"incr:new:{uuid.uuid4()}"
        result = await nexo.store.map.incr(key)
        assert result == 1
        await nexo.store.map.delete(key)

    async def test_incr_existing_integer(self, nexo: NexoClient):
        key = f"incr:existing:{uuid.uuid4()}"
        await nexo.store.map.set(key, 10)
        result = await nexo.store.map.incr(key, 5)
        assert result == 15
        await nexo.store.map.delete(key)

    async def test_incr_negative_delta(self, nexo: NexoClient):
        key = f"incr:neg:{uuid.uuid4()}"
        await nexo.store.map.set(key, 10)
        result = await nexo.store.map.incr(key, -3)
        assert result == 7
        await nexo.store.map.delete(key)

    async def test_incr_non_integer_errors(self, nexo: NexoClient):
        key = f"incr:str:{uuid.uuid4()}"
        await nexo.store.map.set(key, "hello")
        with pytest.raises(Exception):
            await nexo.store.map.incr(key, 1)
        await nexo.store.map.delete(key)

    async def test_incr_preserves_ttl(self, nexo: NexoClient):
        key = f"incr:ttl:{uuid.uuid4()}"
        await nexo.store.map.set(key, 5, {"ttl": 60})
        await nexo.store.map.incr(key, 1)
        await asyncio.sleep(0.2)
        assert await nexo.store.map.get(key) == 6
        await nexo.store.map.delete(key)

    async def test_incr_negative_on_new_key(self, nexo: NexoClient):
        key = f"incr:negnew:{uuid.uuid4()}"
        result = await nexo.store.map.incr(key, -5)
        assert result == -5
        await nexo.store.map.delete(key)

    async def test_incr_get_returns_int(self, nexo: NexoClient):
        key = f"incr:gettype:{uuid.uuid4()}"
        await nexo.store.map.incr(key, 42)
        result = await nexo.store.map.get(key)
        assert result == 42
        assert isinstance(result, int)
        await nexo.store.map.delete(key)

    # ── CLEAR ───────────────────────────────────────────────────

    async def test_clear_all_removes_everything(self, nexo: NexoClient):
        prefix = f"clearall:{uuid.uuid4()}:"
        await nexo.store.map.set(f"{prefix}a", "1")
        await nexo.store.map.set(f"{prefix}b", "2")
        await nexo.store.map.set(f"{prefix}c", "3")

        count = await nexo.store.map.clear_all()
        assert count >= 3

        assert await nexo.store.map.get(f"{prefix}a") is None
        assert await nexo.store.map.get(f"{prefix}b") is None
        assert await nexo.store.map.get(f"{prefix}c") is None

    async def test_clear_with_prefix_removes_only_matching(self, nexo: NexoClient):
        prefix = f"clearprefix:{uuid.uuid4()}:"
        other_key = f"other:{uuid.uuid4()}"
        await nexo.store.map.set(f"{prefix}a", "1")
        await nexo.store.map.set(f"{prefix}b", "2")
        await nexo.store.map.set(other_key, "keep")

        count = await nexo.store.map.clear_with_prefix(prefix)
        assert count == 2

        assert await nexo.store.map.get(f"{prefix}a") is None
        assert await nexo.store.map.get(f"{prefix}b") is None
        assert await nexo.store.map.get(other_key) == "keep"
        await nexo.store.map.delete(other_key)

    async def test_clear_with_prefix_no_match_returns_zero(self, nexo: NexoClient):
        count = await nexo.store.map.clear_with_prefix(f"nomatch:{uuid.uuid4()}")
        assert count == 0
