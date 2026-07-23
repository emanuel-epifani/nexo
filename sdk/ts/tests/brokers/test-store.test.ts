import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { randomUUID } from 'crypto';

describe('STORE (KV)', () => {
    it('should perform basic CRUD operations', async () => {
        const key = `crud:${randomUUID()}`;
        const value = 'persistent_value';

        // Set
        await nexo.store.map.set(key, value);

        // Get
        const result = await nexo.store.map.get(key);
        expect(result).toBe(value);

        // Delete
        await nexo.store.map.del(key);

        // Should be gone
        expect(await nexo.store.map.get(key)).toBeNull();
    });

    it('should expire keys after TTL', async () => {
        const key = `ttl:${randomUUID()}`;
        // Set with 1s TTL
        await nexo.store.map.set(key, 'temp', { ttl: 1 });

        // Should exist immediately
        expect(await nexo.store.map.get(key)).toBe('temp');

        // Wait for expiration (1s + buffer)
        await new Promise(r => setTimeout(r, 1200));

        // Should be gone
        expect(await nexo.store.map.get(key)).toBeNull();
    });

    it('should reject ttl: 0 with an error', async () => {
        const key = `ttl0:${randomUUID()}`;
        await expect(nexo.store.map.set(key, 'val', { ttl: 0 })).rejects.toThrow();
        expect(await nexo.store.map.get(key)).toBeNull();
    });

    it('should persist keys without TTL', async () => {
        const key = `persist:${randomUUID()}`;
        await nexo.store.map.set(key, 'forever');

        // Should still exist after a short wait
        await new Promise(r => setTimeout(r, 200));
        expect(await nexo.store.map.get(key)).toBe('forever');
    });

    // ── Edge cases ──────────────────────────────────────────────

    it('should return null for get on non-existent key', async () => {
        const key = `missing:${randomUUID()}`;
        expect(await nexo.store.map.get(key)).toBeNull();
    });

    it('should succeed del on non-existent key (idempotent)', async () => {
        const key = `del-missing:${randomUUID()}`;
        await nexo.store.map.del(key);
        expect(await nexo.store.map.get(key)).toBeNull();
    });

    it('should overwrite existing key with new value', async () => {
        const key = `overwrite:${randomUUID()}`;
        await nexo.store.map.set(key, 'first');
        expect(await nexo.store.map.get(key)).toBe('first');

        await nexo.store.map.set(key, 'second');
        expect(await nexo.store.map.get(key)).toBe('second');

        await nexo.store.map.del(key);
    });

    it('should handle large values (1MB)', async () => {
        const key = `large:${randomUUID()}`;
        const largeValue = 'x'.repeat(1024 * 1024);
        await nexo.store.map.set(key, largeValue);
        const result = await nexo.store.map.get(key);
        expect(result).toBe(largeValue);
        await nexo.store.map.del(key);
    });
});
