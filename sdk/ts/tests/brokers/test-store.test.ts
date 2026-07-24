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

    // ── INCR ───────────────────────────────────────────────────

    it('should increment a new key from 0', async () => {
        const key = `incr:new:${randomUUID()}`;
        const result = await nexo.store.map.incr(key);
        expect(result).toBe(1);
        await nexo.store.map.del(key);
    });

    it('should increment an existing integer value', async () => {
        const key = `incr:existing:${randomUUID()}`;
        await nexo.store.map.set(key, 10);
        const result = await nexo.store.map.incr(key, 5);
        expect(result).toBe(15);
        await nexo.store.map.del(key);
    });

    it('should decrement with negative delta', async () => {
        const key = `incr:neg:${randomUUID()}`;
        await nexo.store.map.set(key, 10);
        const result = await nexo.store.map.incr(key, -3);
        expect(result).toBe(7);
        await nexo.store.map.del(key);
    });

    it('should error on non-integer value', async () => {
        const key = `incr:str:${randomUUID()}`;
        await nexo.store.map.set(key, 'hello');
        await expect(nexo.store.map.incr(key, 1)).rejects.toThrow();
        await nexo.store.map.del(key);
    });

    it('should preserve TTL after incr', async () => {
        const key = `incr:ttl:${randomUUID()}`;
        await nexo.store.map.set(key, 5, { ttl: 60 });
        await nexo.store.map.incr(key, 1);
        // Should still exist after short wait (TTL=60)
        await new Promise(r => setTimeout(r, 200));
        expect(await nexo.store.map.get(key)).toBe(6);
        await nexo.store.map.del(key);
    });

    it('should handle negative delta on new key', async () => {
        const key = `incr:negnew:${randomUUID()}`;
        const result = await nexo.store.map.incr(key, -5);
        expect(result).toBe(-5);
        await nexo.store.map.del(key);
    });

    it('should return number from get after incr on new key', async () => {
        const key = `incr:gettype:${randomUUID()}`;
        await nexo.store.map.incr(key, 42);
        const result = await nexo.store.map.get(key);
        expect(result).toBe(42);
        expect(typeof result).toBe('number');
        await nexo.store.map.del(key);
    });

    // ── CLEAR ──────────────────────────────────────────────────

    it('should clear all keys and return count', async () => {
        const prefix = `clearall:${randomUUID()}:`;
        await nexo.store.map.set(`${prefix}a`, '1');
        await nexo.store.map.set(`${prefix}b`, '2');
        await nexo.store.map.set(`${prefix}c`, '3');

        const count = await nexo.store.map.clearAll();
        expect(count).toBeGreaterThanOrEqual(3);

        expect(await nexo.store.map.get(`${prefix}a`)).toBeNull();
        expect(await nexo.store.map.get(`${prefix}b`)).toBeNull();
        expect(await nexo.store.map.get(`${prefix}c`)).toBeNull();
    });

    it('should clear only matching prefix and return count', async () => {
        const prefix = `clearprefix:${randomUUID()}:`;
        const otherKey = `other:${randomUUID()}`;
        await nexo.store.map.set(`${prefix}a`, '1');
        await nexo.store.map.set(`${prefix}b`, '2');
        await nexo.store.map.set(otherKey, 'keep');

        const count = await nexo.store.map.clearWithPrefix(prefix);
        expect(count).toBe(2);

        expect(await nexo.store.map.get(`${prefix}a`)).toBeNull();
        expect(await nexo.store.map.get(`${prefix}b`)).toBeNull();
        expect(await nexo.store.map.get(otherKey)).toBe('keep');
        await nexo.store.map.del(otherKey);
    });

    it('should return 0 when clearWithPrefix matches nothing', async () => {
        const count = await nexo.store.map.clearWithPrefix(`nomatch:${randomUUID()}`);
        expect(count).toBe(0);
    });
});
