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
});
