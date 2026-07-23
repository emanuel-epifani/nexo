import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../src/client';
import { randomUUID } from 'crypto';

describe('CONNECTION', () => {
    // =========================================
    // TEST 1: Request timeout (sweep interval)
    // =========================================
    it('should reject with RequestTimeoutError when server does not respond in time', async () => {
        const client = await NexoClient.connect();
        const qName = `timeout-test-${randomUUID()}`;
        await client.queue(qName).create();

        // Consume with waitMs=10000 but timeoutMs=300 — server holds the request
        // for 10s, but the sweep interval must reject after 300ms
        const conn = (client as any).conn;
        const consumePromise = conn.send(0x12, w => w.string(qName).u32(1).u32(10000), { timeoutMs: 300 });

        const start = Date.now();
        await expect(consumePromise).rejects.toThrow(/Request timeout after 300ms/);
        const elapsed = Date.now() - start;

        // Should reject around 300ms + sweep interval (1s), not 10s
        expect(elapsed).toBeLessThan(3000);

        client.disconnect();
    });

    // =========================================
    // TEST 2: sendFireAndForget during disconnect
    // =========================================
    it('sendFireAndForget should silently return when disconnected (no throw)', async () => {
        const client = await NexoClient.connect();
        const qName = `fire-forget-${randomUUID()}`;
        await client.queue(qName).create();

        // Push a message and consume it to get a valid ID
        await client.queue(qName).push('test-data');
        const conn = (client as any).conn;
        const consumeRes = await conn.send(0x12, w => w.string(qName).u32(1).u32(1000));
        const msgId = consumeRes.cursor.readUUID();

        // Disconnect
        client.disconnect();
        expect(conn.isConnected).toBe(false);

        // sendFireAndForget must not throw — just return silently
        expect(() => {
            conn.sendFireAndForget(0x13, w => w.uuid(msgId).string(qName));
        }).not.toThrow();
    });
});
