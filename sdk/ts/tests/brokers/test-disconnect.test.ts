import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../src/client';
import { ConnectionClosedError } from '../../src/errors';
import { randomUUID } from 'crypto';

describe('DISCONNECT', () => {
    it('should reject pending requests on disconnect', async () => {
        const client = await NexoClient.connect();
        const qName = `disconnect-pending-${randomUUID()}`;
        await client.queue(qName).create();

        // Consume with long waitMs → request is guaranteed in-flight
        // (no messages in queue, server holds the request for 5s)
        const conn = (client as any).conn;
        const consumePromise = conn.send(0x12, w => w.string(qName).u32(1).u32(5000), { timeoutMs: 10000 });

        // Let the request reach the server
        await new Promise(r => setTimeout(r, 100));

        // Disconnect while the request is pending
        client.disconnect();

        // Must reject with ConnectionClosedError, not hang
        await expect(consumePromise).rejects.toThrow('Connection closed');
    });

    it('should register and remove SIGINT/SIGTERM listeners per client', async () => {
        const beforeCount = process.listenerCount('SIGINT');

        const client = await NexoClient.connect();

        // Each client registers its own listener
        expect(process.listenerCount('SIGINT')).toBe(beforeCount + 1);
        expect(process.listenerCount('SIGTERM')).toBe(beforeCount + 1);

        client.disconnect();

        // disconnect() removes the listener — no leak
        expect(process.listenerCount('SIGINT')).toBe(beforeCount);
        expect(process.listenerCount('SIGTERM')).toBe(beforeCount);
    });

    it('should register listeners for multiple clients independently', async () => {
        const beforeCount = process.listenerCount('SIGINT');

        const clientA = await NexoClient.connect();
        const clientB = await NexoClient.connect();

        // Both clients registered their own listeners
        expect(process.listenerCount('SIGINT')).toBe(beforeCount + 2);

        // Disconnect clientA — only its listener is removed
        clientA.disconnect();
        expect(process.listenerCount('SIGINT')).toBe(beforeCount + 1);

        // ClientB still has its listener and working connection
        expect((clientB as any).conn.isConnected).toBe(true);

        clientB.disconnect();
        expect(process.listenerCount('SIGINT')).toBe(beforeCount);
    });
});
