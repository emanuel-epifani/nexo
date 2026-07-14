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
});
