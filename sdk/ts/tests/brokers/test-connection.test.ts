import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../src/client';
import { NexoConnection } from '../../src/transport/tcp/connection';
import { FrameWriter } from '../../src/protocol/codec';
import { FrameType, HEADER_SIZE } from '../../src/protocol/generated';
import { DEFAULT_CONFIG } from '../../src/config';
import { Logger } from '../../src/utils/logger';
import { randomUUID } from 'crypto';

describe('CONNECTION', () => {
    // =========================================
    // TEST 1: Request timeout (sweep interval)
    // =========================================
    it('should reject with RequestTimeoutError when server does not respond in time', async () => {
        const client = await NexoClient.connect();
        const qName = `timeout-test-${randomUUID()}`;
        await client.queue.create(qName);

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
        await client.queue.create(qName);

        // Push a message and consume it to get a valid ID
        const queue = await client.queue.get(qName);
        await queue.push('test-data');
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

describe('receive buffer ownership', () => {
    const makeConnection = () => new NexoConnection(
        { host: '127.0.0.1', port: 7654, ...DEFAULT_CONFIG.connection },
        new Logger({ level: 'OFF' }),
    );

    const pushFrame = (rawPayload: Buffer) =>
        new FrameWriter().begin().string('ownership/topic').any(rawPayload).finish(0, 0, FrameType.PUSH_PUBSUB);

    const feed = (conn: NexoConnection, chunk: Buffer) => {
        (conn as any).chunks.push(chunk);
        (conn as any).processBuffer();
    };

    const expectReleased = (conn: NexoConnection) => {
        expect((conn as any).buffer.length).toBe(0);
        expect((conn as any).buffer.buffer.byteLength).toBe(0);
    };

    it('releases backing storage after a complete frame from a large pool buffer', async () => {
        const conn = makeConnection();
        try {
            const rawPayload = Buffer.from('payload-bytes');
            const frame = pushFrame(rawPayload);
            const backing = Buffer.allocUnsafeSlow(65536);
            frame.copy(backing, 0);
            const received: Buffer[] = [];
            conn.onPush = (_topic, data) => received.push(data);

            feed(conn, backing.subarray(0, frame.length));

            expect(received).toHaveLength(1);
            expect(received[0].equals(rawPayload)).toBe(true);
            expectReleased(conn);
        } finally {
            conn.disconnect();
        }
    });

    it('releases backing storage after two coalesced frames', async () => {
        const conn = makeConnection();
        try {
            const p1 = Buffer.from('first');
            const p2 = Buffer.from('second');
            const received: Buffer[] = [];
            conn.onPush = (_topic, data) => received.push(data);

            feed(conn, Buffer.concat([pushFrame(p1), pushFrame(p2)]));

            expect(received.map(b => b.toString())).toEqual(['first', 'second']);
            expectReleased(conn);
        } finally {
            conn.disconnect();
        }
    });

    it('keeps the unconsumed tail of a partial frame, then releases storage once it completes', async () => {
        const conn = makeConnection();
        try {
            const p1 = Buffer.from('first');
            const p2 = Buffer.from('second-payload');
            const f1 = pushFrame(p1);
            const f2 = pushFrame(p2);
            const received: Buffer[] = [];
            conn.onPush = (_topic, data) => received.push(data);

            const splitAt = f2.length - 3;
            feed(conn, Buffer.concat([f1, f2.subarray(0, splitAt)]));

            expect(received).toHaveLength(1);
            expect(received[0].equals(p1)).toBe(true);
            expect((conn as any).buffer.length).toBe(splitAt);
            expect((conn as any).buffer.equals(f2.subarray(0, splitAt))).toBe(true);

            feed(conn, f2.subarray(splitAt));

            expect(received).toHaveLength(2);
            expect(received[1].equals(p2)).toBe(true);
            expect(received[0].equals(p1)).toBe(true);
            expectReleased(conn);
        } finally {
            conn.disconnect();
        }
    });

    it.each([1, HEADER_SIZE - 1, HEADER_SIZE + 2, -1])('delivers a fragmented frame exactly once and releases storage (split at %s)', async (splitAt) => {
        const conn = makeConnection();
        try {
            const rawPayload = Buffer.from('fragmented-payload');
            const frame = pushFrame(rawPayload);
            const cut = splitAt < 0 ? frame.length - 1 : splitAt;
            const received: Buffer[] = [];
            conn.onPush = (_topic, data) => received.push(data);

            feed(conn, frame.subarray(0, cut));
            expect(received).toHaveLength(0);

            feed(conn, frame.subarray(cut));
            expect(received).toHaveLength(1);
            expect(received[0].equals(rawPayload)).toBe(true);
            expectReleased(conn);
        } finally {
            conn.disconnect();
        }
    });
});
