import { describe, it, expect } from 'vitest';
import { createQueue, createStream, nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('CROSS-BROKER FEATURES', () => {
    describe('BINARY PAYLOAD SUPPORT', () => {
        const binaryPayload = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]);

        it('STORE: Should store and retrieve raw Buffer', async () => {
            const key = `bin-store-${randomUUID()}`;
            await nexo.store.map.set(key, binaryPayload);

            const retrieved = await nexo.store.map.get(key);

            expect(Buffer.isBuffer(retrieved)).toBe(true);
            expect(retrieved.equals(binaryPayload)).toBe(true);
        });

        it('QUEUE: Should push and pop raw Buffer', async () => {
            const qName = `bin-queue-${randomUUID()}`;
            const q = await createQueue(qName);

            await q.push(binaryPayload);

            const received: any[] = [];
            const sub = await q.subscribe(msg => received.push(msg));

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            await sub.stop();
        });

        it('PUBSUB: Should publish and subscribe raw Buffer', async () => {
            const topic = `bin-pubsub-${randomUUID()}`;
            const received: any[] = [];

            const pubsubTopic = nexo.pubsub.topic(topic);
            const pubsubSub = await pubsubTopic.subscribe(msg => received.push(msg));
            await pubsubTopic.publish(binaryPayload);

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            await pubsubSub.stop();
        });

        it('STREAM: Should stream raw Buffer', async () => {
            const topic = `bin-stream-${randomUUID()}`;
            const stream = await createStream(topic);

            await stream.publish(binaryPayload);

            const received: any[] = [];
            const sub = await stream.group('g1').subscribe(msg => received.push(msg));

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            await sub.stop();
        });
    });

    describe('SYSTEM & PROTOCOL', () => {
        it('should handle JSON serialization with special chars and nested objects', async () => {
            const complexData = {
                string: "Nexo Engine 🚀",
                number: 42.5,
                boolean: true,
                nullValue: null,
                nested: {
                    id: "abc-123",
                    meta: { active: true, deep: { value: "ok" } }
                },
                unicode: "こんにちは"
            };

            const key = `proto:complex:${randomUUID()}`;
            await nexo.store.map.set(key, complexData);
            const result = await nexo.store.map.get(key);

            expect(result).toEqual(complexData);
        });

        it('should distinguish between empty string and null', async () => {
            const keyEmpty = `proto:empty:${randomUUID()}`;
            const keyNull = `proto:null:${randomUUID()}`;

            await nexo.store.map.set(keyEmpty, '');
            await nexo.store.map.set(keyNull, null);

            expect(await nexo.store.map.get(keyEmpty)).toBe('');
            expect(await nexo.store.map.get(keyNull)).toBeNull();
        });
    });
});
