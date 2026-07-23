import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
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
            const q = await nexo.queue(qName).create();

            await q.push(binaryPayload);

            const received: any[] = [];
            const sub = await q.subscribe(msg => received.push(msg));

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            sub.stop();
        });

        it('PUBSUB: Should publish and subscribe raw Buffer', async () => {
            const topic = `bin-pubsub-${randomUUID()}`;
            const received: any[] = [];

            await nexo.pubsub(topic).subscribe(msg => received.push(msg));
            await nexo.pubsub(topic).publish(binaryPayload);

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
        });

        it('STREAM: Should stream raw Buffer', async () => {
            const topic = `bin-stream-${randomUUID()}`;
            await nexo.stream(topic).create();

            await nexo.stream(topic).publish(binaryPayload);

            const received: any[] = [];
            const sub = await nexo.stream(topic).subscribe('g1', msg => received.push(msg));

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            sub.stop();
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
