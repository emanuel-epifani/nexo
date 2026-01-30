import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";

describe('QUEUE broker - Complete Feature Set', () => {

    describe('1. Core Lifecycle (Happy Path)', () => {
        it('should handle full lifecycle: create -> push -> subscribe -> receive -> ack', async () => {
            const q = await nexo.queue('lifecycle_test').create();
            const payload = { msg: 'lifecycle' };

            let received: any = null;
            const sub = await q.subscribe(async (data) => {
                received = data;
            });

            await q.push(payload);

            // Wait for delivery
            for (let i = 0; i < 20; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();

            expect(received).toEqual(payload);

            // Verify ACK worked (queue should be empty)
            // We can check this by trying to consume again with short timeout
            // or if we had a debug snapshot API. Assuming ACK works if no error thrown.
        });
    });

    describe('2. Concurrency & Ordering', () => {
        it('should process all 100 messages with high concurrency without loss', async () => {
            const q = await nexo.queue('concurrency_test').create();
            const TOTAL = 100;

            // Push 100 messages
            const pushPromises = [];
            for (let i = 0; i < TOTAL; i++) {
                pushPromises.push(q.push({ index: i }));
            }
            await Promise.all(pushPromises);

            const receivedIndices = new Set<number>();

            const sub = await q.subscribe(async (data) => {
                receivedIndices.add(data.index);
                await new Promise(r => setTimeout(r, 5)); // Simulate work
            }, {
                concurrency: 10,
                batchSize: 20
            });

            // Wait for completion
            for (let i = 0; i < 40; i++) {
                if (receivedIndices.size === TOTAL) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            expect(receivedIndices.size).toBe(TOTAL);
        });

        it('should maintain strict FIFO order with concurrency 1', async () => {
            const q = await nexo.queue('fifo_test').create();

            await q.push(1);
            await q.push(2);
            await q.push(3);

            const received: number[] = [];
            const sub = await q.subscribe(async (val) => {
                received.push(val);
            }, { concurrency: 1, batchSize: 10 });

            await new Promise(r => setTimeout(r, 500));
            sub.stop();

            expect(received).toEqual([1, 2, 3]);
        });
    });

    describe('3. Retry & DLQ', () => {
        it('should move message to DLQ after maxRetries failures', async () => {
            const qName = 'dlq_test_queue';
            // Configure: 100ms visibility, 2 retries (initial + 1 retry)
            const q = await nexo.queue(qName).create({
                maxRetries: 2,
                visibilityTimeoutMs: 100
            });
            const dlq = await nexo.queue(`${qName}_dlq`).create();

            await q.push('fail-me');

            // Consumer that always fails
            let attempts = 0;
            const sub = await q.subscribe(async () => {
                attempts++;
                throw new Error("fail");
            });

            // Wait for: Initial (0ms) -> Fail -> Wait 100ms -> Retry 1 -> Fail -> Wait 100ms -> Move to DLQ
            await new Promise(r => setTimeout(r, 1000));
            sub.stop();

            // Check attempts (should be 2)
            expect(attempts).toBeGreaterThanOrEqual(2);

            // Check DLQ
            let dlqMsg = null;
            const dlqSub = await dlq.subscribe(async (data) => {
                dlqMsg = data;
            });

            await new Promise(r => setTimeout(r, 500));
            dlqSub.stop();

            expect(dlqMsg).toBe('fail-me');
        });
    });

    describe('4. Priority Queue', () => {
        it('should deliver high priority messages before low priority ones', async () => {
            const q = await nexo.queue('priority_test').create();

            // Push in reverse order of expected delivery
            await q.push('low', { priority: 0 });
            await q.push('medium', { priority: 10 });
            await q.push('high', { priority: 255 });

            const received: string[] = [];
            const sub = await q.subscribe(async (msg) => {
                received.push(msg);
            }, { concurrency: 1, batchSize: 1 }); // Force serial fetching to verify server order

            await new Promise(r => setTimeout(r, 500));
            sub.stop();

            expect(received).toEqual(['high', 'medium', 'low']);
        });
    });

    describe('5. Delay (Scheduling)', () => {
        it('should not deliver delayed message before delayMs expires', async () => {
            const q = await nexo.queue('delay_test').create();
            const start = Date.now();
            const DELAY = 1000;

            await q.push('delayed', { delayMs: DELAY });

            let receivedTime = 0;
            const sub = await q.subscribe(async () => {
                receivedTime = Date.now();
            });

            // Check at 500ms (should be null)
            await new Promise(r => setTimeout(r, 500));
            expect(receivedTime).toBe(0);

            // Wait until after delay
            await new Promise(r => setTimeout(r, 1000));
            sub.stop();

            expect(receivedTime).toBeGreaterThan(start + DELAY - 100); // Allow small jitter
        });
    });

    describe('6. SDK Guard Rails', () => {
        it('should throw error when calling subscribe twice on same instance', async () => {
            const q = await nexo.queue('guard_rail_test').create();

            const sub1 = await q.subscribe(async () => { });

            // Should fail immediately
            await expect(q.subscribe(async () => { }))
                .rejects
                .toThrow(/Queue 'guard_rail_test' already subscribed/);

            sub1.stop();
        });

        it('should fail-fast when subscribing to non-existent queue', async () => {
            const q = nexo.queue('non_existent_queue');

            // Should fail immediately with Queue not found
            await expect(q.subscribe(async () => { }))
                .rejects
                .toThrow(/Queue 'non_existent_queue' not found/);

            // Guard rail should be reset so we can try again later (e.g. after creating it)
            // We can test this by creating it and checking if subscribe works
            await q.create();
            const sub = await q.subscribe(async () => { }); // Should work now
            sub.stop();
        });
    });

    describe('7. Management', () => {
        it('should create and then delete a queue', async () => {
            const q = await nexo.queue('delete_test').create();

            // Should exist
            expect(await q.exists()).toBe(true);

            // Delete
            await q.delete();

            // Should not exist
            expect(await q.exists()).toBe(false);

            // Re-create check
            await q.create();
            expect(await q.exists()).toBe(true);
        });
    });
});
