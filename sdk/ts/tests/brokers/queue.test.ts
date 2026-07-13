import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('QUEUE', () => {
    it('should reject batchSize=0 in subscribe', async () => {
        const qName = `queue-batch-zero-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        await expect(q.subscribe(async () => {}, { batchSize: 0 })).rejects.toThrow(/batchSize must be >= 1/);
        await q.delete();
    });

    it('should reject concurrency=0 in subscribe', async () => {
        const qName = `queue-conc-zero-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        await expect(q.subscribe(async () => {}, { concurrency: 0 })).rejects.toThrow(/concurrency must be >= 1/);
        await q.delete();
    });

    it('should not DLQ messages on graceful shutdown (requeue via visibility timeout)', async () => {
        const qName = `queue-shutdown-${randomUUID()}`;
        const q = await nexo.queue(qName).create({
            visibilityTimeoutMs: 500,
            maxRetries: 3,
        });

        await q.push('msg1');
        await q.push('msg2');

        // Subscribe with slow callback + batchSize=2 + concurrency=1
        // Both messages are consumed (in-flight) but only msg1 starts processing
        const sub = await q.subscribe(async () => {
            await new Promise(r => setTimeout(r, 1000));
        }, { batchSize: 2, waitMs: 100, concurrency: 1 });

        // Wait for both messages to be consumed and msg1 to start processing
        await new Promise(r => setTimeout(r, 200));

        // Graceful stop — waits for msg1 callback to finish, msg2 is skipped (no nack)
        await sub.stop();

        // By now visibility timeout (500ms) has expired for msg2, server requeued it
        // maxRetries=3, attempts=1 → 1 < 3 → requeue, NOT DLQ
        const dlqResult = await q.dlq.peek(10);
        expect(dlqResult.total).toBe(0);

        // msg2 should be re-delivered to a new consumer
        const received: string[] = [];
        const sub2 = await q.subscribe(async (data) => {
            received.push(data);
        }, { batchSize: 5, waitMs: 500, concurrency: 1 });

        await waitFor(() => expect(received).toContain('msg2'));
        sub2.stop();

        await q.delete();
    });

    it('should stop consumer when queue is deleted during subscribe', async () => {
        const qName = `queue-deleted-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        await q.push('msg1');

        const sub = await q.subscribe(async () => {
            await new Promise(r => setTimeout(r, 500));
        }, { batchSize: 1, waitMs: 500, concurrency: 1 });

        // Wait for consumer to be active
        await new Promise(r => setTimeout(r, 200));

        // Delete queue while consumer is running
        await q.delete();

        // Wait for consumer to detect the error and break
        await new Promise(r => setTimeout(r, 2000));

        // Consumer should have stopped — no infinite loop
        // We verify by checking that no errors are thrown (loop exited cleanly)
        sub.stop();
    });

    it('should stop consumer when subscribing to non-existent queue', async () => {
        const qName = `queue-nonexist-${randomUUID()}`;
        const q = nexo.queue(qName); // not created

        // subscribe should not throw "Queue not found" — consumer loop handles it
        const sub = await q.subscribe(async () => {}, { batchSize: 1, waitMs: 100, concurrency: 1 });

        // Wait for consumer to detect "not found" error and break
        await new Promise(r => setTimeout(r, 500));

        // Consumer should have stopped gracefully
        sub.stop();
    });

    it('should handle full lifecycle: Push -> Subscribe -> Ack', async () => {
        const qName = `queue-life-${randomUUID()}`;
        const q = await nexo.queue(qName).create();
        const payload = { task: 'process_me' };

        let received: any = null;
        const sub = await q.subscribe(async (data) => {
            received = data;
            // Implicit Ack when function returns
        });

        await q.push(payload);

        await waitFor(() => expect(received).toEqual(payload));
        sub.stop();
    });

    it('should move failed messages to DLQ', async () => {
        const qName = `queue-dlq-${randomUUID()}`;
        // Max 1 retry (2 attempts total)
        const q = await nexo.queue(qName).create({ maxRetries: 1, visibilityTimeoutMs: 100 });

        await q.push('fail_payload');

        // Consumer that always fails
        const sub = await q.subscribe(async () => {
            throw new Error("Simulated Failure");
        });

        // Wait for retries to exhaust
        await new Promise(r => setTimeout(r, 1000));
        sub.stop();

        // Check DLQ using peek
        const dlqResult = await q.dlq.peek(10);
        expect(dlqResult.total).toBe(1);
        expect(dlqResult.items[0].data).toBe('fail_payload');
    });

    it('should respect priority (High before Low)', async () => {
        const qName = `queue-prio-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        // Push Low then High
        await q.push('low', { priority: 0 });
        await q.push('high', { priority: 10 });

        const received: string[] = [];
        // Concurrency 1 to force ordering check
        const sub = await q.subscribe(async (msg) => {
            received.push(msg);
        }, { concurrency: 1 });

        await waitFor(() => expect(received.length).toBe(2));
        sub.stop();

        expect(received).toEqual(['high', 'low']);
    });

    it('Should handle DLQ workflow: peek, moveToQueue, delete, purge', async () => {
        const qName = `dlq-test-${randomUUID()}`;

        const q = await nexo.queue(qName).create({
            visibilityTimeoutMs: 5000, 
            maxRetries: 1,
        });

        // Push 3 messages
        await q.push({ order: 'order1' });
        await q.push({ order: 'order2' });
        await q.push({ order: 'order3' });

        // Consume messages but don't ack (simulate failure)
        const OLD_CONSUME_WAIT_MS = 100;

        const received: any[] = [];
        const sub = await q.subscribe(async (msg) => {
            received.push(msg);
            throw new Error('Simulated processing error');
        }, { batchSize: 10, waitMs: OLD_CONSUME_WAIT_MS });

        await waitFor(() => expect(received.length).toBe(3));
        sub.stop();

        // Wait for old consume(waitMs=100) to expire on server before proceeding.
        // Calculated delay: waitMs + margin. Not arbitrary — prevents old waiter from
        // stealing the replayed message and NACKing it back to DLQ.
        await new Promise(r => setTimeout(r, OLD_CONSUME_WAIT_MS + 100));

        // 1. Peek DLQ - should have 3 messages
        let dlqResult;
        await waitFor(async () => {
            dlqResult = await q.dlq.peek(10);
            expect(dlqResult.total).toBe(3);
            expect(dlqResult.items.length).toBe(3);
        });
        
        expect(dlqResult.items[0].attempts).toBeGreaterThanOrEqual(1);

        const targetMsg = dlqResult.items.find((i: any) => i.data.order === 'order3');
        const otherMsg = dlqResult.items.find((i: any) => i.data.order === 'order2');
        
        expect(targetMsg).toBeDefined();
        expect(otherMsg).toBeDefined();

        const msgToReplayId = targetMsg!.id;
        const msgToDeleteId = otherMsg!.id;

        // 2. Move message back to main queue FIRST
        const moved = await q.dlq.moveToQueue(msgToReplayId);
        expect(moved).toBe(true);

        // 3. Subscribe AFTER move (message is already Ready in queue)
        const replayed: any[] = [];
        const sub2 = await nexo.queue(qName).subscribe(async (msg) => {
            replayed.push(msg);
        }, { batchSize: 1, waitMs: 100, concurrency: 1 });

        // 4. Verify it's received
        await waitFor(() => expect(replayed.length).toBe(1));
        expect(replayed[0].order).toBe('order3');
        sub2.stop();

        // 3. Delete one specific message from DLQ
        const deleted = await q.dlq.delete(msgToDeleteId);
        expect(deleted).toBe(true);

        const dlqAfterDelete = await q.dlq.peek(10);
        expect(dlqAfterDelete.total).toBe(1);
        expect(dlqAfterDelete.items.length).toBe(1);

        // 4. Purge all remaining messages
        const purgedCount = await q.dlq.purge();
        expect(purgedCount).toBe(1);

        const dlqAfterPurge = await q.dlq.peek(10);
        expect(dlqAfterPurge.total).toBe(0);
        expect(dlqAfterPurge.items.length).toBe(0);

        await q.delete();
    });

    it('should serialize callbacks with concurrency=1', async () => {
        const qName = `queue-conc1-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        const COUNT = 10;
        const CALLBACK_DELAY = 50;

        let inFlight = 0;
        let maxInFlight = 0;
        const received: number[] = [];

        const sub = await q.subscribe(async (msg: any) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise(r => setTimeout(r, CALLBACK_DELAY));
            received.push(msg.i);
            inFlight--;
        }, { batchSize: COUNT, concurrency: 1 });

        for (let i = 0; i < COUNT; i++) await q.push({ i });

        await waitFor(() => expect(received.length).toBe(COUNT));
        sub.stop();

        expect(maxInFlight).toBe(1);
        expect(new Set(received).size).toBe(COUNT);
    });

    it('should process messages in parallel with concurrency > 1', async () => {
        const qName = `queue-conc-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        const COUNT = 20;
        const CALLBACK_DELAY = 100;
        const CONCURRENCY = 10;

        let inFlight = 0;
        let maxInFlight = 0;
        const received: number[] = [];

        const sub = await q.subscribe(async (msg: any) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise(r => setTimeout(r, CALLBACK_DELAY));
            received.push(msg.i);
            inFlight--;
        }, { batchSize: COUNT, concurrency: CONCURRENCY });

        for (let i = 0; i < COUNT; i++) await q.push({ i });

        const start = Date.now();
        await waitFor(() => expect(received.length).toBe(COUNT), { timeout: 10000 });
        const elapsed = Date.now() - start;

        sub.stop();

        expect(new Set(received).size).toBe(COUNT);
        expect(maxInFlight).toBeGreaterThan(1);
        expect(elapsed).toBeLessThan(COUNT * CALLBACK_DELAY * 0.6);
    });

    it('should allow multiple parallel subscribers on the same queue (in-process scaling)', async () => {
        const qName = `queue-multi-sub-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        const COUNT = 20;
        const received: number[] = [];

        // Two independent consume loops on the same queue handle.
        // The server reserves each msg via visibility timeout so it can never be delivered twice.
        // batchSize=1 forces per-message reservation so either loop can pick up any message.
        const subA = await q.subscribe(async (msg: any) => { received.push(msg.i); }, { batchSize: 1, waitMs: 200, concurrency: 1 });
        const subB = await q.subscribe(async (msg: any) => { received.push(msg.i); }, { batchSize: 1, waitMs: 200, concurrency: 1 });

        for (let i = 0; i < COUNT; i++) await q.push({ i });

        await waitFor(() => expect(received.length).toBe(COUNT));
        subA.stop();
        subB.stop();

        // Every message processed exactly once across both subscribers (no duplication).
        const sorted = [...received].sort((a, b) => a - b);
        expect(sorted).toEqual(Array.from({ length: COUNT }, (_, i) => i));
    });

    it('Should handle explicit NACK and persist failure reason in DLQ', async () => {
        const qName = `nack-reason-${randomUUID()}`;
        const q = await nexo.queue(qName).create({
            maxRetries: 0,
            visibilityTimeoutMs: 10000
        });

        await q.push({ task: 'fail_me' });

        // Consumer throws error
        const sub = await q.subscribe(async () => {
            throw new Error("Specific Failure Reason");
        });

        await new Promise(r => setTimeout(r, 500));
        sub.stop();

        // Check DLQ
        const dlqResult = await q.dlq.peek(10);
        expect(dlqResult.total).toBe(1);
        expect(dlqResult.items[0].failureReason).toBe("Specific Failure Reason");
    });

    it('should push batch of messages', async () => {
        const qName = `batch-push-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        await q.pushBatch([
            { data: 'msg1' },
            { data: 'msg2' },
            { data: 'msg3' },
        ]);

        const received: string[] = [];
        const sub = await q.subscribe(async (data) => {
            received.push(data);
        }, { batchSize: 10, waitMs: 500, concurrency: 1 });

        await waitFor(() => expect(received.length).toBe(3));
        sub.stop();
        await q.delete();
    });

    it('should push batch with mixed priorities', async () => {
        const qName = `batch-prio-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        await q.pushBatch([
            { data: 'low', options: { priority: 0 } },
            { data: 'high', options: { priority: 10 } },
            { data: 'mid', options: { priority: 5 } },
        ]);

        const received: string[] = [];
        const sub = await q.subscribe(async (data) => {
            received.push(data);
        }, { batchSize: 3, waitMs: 500, concurrency: 1 });

        await waitFor(() => expect(received.length).toBe(3));
        expect(received[0]).toBe('high');
        expect(received[1]).toBe('mid');
        expect(received[2]).toBe('low');
        sub.stop();
        await q.delete();
    });

    it('should handle empty pushBatch gracefully', async () => {
        const qName = `batch-empty-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        await q.pushBatch([]);
        await q.delete();
    });
});
