import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('QUEUE', () => {
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
            persistence: 'file_sync'
        });

        // Push 3 messages
        await q.push({ order: 'order1' });
        await q.push({ order: 'order2' });
        await q.push({ order: 'order3' });

        // Consume messages but don't ack (simulate failure)
        const received: any[] = [];
        const sub = await q.subscribe(async (msg) => {
            received.push(msg);
            throw new Error('Simulated processing error');
        }, { batchSize: 10, waitMs: 100 });

        await waitFor(() => expect(received.length).toBe(3));
        sub.stop();

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

        // 2. Move one message back to main queue (replay)
        const moved = await q.dlq.moveToQueue(msgToReplayId);
        expect(moved).toBe(true);

        // Verify it's back in main queue
        const replayed: any[] = [];
        const sub2 = await nexo.queue(qName).subscribe(async (msg) => {
            replayed.push(msg);
        }, { batchSize: 1, waitMs: 200, concurrency: 1 });

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
});
