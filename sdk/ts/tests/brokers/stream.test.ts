import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { NexoClient } from '../../src/client';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('STREAM', () => {
    let clientA: NexoClient;
    let clientB: NexoClient;

    beforeAll(async () => {
        clientA = await NexoClient.connect();
        clientB = await NexoClient.connect();
    });

    afterAll(async () => {
        await Promise.allSettled([
            clientA?.disconnect(),
            clientB?.disconnect()
        ]);
    });

    it('should support Happy Path (Publish/Subscribe)', async () => {
        const topic = `stream-basic-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe('g1', (data) => received.push(data));

        await nexo.stream(topic).publish({ id: 1 });
        await nexo.stream(topic).publish({ id: 2 });

        await waitFor(() => expect(received.length).toBe(2));
        await sub.stop();
    });

    it('should fail subscribe when stream does not exist', async () => {
        const topic = `stream-missing-${randomUUID()}`;
        await expect(
            clientA.stream(topic).subscribe('missing-group', () => {
            })
        ).rejects.toThrow();
    });

    it('Independent CONSUMER GROUPS => should deliver all messages to each group', async () => {
        const topic = `stream-groups-${randomUUID()}`;
        await nexo.stream(topic).create();

        const recvA: any[] = [];
        const recvB: any[] = [];

        const subA = await clientA.stream(topic).subscribe('group_A', (d) => recvA.push(d));
        const subB = await clientB.stream(topic).subscribe('group_B', (d) => recvB.push(d));

        await nexo.stream(topic).publish({ msg: 'hello' });

        await waitFor(() => {
            expect(recvA.length).toBe(1);
            expect(recvB.length).toBe(1);
        });

        await subA.stop();
        await subB.stop();
    });

    it('Same CONSUMER GROUP => should distribute messages without duplicates', async () => {
        const topic = `parallel-consumers-${randomUUID()}`;
        const group = 'parallel_group';
        await nexo.stream(topic).create();

        const receivedA = new Set<number>();
        const receivedB = new Set<number>();

        // Start 2 consumers in same group
        const subA = await clientA.stream(topic).subscribe(group, (d) => {
            if (receivedA.has(d.id)) throw new Error(`Duplicate in A: ${d.id}`);
            receivedA.add(d.id);
        });

        const subB = await clientB.stream(topic).subscribe(group, (d) => {
            if (receivedB.has(d.id)) throw new Error(`Duplicate in B: ${d.id}`);
            receivedB.add(d.id);
        });

        // Wait for join
        await new Promise(r => setTimeout(r, 500));

        // Publish messages
        for (let i = 0; i < 100; i++) {
            await nexo.stream(topic).publish({ id: i });
        }

        await waitFor(() => expect(receivedA.size + receivedB.size).toBe(100));

        // Verify NO overlap
        const overlap = [...receivedA].filter(id => receivedB.has(id));
        expect(overlap.length).toBe(0);

        await subA.stop();
        await subB.stop();
    });

    it('should handle consumer disconnect with zero data loss', async () => {
        const tempClientA = await NexoClient.connect();
        const tempClientB = await NexoClient.connect();

        const topic = `stream-disconnect-${randomUUID()}`;
        const group = 'group_disconnect';
        await nexo.stream(topic).create();

        const producer = nexo.stream(topic);
        const allReceivedIds = new Set<number>();
        const track = (d: any) => allReceivedIds.add(d.i);

        // 1. Start A & B 
        const subA = await tempClientA.stream(topic).subscribe(group, track);
        const subB = await tempClientB.stream(topic).subscribe(group, track);

        // 2. Warm up (0-19)
        for (let i = 0; i < 20; i++) await producer.publish({ i });
        await waitFor(() => expect(allReceivedIds.size).toBe(20));

        // 3. Disconnect Client A
        await tempClientA.disconnect();

        // 4. Wait for pending messages to be redelivered
        await new Promise(r => setTimeout(r, 500));

        // 5. Publish more (20-59) — B should handle everything
        for (let i = 20; i < 60; i++) await producer.publish({ i });

        // 6. Verify
        await waitFor(() => {
            expect(allReceivedIds.size).toBe(60);
        }, { timeout: 10000 });

        for (let i = 0; i < 60; i++) {
            if (!allReceivedIds.has(i)) throw new Error(`Missing message index ${i}`);
        }

        await subB.stop();
        await tempClientB.disconnect();
    });

    it('should support History Sync (new groups start from beginning)', async () => {
        const topic = `stream-history-${randomUUID()}`;
        await nexo.stream(topic).create();

        // 1. Publish 5 messages before anyone joins
        for (let i = 0; i < 5; i++) {
            await nexo.stream(topic).publish({ i });
        }

        // 2. Join with a new group
        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe('history-group', (d) => received.push(d));

        // 3. Should receive all 5 messages
        await waitFor(() => expect(received.length).toBe(5));
        expect(received[0].i).toBe(0);
        expect(received[4].i).toBe(4);

        await sub.stop();
    });

    it('should stop subscription quickly (not wait for long-poll timeout)', async () => {
        const topic = `stream-fast-stop-${randomUUID()}`;
        await nexo.stream(topic).create();

        const sub = await clientA.stream(topic).subscribe('fast-stop-group', () => {});

        const start = Date.now();
        await sub.stop();
        const elapsed = Date.now() - start;

        expect(elapsed).toBeLessThan(2000);
    });

    it('should preserve ordering with default concurrency=1', async () => {
        const topic = `stream-order-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: number[] = [];
        const sub = await clientA.stream(topic).subscribe('order-group', async (d: any) => {
            received.push(d.i);
        });

        for (let i = 0; i < 30; i++) await nexo.stream(topic).publish({ i });

        await waitFor(() => expect(received.length).toBe(30), { timeout: 10000 });

        for (let i = 0; i < 30; i++) {
            expect(received[i]).toBe(i);
        }

        await sub.stop();
    });

    it('should process messages in parallel with concurrency > 1', async () => {
        const topic = `stream-concurrent-${randomUUID()}`;
        await nexo.stream(topic).create();

        const CALLBACK_DELAY = 100;
        const COUNT = 20;
        const CONCURRENCY = 10;

        const received: number[] = [];
        let inFlight = 0;
        let maxInFlight = 0;

        const sub = await clientA.stream(topic).subscribe('concurrent-group', async (d: any) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise(r => setTimeout(r, CALLBACK_DELAY));
            received.push(d.i);
            inFlight--;
        }, { batchSize: COUNT, concurrency: CONCURRENCY });

        for (let i = 0; i < COUNT; i++) await nexo.stream(topic).publish({ i });

        const start = Date.now();
        await waitFor(() => expect(received.length).toBe(COUNT), { timeout: 10000 });
        const elapsed = Date.now() - start;

        // All ids delivered (no loss/duplicates)
        const ids = new Set(received);
        expect(ids.size).toBe(COUNT);

        // Parallelism observed
        expect(maxInFlight).toBeGreaterThan(1);

        // Total time should be much less than fully sequential (COUNT * CALLBACK_DELAY = 2000ms)
        expect(elapsed).toBeLessThan(COUNT * CALLBACK_DELAY * 0.6);

        await sub.stop();
    });

    it('should support Seek (Beginning/End)', async () => {
        const topic = `stream-seek-${randomUUID()}`;
        const group = 'seek-group';
        await nexo.stream(topic).create();

        // 1. Push 10 messages
        for (let i = 0; i < 10; i++) await nexo.stream(topic).publish({ i });

        // 2. Scenario: Group joins and skips to END
        await clientA.stream(topic).seek(group, 'end');
        
        const receivedEnd: any[] = [];
        const subEnd = await clientA.stream(topic).subscribe(group, (d) => receivedEnd.push(d));

        // 3. Publish #11, only #11 should be received
        await nexo.stream(topic).publish({ i: 10 });
        await waitFor(() => expect(receivedEnd.length).toBe(1));
        expect(receivedEnd[0].i).toBe(10);
        await subEnd.stop();

        // 4. Scenario: Seek back to BEGINNING
        await clientA.stream(topic).seek(group, 'beginning');
        
        const receivedStart: any[] = [];
        const subStart = await clientA.stream(topic).subscribe(group, (d) => receivedStart.push(d));

        // 5. Should receive ALL 11 messages
        await waitFor(() => expect(receivedStart.length).toBe(11));
        expect(receivedStart[0].i).toBe(0);
        expect(receivedStart[10].i).toBe(10);
        
        await subStart.stop();
    });

    it('should peek DLT after poison message', async () => {
        const topic = `stream-dlt-peek-${randomUUID()}`;
        const group = 'dlt-peek-group';
        await nexo.stream(topic).create();

        // Publish a poison message that always throws
        await nexo.stream(topic).publish({ crash: true });

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe(group, async (d: any) => {
            if (d.crash) throw new Error('poison');
            received.push(d);
        });

        // Wait for redelivery to exhaust max_deliveries and park in DLT
        await new Promise(r => setTimeout(r, 3000));

        const dlt = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(dlt.length).toBeGreaterThanOrEqual(1);
        expect(dlt[0].reason).toContain('max_deliveries');

        await sub.stop();
    });

    it('should moveToStream and redeliver from DLT', async () => {
        const topic = `stream-dlt-move-${randomUUID()}`;
        const group = 'dlt-move-group';
        await nexo.stream(topic).create();

        await nexo.stream(topic).publish({ crash: true });

        let canProcess = false;
        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe(group, async (d: any) => {
            if (d.crash && !canProcess) throw new Error('poison');
            received.push(d);
        });

        // Wait for DLT
        await new Promise(r => setTimeout(r, 3000));
        const dlt = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(dlt.length).toBeGreaterThanOrEqual(1);

        // Move back to stream and allow processing
        canProcess = true;
        await nexo.stream(topic).moveToStream(group, dlt[0].seq);

        await waitFor(() => expect(received.length).toBeGreaterThanOrEqual(1), { timeout: 5000 });

        await sub.stop();
    });

    it('should deleteDlt and prevent redelivery', async () => {
        const topic = `stream-dlt-delete-${randomUUID()}`;
        const group = 'dlt-delete-group';
        await nexo.stream(topic).create();

        await nexo.stream(topic).publish({ crash: true });

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe(group, async (d: any) => {
            if (d.crash) throw new Error('poison');
            received.push(d);
        });

        // Wait for DLT
        await new Promise(r => setTimeout(r, 3000));
        const dlt = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(dlt.length).toBeGreaterThanOrEqual(1);

        // Delete from DLT
        await nexo.stream(topic).deleteDlt(group, dlt[0].seq);

        const dltAfter = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(dltAfter.length).toBe(0);

        await sub.stop();
    });

    it('should purgeDlt and clear all entries', async () => {
        const topic = `stream-dlt-purge-${randomUUID()}`;
        const group = 'dlt-purge-group';
        await nexo.stream(topic).create();

        for (let i = 0; i < 3; i++) {
            await nexo.stream(topic).publish({ crash: true });
        }

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe(group, async (d: any) => {
            if (d.crash) throw new Error('poison');
            received.push(d);
        });

        // Wait for DLT
        await new Promise(r => setTimeout(r, 3000));
        const dlt = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(dlt.length).toBeGreaterThanOrEqual(1);

        // Purge all
        const count = await nexo.stream(topic).purgeDlt(group);
        expect(count).toBeGreaterThanOrEqual(1);

        const dltAfter = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(dltAfter.length).toBe(0);

        await sub.stop();
    });
});
