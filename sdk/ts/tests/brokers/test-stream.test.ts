import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { NexoClient } from '../../src/client';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';
import { rm, writeFile } from 'node:fs/promises';
import path from 'node:path';

const STREAM_DATA_DIR = path.resolve(__dirname, '../../../../data/streams');

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

        // 4. Wait for group rebalance (B picks up A's partition)
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

        const sub = await clientA.stream(topic).subscribe('fast-stop-group', () => { });

        const start = Date.now();
        await sub.stop();
        const elapsed = Date.now() - start;

        expect(elapsed).toBeLessThan(2000);
    });

    it('should commit a started callback before leaving the group', async () => {
        const topic = `stream-stop-processing-${randomUUID()}`;
        const group = 'stop-processing-group';
        await nexo.stream(topic).create();

        let callbackCount = 0;
        let signalStarted!: () => void;
        let releaseCallback!: () => void;
        const started = new Promise<void>(resolve => { signalStarted = resolve; });
        const released = new Promise<void>(resolve => { releaseCallback = resolve; });
        const sub = await clientA.stream(topic).subscribe(group, async () => {
            callbackCount++;
            signalStarted();
            await released;
        });

        await nexo.stream(topic).publish({ id: 1 });
        await started;

        let stopCompleted = false;
        const stopping = sub.stop().then(() => { stopCompleted = true; });
        await new Promise(resolve => setTimeout(resolve, 100));
        expect(stopCompleted).toBe(false);

        releaseCallback();
        await stopping;

        const redelivered: any[] = [];
        const resumed = await clientA.stream(topic).subscribe(group, data => redelivered.push(data));
        await new Promise(resolve => setTimeout(resolve, 200));
        await resumed.stop();

        expect(callbackCount).toBe(1);
        expect(redelivered).toEqual([]);
        await nexo.stream(topic).delete();
    });

    it('should fail stop when a started callback exceeds the stop timeout', async () => {
        const topic = `stream-stop-timeout-${randomUUID()}`;
        await nexo.stream(topic).create();

        let signalStarted!: () => void;
        let releaseCallback!: () => void;
        const started = new Promise<void>(resolve => { signalStarted = resolve; });
        const released = new Promise<void>(resolve => { releaseCallback = resolve; });
        const sub = await clientA.stream(topic).subscribe('stop-timeout-group', async () => {
            signalStarted();
            await released;
        }, { stopTimeoutMs: 100 });

        await nexo.stream(topic).publish({ id: 1 });
        await started;

        await expect(sub.stop()).rejects.toThrow('stop timed out');
        releaseCallback();
        await new Promise(resolve => setTimeout(resolve, 100));
        await nexo.stream(topic).delete();
    });

    it('should expose an ACK failure during stop', async () => {
        const topic = `stream-stop-ack-failure-${randomUUID()}`;
        const group = 'stop-ack-failure-group';
        const stream = nexo.stream(topic);
        await stream.create();

        let signalStarted!: () => void;
        let releaseCallback!: () => void;
        const started = new Promise<void>(resolve => { signalStarted = resolve; });
        const released = new Promise<void>(resolve => { releaseCallback = resolve; });
        const sub = await clientA.stream(topic).subscribe(group, async () => {
            signalStarted();
            await released;
        });

        await stream.publish({ id: 1 });
        await started;
        await stream.seek(group, 'beginning');

        const stopping = sub.stop();
        releaseCallback();
        await expect(stopping).rejects.toThrow('stream ACK request(s) failed');
        await stream.delete();
    });

    it('should rejoin and redeliver after an ACK failure while active', async () => {
        const topic = `stream-active-ack-failure-${randomUUID()}`;
        const group = 'active-ack-failure-group';
        const stream = nexo.stream(topic);
        await stream.create();

        let attempts = 0;
        let signalFirstStarted!: () => void;
        let releaseFirst!: () => void;
        const firstStarted = new Promise<void>(resolve => { signalFirstStarted = resolve; });
        const firstReleased = new Promise<void>(resolve => { releaseFirst = resolve; });
        const sub = await clientA.stream(topic).subscribe(group, async () => {
            attempts++;
            if (attempts === 1) {
                signalFirstStarted();
                await firstReleased;
            }
        });

        await stream.publish({ id: 1 });
        await firstStarted;
        await stream.seek(group, 'beginning');
        releaseFirst();

        await waitFor(() => expect(attempts).toBe(2), { timeout: 5000 });
        await sub.stop();
        await stream.delete();
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

    it('should ACK a fast callback without waiting for a slow callback in the same batch', async () => {
        const topic = `stream-incremental-ack-${randomUUID()}`;
        const group = 'incremental-ack-group';
        const stream = nexo.stream(topic);
        await stream.create();
        await stream.publishBatch([
            { data: { id: 1 }, key: 'A' },
            { data: { id: 2 }, key: 'B' },
            { data: { id: 3 }, key: 'A' },
        ]);

        let signalFastFinished!: () => void;
        let signalSlowStarted!: () => void;
        let releaseSlow!: () => void;
        const fastFinished = new Promise<void>(resolve => { signalFastFinished = resolve; });
        const slowStarted = new Promise<void>(resolve => { signalSlowStarted = resolve; });
        const slowReleased = new Promise<void>(resolve => { releaseSlow = resolve; });

        const first = await clientA.stream(topic).subscribe(group, async (data: any) => {
            if (data.id === 1) {
                signalFastFinished();
                return;
            }
            if (data.id === 2) {
                signalSlowStarted();
                await slowReleased;
            }
        }, { batchSize: 2, concurrency: 2 });

        await Promise.all([fastFinished, slowStarted]);

        const receivedBySecond: number[] = [];
        const second = await clientB.stream(topic).subscribe(group, (data: any) => {
            receivedBySecond.push(data.id);
        }, { batchSize: 1 });

        await waitFor(() => expect(receivedBySecond).toEqual([3]), { timeout: 2000 });
        releaseSlow();
        await first.stop();
        await second.stop();
        await stream.delete();
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

    // DLT mechanism (redelivery → park → peek/delete/purge/move) is tested
    // comprehensively in Rust tests with per-test config (ack_wait_ms=50, max_deliveries=2).
    // No need to duplicate here — it would require either slow timeouts or env var hacks.

    it('should stop quickly during long-poll wait (no messages available)', async () => {
        const topic = `stream-stop-idle-${randomUUID()}`;
        await nexo.stream(topic).create();

        const sub = await clientA.stream(topic).subscribe('idle-stop-group', () => { });

        // Let the consumer enter long-poll (no messages published)
        await new Promise(r => setTimeout(r, 200));

        const start = Date.now();
        await sub.stop();
        const elapsed = Date.now() - start;

        // Should stop in under 2s, not wait for the full 20s long-poll
        expect(elapsed).toBeLessThan(2000);
    });

    it('should not deliver messages after stop() returns', async () => {
        const topic = `stream-stop-nodeliver-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe('nodeliver-group', (d) => received.push(d));

        await nexo.stream(topic).publish({ id: 1 });
        await waitFor(() => expect(received.length).toBe(1));

        await sub.stop();

        // Publish after stop — should not be received
        await nexo.stream(topic).publish({ id: 2 });
        await new Promise(r => setTimeout(r, 500));

        expect(received.length).toBe(1);
    });

    it('should publish batch and return seq numbers', async () => {
        const topic = `stream-batch-${randomUUID()}`;
        await nexo.stream(topic).create();

        const seqs = await nexo.stream(topic).publishBatch([
            { data: 'msg1' },
            { data: 'msg2' },
            { data: 'msg3' },
        ]);

        expect(seqs.length).toBe(3);
        expect(seqs[0]).toBe(1n);
        expect(seqs[1]).toBe(2n);
        expect(seqs[2]).toBe(3n);
    });

    it('should publish batch with keys', async () => {
        const topic = `stream-batch-keys-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: { data: any, key?: Uint8Array }[] = [];
        const sub = await clientA.stream(topic).subscribe('g-batch-keys', (data, meta) => {
            received.push({ data, key: meta.key });
        });

        await nexo.stream(topic).publishBatch([
            { data: 'msg1', key: 'key-A' },
            { data: 'msg2', key: 'key-B' },
            { data: 'msg3' },
        ]);

        await waitFor(() => expect(received.length).toBe(3));
        await sub.stop();
    });

    it('should publish with string key and verify receipt', async () => {
        const topic = `stream-pub-key-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: { data: any, key?: Uint8Array }[] = [];
        const sub = await clientA.stream(topic).subscribe('g-pub-key', (data, meta) => {
            received.push({ data, key: meta.key });
        });

        const seq = await nexo.stream(topic).publish({ x: 1 }, { key: 'my-key' });
        expect(seq).toBeGreaterThan(0n);

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0].data).toEqual({ x: 1 });
        expect(received[0].key).toBeDefined();
        expect(Buffer.from(received[0].key!).toString('utf8')).toBe('my-key');

        await nexo.stream(topic).delete();
    });

    it('should publish with Uint8Array key and verify receipt', async () => {
        const topic = `stream-pub-rawkey-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: { data: any, key?: Uint8Array }[] = [];
        const sub = await clientA.stream(topic).subscribe('g-pub-rawkey', (data, meta) => {
            received.push({ data, key: meta.key });
        });

        const rawKey = new Uint8Array([0x01, 0x02, 0xFF]);
        await nexo.stream(topic).publish('payload', { key: rawKey });

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0].data).toBe('payload');
        expect(received[0].key).toBeDefined();
        expect(Buffer.from(received[0].key!)).toEqual(Buffer.from(rawKey));

        await nexo.stream(topic).delete();
    });

    it('should publish Uint8Array data and receive raw bytes back', async () => {
        const topic = `stream-pub-uint8-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe('g-pub-uint8', (data) => {
            received.push(data);
        });

        const payload = new Uint8Array([0x01, 0x02, 0xFF, 0x00]);
        await nexo.stream(topic).publish(payload);

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0]).toBeInstanceOf(Uint8Array);
        expect(Buffer.from(received[0])).toEqual(Buffer.from(payload));

        await nexo.stream(topic).delete();
    });

    it('should publish ArrayBuffer data and receive raw bytes back', async () => {
        const topic = `stream-pub-arraybuf-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe('g-pub-arraybuf', (data) => {
            received.push(data);
        });

        const payload = new ArrayBuffer(3);
        new Uint8Array(payload).set([0x41, 0x42, 0x43]);
        await nexo.stream(topic).publish(payload);

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0]).toBeInstanceOf(Uint8Array);
        expect(Buffer.from(received[0])).toEqual(Buffer.from(new Uint8Array(payload)));

        await nexo.stream(topic).delete();
    });

    it('should handle empty publishBatch gracefully', async () => {
        const topic = `stream-batch-empty-${randomUUID()}`;
        await nexo.stream(topic).create();

        const seqs = await nexo.stream(topic).publishBatch([]);
        expect(seqs.length).toBe(0);
    });

    it('should reject empty stream keys', async () => {
        const stream = nexo.stream(`stream-empty-key-${randomUUID()}`);
        await expect(stream.publish({ x: 1 }, { key: '' })).rejects.toThrow('must not be empty');
        await expect(stream.publishBatch([{ data: { x: 1 }, key: new Uint8Array() }]))
            .rejects.toThrow('must not be empty');
    });

    it('should reject publish batches above the protocol limit', async () => {
        const items = Array.from({ length: 65_537 }, () => ({ data: null }));
        await expect(nexo.stream('stream-large-batch').publishBatch(items))
            .rejects.toThrow('Publish batch too large');
    });

    // ── Edge cases ──────────────────────────────────────────────

    it('should return exists=true after create, false before', async () => {
        const topic = `stream-exists-${randomUUID()}`;
        expect(await nexo.stream(topic).exists()).toBe(false);
        await nexo.stream(topic).create();
        expect(await nexo.stream(topic).exists()).toBe(true);
        await nexo.stream(topic).delete();
        expect(await nexo.stream(topic).exists()).toBe(false);
    });

    it('should be idempotent on create (create twice succeeds)', async () => {
        const topic = `stream-idempotent-${randomUUID()}`;
        await nexo.stream(topic).create();
        await nexo.stream(topic).create();
        expect(await nexo.stream(topic).exists()).toBe(true);
        await nexo.stream(topic).delete();
    });

    it('should reject topic names that escape the stream directory', async () => {
        await expect(nexo.stream('../outside').create()).rejects.toThrow('Invalid topic name');
    });

    it('should reject invalid seek and subscription polling options', async () => {
        const stream = nexo.stream('stream-invalid-options');
        await expect(stream.seek('group', 'invalid' as any)).rejects.toThrow('Invalid seek target');
        await expect(stream.subscribe('group', () => { }, { batchSize: 0 }))
            .rejects.toThrow('batchSize must be an integer between');
        await expect(stream.subscribe('group', () => { }, { waitMs: 0 }))
            .rejects.toThrow('waitMs must be a positive integer');
        await expect(stream.subscribe('group', () => { }, { stopTimeoutMs: 0 }))
            .rejects.toThrow('stopTimeoutMs must be a positive integer');
    });

    it('should fail publish to non-existent stream', async () => {
        const topic = `stream-pub-missing-${randomUUID()}`;
        await expect(nexo.stream(topic).publish({ x: 1 })).rejects.toThrow();
    });

    it('should fail publish when storage cannot write the message', async () => {
        const topic = `stream-write-failure-${randomUUID()}`;
        const stream = nexo.stream(topic);
        const topicPath = path.join(STREAM_DATA_DIR, topic);
        await stream.create();
        await rm(topicPath, { recursive: true, force: true });
        await writeFile(topicPath, 'not-a-directory');

        try {
            await expect(stream.publish({ x: 1 })).rejects.toThrow('Storage append failed');
        } finally {
            await rm(topicPath, { force: true });
            await stream.delete();
        }
    });

    it('should fail operations after delete', async () => {
        const topic = `stream-del-ops-${randomUUID()}`;
        await nexo.stream(topic).create();
        await nexo.stream(topic).delete();
        await expect(nexo.stream(topic).publish({ x: 1 })).rejects.toThrow();
        await expect(
            clientA.stream(topic).subscribe('g-del', () => { })
        ).rejects.toThrow();
    });

    it('should return empty array from peekDlt when DLT is empty', async () => {
        const topic = `stream-dlt-empty-${randomUUID()}`;
        const group = 'g-dlt-empty';
        await nexo.stream(topic).create();
        // Create the group by subscribing and immediately stopping
        const sub = await clientA.stream(topic).subscribe(group, () => { });
        await sub.stop();
        const entries = await nexo.stream(topic).peekDlt(group, 10, 0);
        expect(entries).toEqual([]);
        await nexo.stream(topic).delete();
    });

    it('should return 0 from purgeDlt when DLT is empty', async () => {
        const topic = `stream-dlt-purge-empty-${randomUUID()}`;
        const group = 'g-dlt-purge';
        await nexo.stream(topic).create();
        // Create the group by subscribing and immediately stopping
        const sub = await clientA.stream(topic).subscribe(group, () => { });
        await sub.stop();
        const count = await nexo.stream(topic).purgeDlt(group);
        expect(count).toBe(0);
        await nexo.stream(topic).delete();
    });

    it('should resubscribe same group after stop and receive only new messages', async () => {
        const topic = `stream-resub-${randomUUID()}`;
        const group = 'g-resub';
        await nexo.stream(topic).create();

        const recv1: any[] = [];
        const sub1 = await clientA.stream(topic).subscribe(group, (d) => recv1.push(d));
        await nexo.stream(topic).publish({ i: 1 });
        await nexo.stream(topic).publish({ i: 2 });
        await waitFor(() => expect(recv1.length).toBe(2));
        await sub1.stop();

        // Publish while no consumer is active
        await nexo.stream(topic).publish({ i: 3 });

        // Resubscribe — should receive only msg 3 (msgs 1-2 were acked)
        const recv2: any[] = [];
        const sub2 = await clientA.stream(topic).subscribe(group, (d) => recv2.push(d));
        await waitFor(() => expect(recv2.length).toBe(1));
        expect(recv2[0].i).toBe(3);
        await sub2.stop();

        await nexo.stream(topic).delete();
    });

    it('should deliver messages to multiple independent groups simultaneously', async () => {
        const topic = `stream-multi-groups-${randomUUID()}`;
        await nexo.stream(topic).create();

        const recvA: any[] = [];
        const recvB: any[] = [];
        const recvC: any[] = [];

        const subA = await clientA.stream(topic).subscribe('multi-a', (d) => recvA.push(d));
        const subB = await clientB.stream(topic).subscribe('multi-b', (d) => recvB.push(d));
        const subC = await clientA.stream(topic).subscribe('multi-c', (d) => recvC.push(d));

        for (let i = 0; i < 5; i++) await nexo.stream(topic).publish({ i });

        await waitFor(() => {
            expect(recvA.length).toBe(5);
            expect(recvB.length).toBe(5);
            expect(recvC.length).toBe(5);
        });

        await subA.stop();
        await subB.stop();
        await subC.stop();
        await nexo.stream(topic).delete();
    });
});
