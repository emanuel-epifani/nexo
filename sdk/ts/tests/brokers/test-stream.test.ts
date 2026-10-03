import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { NexoClient } from '../../src/client';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';
import {
    FencedError,
    NotConnectedError,
    NotMemberError,
    ProtocolError,
    ResourceConfigurationConflictError,
    ResourceNotFoundError,
} from '../../src/errors';
import { decodeFetchPage, FetchPageLimits, NexoStreamGroup } from '../../src/brokers/stream';
import { Cursor, FrameWriter } from '../../src/protocol/codec';
import {
    DataType,
    ErrorCode,
    FLAG_STREAM_S_UPDATE_BATCH_RELEASE,
    FLAG_STREAM_S_UPDATE_BATCH_RETRY,
    HEADER_SIZE,
    ResponseStatus,
    StreamOpcode,
} from '../../src/protocol/generated';
import { Logger } from '../../src/utils/logger';

const streamHandles = new WeakMap<NexoClient, Map<string, Promise<any>>>();

function getStream(client: NexoClient, name: string): Promise<any> {
    let handles = streamHandles.get(client);
    if (!handles) {
        handles = new Map();
        streamHandles.set(client, handles);
    }
    let handle = handles.get(name);
    if (!handle) {
        handle = client.stream.get(name);
        handles.set(name, handle);
        void handle.catch(() => handles!.delete(name));
    }
    return handle;
}

async function publish(name: string, data: any, options: any = {}) {
    return (await getStream(nexo, name)).publish(data, options);
}

async function publishBatch(name: string, items: any[]) {
    return (await getStream(nexo, name)).publishBatch(items);
}

async function subscribe(client: NexoClient, name: string, group: string, callback: (...args: any[]) => any, options: any = {}) {
    return (await getStream(client, name)).group(group).subscribe(callback, options);
}

async function seek(client: NexoClient, name: string, group: string, target: 'beginning' | 'end') {
    return (await getStream(client, name)).group(group).seek(target);
}

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
        const name = `stream-basic-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: any[] = [];
        const sub = await subscribe(clientA, name, 'g1', (data) => received.push(data));

        await publish(name, { id: 1 });
        await publish(name, { id: 2 });

        await waitFor(() => expect(received.length).toBe(2));
        await sub.stop();
    });

    it('should fail subscribe when stream does not exist', async () => {
        const name = `stream-missing-${randomUUID()}`;
        await expect(
            subscribe(clientA, name, 'missing-group', () => {
            })
        ).rejects.toBeInstanceOf(ResourceNotFoundError);
    });

    it('Independent CONSUMER GROUPS => should deliver all messages to each group', async () => {
        const name = `stream-groups-${randomUUID()}`;
        await nexo.stream.create(name);

        const recvA: any[] = [];
        const recvB: any[] = [];

        const subA = await subscribe(clientA, name, 'group_A', (d) => recvA.push(d));
        const subB = await subscribe(clientB, name, 'group_B', (d) => recvB.push(d));

        await publish(name, { msg: 'hello' });

        await waitFor(() => {
            expect(recvA.length).toBe(1);
            expect(recvB.length).toBe(1);
        });

        await subA.stop();
        await subB.stop();
    });

    it('Same CONSUMER GROUP => should distribute messages without duplicates', async () => {
        const name = `parallel-consumers-${randomUUID()}`;
        const group = 'parallel_group';
        await nexo.stream.create(name);

        const receivedA = new Set<number>();
        const receivedB = new Set<number>();

        // Start 2 consumers in same group
        const subA = await subscribe(clientA, name, group, (d) => {
            if (receivedA.has(d.id)) throw new Error(`Duplicate in A: ${d.id}`);
            receivedA.add(d.id);
        });

        const subB = await subscribe(clientB, name, group, (d) => {
            if (receivedB.has(d.id)) throw new Error(`Duplicate in B: ${d.id}`);
            receivedB.add(d.id);
        });

        // Publish messages
        for (let i = 0; i < 100; i++) {
            await publish(name, { id: i });
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

        const name = `stream-disconnect-${randomUUID()}`;
        const group = 'group_disconnect';
        await nexo.stream.create(name);

        const producer = await getStream(nexo, name);
        const allReceivedIds = new Set<number>();
        const track = (d: any) => allReceivedIds.add(d.i);

        // 1. Start A & B 
        const subA = await subscribe(tempClientA, name, group, track);
        const subB = await subscribe(tempClientB, name, group, track);

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
        const name = `stream-history-${randomUUID()}`;
        await nexo.stream.create(name);

        // 1. Publish 5 messages before anyone joins
        for (let i = 0; i < 5; i++) {
            await publish(name, { i });
        }

        // 2. Join with a new group
        const received: any[] = [];
        const sub = await subscribe(clientA, name, 'history-group', (d) => received.push(d));

        // 3. Should receive all 5 messages
        await waitFor(() => expect(received.length).toBe(5));
        expect(received[0].i).toBe(0);
        expect(received[4].i).toBe(4);

        await sub.stop();
    });

    it('should stop subscription quickly (not wait for long-poll timeout)', async () => {
        const name = `stream-fast-stop-${randomUUID()}`;
        await nexo.stream.create(name);

        const sub = await subscribe(clientA, name, 'fast-stop-group', () => { });

        const start = Date.now();
        await sub.stop();
        const elapsed = Date.now() - start;

        expect(elapsed).toBeLessThan(2000);
    });

    it('should commit a started callback before leaving the group', async () => {
        const name = `stream-stop-processing-${randomUUID()}`;
        const group = 'stop-processing-group';
        await nexo.stream.create(name);

        let callbackCount = 0;
        let signalStarted!: () => void;
        let releaseCallback!: () => void;
        const started = new Promise<void>(resolve => { signalStarted = resolve; });
        const released = new Promise<void>(resolve => { releaseCallback = resolve; });
        const sub = await subscribe(clientA, name, group, async () => {
            callbackCount++;
            signalStarted();
            await released;
        });

        await publish(name, { id: 1 });
        await started;

        let stopCompleted = false;
        const stopping = sub.stop().then(() => { stopCompleted = true; });
        await new Promise(resolve => setTimeout(resolve, 100));
        expect(stopCompleted).toBe(false);

        releaseCallback();
        await stopping;

        const redelivered: any[] = [];
        const resumed = await subscribe(clientA, name, group, data => redelivered.push(data));
        await new Promise(resolve => setTimeout(resolve, 200));
        await resumed.stop();

        expect(callbackCount).toBe(1);
        expect(redelivered).toEqual([]);
        await nexo.stream.delete(name);
    });

    it('should fail stop when a started callback exceeds the stop timeout', async () => {
        const name = `stream-stop-timeout-${randomUUID()}`;
        await nexo.stream.create(name);

        let signalStarted!: () => void;
        let releaseCallback!: () => void;
        const started = new Promise<void>(resolve => { signalStarted = resolve; });
        const released = new Promise<void>(resolve => { releaseCallback = resolve; });
        const sub = await subscribe(clientA, name, 'stop-timeout-group', async () => {
            signalStarted();
            await released;
        }, { stopTimeoutMs: 100 });

        await publish(name, { id: 1 });
        await started;

        await expect(sub.stop()).rejects.toThrow('stop timed out');
        releaseCallback();
        await new Promise(resolve => setTimeout(resolve, 100));
        await nexo.stream.delete(name);
    });

    it('should expose a batch update failure during stop', async () => {
        const name = `stream-stop-ack-failure-${randomUUID()}`;
        const group = 'stop-ack-failure-group';
        await nexo.stream.create(name);
        const stream = await getStream(nexo, name);

        let signalStarted!: () => void;
        let releaseCallback!: () => void;
        const started = new Promise<void>(resolve => { signalStarted = resolve; });
        const released = new Promise<void>(resolve => { releaseCallback = resolve; });
        const sub = await subscribe(clientA, name, group, async () => {
            signalStarted();
            await released;
        });

        await stream.publish({ id: 1 });
        await started;
        await stream.group(group).seek('beginning');

        const stopping = sub.stop();
        releaseCallback();
        await expect(stopping).rejects.toThrow('epoch is fenced');
        await nexo.stream.delete(name);
    });

    it('should rejoin and redeliver after a batch update failure while active', async () => {
        const name = `stream-active-ack-failure-${randomUUID()}`;
        const group = 'active-ack-failure-group';
        await nexo.stream.create(name);
        const stream = await getStream(nexo, name);

        let attempts = 0;
        let signalFirstStarted!: () => void;
        let releaseFirst!: () => void;
        const firstStarted = new Promise<void>(resolve => { signalFirstStarted = resolve; });
        const firstReleased = new Promise<void>(resolve => { releaseFirst = resolve; });
        const sub = await subscribe(clientA, name, group, async () => {
            attempts++;
            if (attempts === 1) {
                signalFirstStarted();
                await firstReleased;
            }
        });

        await stream.publish({ id: 1 });
        await firstStarted;
        await stream.group(group).seek('beginning');
        releaseFirst();

        await waitFor(() => expect(attempts).toBe(2), { timeout: 5000 });
        await sub.stop();
        await nexo.stream.delete(name);
    });

    it('should preserve ordering with concurrency=1', async () => {
        const name = `stream-order-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: number[] = [];
        const sub = await subscribe(clientA, name, 'order-group', async (d: any) => {
            received.push(d.i);
        }, { concurrency: 1 });

        for (let i = 0; i < 30; i++) await publish(name, { i });

        await waitFor(() => expect(received.length).toBe(30), { timeout: 10000 });

        for (let i = 0; i < 30; i++) {
            expect(received[i]).toBe(i);
        }

        await sub.stop();
    });

    it('should process messages in parallel with concurrency > 1', async () => {
        const name = `stream-concurrent-${randomUUID()}`;
        await nexo.stream.create(name);

        const CALLBACK_DELAY = 100;
        const COUNT = 20;
        const CONCURRENCY = 10;

        const received: number[] = [];
        let inFlight = 0;
        let maxInFlight = 0;

        const sub = await subscribe(clientA, name, 'concurrent-group', async (d: any) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise(r => setTimeout(r, CALLBACK_DELAY));
            received.push(d.i);
            inFlight--;
        }, { batchSize: COUNT, concurrency: CONCURRENCY });

        for (let i = 0; i < COUNT; i++) await publish(name, { i });

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
        const name = `stream-incremental-ack-${randomUUID()}`;
        const group = 'incremental-ack-group';
        await nexo.stream.create(name);
        const stream = await getStream(nexo, name);
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

        const received: number[] = [];
        const first = await subscribe(clientA, name, group, async (data: any) => {
            received.push(data.id);
            if (data.id === 1) {
                signalFastFinished();
                return;
            }
            if (data.id === 2) {
                signalSlowStarted();
                await slowReleased;
            }
        }, { batchSize: 2, concurrency: 2, maxBufferedMessages: 2 });

        await Promise.all([fastFinished, slowStarted]);

        const receivedBySecond: number[] = [];
        const second = await subscribe(clientB, name, group, (data: any) => {
            receivedBySecond.push(data.id);
        }, { batchSize: 1 });

        await waitFor(
            () => expect([...received, ...receivedBySecond]).toContain(3),
            { timeout: 2000 },
        );
        releaseSlow();
        await first.stop();
        await second.stop();
        await nexo.stream.delete(name);
    });

    it('should support Seek (Beginning/End)', async () => {
        const name = `stream-seek-${randomUUID()}`;
        const group = 'seek-group';
        await nexo.stream.create(name);

        // 1. Push 10 messages
        for (let i = 0; i < 10; i++) await publish(name, { i });

        // 2. Scenario: Group joins and skips to END
        await seek(clientA, name, group, 'end');

        const receivedEnd: any[] = [];
        const subEnd = await subscribe(clientA, name, group, (d) => receivedEnd.push(d));

        // 3. Publish #11, only #11 should be received
        await publish(name, { i: 10 });
        await waitFor(() => expect(receivedEnd.length).toBe(1));
        expect(receivedEnd[0].i).toBe(10);
        await subEnd.stop();

        // 4. Scenario: Seek back to BEGINNING
        await seek(clientA, name, group, 'beginning');

        const receivedStart: any[] = [];
        const subStart = await subscribe(clientA, name, group, (d) => receivedStart.push(d));

        // 5. Should receive ALL 11 messages
        await waitFor(() => expect(receivedStart.length).toBe(11));
        expect(receivedStart[0].i).toBe(0);
        expect(receivedStart[10].i).toBe(10);

        await subStart.stop();
    });

    // DLS mechanism (redelivery → park → peek/delete/purge/move) is tested
    // comprehensively in Rust tests with per-test config (ack_wait_ms=50, max_deliveries=2).
    // No need to duplicate here — it would require either slow timeouts or env var hacks.

    it('should stop quickly during long-poll wait (no messages available)', async () => {
        const name = `stream-stop-idle-${randomUUID()}`;
        await nexo.stream.create(name);

        const sub = await subscribe(clientA, name, 'idle-stop-group', () => { });

        // Let the consumer enter long-poll (no messages published)
        await new Promise(r => setTimeout(r, 200));

        const start = Date.now();
        await sub.stop();
        const elapsed = Date.now() - start;

        // Should stop in under 2s, not wait for the full 20s long-poll
        expect(elapsed).toBeLessThan(2000);
    });

    it('should not deliver messages after stop() returns', async () => {
        const name = `stream-stop-nodeliver-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: any[] = [];
        const sub = await subscribe(clientA, name, 'nodeliver-group', (d) => received.push(d));

        await publish(name, { id: 1 });
        await waitFor(() => expect(received.length).toBe(1));

        await sub.stop();

        // Publish after stop — should not be received
        await publish(name, { id: 2 });
        await new Promise(r => setTimeout(r, 500));

        expect(received.length).toBe(1);
    });

    it('should publish batch and return seq numbers', async () => {
        const name = `stream-batch-${randomUUID()}`;
        await nexo.stream.create(name);

        const seqs = await publishBatch(name, [
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
        const name = `stream-batch-keys-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: { data: any, key?: Uint8Array }[] = [];
        const sub = await subscribe(clientA, name, 'g-batch-keys', (data, meta) => {
            received.push({ data, key: meta.key });
        });

        await publishBatch(name, [
            { data: 'msg1', key: 'key-A' },
            { data: 'msg2', key: 'key-B' },
            { data: 'msg3' },
        ]);

        await waitFor(() => expect(received.length).toBe(3));
        await sub.stop();
    });

    it('should publish with string key and verify receipt', async () => {
        const name = `stream-pub-key-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: { data: any, key?: Uint8Array }[] = [];
        const sub = await subscribe(clientA, name, 'g-pub-key', (data, meta) => {
            received.push({ data, key: meta.key });
        });

        const seq = await publish(name, { x: 1 }, { key: 'my-key' });
        expect(seq).toBeGreaterThan(0n);

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0].data).toEqual({ x: 1 });
        expect(received[0].key).toBeDefined();
        expect(Buffer.from(received[0].key!).toString('utf8')).toBe('my-key');

        await nexo.stream.delete(name);
    });

    it('should publish with Uint8Array key and verify receipt', async () => {
        const name = `stream-pub-rawkey-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: { data: any, key?: Uint8Array }[] = [];
        const sub = await subscribe(clientA, name, 'g-pub-rawkey', (data, meta) => {
            received.push({ data, key: meta.key });
        });

        const rawKey = new Uint8Array([0x01, 0x02, 0xFF]);
        await publish(name, 'payload', { key: rawKey });

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0].data).toBe('payload');
        expect(received[0].key).toBeDefined();
        expect(Buffer.from(received[0].key!)).toEqual(Buffer.from(rawKey));

        await nexo.stream.delete(name);
    });

    it('should publish Uint8Array data and receive raw bytes back', async () => {
        const name = `stream-pub-uint8-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: any[] = [];
        const sub = await subscribe(clientA, name, 'g-pub-uint8', (data) => {
            received.push(data);
        });

        const payload = new Uint8Array([0x01, 0x02, 0xFF, 0x00]);
        await publish(name, payload);

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0]).toBeInstanceOf(Uint8Array);
        expect(Buffer.from(received[0])).toEqual(Buffer.from(payload));

        await nexo.stream.delete(name);
    });

    it('should publish ArrayBuffer data and receive raw bytes back', async () => {
        const name = `stream-pub-arraybuf-${randomUUID()}`;
        await nexo.stream.create(name);

        const received: any[] = [];
        const sub = await subscribe(clientA, name, 'g-pub-arraybuf', (data) => {
            received.push(data);
        });

        const payload = new ArrayBuffer(3);
        new Uint8Array(payload).set([0x41, 0x42, 0x43]);
        await publish(name, payload);

        await waitFor(() => expect(received.length).toBe(1));
        await sub.stop();

        expect(received[0]).toBeInstanceOf(Uint8Array);
        expect(Buffer.from(received[0])).toEqual(Buffer.from(new Uint8Array(payload)));

        await nexo.stream.delete(name);
    });

    it('should handle empty publishBatch gracefully', async () => {
        const name = `stream-batch-empty-${randomUUID()}`;
        await nexo.stream.create(name);

        const seqs = await publishBatch(name, []);
        expect(seqs.length).toBe(0);
    });

    it('should reject empty stream keys', async () => {
        const name = `stream-empty-key-${randomUUID()}`;
        await nexo.stream.create(name);
        const stream = await getStream(nexo, name);
        await expect(stream.publish({ x: 1 }, { key: '' })).rejects.toThrow('must not be empty');
        await expect(stream.publishBatch([{ data: { x: 1 }, key: new Uint8Array() }]))
            .rejects.toThrow('must not be empty');
        await nexo.stream.delete(name);
    });

    it('should reject publish batches above the protocol limit', async () => {
        const name = `stream-large-batch-${randomUUID()}`;
        await nexo.stream.create(name);
        const stream = await getStream(nexo, name);
        const items = Array.from({ length: 65_537 }, () => ({ data: null }));
        await expect(stream.publishBatch(items)).rejects.toThrow('Publish batch too large');
        await nexo.stream.delete(name);
    });

    // ── Edge cases ──────────────────────────────────────────────

    it('should return exists=true after create, false before', async () => {
        const name = `stream-exists-${randomUUID()}`;
        expect(await nexo.stream.exists(name)).toBe(false);
        await nexo.stream.create(name);
        expect(await nexo.stream.exists(name)).toBe(true);
        await nexo.stream.delete(name);
        expect(await nexo.stream.exists(name)).toBe(false);
    });

    it('should return provisioning results and reject configuration conflicts', async () => {
        const name = `stream-provision-${randomUUID()}`;
        const options = { retention: { maxAgeMs: 5000, maxBytes: 100000 } };

        const created = await nexo.stream.create(name, options);
        expect(created.status).toBe('created');
        expect(created.definition).toMatchObject({ name: name, config: options });

        const unchanged = await nexo.stream.create(name, options);
        expect(unchanged.status).toBe('unchanged');
        expect(unchanged.definition).toEqual(created.definition);

        try {
            await nexo.stream.create(name, { retention: { ...options.retention, maxAgeMs: 6000 } });
            throw new Error('Expected stream configuration conflict');
        } catch (error) {
            expect(error).toBeInstanceOf(ResourceConfigurationConflictError);
            expect((error as ResourceConfigurationConflictError).details).toMatchObject({
                resourceKind: 'stream',
                resourceName: name,
                differences: [{ path: 'config.retention.maxAgeMs', requested: 6000, actual: 5000 }],
            });
        }

        expect(await nexo.stream.describe(name)).toEqual(created.definition);
        await nexo.stream.delete(name);
    });

    it('should reject stream names that escape the stream directory', async () => {
        await expect(nexo.stream.create('../outside')).rejects.toThrow('Invalid stream name');
    });

    it('should reject invalid seek and subscription polling options', async () => {
        const name = `stream-invalid-options-${randomUUID()}`;
        await nexo.stream.create(name);
        const group = (await getStream(nexo, name)).group('group');
        await expect(group.seek('invalid' as any)).rejects.toThrow('Invalid seek target');
        await expect(group.subscribe(() => { }, { batchSize: 0 }))
            .rejects.toThrow('batchSize must be an integer between');
        await expect(group.subscribe(() => { }, { waitMs: 0 }))
            .rejects.toThrow('waitMs must be a positive integer');
        await expect(group.subscribe(() => { }, { stopTimeoutMs: 0 }))
            .rejects.toThrow('stopTimeoutMs must be a positive integer');
        await nexo.stream.delete(name);
    });

    it('should reject invalid continuous-executor subscribe options before joining', async () => {
        const name = `stream-opt-validation-${randomUUID()}`;
        await nexo.stream.create(name);
        const group = (await getStream(nexo, name)).group('group');

        for (const v of [0, -1, 1.5, NaN, Infinity] as any[]) {
            await expect(group.subscribe(() => { }, { concurrency: v }))
                .rejects.toThrow('concurrency');
            await expect(group.subscribe(() => { }, { batchSize: v }))
                .rejects.toThrow('batchSize');
            await expect(group.subscribe(() => { }, { keyBatchSize: v }))
                .rejects.toThrow('keyBatchSize');
            await expect(group.subscribe(() => { }, { waitMs: v }))
                .rejects.toThrow('waitMs');
            await expect(group.subscribe(() => { }, { stopTimeoutMs: v }))
                .rejects.toThrow('stopTimeoutMs');
            await expect(group.subscribe(() => { }, { callbackTimeoutMs: v }))
                .rejects.toThrow('callbackTimeoutMs');
            await expect(group.subscribe(() => { }, { maxBufferedMessages: v }))
                .rejects.toThrow('maxBufferedMessages');
            await expect(group.subscribe(() => { }, { maxBufferedBytes: v }))
                .rejects.toThrow('maxBufferedBytes');
        }

        await expect(group.subscribe(() => { }, { waitMs: 0x80000000 }))
            .rejects.toThrow('waitMs');
        await expect(group.subscribe(() => { }, { stopTimeoutMs: 0x80000000 }))
            .rejects.toThrow('stopTimeoutMs');
        await expect(group.subscribe(() => { }, { callbackTimeoutMs: 0x80000000 }))
            .rejects.toThrow('callbackTimeoutMs');
        await expect(group.subscribe(() => { }, { concurrency: 65537 }))
            .rejects.toThrow('concurrency');
        await expect(group.subscribe(() => { }, { batchSize: 65537 }))
            .rejects.toThrow('batchSize');
        await expect(group.subscribe(() => { }, { keyBatchSize: 1025 }))
            .rejects.toThrow('keyBatchSize');
        await expect(group.subscribe(() => { }, { maxBufferedMessages: 131073 }))
            .rejects.toThrow('maxBufferedMessages');
        await expect(group.subscribe(() => { }, { maxBufferedBytes: 53 }))
            .rejects.toThrow('maxBufferedBytes');
        await expect(group.subscribe(() => { }, { maxBufferedBytes: 0x100000000 }))
            .rejects.toThrow('maxBufferedBytes');
        await nexo.stream.delete(name);
    });

    it('should fail fast when getting a non-existent stream', async () => {
        const name = `stream-get-missing-${randomUUID()}`;
        await expect(nexo.stream.get(name)).rejects.toBeInstanceOf(ResourceNotFoundError);
    });

    it('should propagate connection errors from exists', async () => {
        const disconnected = new NexoClient();
        try {
            await expect(disconnected.stream.exists('events')).rejects.toBeInstanceOf(NotConnectedError);
        } finally {
            disconnected.disconnect();
        }
    });

    it('should fail operations after delete', async () => {
        const name = `stream-del-ops-${randomUUID()}`;
        await nexo.stream.create(name);
        await nexo.stream.delete(name);
        await expect(publish(name, { x: 1 })).rejects.toThrow();
        await expect(
            subscribe(clientA, name, 'g-del', () => { })
        ).rejects.toThrow();
    });

    it('should expose group DLS peek when the DLS is empty', async () => {
        const name = `stream-dls-empty-${randomUUID()}`;
        const group = 'g-dls-empty';
        await nexo.stream.create(name);
        const groupHandle = (await getStream(clientA, name)).group(group);
        // Create the group by subscribing and immediately stopping
        const sub = await groupHandle.subscribe(() => { });
        await sub.stop();
        const entries = await groupHandle.dls.peek({ limit: 10, offset: 0 });
        expect(entries).toEqual([]);
        expect(groupHandle.dls.replay).toBeTypeOf('function');
        expect(groupHandle.dls.delete).toBeTypeOf('function');
        await nexo.stream.delete(name);
    });

    it('should expose group DLS purge when the DLS is empty', async () => {
        const name = `stream-dls-purge-empty-${randomUUID()}`;
        const group = 'g-dls-purge';
        await nexo.stream.create(name);
        const groupHandle = (await getStream(clientA, name)).group(group);
        // Create the group by subscribing and immediately stopping
        const sub = await groupHandle.subscribe(() => { });
        await sub.stop();
        const count = await groupHandle.dls.purge();
        expect(count).toBe(0);
        await nexo.stream.delete(name);
    });

    it('should resubscribe same group after stop and receive only new messages', async () => {
        const name = `stream-resub-${randomUUID()}`;
        const group = 'g-resub';
        await nexo.stream.create(name);

        const recv1: any[] = [];
        const sub1 = await subscribe(clientA, name, group, (d) => recv1.push(d));
        await publish(name, { i: 1 });
        await publish(name, { i: 2 });
        await waitFor(() => expect(recv1.length).toBe(2));
        await sub1.stop();

        // Publish while no consumer is active
        await publish(name, { i: 3 });

        // Resubscribe — should receive only msg 3 (msgs 1-2 were acked)
        const recv2: any[] = [];
        const sub2 = await subscribe(clientA, name, group, (d) => recv2.push(d));
        await waitFor(() => expect(recv2.length).toBe(1));
        expect(recv2[0].i).toBe(3);
        await sub2.stop();

        await nexo.stream.delete(name);
    });

    it('should deliver messages to multiple independent groups simultaneously', async () => {
        const name = `stream-multi-groups-${randomUUID()}`;
        await nexo.stream.create(name);

        const recvA: any[] = [];
        const recvB: any[] = [];
        const recvC: any[] = [];

        const subA = await subscribe(clientA, name, 'multi-a', (d) => recvA.push(d));
        const subB = await subscribe(clientB, name, 'multi-b', (d) => recvB.push(d));
        const subC = await subscribe(clientA, name, 'multi-c', (d) => recvC.push(d));

        for (let i = 0; i < 5; i++) await publish(name, { i });

        await waitFor(() => {
            expect(recvA.length).toBe(5);
            expect(recvB.length).toBe(5);
            expect(recvC.length).toBe(5);
        });

        await subA.stop();
        await subB.stop();
        await subC.stop();
        await nexo.stream.delete(name);
    });

    // ── Per-key ordering end-to-end ──────────────────────────────

    it('should enforce per-key ordering: same key delivered one at a time, different keys in parallel', async () => {
        const name = `stream-perkey-order-${randomUUID()}`;
        const group = 'perkey-order-group';
        await nexo.stream.create(name);

        // Publish all messages BEFORE subscribing so the first fetch sees all 6
        // and per-key blocking is active immediately.
        // Interleaved: A0, B0, A1, no-key, B1, A2
        await publish(name, { i: 0 }, { key: 'user-A' });
        await publish(name, { i: 0 }, { key: 'user-B' });
        await publish(name, { i: 1 }, { key: 'user-A' });
        await publish(name, { i: 0 });
        await publish(name, { i: 1 }, { key: 'user-B' });
        await publish(name, { i: 2 }, { key: 'user-A' });

        // Track delivery order per key. Use a gate to hold ACKs until we verify
        // the first batch, preventing the server from unblocking the next message.
        const delivered: { key: string; i: number }[] = [];
        let firstBatchGate: () => void;
        const firstBatchReady = new Promise<void>(r => { firstBatchGate = r; });
        let releaseGate: () => void;
        const gateReleased = new Promise<void>(r => { releaseGate = r; });
        let count = 0;

        const sub = await subscribe(clientA, name, group, async (d: any, meta: { key?: Uint8Array }) => {
            const key = meta.key ? Buffer.from(meta.key).toString('utf-8') : 'none';
            delivered.push({ key, i: d.i });
            count++;
            if (count === 3) firstBatchGate();
            // Block all callbacks so ACKs don't fire until we've verified
            await gateReleased;
        }, { batchSize: 10, concurrency: 10 });

        // Wait for first batch: A0, B0, no-key (3 messages — one per key + keyless)
        await waitFor(() => expect(count).toBe(3), { timeout: 5000 });

        const firstKeys = delivered.slice(0, 3).map(d => d.key).sort();
        expect(firstKeys).toEqual(['none', 'user-A', 'user-B']);

        // No second message for any key should have been delivered yet
        // (ACKs are blocked, so keys remain in-flight on the server)
        const aMessages = delivered.filter(d => d.key === 'user-A');
        const bMessages = delivered.filter(d => d.key === 'user-B');
        expect(aMessages.length).toBe(1);
        expect(bMessages.length).toBe(1);

        // Release the gate so ACKs proceed and the test can clean up
        releaseGate!();

        await sub.stop();
        await nexo.stream.delete(name);
    });

    it('should process same-key messages strictly sequentially', async () => {
        const name = `stream-keyed-seq-${randomUUID()}`;
        const group = 'keyed-seq-group';
        await nexo.stream.create(name);

        for (let i = 0; i < 6; i++) await publish(name, { i }, { key: 'user-A' });

        const received: number[] = [];
        let inFlight = 0;
        const sub = await subscribe(clientA, name, group, async (d: any) => {
            inFlight++;
            expect(inFlight).toBe(1);
            await new Promise(r => setTimeout(r, 5));
            received.push(d.i);
            inFlight--;
        }, { batchSize: 10, keyBatchSize: 10, concurrency: 5 });

        await waitFor(() => expect(received).toEqual([0, 1, 2, 3, 4, 5]), { timeout: 5000 });
        await sub.stop();
        await nexo.stream.delete(name);
    });

    it('should not replay the confirmed prefix after a mid-batch callback failure', async () => {
        const name = `stream-keyed-retry-${randomUUID()}`;
        const group = 'keyed-retry-group';
        await nexo.stream.create(name);

        for (let i = 0; i < 5; i++) await publish(name, { i }, { key: 'user-A' });

        const attempts: number[] = [];
        let failedOnce = false;
        const sub = await subscribe(clientA, name, group, async (d: any) => {
            attempts.push(d.i);
            if (d.i === 2 && !failedOnce) {
                failedOnce = true;
                throw new Error('boom');
            }
        }, { batchSize: 10, keyBatchSize: 10 });

        await waitFor(
            () => expect(attempts).toEqual([0, 1, 2, 2, 3, 4]),
            { timeout: 45000 },
        );
        await sub.stop();
        await nexo.stream.delete(name);
    });

    it('should not lease the same key to two consumers at once', async () => {
        const name = `stream-keyed-exclusive-${randomUUID()}`;
        const group = 'keyed-exclusive-group';
        await nexo.stream.create(name);

        for (let i = 0; i < 4; i++) await publish(name, { i }, { key: 'user-A' });

        const receivedA: number[] = [];
        const receivedB: number[] = [];
        let releaseA!: () => void;
        let signalA!: () => void;
        const heldA = new Promise<void>(r => { releaseA = r; });
        const startedA = new Promise<void>(r => { signalA = r; });

        const subA = await subscribe(clientA, name, group, async (d: any) => {
            receivedA.push(d.i);
            if (d.i === 0) signalA();
            await heldA;
        });

        await startedA;
        const subB = await subscribe(clientB, name, group, async (d: any) => {
            receivedB.push(d.i);
        });
        await new Promise(r => setTimeout(r, 300));
        expect(receivedB).toEqual([]);

        releaseA();
        await subA.stop();
        await waitFor(() => expect(receivedB).toEqual([1, 2, 3]), { timeout: 5000 });
        await subB.stop();
        await nexo.stream.delete(name);
    });

    it('should keep the confirmed prefix on stop and redeliver only the tail', async () => {
        const name = `stream-stop-midbatch-${randomUUID()}`;
        const group = 'stop-midbatch-group';
        await nexo.stream.create(name);

        for (let i = 0; i < 5; i++) await publish(name, { i }, { key: 'user-A' });

        const receivedA: number[] = [];
        let releaseThird!: () => void;
        let signalThird!: () => void;
        const heldThird = new Promise<void>(r => { releaseThird = r; });
        const startedThird = new Promise<void>(r => { signalThird = r; });
        const subA = await subscribe(clientA, name, group, async (d: any) => {
            receivedA.push(d.i);
            if (d.i === 2) {
                signalThird();
                await heldThird;
            }
        });
        await startedThird;

        const stopping = subA.stop();
        releaseThird();
        await stopping;
        expect(receivedA).toEqual([0, 1, 2]);

        const receivedB: number[] = [];
        const subB = await subscribe(clientA, name, group, async (d: any) => {
            receivedB.push(d.i);
        });
        await waitFor(() => expect(receivedB).toEqual([3, 4]), { timeout: 5000 });
        await subB.stop();
        await nexo.stream.delete(name);
    });

    it('should continuously refill independent keys while a batch is held', async () => {
        const name = `stream-refill-${randomUUID()}`;
        const group = 'refill-group';
        await nexo.stream.create(name);

        for (let i = 0; i < 4; i++) await publish(name, { i }, { key: 'key-A' });
        for (let i = 0; i < 6; i++) await publish(name, { i }, { key: 'key-B' });

        const startedA: number[] = [];
        const doneB: number[] = [];
        let inFlight = 0;
        let maxInFlight = 0;
        let aHeld = true;
        let releaseA!: () => void;
        const gateA = new Promise<void>(r => { releaseA = r; });

        const sub = await subscribe(clientA, name, group, async (d: any, meta: any) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            try {
                if (Buffer.from(meta.key ?? []).toString() === 'key-A') {
                    startedA.push(d.i);
                    if (aHeld) { aHeld = false; await gateA; }
                } else {
                    doneB.push(d.i);
                }
            } finally {
                inFlight--;
            }
        }, {
            batchSize: 4,
            keyBatchSize: 2,
            concurrency: 2,
            maxBufferedMessages: 8,
        });

        try {
            await waitFor(() => expect(doneB).toEqual([0, 1, 2, 3, 4, 5]), { timeout: 10000 });
            expect(startedA).toEqual([0]);
            expect(maxInFlight).toBeLessThanOrEqual(2);
        } finally {
            releaseA();
            await sub.stop();
        }
        await nexo.stream.delete(name);
    });

    it('should abort a timed-out callback and keep serving other keys', async () => {
        const name = `stream-cb-timeout-${randomUUID()}`;
        const group = 'cb-timeout-group';
        await nexo.stream.create(name);

        for (let i = 0; i < 2; i++) await publish(name, { i }, { key: 'key-A' });
        for (let i = 0; i < 4; i++) await publish(name, { i }, { key: 'key-B' });

        const startedA: number[] = [];
        const doneB: number[] = [];
        let inFlight = 0;
        let maxInFlight = 0;
        let aSignal: AbortSignal | null = null;
        let aHeld = true;
        let releaseA!: () => void;
        const gateA = new Promise<void>(r => { releaseA = r; });

        const sub = await subscribe(clientA, name, group, async (d: any, meta: any) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            try {
                if (Buffer.from(meta.key ?? []).toString() === 'key-A') {
                    startedA.push(d.i);
                    aSignal = meta.signal;
                    if (aHeld) { aHeld = false; await gateA; }
                } else {
                    doneB.push(d.i);
                }
            } finally {
                inFlight--;
            }
        }, {
            batchSize: 4,
            keyBatchSize: 2,
            concurrency: 2,
            maxBufferedMessages: 8,
            callbackTimeoutMs: 60,
        });

        try {
            await waitFor(() => expect(doneB).toEqual([0, 1, 2, 3]), { timeout: 10000 });
            await waitFor(() => expect(aSignal?.aborted).toBe(true));
            expect(startedA).toEqual([0]);
            expect(maxInFlight).toBeLessThanOrEqual(2);
        } finally {
            releaseA();
            await sub.stop();
        }
        await nexo.stream.delete(name);
    });

    it('should keep a local key gate across seek while an old callback ignores cancellation', async () => {
        const name = `stream-seek-key-gate-${randomUUID()}`;
        const group = 'seek-key-gate-group';
        await nexo.stream.create(name);
        await publish(name, { i: 1 }, { key: 'key-A' });
        await publish(name, { i: 2 }, { key: 'key-B' });

        const aStarted: number[] = [];
        let doneB = 0;
        let aInFlight = 0;
        let aPeak = 0;
        let firstA = true;
        let aSignal: AbortSignal | null = null;
        let releaseA!: () => void;
        const gateA = new Promise<void>(r => { releaseA = r; });

        const sub = await subscribe(clientA, name, group, async (_d: any, meta: any) => {
            if (Buffer.from(meta.key ?? []).toString() === 'key-A') {
                aInFlight++;
                aPeak = Math.max(aPeak, aInFlight);
                try {
                    aStarted.push(Number(meta.seq));
                    if (firstA) {
                        firstA = false;
                        aSignal = meta.signal;
                        await gateA;
                    }
                } finally {
                    aInFlight--;
                }
            } else {
                doneB++;
            }
        }, {
            batchSize: 4,
            keyBatchSize: 2,
            concurrency: 2,
            callbackTimeoutMs: 30000,
        });

        try {
            await waitFor(() => expect(aStarted).toEqual([1]), { timeout: 10000 });
            await waitFor(() => expect(doneB).toBe(1), { timeout: 10000 });

            await seek(clientA, name, group, 'beginning');
            await waitFor(() => expect(doneB).toBe(2), { timeout: 10000 });
            await waitFor(() => expect(aSignal?.aborted).toBe(true), { timeout: 10000 });
            expect(aStarted).toEqual([1]);
            expect(aPeak).toBe(1);
            expect(sub.error ?? null).toBeNull();

            releaseA();
            await waitFor(() => expect(aStarted).toHaveLength(2), { timeout: 10000 });
            expect(aPeak).toBe(1);
        } finally {
            releaseA();
            await sub.stop();
            await nexo.stream.delete(name);
        }
    });
});

describe('decode fetch page', () => {
    const LIMITS: FetchPageLimits = {
        maxMessages: 10,
        maxKeyMessages: 5,
        maxKeyBatches: 4,
        maxBytes: 1 << 20,
    };
    const PAYLOAD = Buffer.concat([Buffer.from([DataType.STRING]), Buffer.from('x', 'utf8')]);

    function writePage(
        writer: (w: FrameWriter) => void,
        leaseMs: number = 30000,
    ): Buffer {
        const w = new FrameWriter().begin();
        w.u64(leaseMs);
        writer(w);
        return w.finish().subarray(HEADER_SIZE);
    }

    function writeBatch(w: FrameWriter, key: Buffer | null, seqs: bigint[], receipt = randomUUID()) {
        w.uuid(receipt);
        const k = key ?? Buffer.alloc(0);
        w.u16(k.length);
        if (k.length > 0) w.bytes(k);
        w.u32(seqs.length);
        for (const seq of seqs) w.u64(seq).u64(0).u32(PAYLOAD.length).bytes(PAYLOAD);
    }

    function page(buf: Buffer, limits: FetchPageLimits = LIMITS) {
        return decodeFetchPage<any>(new Cursor(buf), limits);
    }

    it('decode fetch page: decodes a valid mixed page', () => {
        const buf = writePage(w => {
            w.u32(3);
            writeBatch(w, Buffer.from('a'), [1n, 2n]);
            writeBatch(w, Buffer.from('b'), [3n]);
            writeBatch(w, null, [4n]);
        });
        const p = page(buf);
        expect(p.leaseMs).toBe(30000);
        expect(p.batches.map(b => b.messages.map(m => m.seq))).toEqual([[1n, 2n], [3n], [4n]]);
        expect(p.batches[2].key).toBeUndefined();
        expect(p.batches[2].messages[0].data).toBe('x');
    });

    it('decode fetch page: rejects an oversized response', () => {
        const buf = writePage(w => w.u32(0));
        expect(() => page(buf, { ...LIMITS, maxBytes: 4 })).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects an invalid lease_ms', () => {
        expect(() => page(writePage(w => w.u32(0), 0))).toThrow(ProtocolError);
        expect(() => page(writePage(w => w.u32(0), 0x100000000))).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects a batch count beyond the payload', () => {
        const buf = writePage(w => {
            w.u32(2);
            writeBatch(w, Buffer.from('a'), [1n]);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects a zero-message batch', () => {
        const buf = writePage(w => {
            w.u32(1);
            writeBatch(w, Buffer.from('a'), []);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects a non-singleton keyless batch', () => {
        const buf = writePage(w => {
            w.u32(1);
            writeBatch(w, null, [1n, 2n]);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects a keyed batch above max_key_messages', () => {
        const buf = writePage(w => {
            w.u32(1);
            writeBatch(w, Buffer.from('a'), [1n, 2n, 3n, 4n, 5n, 6n]);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects more keyed batches than requested', () => {
        const buf = writePage(w => {
            w.u32(5);
            for (let i = 0; i < 5; i++) writeBatch(w, Buffer.from([i + 1]), [BigInt(i + 1)]);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects more messages than requested', () => {
        const buf = writePage(w => {
            w.u32(1);
            writeBatch(w, Buffer.from('a'), [1n, 2n, 3n, 4n]);
        });
        expect(() => page(buf, { ...LIMITS, maxMessages: 3, maxKeyBatches: 1 })).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects a message count beyond the payload', () => {
        const buf = writePage(w => {
            w.u32(1);
            w.uuid(randomUUID()).u16(1).bytes(Buffer.from('a')).u32(3);
            w.u64(1n).u64(0).u32(PAYLOAD.length).bytes(PAYLOAD);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects out-of-order keyed seqs', () => {
        const buf = writePage(w => {
            w.u32(1);
            writeBatch(w, Buffer.from('a'), [2n, 1n]);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects duplicate receipts and duplicate keys', () => {
        const dup = randomUUID().replace(/-/g, '');
        const buf = writePage(w => {
            w.u32(2);
            writeBatch(w, Buffer.from('a'), [1n], dup);
            writeBatch(w, Buffer.from('b'), [2n], dup);
        });
        expect(() => page(buf)).toThrow(ProtocolError);
        const buf2 = writePage(w => {
            w.u32(2);
            writeBatch(w, Buffer.from('a'), [1n]);
            writeBatch(w, Buffer.from('a'), [2n]);
        });
        expect(() => page(buf2)).toThrow(ProtocolError);
    });

    it('decode fetch page: rejects trailing bytes', () => {
        const buf = writePage(w => {
            w.u32(1);
            writeBatch(w, null, [1n]);
        });
        expect(() => page(Buffer.concat([buf, Buffer.from([0])]))).toThrow(ProtocolError);
        const empty = writePage(w => w.u32(0));
        expect(() => page(Buffer.concat([empty, Buffer.from([0])]))).toThrow(ProtocolError);
    });

    it('decode fetch page: detaches key and raw payload from the page backing store', () => {
        const raw = Buffer.concat([Buffer.from([DataType.RAW]), Buffer.alloc(64, 7)]);
        const inner = writePage(w => {
            w.u32(1);
            w.uuid(randomUUID()).u16(1).bytes(Buffer.from('a')).u32(1);
            w.u64(1n).u64(0).u32(raw.length).bytes(raw);
        });
        const slab = Buffer.alloc(64 * 1024);
        inner.copy(slab, 0);
        const p = page(slab.subarray(0, inner.length));
        const key = p.batches[0].key!;
        expect(Buffer.from(key).toString()).toBe('a');
        expect(key.buffer.byteLength).toBeLessThan(slab.length);
        const data = p.batches[0].messages[0].data as Uint8Array;
        expect(data.length).toBe(64);
        expect(data.buffer.byteLength).toBeLessThan(slab.length);
    });
});

class FakeStreamConnection {
    isConnected = true;
    joins: string[] = [];
    leaves: { consumer: string; epoch: string }[] = [];
    updates: {
        consumer: string;
        epoch: string;
        items: { receipt: string; completed: number; flags: number }[];
    }[] = [];
    fetches: {
        maxMessages: number;
        maxKeyMessages: number;
        maxKeyBatches: number;
        maxBytes: number;
        waitMs: number;
    }[] = [];
    order: StreamOpcode[] = [];
    pendingFetch: { resolve: (v: any) => void; reject: (e: any) => void } | null = null;
    fetchQueue: (Buffer | Error | null)[] = [];
    fetchHandler: (() => Buffer | Error | null | Promise<Buffer | Error | null>) | null = null;
    updateHandler: ((items: { receipt: string; completed: number; flags: number }[]) => Buffer | Promise<Buffer>) | null = null;
    nextJoinEpochs: string[] = [];
    joinHold: Promise<void> | null = null;
    epoch = 'e'.repeat(32);

    async send(opcode: number, build?: (w: FrameWriter) => void) {
        const w = new FrameWriter().begin();
        if (build) build(w);
        const cursor = new Cursor(w.finish(1, opcode).subarray(HEADER_SIZE));
        this.order.push(opcode);
        if (opcode === StreamOpcode.S_JOIN) {
            const epoch = this.nextJoinEpochs.length > 0 ? this.nextJoinEpochs.shift()! : this.epoch;
            this.joins.push(epoch);
            const member = `consumer-${this.joins.length}`;
            if (this.joinHold !== null && this.joins.length > 1) await this.joinHold;
            const out = new FrameWriter().begin().u64(0n).uuid(epoch).string(member).finish(0, opcode);
            return { status: ResponseStatus.DATA, cursor: new Cursor(out.subarray(HEADER_SIZE)) };
        }
        if (opcode === StreamOpcode.S_LEAVE) {
            cursor.readString();
            cursor.readString();
            const consumer = cursor.readString();
            const epoch = cursor.readUUID();
            this.leaves.push({ consumer, epoch });
            const pending = this.pendingFetch;
            if (pending) {
                this.pendingFetch = null;
                pending.reject(new NotMemberError('member left'));
            }
            return { status: ResponseStatus.OK, cursor: new Cursor(Buffer.alloc(0)) };
        }
        if (opcode === StreamOpcode.S_FETCH) {
            cursor.readString();
            cursor.readString();
            cursor.readString();
            cursor.readUUID();
            this.fetches.push({
                maxMessages: cursor.readU32(),
                maxKeyMessages: cursor.readU32(),
                maxKeyBatches: cursor.readU32(),
                maxBytes: cursor.readU32(),
                waitMs: cursor.readU32(),
            });
            const next = this.fetchQueue.length > 0
                ? this.fetchQueue.shift()!
                : this.fetchHandler ? await this.fetchHandler() : null;
            if (next instanceof Error) throw next;
            if (next !== null) {
                return { status: ResponseStatus.DATA, cursor: new Cursor(next) };
            }
            return new Promise((resolve, reject) => {
                this.pendingFetch = { resolve, reject };
            });
        }
        if (opcode === StreamOpcode.S_UPDATE_BATCHES) {
            cursor.readString();
            cursor.readString();
            const consumer = cursor.readString();
            const epoch = cursor.readUUID();
            const count = cursor.readU32();
            const items: { receipt: string; completed: number; flags: number }[] = [];
            for (let i = 0; i < count; i++) {
                items.push({
                    receipt: cursor.readUUID(),
                    completed: cursor.readU32(),
                    flags: cursor.readU8(),
                });
            }
            this.updates.push({ consumer, epoch, items });
            const payload = this.updateHandler
                ? await this.updateHandler(items)
                : new FrameWriter().begin().u32(0).finish(0, 0).subarray(HEADER_SIZE);
            return { status: ResponseStatus.DATA, cursor: new Cursor(payload) };
        }
        throw new Error(`unexpected opcode ${opcode}`);
    }
}

describe('stream continuous executor (mock)', () => {
    const logger = new Logger({ level: 'OFF' });
    const RECEIPT_A = 'a'.repeat(32);
    const RECEIPT_B = 'b'.repeat(32);
    const RECEIPT_C = 'c'.repeat(32);
    const RECEIPT_D = 'd'.repeat(32);
    const EPOCH_2 = '2'.repeat(32);
    const PAYLOAD = Buffer.concat([Buffer.from([DataType.STRING]), Buffer.from('x', 'utf8')]);

    function page(
        batches: { key?: Buffer; seqs: number[]; receipt: string }[],
        leaseMs = 30000,
    ): Buffer {
        const w = new FrameWriter().begin();
        w.u64(leaseMs).u32(batches.length);
        for (const b of batches) {
            w.uuid(b.receipt);
            const k = b.key ?? Buffer.alloc(0);
            w.u16(k.length);
            if (k.length > 0) w.bytes(k);
            w.u32(b.seqs.length);
            for (const seq of b.seqs) {
                w.u64(BigInt(seq)).u64(0).u32(PAYLOAD.length).bytes(PAYLOAD);
            }
        }
        return w.finish(0, 0).subarray(HEADER_SIZE);
    }

    function group(conn: FakeStreamConnection) {
        return new NexoStreamGroup<any>(conn as any, 's', 'g', logger);
    }

    it('renews a held batch lease through the coalesced update channel', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A }], 90));

        let release!: () => void;
        const held = new Promise<void>(r => { release = r; });
        const sub = await group(fake).subscribe(async () => { await held; }, {
            callbackTimeoutMs: 1000,
        });

        await waitFor(() => {
            const renewals = fake.updates.filter(u =>
                u.epoch === fake.epoch &&
                u.items.some(i => i.receipt === RECEIPT_A && i.completed === 0 && i.flags === 0));
            expect(renewals.length).toBeGreaterThanOrEqual(2);
        }, { timeout: 3000 });

        release();
        await waitFor(() => {
            expect(fake.updates.some(u =>
                u.items.some(i => i.receipt === RECEIPT_A && i.completed === 1 && i.flags === 0)
            )).toBe(true);
        });
        await sub.stop();
    });

    it('keeps other keys running when one batch is fenced', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([
            { key: Buffer.from('a'), seqs: [1, 2], receipt: RECEIPT_A },
            { key: Buffer.from('b'), seqs: [10, 11], receipt: RECEIPT_B },
        ], 60));
        fake.fetchQueue.push(page([{ key: Buffer.from('c'), seqs: [12], receipt: RECEIPT_C }], 60));

        let fencedA = false;
        fake.updateHandler = items => {
            const failures = items.filter(i => {
                if (i.receipt === RECEIPT_A && !fencedA) { fencedA = true; return true; }
                return false;
            });
            const w = new FrameWriter().begin().u32(failures.length);
            for (const f of failures) w.uuid(f.receipt).u8(ErrorCode.FENCED);
            return w.finish(0, 0).subarray(HEADER_SIZE);
        };

        const gotA: number[] = [];
        const gotB: number[] = [];
        let releaseA!: () => void;
        const heldA = new Promise<void>(r => { releaseA = r; });
        let firstA = true;
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            const seq = Number(meta.seq);
            if (Buffer.from(meta.key ?? []).toString() === 'a') {
                gotA.push(seq);
                if (firstA) { firstA = false; await heldA; }
            } else {
                gotB.push(seq);
            }
        }, { callbackTimeoutMs: 5000 });

        try {
            await waitFor(() => expect(fencedA).toBe(true), { timeout: 3000 });
            await waitFor(() => expect([...gotB].sort()).toEqual([10, 11, 12]), { timeout: 3000 });
            expect(fake.joins.length).toBe(1);
            releaseA();
            await new Promise(r => setTimeout(r, 50));
            expect(gotA).toEqual([1]);
        } finally {
            releaseA();
            await sub.stop();
        }
    });

    it('rejoins under a new epoch when the session is fenced', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([
            { key: Buffer.from('a'), seqs: [1, 2], receipt: RECEIPT_A },
            { key: Buffer.from('b'), seqs: [10], receipt: RECEIPT_B },
        ]));
        fake.nextJoinEpochs.push(fake.epoch, EPOCH_2);
        let startedA!: () => void;
        const aStarted = new Promise<void>(r => { startedA = r; });
        let phase = 0;
        fake.fetchHandler = async () => {
            if (phase === 0) {
                await aStarted;
                phase = 1;
                return new FencedError('epoch is fenced');
            }
            if (phase === 1) {
                phase = 2;
                return page([{ key: Buffer.from('a'), seqs: [1, 2], receipt: RECEIPT_C }]);
            }
            return null;
        };

        const gotA: number[] = [];
        const gotB: number[] = [];
        let releaseA!: () => void;
        const heldA = new Promise<void>(r => { releaseA = r; });
        let firstA = true;
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            const seq = Number(meta.seq);
            if (Buffer.from(meta.key ?? []).toString() === 'a') {
                gotA.push(seq);
                if (firstA) { firstA = false; startedA(); await heldA; }
            } else {
                gotB.push(seq);
            }
        }, { callbackTimeoutMs: 5000 });

        try {
            await waitFor(() => expect(fake.joins).toEqual([fake.epoch, EPOCH_2]), { timeout: 3000 });
            releaseA();
            await waitFor(() => expect(gotA.length).toBeGreaterThanOrEqual(3));
            expect(gotB).toEqual([10]);
            await new Promise(r => setTimeout(r, 50));
            expect(fake.updates.some(u => u.epoch === EPOCH_2
                && u.items.some(i => i.receipt === RECEIPT_C))).toBe(true);
            expect(fake.updates.every(u => u.epoch !== EPOCH_2
                || u.items.every(i => i.receipt === RECEIPT_C))).toBe(true);
            expect(gotA).toEqual([1, 1, 2]);
        } finally {
            releaseA();
            await sub.stop();
        }
    });

    it('leaves without waiting out the long poll when stopping during a pending fetch', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1, 2], receipt: RECEIPT_A }]));

        const got: number[] = [];
        let release!: () => void;
        const held = new Promise<void>(r => { release = r; });
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            got.push(Number(meta.seq));
            await held;
        }, { batchSize: 4, waitMs: 20000 });

        await waitFor(() => expect(got).toEqual([1]));
        await waitFor(() => expect(fake.pendingFetch !== null).toBe(true));

        const started = Date.now();
        const stopping = sub.stop();
        release();
        await stopping;
        expect(Date.now() - started).toBeLessThan(5000);

        const lastUpdate = fake.order.lastIndexOf(StreamOpcode.S_UPDATE_BATCHES);
        const leaveIdx = fake.order.indexOf(StreamOpcode.S_LEAVE);
        expect(lastUpdate).toBeGreaterThan(-1);
        expect(lastUpdate).toBeLessThan(leaveIdx);
        expect(fake.leaves.length).toBe(1);
        expect(fake.updates.some(u => u.items.some(i =>
            i.receipt === RECEIPT_A && i.completed === 1
            && (i.flags & FLAG_STREAM_S_UPDATE_BATCH_RELEASE) !== 0))).toBe(true);
        expect(got).toEqual([1]);
    });

    it('never exceeds buffered message and byte credits while callbacks are held', async () => {
        const fake = new FakeStreamConnection();
        let pages = 0;
        fake.fetchHandler = () => {
            pages++;
            if (pages > 2) return null;
            const limit = Math.max(1, fake.fetches[fake.fetches.length - 1].maxMessages);
            const seqs = Array.from({ length: Math.min(4, limit) }, (_, i) => i + 1);
            return page(seqs.map(seq => ({ seqs: [seq], receipt: randomUUID().replace(/-/g, '') })));
        };

        let running = 0;
        let maxRunning = 0;
        let calls = 0;
        let releaseAll!: () => void;
        const gate = new Promise<void>(r => { releaseAll = r; });
        const sub = await group(fake).subscribe(async () => {
            calls++;
            running++;
            maxRunning = Math.max(maxRunning, running);
            try { await gate; } finally { running--; }
        }, {
            batchSize: 4,
            keyBatchSize: 2,
            concurrency: 2,
            maxBufferedMessages: 6,
            waitMs: 20000,
        });

        try {
            await waitFor(() => expect(calls).toBeGreaterThanOrEqual(2), { timeout: 3000 });
            await new Promise(r => setTimeout(r, 200));
            expect(fake.fetches.length).toBe(1);
            expect(fake.fetches[0].maxMessages).toBeLessThanOrEqual(6);
            expect(fake.fetches[0].maxBytes).toBeLessThanOrEqual(20 * 1024 * 1024);
            expect(fake.fetches[0].maxKeyBatches).toBeLessThanOrEqual(2);
            expect(maxRunning).toBeLessThanOrEqual(2);

            releaseAll();
            await waitFor(() => expect(fake.fetches.length).toBeGreaterThanOrEqual(2), { timeout: 3000 });
            expect(fake.fetches.length).toBeLessThanOrEqual(8);
            expect(maxRunning).toBeLessThanOrEqual(2);
            const admitted = fake.updates.reduce(
                (n, u) => n + u.items.filter(i => i.completed === 1).length, 0);
            expect(admitted).toBeGreaterThan(0);
        } finally {
            releaseAll();
            await sub.stop();
        }
    });

    it('reserves callback slots before starting runners', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([
            { seqs: [1], receipt: RECEIPT_A },
            { seqs: [2], receipt: RECEIPT_B },
            { seqs: [3], receipt: RECEIPT_C },
            { seqs: [4], receipt: 'd'.repeat(32) },
        ]));

        let running = 0;
        let maxRunning = 0;
        let releaseAll!: () => void;
        const gate = new Promise<void>(r => { releaseAll = r; });
        const sub = await group(fake).subscribe(async () => {
            running++;
            maxRunning = Math.max(maxRunning, running);
            try { await gate; } finally { running--; }
        }, { concurrency: 2, maxBufferedMessages: 8 });

        try {
            await waitFor(() => expect(running).toBeGreaterThanOrEqual(2), { timeout: 3000 });
            await new Promise(r => setTimeout(r, 30));
            expect(maxRunning).toBeLessThanOrEqual(2);
            releaseAll();
            await waitFor(() => {
                const completed = fake.updates.reduce(
                    (n, u) => n + u.items.filter(i => i.completed === 1).length, 0);
                expect(completed).toBe(4);
            }, { timeout: 3000 });
            expect(maxRunning).toBeLessThanOrEqual(2);
        } finally {
            releaseAll();
            await sub.stop();
        }
    });

    it('leaves exactly once and settles closed on a fatal update outcome with a pending fetch', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A }]));
        fake.updateHandler = items => {
            const w = new FrameWriter().begin().u32(items.length);
            for (const f of items) w.uuid(f.receipt).u8(ErrorCode.INVALID_ARGUMENT);
            return w.finish(0, 0).subarray(HEADER_SIZE);
        };

        const sub = await group(fake).subscribe(async () => { }, { callbackTimeoutMs: 5000 });

        await waitFor(() => {
            expect(sub.error).toBeTruthy();
            expect(String(sub.error)).toContain('rejected');
        }, { timeout: 5000 });
        await waitFor(() => expect(fake.leaves.length).toBe(1), { timeout: 3000 });
        await sub.closed;
        await expect(sub.stop()).rejects.toThrow();
    });

    it('does not fail the subscription when a timed-out callback cooperates with abort', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1, 2], receipt: RECEIPT_A }]));
        fake.fetchQueue.push(page([{ key: Buffer.from('b'), seqs: [10], receipt: RECEIPT_B }]));
        fake.fetchQueue.push(page([{ key: Buffer.from('b'), seqs: [11], receipt: RECEIPT_C }]));

        const gotB: number[] = [];
        let signalAborted = false;
        let releaseA!: () => void;
        const gateA = new Promise<void>(r => { releaseA = r; });
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            const key = Buffer.from(meta.key ?? []).toString();
            if (key === 'a') {
                await Promise.race([
                    gateA,
                    new Promise<void>(r => {
                        meta.signal.addEventListener('abort', () => { signalAborted = true; r(); });
                    }),
                ]);
            } else {
                gotB.push(Number(meta.seq));
            }
        }, { concurrency: 1, callbackTimeoutMs: 40 });

        try {
            await waitFor(() => expect(signalAborted).toBe(true), { timeout: 3000 });
            await waitFor(() => expect([...gotB].sort()).toEqual([10, 11]), { timeout: 3000 });
            await waitFor(() => {
                expect(fake.updates.some(u => u.items.some(i =>
                    i.receipt === RECEIPT_A && i.completed === 0
                    && (i.flags & FLAG_STREAM_S_UPDATE_BATCH_RETRY) !== 0))).toBe(true);
            }, { timeout: 3000 });
            releaseA();
            await new Promise(r => setTimeout(r, 80));
            expect(sub.error).toBeNull();
            expect(fake.updates.every(u => u.items.every(i =>
                i.receipt !== RECEIPT_A || i.completed === 0))).toBe(true);
        } finally {
            releaseA();
            await sub.stop();
        }
    });

    it('fails on a duplicate outcome receipt in an update response', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([
            { key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A },
            { key: Buffer.from('b'), seqs: [2], receipt: RECEIPT_B },
        ], 60));
        let first = true;
        let releaseUpdates!: () => void;
        const updateGate = new Promise<Buffer>(r => {
            releaseUpdates = () => r(new FrameWriter().begin().u32(0).finish(0, 0).subarray(HEADER_SIZE));
        });
        fake.updateHandler = items => {
            if (first) {
                first = false;
                return updateGate;
            }
            const w = new FrameWriter().begin().u32(items.length);
            for (const _f of items) w.uuid(items[0].receipt).u8(ErrorCode.FENCED);
            return w.finish(0, 0).subarray(HEADER_SIZE);
        };

        const releases: (() => void)[] = [];
        const sub = await group(fake).subscribe(async () => {
            await new Promise<void>(r => releases.push(r));
        }, { concurrency: 2, callbackTimeoutMs: 60000 });

        try {
            await waitFor(() => expect(fake.updates.length).toBe(1), { timeout: 3000 });
            await new Promise(r => setTimeout(r, 60));
            releaseUpdates();
            await waitFor(() => {
                expect(sub.error).toBeTruthy();
                expect(String(sub.error)).toContain('duplicate');
            }, { timeout: 5000 });
            await expect(sub.stop()).rejects.toThrow();
        } finally {
            releaseUpdates();
            releases.forEach(r => r());
        }
    });

    it('fails the subscription when timed-out callbacks never settle', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([
            { key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A },
            { key: Buffer.from('b'), seqs: [2], receipt: RECEIPT_B },
        ]));

        const releases: (() => void)[] = [];
        let calls = 0;
        const sub = await group(fake).subscribe(async () => {
            calls++;
            await new Promise<void>(r => releases.push(r));
        }, { concurrency: 2, callbackTimeoutMs: 40 });

        try {
            await waitFor(() => {
                expect(sub.error).toBeTruthy();
                expect(String(sub.error)).toContain('capacity exhausted');
            }, { timeout: 5000 });
            expect(calls).toBe(2);
            await waitFor(() => expect(fake.leaves.length).toBe(1), { timeout: 3000 });
            await sub.closed;

            const started = Date.now();
            await expect(sub.stop()).rejects.toThrow('capacity exhausted');
            expect(Date.now() - started).toBeLessThan(1000);
        } finally {
            releases.forEach(r => r());
        }
    });

    it('keeps capacity accounting correct when an abort races callback settlement', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A }], 90));
        fake.fetchQueue.push(page([{ key: Buffer.from('b'), seqs: [10], receipt: RECEIPT_B }], 90));
        let fencedA = false;
        fake.updateHandler = items => {
            const failures = items.filter(i => {
                if (i.receipt === RECEIPT_A && !fencedA) { fencedA = true; return true; }
                return false;
            });
            const w = new FrameWriter().begin().u32(failures.length);
            for (const f of failures) w.uuid(f.receipt).u8(ErrorCode.FENCED);
            return w.finish(0, 0).subarray(HEADER_SIZE);
        };

        const got: number[] = [];
        let releaseA!: () => void;
        const gateA = new Promise<void>(r => { releaseA = r; });
        let firstA = true;
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            if (Buffer.from(meta.key ?? []).toString() === 'a' && firstA) {
                firstA = false;
                meta.signal?.addEventListener('abort', () => releaseA());
                await gateA;
            } else {
                got.push(Number(meta.seq));
            }
        }, { concurrency: 1, callbackTimeoutMs: 60000 });

        try {
            await waitFor(() => expect(fencedA).toBe(true), { timeout: 3000 });
            await waitFor(() => expect(got).toEqual([10]), { timeout: 3000 });
            expect(fake.joins).toEqual([fake.epoch]);
            await new Promise(r => setTimeout(r, 400));
            expect(sub.error).toBeFalsy();
        } finally {
            releaseA();
            await sub.stop();
        }
    });

    it('leaves the member whose join completes while stop is in progress', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(new FencedError('epoch is fenced'));
        fake.nextJoinEpochs.push(fake.epoch, EPOCH_2);
        let releaseJoin!: () => void;
        fake.joinHold = new Promise<void>(r => { releaseJoin = r; });
        const sub = await group(fake).subscribe(() => { });
        await waitFor(() => expect(fake.joins).toEqual([fake.epoch, EPOCH_2]), { timeout: 3000 });

        const stopping = sub.stop();
        await new Promise(r => setTimeout(r, 50));
        expect(fake.leaves.some(l => l.consumer === 'consumer-2')).toBe(false);
        releaseJoin();
        await stopping;

        expect(fake.leaves).toContainEqual({ consumer: 'consumer-2', epoch: EPOCH_2 });
    });

    it('reports an unsettled join during teardown and still leaves the late member', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(new FencedError('epoch is fenced'));
        fake.nextJoinEpochs.push(fake.epoch, EPOCH_2);
        let releaseJoin!: () => void;
        fake.joinHold = new Promise<void>(r => { releaseJoin = r; });
        const sub = await group(fake).subscribe(() => { });
        await waitFor(() => expect(fake.joins).toEqual([fake.epoch, EPOCH_2]), { timeout: 3000 });

        await expect(sub.stop()).rejects.toThrow('Stream join did not settle during teardown');
        await waitFor(() => expect(sub.error).toBeTruthy(), { timeout: 1000 });
        releaseJoin();
        await waitFor(() => expect(fake.leaves)
            .toContainEqual({ consumer: 'consumer-2', epoch: EPOCH_2 }), { timeout: 3000 });
    });

    it('accepts a successor batch before the previous update response', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A }]));
        let releaseFetch!: () => void;
        const fetchGate = new Promise<void>(r => { releaseFetch = r; });
        let releaseReply!: () => void;
        const replyGate = new Promise<void>(r => { releaseReply = r; });
        let fetchesSeen = 0;
        fake.fetchHandler = async () => {
            fetchesSeen += 1;
            if (fetchesSeen === 1) {
                await fetchGate;
                return page([{ key: Buffer.from('a'), seqs: [2], receipt: RECEIPT_B }]);
            }
            return null;
        };
        let first = true;
        fake.updateHandler = async items => {
            if (first) {
                first = false;
                expect(items).toEqual([{ receipt: RECEIPT_A, completed: 1, flags: 0 }]);
                expect(fake.fetches.length).toBe(2);
                releaseFetch();
                await replyGate;
            }
            return new FrameWriter().begin().u32(0).finish(0, 0).subarray(HEADER_SIZE);
        };
        const got: number[] = [];
        const sub = await group(fake).subscribe((_d: any, meta: any) => {
            got.push(Number(meta.seq));
        }, { concurrency: 2 });
        try {
            await waitFor(() => expect(got).toEqual([1, 2]), { timeout: 3000 });
            expect(fake.joins.length).toBe(1);
            expect(sub.error ?? null).toBeNull();
            releaseReply();
            await waitFor(() => expect(fake.updates.some(u =>
                u.items.some(i => i.receipt === RECEIPT_B && i.completed === 1)
            )).toBe(true), { timeout: 3000 });
        } finally {
            releaseReply();
            await sub.stop();
        }
    });

    it('waits for an uncooperative prior callback before starting a regranted key', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([
            { key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A },
            { key: Buffer.from('b'), seqs: [10], receipt: RECEIPT_C },
        ]));
        let releaseRegrant!: () => void;
        const regrantGate = new Promise<void>(r => { releaseRegrant = r; });
        let regrantCalls = 0;
        fake.fetchHandler = async () => {
            if (regrantCalls === 0) {
                regrantCalls = 1;
                await regrantGate;
                return page([{ key: Buffer.from('a'), seqs: [2], receipt: RECEIPT_B }]);
            }
            if (regrantCalls === 1) {
                regrantCalls = 2;
                return page([{ key: Buffer.from('b'), seqs: [11], receipt: RECEIPT_D }]);
            }
            return null;
        };
        let releaseA!: () => void;
        const heldA = new Promise<void>(r => { releaseA = r; });
        let signalA: AbortSignal | undefined;
        let firstA = true;
        let inFlight = 0;
        let peak = 0;
        let aInFlight = 0;
        let aPeak = 0;
        const gotA: number[] = [];
        const gotB: number[] = [];
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            inFlight += 1;
            peak = Math.max(peak, inFlight);
            try {
                const seq = Number(meta.seq);
                if (Buffer.from(meta.key ?? []).toString() === 'a') {
                    aInFlight += 1;
                    aPeak = Math.max(aPeak, aInFlight);
                    try {
                        gotA.push(seq);
                        if (firstA) {
                            firstA = false;
                            signalA = meta.signal;
                            await heldA;
                        }
                    } finally {
                        aInFlight -= 1;
                    }
                } else {
                    gotB.push(seq);
                }
            } finally {
                inFlight -= 1;
            }
        }, { concurrency: 2, callbackTimeoutMs: 5000 });
        try {
            await waitFor(() => expect(signalA).not.toBeUndefined(), { timeout: 3000 });
            await waitFor(() => expect(gotB).toEqual([10]), { timeout: 3000 });
            releaseRegrant();
            await waitFor(() => expect([...gotB].sort()).toEqual([10, 11]), { timeout: 3000 });
            await new Promise(r => setTimeout(r, 100));
            expect(signalA!.aborted).toBe(true);
            expect(gotA).toEqual([1]);
            expect(sub.error ?? null).toBeNull();
            releaseA();
            await waitFor(() => expect(gotA).toEqual([1, 2]), { timeout: 3000 });
            expect(aPeak).toBe(1);
            expect(peak).toBeLessThanOrEqual(2);
            expect(fake.joins.length).toBe(1);
            expect(fake.updates.every(u => u.items.every(i =>
                !(i.receipt === RECEIPT_A && i.completed > 0)))).toBe(true);
            await waitFor(() => expect(fake.updates.some(u =>
                u.items.some(i => i.receipt === RECEIPT_B && i.completed === 1)
            )).toBe(true), { timeout: 3000 });
        } finally {
            releaseA();
            await sub.stop();
        }
    });

    it('waits for an uncooperative prior callback before a regranted key across an epoch reset', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A }]));
        fake.nextJoinEpochs.push(fake.epoch, EPOCH_2);
        let phase = 0;
        let aStarted!: () => void;
        const aStartedGate = new Promise<void>(r => { aStarted = r; });
        fake.fetchHandler = async () => {
            if (phase === 0) {
                await aStartedGate;
                phase = 1;
                return new FencedError('epoch is fenced');
            }
            if (phase === 1) {
                phase = 2;
                return page([
                    { key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_B },
                    { key: Buffer.from('b'), seqs: [10], receipt: RECEIPT_C },
                ]);
            }
            return null;
        };
        const gotA: number[] = [];
        const gotB: number[] = [];
        let releaseA!: () => void;
        const heldA = new Promise<void>(r => { releaseA = r; });
        let signalA: AbortSignal | undefined;
        let firstA = true;
        let aInFlight = 0;
        let aPeak = 0;
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            if (Buffer.from(meta.key ?? []).toString() === 'a') {
                aInFlight += 1;
                aPeak = Math.max(aPeak, aInFlight);
                try {
                    const seq = Number(meta.seq);
                    gotA.push(seq);
                    if (firstA) {
                        firstA = false;
                        signalA = meta.signal;
                        aStarted();
                        await heldA;
                    }
                } finally {
                    aInFlight -= 1;
                }
            } else {
                gotB.push(Number(meta.seq));
            }
        }, { concurrency: 2, callbackTimeoutMs: 5000 });
        try {
            await waitFor(() => expect(fake.joins).toEqual([fake.epoch, EPOCH_2]), { timeout: 3000 });
            await waitFor(() => expect(gotB).toEqual([10]), { timeout: 3000 });
            await new Promise(r => setTimeout(r, 100));
            expect(signalA!.aborted).toBe(true);
            expect(gotA).toEqual([1]);
            expect(sub.error ?? null).toBeNull();
            releaseA();
            await waitFor(() => expect(gotA).toEqual([1, 1]), { timeout: 3000 });
            expect(aPeak).toBe(1);
            expect(fake.updates.every(u => u.epoch !== EPOCH_2
                || u.items.every(i => i.receipt !== RECEIPT_A))).toBe(true);
        } finally {
            releaseA();
            await sub.stop();
        }
    });

    it('releases an unstarted blocked batch during stop without starting it', async () => {
        const fake = new FakeStreamConnection();
        fake.fetchQueue.push(page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_A }]));
        let releaseRegrant!: () => void;
        const regrantGate = new Promise<void>(r => { releaseRegrant = r; });
        let regrantSent = false;
        fake.fetchHandler = async () => {
            if (!regrantSent) {
                regrantSent = true;
                await regrantGate;
                return page([{ key: Buffer.from('a'), seqs: [2], receipt: RECEIPT_B }]);
            }
            return null;
        };
        const gotA: number[] = [];
        let releaseA!: () => void;
        const heldA = new Promise<void>(r => { releaseA = r; });
        let signalA: AbortSignal | undefined;
        let firstA = true;
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            if (Buffer.from(meta.key ?? []).toString() === 'a') {
                gotA.push(Number(meta.seq));
                if (firstA) {
                    firstA = false;
                    signalA = meta.signal;
                    await heldA;
                }
            }
        }, { concurrency: 2, callbackTimeoutMs: 5000 });
        try {
            await waitFor(() => expect(signalA).not.toBeUndefined(), { timeout: 3000 });
            releaseRegrant();
            await waitFor(() => expect(signalA!.aborted).toBe(true), { timeout: 3000 });
            await new Promise(r => setTimeout(r, 100));
            expect(gotA).toEqual([1]);
            await sub.stop();
            expect(gotA).toEqual([1]);
            releaseA();
            await new Promise(r => setTimeout(r, 100));
            expect(gotA).toEqual([1]);
            expect(fake.updates.some(u => u.items.some(i =>
                i.receipt === RECEIPT_B && i.completed === 0
                && (i.flags & FLAG_STREAM_S_UPDATE_BATCH_RELEASE) !== 0
            ))).toBe(true);
        } finally {
            releaseA();
        }
    });

    it('keeps cleanup storage bounded across repeated session resets', async () => {
        const fake = new FakeStreamConnection();
        const epochs = Array.from({ length: 21 }, (_, i) => String(i + 1).padStart(32, '0'));
        fake.nextJoinEpochs.push(...epochs);
        let resets = 0;
        let finalSent = false;
        fake.fetchHandler = () => {
            if (resets >= 20) {
                if (!finalSent) {
                    finalSent = true;
                    return page([{ key: Buffer.from('a'), seqs: [1], receipt: RECEIPT_C }]);
                }
                return null;
            }
            resets++;
            return new FencedError('epoch is fenced');
        };

        const got: number[] = [];
        const sub = await group(fake).subscribe(async (_d: any, meta: any) => {
            got.push(Number(meta.seq));
        });
        try {
            await waitFor(() => expect(fake.joins.length).toBe(21), { timeout: 8000 });
            await waitFor(() => expect(got).toEqual([1]), { timeout: 3000 });
            await waitFor(() => expect(fake.leaves.length).toBe(20), { timeout: 3000 });
            await waitFor(() => expect(fake.updates.some(u =>
                u.epoch === epochs[20]
                && u.items.some(i => i.receipt === RECEIPT_C))).toBe(true), { timeout: 3000 });
            expect(fake.updates.every(u => epochs.includes(u.epoch))).toBe(true);
            expect(sub.error).toBeFalsy();
        } finally {
            await sub.stop();
        }
    });
});
