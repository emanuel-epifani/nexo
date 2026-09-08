import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '../../src/client';
import { SlowConsumerError } from '../../src/errors';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('PUBSUB', () => {
    it('should handle Exact Matches and ignore noise', async () => {
        const targetTopic = `chat/room1-${randomUUID()}`;
        const noiseTopic = `chat/room2-${randomUUID()}`;

        const received: any[] = [];

        // Subscribe only to target
        const sub = await nexo.pubsub.topic(targetTopic).subscribe((data) => received.push(data));

        // Publish to target and noise
        await nexo.pubsub.topic(targetTopic).publish({ msg: 'target' });
        await nexo.pubsub.topic(noiseTopic).publish({ msg: 'noise' });

        // Verify: should receive ONLY target message
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0].msg).toBe('target');

        await sub.stop();
    });

    it('should handle Single-Level Wildcard (+) with strict isolation', async () => {
        // Pattern: home/+/temp
        // Should match: home/kitchen/temp
        // Should NOT match: home/kitchen/light (different suffix)
        // Should NOT match: home/kitchen/cupboard/temp (extra level)
        const baseId = randomUUID();
        const pattern = `home-${baseId}/+/temp`;

        const received: any[] = [];
        const sub = await nexo.pubsub.pattern(pattern).subscribe((data) => received.push(data));

        // Positive cases
        await nexo.pubsub.topic(`home-${baseId}/kitchen/temp`).publish({ id: 'match-1' });
        await nexo.pubsub.topic(`home-${baseId}/garage/temp`).publish({ id: 'match-2' });

        // Negative cases
        await nexo.pubsub.topic(`home-${baseId}/kitchen/light`).publish({ id: 'fail-suffix' });
        await nexo.pubsub.topic(`home-${baseId}/kitchen/cupboard/temp`).publish({ id: 'fail-deep' });
        await nexo.pubsub.topic(`office-${baseId}/kitchen/temp`).publish({ id: 'fail-prefix' });

        // Verify
        await waitFor(() => expect(received.length).toBe(2));
        const ids = received.map(r => r.id).sort();
        expect(ids).toEqual(['match-1', 'match-2']);

        await sub.stop();
    });

    it('should clear retained messages', async () => {
        const topic = `clear-retained-${randomUUID()}`;

        // Publish retained value
        await nexo.pubsub.topic<string>(topic).publish('dark', { retain: true });

        // First subscriber receives retained
        const received: string[] = [];
        const firstSub = await nexo.pubsub.topic<string>(topic).subscribe((data) => received.push(data));
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('dark');

        // Clear retained
        await nexo.pubsub.topic<string>(topic).clearRetained();

        // Stop and resubscribe - should not receive retained
        await firstSub.stop();
        const afterClear: string[] = [];
        const secondSub = await nexo.pubsub.topic<string>(topic).subscribe((data) => afterClear.push(data));
        await new Promise(r => setTimeout(r, 200));
        expect(afterClear).toEqual([]);
        await secondSub.stop();
    });

    it('should reject invalid ttl values', async () => {
        const topic = `ttl-invalid-${randomUUID()}`;

        // Negative ttl
        await expect(nexo.pubsub.topic(topic).publish('x', { ttl: -1 }))
            .rejects.toThrow(/Invalid ttl/);

        // Non-integer ttl
        await expect(nexo.pubsub.topic(topic).publish('x', { ttl: 1.5 }))
            .rejects.toThrow(/Invalid ttl/);

        // Out of u32 range
        await expect(nexo.pubsub.topic(topic).publish('x', { ttl: 0xFFFFFFFF + 1 }))
            .rejects.toThrow();
    });

    it('should handle Multi-Level Wildcard (#) correctly', async () => {
        // Pattern: sensors/#
        // Should match: sensors/temp
        // Should match: sensors/floor1/room2/temp (nested)
        // Should NOT match: other/sensors/temp (different prefix)
        const baseId = randomUUID();
        const pattern = `sensors-${baseId}/#`;

        const received: any[] = [];
        const sub = await nexo.pubsub.pattern(pattern).subscribe((data) => received.push(data));

        // Positive cases
        await nexo.pubsub.topic(`sensors-${baseId}/main`).publish({ id: 'root' });
        await nexo.pubsub.topic(`sensors-${baseId}/a/b/c`).publish({ id: 'deep' });

        // Negative cases
        await nexo.pubsub.topic(`other-${baseId}/main`).publish({ id: 'fail-prefix' });

        await waitFor(() => expect(received.length).toBe(2));
        const ids = received.map(r => r.id).sort();
        expect(ids).toEqual(['deep', 'root']);

        await sub.stop();
    });

    it('should support async callbacks', async () => {
        const topic = `async-cb-${randomUUID()}`;
        const received: any[] = [];

        const sub = await nexo.pubsub.topic(topic).subscribe(async (data) => {
            await new Promise(r => setTimeout(r, 10));
            received.push(data);
        });

        await nexo.pubsub.topic(topic).publish({ msg: 'hello' });
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0].msg).toBe('hello');

        await sub.stop();
    });

    it('should not block other operations when callback is slow', async () => {
        const pubsubTopic = `slow-cb-${randomUUID()}`;
        const storeKey = `store-key-${randomUUID()}`;
        let callbackStarted = false;

        const sub = await nexo.pubsub.topic(pubsubTopic).subscribe(async () => {
            callbackStarted = true;
            await new Promise(r => setTimeout(r, 500));
        });

        await nexo.pubsub.topic(pubsubTopic).publish({ msg: 'trigger' });
        await waitFor(() => expect(callbackStarted).toBe(true));

        const t0 = Date.now();
        await nexo.store.map.set(storeKey, 'value');
        const elapsed = Date.now() - t0;

        expect(elapsed).toBeLessThan(300);
        await sub.stop();
    });

    it('should run parallel subscriptions independently', async () => {
        const topicA = `par-a-${randomUUID()}`;
        const topicB = `par-b-${randomUUID()}`;
        const order: string[] = [];

        const subA = await nexo.pubsub.topic(topicA).subscribe(async () => {
            await new Promise(r => setTimeout(r, 100));
            order.push('a');
        });
        const subB = await nexo.pubsub.topic(topicB).subscribe(async () => {
            order.push('b');
        });

        await nexo.pubsub.topic(topicA).publish({ msg: 'x' });
        await nexo.pubsub.topic(topicB).publish({ msg: 'y' });

        await waitFor(() => expect(order.length).toBe(2));
        expect(order).toEqual(['b', 'a']);

        await subA.stop();
        await subB.stop();
    });

    // ── Edge cases ──────────────────────────────────────────────

    it('should deliver retained message to new subscriber', async () => {
        const topic = `retained-new-${randomUUID()}`;

        await nexo.pubsub.topic<string>(topic).publish('retained-value', { retain: true });

        const received: string[] = [];
        const sub = await nexo.pubsub.topic<string>(topic).subscribe((data) => received.push(data));
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('retained-value');

        await sub.stop();
        await nexo.pubsub.topic(topic).clearRetained();
    });

    it('should overwrite retained message on second publish', async () => {
        const topic = `retained-overwrite-${randomUUID()}`;

        await nexo.pubsub.topic<string>(topic).publish('first', { retain: true });
        await nexo.pubsub.topic<string>(topic).publish('second', { retain: true });

        const received: string[] = [];
        const sub = await nexo.pubsub.topic<string>(topic).subscribe((data) => received.push(data));
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('second');

        await sub.stop();
        await nexo.pubsub.topic(topic).clearRetained();
    });

    it('should not deliver retained message after TTL expiry', async () => {
        const topic = `retained-ttl-${randomUUID()}`;

        await nexo.pubsub.topic<string>(topic).publish('temp-retained', { retain: true, ttl: 1 });

        // Wait for TTL to expire
        await new Promise(r => setTimeout(r, 1200));

        const received: string[] = [];
        const sub = await nexo.pubsub.topic<string>(topic).subscribe((data) => received.push(data));
        await new Promise(r => setTimeout(r, 300));
        expect(received).toEqual([]);

        await sub.stop();
    });

    it('should stop delivery after subscription stop', async () => {
        const topic = `unsub-stop-${randomUUID()}`;

        const received: any[] = [];
        const sub = await nexo.pubsub.topic(topic).subscribe((data) => received.push(data));

        await nexo.pubsub.topic(topic).publish({ msg: 'before' });
        await waitFor(() => expect(received.length).toBe(1));

        await sub.stop();

        await nexo.pubsub.topic(topic).publish({ msg: 'after' });
        await new Promise(r => setTimeout(r, 300));
        expect(received.length).toBe(1);
    });

    it('should isolate two local listeners on the same pattern', async () => {
        const baseId = randomUUID();
        const pattern = `local-${baseId}/+`;
        const topic = nexo.pubsub.topic(`local-${baseId}/value`);
        const receivedA: string[] = [];
        const receivedB: string[] = [];
        const concreteTopics: string[] = [];

        const subA = await nexo.pubsub.pattern<string>(pattern).subscribe(data => receivedA.push(data));
        const subB = await nexo.pubsub.pattern<string>(pattern).subscribe((data, meta) => {
            receivedB.push(data);
            concreteTopics.push(meta.topic);
        });
        expect(subA.active).toBe(true);
        expect(subB.active).toBe(true);

        await topic.publish('both');
        await waitFor(() => {
            expect(receivedA).toEqual(['both']);
            expect(receivedB).toEqual(['both']);
            expect(concreteTopics).toEqual([topic.name]);
        });

        await subA.stop();
        await subA.stop();
        await subA.closed;
        expect(subA.active).toBe(false);
        expect(subB.active).toBe(true);

        await topic.publish('only-b');
        await waitFor(() => expect(receivedB).toEqual(['both', 'only-b']));
        expect(receivedA).toEqual(['both']);

        await subB.stop();
        await subB.closed;
    });

    it('should stop only the overflowing local listener', async () => {
        const topic = nexo.pubsub.topic<string>(`overflow-${randomUUID()}`);
        let release!: () => void;
        let markStarted!: () => void;
        const blocker = new Promise<void>(resolve => { release = resolve; });
        const started = new Promise<void>(resolve => { markStarted = resolve; });
        const sub = await topic.subscribe(async () => {
            markStarted();
            await blocker;
        }, { queueCapacity: 1 });

        await topic.publish('one');
        await started;
        await topic.publish('two');
        await topic.publish('three');

        await waitFor(() => expect(sub.active).toBe(false));
        expect(sub.error).toBeInstanceOf(SlowConsumerError);
        release();
        await sub.closed;
    });

    it('should match combined wildcards a/+/b/#', async () => {
        const baseId = randomUUID();
        const pattern = `combo-${baseId}/+/b/#`;

        const received: any[] = [];
        const sub = await nexo.pubsub.pattern(pattern).subscribe((data) => received.push(data));

        // Positive: a/x/b/y/z matches
        await nexo.pubsub.topic(`combo-${baseId}/x/b/y/z`).publish({ id: 'deep-match' });
        // Positive: a/x/b matches (+ matches x, # matches zero levels)
        await nexo.pubsub.topic(`combo-${baseId}/x/b`).publish({ id: 'shallow-match' });

        // Negative: a/x/c/y — b is not at position 2
        await nexo.pubsub.topic(`combo-${baseId}/x/c/y`).publish({ id: 'fail-wrong-segment' });

        await waitFor(() => expect(received.length).toBe(2));
        const ids = received.map(r => r.id).sort();
        expect(ids).toEqual(['deep-match', 'shallow-match']);

        await sub.stop();
    });

    it('should broadcast to 3+ subscribers on same topic', async () => {
        const topic = `broadcast-${randomUUID()}`;

        const client1 = await NexoClient.connect();
        const client2 = await NexoClient.connect();

        const recv1: any[] = [];
        const recv2: any[] = [];
        const recv3: any[] = [];

        const sub1 = await client1.pubsub.topic(topic).subscribe((d) => recv1.push(d));
        const sub2 = await client2.pubsub.topic(topic).subscribe((d) => recv2.push(d));
        const sub3 = await nexo.pubsub.topic(topic).subscribe((d) => recv3.push(d));

        await nexo.pubsub.topic(topic).publish({ msg: 'broadcast' });

        await waitFor(() => {
            expect(recv1.length).toBe(1);
            expect(recv2.length).toBe(1);
            expect(recv3.length).toBe(1);
        });
        expect(recv1[0].msg).toBe('broadcast');
        expect(recv2[0].msg).toBe('broadcast');
        expect(recv3[0].msg).toBe('broadcast');

        await sub1.stop();
        await sub2.stop();
        await sub3.stop();
        await client1.disconnect();
        await client2.disconnect();
    });

    it('should clean up subscriber on disconnect without breaking topic', async () => {
        const topic = `disconnect-cleanup-${randomUUID()}`;

        const tempClient = await NexoClient.connect();
        await tempClient.pubsub.topic(topic).subscribe(() => { });
        await new Promise(r => setTimeout(r, 100));

        // Disconnect temp client — server should clean up its subscription
        await tempClient.disconnect();
        await new Promise(r => setTimeout(r, 300));

        // A new subscriber should still receive messages normally
        const received: any[] = [];
        const sub = await nexo.pubsub.topic(topic).subscribe((d) => received.push(d));
        await nexo.pubsub.topic(topic).publish({ msg: 'after-disconnect' });

        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0].msg).toBe('after-disconnect');

        await sub.stop();
    });

    it('should deliver retained messages to wildcard + subscriber', async () => {
        const baseId = randomUUID();

        await nexo.pubsub.topic(`ret-plus-${baseId}/x`).publish('val-x', { retain: true });
        await nexo.pubsub.topic(`ret-plus-${baseId}/y`).publish('val-y', { retain: true });

        const received: string[] = [];
        const sub = await nexo.pubsub.pattern<string>(`ret-plus-${baseId}/+`).subscribe((d) => received.push(d));

        await waitFor(() => expect(received.length).toBe(2));
        expect(received.sort()).toEqual(['val-x', 'val-y']);

        await sub.stop();
        await nexo.pubsub.topic(`ret-plus-${baseId}/x`).clearRetained();
        await nexo.pubsub.topic(`ret-plus-${baseId}/y`).clearRetained();
    });

    it('should deliver retained messages to wildcard # subscriber', async () => {
        const baseId = randomUUID();

        await nexo.pubsub.topic(`ret-hash-${baseId}/a/b/c`).publish('deep', { retain: true });

        const received: string[] = [];
        const sub = await nexo.pubsub.pattern<string>(`ret-hash-${baseId}/#`).subscribe((d) => received.push(d));

        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('deep');

        await sub.stop();
        await nexo.pubsub.topic(`ret-hash-${baseId}/a/b/c`).clearRetained();
    });

    it('should not affect other subscribers when one is disconnected by server (slow consumer)', async () => {
        const topic = `slow-consumer-${randomUUID()}`;

        // Fast subscriber on shared client
        const fastReceived: any[] = [];
        const fastSub = await nexo.pubsub.topic(topic).subscribe((d) => fastReceived.push(d));

        // "Slow" subscriber on a separate client — server will disconnect it
        const slowClient = await NexoClient.connect();
        const slowReceived: any[] = [];
        const slowSub = await slowClient.pubsub.topic(topic).subscribe((d) => slowReceived.push(d));

        // Publish a message — both should receive
        await nexo.pubsub.topic(topic).publish({ msg: 'first' });
        await waitFor(() => expect(fastReceived.length).toBe(1));
        await waitFor(() => expect(slowReceived.length).toBe(1));

        // Simulate server-initiated disconnect (as would happen for slow consumer)
        (slowClient as any).conn.socket.destroy();
        await waitFor(() => expect((slowClient as any).conn.isConnected).toBe(false), { timeout: 3000 });

        // Fast subscriber should still receive messages
        await nexo.pubsub.topic(topic).publish({ msg: 'second' });
        await waitFor(() => expect(fastReceived.length).toBe(2));
        expect(fastReceived[1].msg).toBe('second');

        // Slow client should auto-reconnect and resubscribe
        await waitFor(() => expect((slowClient as any).conn.isConnected).toBe(true), { timeout: 5000 });
        await new Promise(r => setTimeout(r, 500)); // allow resubscribe

        await nexo.pubsub.topic(topic).publish({ msg: 'after-reconnect' });
        await waitFor(() => expect(slowReceived.some(r => r.msg === 'after-reconnect')).toBe(true), { timeout: 5000 });

        await fastSub.stop();
        await slowSub.stop();
        await slowClient.disconnect();
    });
});
