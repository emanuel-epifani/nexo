import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '../../src/client';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('PUBSUB', () => {
    it('should handle Exact Matches and ignore noise', async () => {
        const targetTopic = `chat/room1-${randomUUID()}`;
        const noiseTopic = `chat/room2-${randomUUID()}`;

        const received: any[] = [];

        // Subscribe only to target
        await nexo.pubsub(targetTopic).subscribe((data) => received.push(data));

        // Publish to target and noise
        await nexo.pubsub(targetTopic).publish({ msg: 'target' });
        await nexo.pubsub(noiseTopic).publish({ msg: 'noise' });

        // Verify: should receive ONLY target message
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0].msg).toBe('target');

        await nexo.pubsub(targetTopic).unsubscribe();
    });

    it('should handle Single-Level Wildcard (+) with strict isolation', async () => {
        // Pattern: home/+/temp
        // Should match: home/kitchen/temp
        // Should NOT match: home/kitchen/light (different suffix)
        // Should NOT match: home/kitchen/cupboard/temp (extra level)
        const baseId = randomUUID();
        const pattern = `home-${baseId}/+/temp`;

        const received: any[] = [];
        await nexo.pubsub(pattern).subscribe((data) => received.push(data));

        // Positive cases
        await nexo.pubsub(`home-${baseId}/kitchen/temp`).publish({ id: 'match-1' });
        await nexo.pubsub(`home-${baseId}/garage/temp`).publish({ id: 'match-2' });

        // Negative cases
        await nexo.pubsub(`home-${baseId}/kitchen/light`).publish({ id: 'fail-suffix' });
        await nexo.pubsub(`home-${baseId}/kitchen/cupboard/temp`).publish({ id: 'fail-deep' });
        await nexo.pubsub(`office-${baseId}/kitchen/temp`).publish({ id: 'fail-prefix' });

        // Verify
        await waitFor(() => expect(received.length).toBe(2));
        const ids = received.map(r => r.id).sort();
        expect(ids).toEqual(['match-1', 'match-2']);

        await nexo.pubsub(pattern).unsubscribe();
    });

    it('should clear retained messages', async () => {
        const topic = `clear-retained-${randomUUID()}`;

        // Publish retained value
        await nexo.pubsub<string>(topic).publish('dark', { retain: true });

        // First subscriber receives retained
        const received: string[] = [];
        await nexo.pubsub<string>(topic).subscribe((data) => received.push(data));
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('dark');

        // Clear retained
        await nexo.pubsub<string>(topic).clear();

        // Unsubscribe and resubscribe - should not receive retained
        await nexo.pubsub(topic).unsubscribe();
        const afterClear: string[] = [];
        await nexo.pubsub<string>(topic).subscribe((data) => afterClear.push(data));
        await new Promise(r => setTimeout(r, 200));
        expect(afterClear).toEqual([]);
    });

    it('should reject invalid ttl values', async () => {
        const topic = `ttl-invalid-${randomUUID()}`;

        // Negative ttl
        await expect(nexo.pubsub(topic).publish('x', { ttl: -1 }))
            .rejects.toThrow(/Invalid ttl/);

        // Non-integer ttl
        await expect(nexo.pubsub(topic).publish('x', { ttl: 1.5 }))
            .rejects.toThrow(/Invalid ttl/);

        // Out of u32 range
        await expect(nexo.pubsub(topic).publish('x', { ttl: 0xFFFFFFFF + 1 }))
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
        await nexo.pubsub(pattern).subscribe((data) => received.push(data));

        // Positive cases
        await nexo.pubsub(`sensors-${baseId}/main`).publish({ id: 'root' });
        await nexo.pubsub(`sensors-${baseId}/a/b/c`).publish({ id: 'deep' });

        // Negative cases
        await nexo.pubsub(`other-${baseId}/main`).publish({ id: 'fail-prefix' });

        await waitFor(() => expect(received.length).toBe(2));
        const ids = received.map(r => r.id).sort();
        expect(ids).toEqual(['deep', 'root']);

        await nexo.pubsub(pattern).unsubscribe();
    });

    it('should support async callbacks', async () => {
        const topic = `async-cb-${randomUUID()}`;
        const received: any[] = [];

        await nexo.pubsub(topic).subscribe(async (data) => {
            await new Promise(r => setTimeout(r, 10));
            received.push(data);
        });

        await nexo.pubsub(topic).publish({ msg: 'hello' });
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0].msg).toBe('hello');

        await nexo.pubsub(topic).unsubscribe();
    });

    it('should not block other operations when callback is slow', async () => {
        const pubsubTopic = `slow-cb-${randomUUID()}`;
        const storeKey = `store-key-${randomUUID()}`;
        let callbackStarted = false;

        await nexo.pubsub(pubsubTopic).subscribe(async (data) => {
            callbackStarted = true;
            await new Promise(r => setTimeout(r, 500));
        });

        await nexo.pubsub(pubsubTopic).publish({ msg: 'trigger' });
        await waitFor(() => expect(callbackStarted).toBe(true));

        const t0 = Date.now();
        await nexo.store.map.set(storeKey, 'value');
        const elapsed = Date.now() - t0;

        expect(elapsed).toBeLessThan(300);
        await nexo.pubsub(pubsubTopic).unsubscribe();
    });

    it('should run parallel subscriptions independently', async () => {
        const topicA = `par-a-${randomUUID()}`;
        const topicB = `par-b-${randomUUID()}`;
        const order: string[] = [];

        await nexo.pubsub(topicA).subscribe(async (data) => {
            await new Promise(r => setTimeout(r, 100));
            order.push('a');
        });
        await nexo.pubsub(topicB).subscribe(async (data) => {
            order.push('b');
        });

        await nexo.pubsub(topicA).publish({ msg: 'x' });
        await nexo.pubsub(topicB).publish({ msg: 'y' });

        await waitFor(() => expect(order.length).toBe(2));
        expect(order).toEqual(['b', 'a']);

        await nexo.pubsub(topicA).unsubscribe();
        await nexo.pubsub(topicB).unsubscribe();
    });

    // ── Edge cases ──────────────────────────────────────────────

    it('should deliver retained message to new subscriber', async () => {
        const topic = `retained-new-${randomUUID()}`;

        await nexo.pubsub<string>(topic).publish('retained-value', { retain: true });

        const received: string[] = [];
        await nexo.pubsub<string>(topic).subscribe((data) => received.push(data));
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('retained-value');

        await nexo.pubsub(topic).unsubscribe();
        await nexo.pubsub(topic).clear();
    });

    it('should overwrite retained message on second publish', async () => {
        const topic = `retained-overwrite-${randomUUID()}`;

        await nexo.pubsub<string>(topic).publish('first', { retain: true });
        await nexo.pubsub<string>(topic).publish('second', { retain: true });

        const received: string[] = [];
        await nexo.pubsub<string>(topic).subscribe((data) => received.push(data));
        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('second');

        await nexo.pubsub(topic).unsubscribe();
        await nexo.pubsub(topic).clear();
    });

    it('should not deliver retained message after TTL expiry', async () => {
        const topic = `retained-ttl-${randomUUID()}`;

        await nexo.pubsub<string>(topic).publish('temp-retained', { retain: true, ttl: 1 });

        // Wait for TTL to expire
        await new Promise(r => setTimeout(r, 1200));

        const received: string[] = [];
        await nexo.pubsub<string>(topic).subscribe((data) => received.push(data));
        await new Promise(r => setTimeout(r, 300));
        expect(received).toEqual([]);

        await nexo.pubsub(topic).unsubscribe();
    });

    it('should stop delivery after unsubscribe', async () => {
        const topic = `unsub-stop-${randomUUID()}`;

        const received: any[] = [];
        await nexo.pubsub(topic).subscribe((data) => received.push(data));

        await nexo.pubsub(topic).publish({ msg: 'before' });
        await waitFor(() => expect(received.length).toBe(1));

        await nexo.pubsub(topic).unsubscribe();

        await nexo.pubsub(topic).publish({ msg: 'after' });
        await new Promise(r => setTimeout(r, 300));
        expect(received.length).toBe(1);
    });

    it('should match combined wildcards a/+/b/#', async () => {
        const baseId = randomUUID();
        const pattern = `combo-${baseId}/+/b/#`;

        const received: any[] = [];
        await nexo.pubsub(pattern).subscribe((data) => received.push(data));

        // Positive: a/x/b/y/z matches
        await nexo.pubsub(`combo-${baseId}/x/b/y/z`).publish({ id: 'deep-match' });
        // Positive: a/x/b matches (+ matches x, # matches zero levels)
        await nexo.pubsub(`combo-${baseId}/x/b`).publish({ id: 'shallow-match' });

        // Negative: a/x/c/y — b is not at position 2
        await nexo.pubsub(`combo-${baseId}/x/c/y`).publish({ id: 'fail-wrong-segment' });

        await waitFor(() => expect(received.length).toBe(2));
        const ids = received.map(r => r.id).sort();
        expect(ids).toEqual(['deep-match', 'shallow-match']);

        await nexo.pubsub(pattern).unsubscribe();
    });

    it('should broadcast to 3+ subscribers on same topic', async () => {
        const topic = `broadcast-${randomUUID()}`;

        const client1 = await NexoClient.connect();
        const client2 = await NexoClient.connect();

        const recv1: any[] = [];
        const recv2: any[] = [];
        const recv3: any[] = [];

        await client1.pubsub(topic).subscribe((d) => recv1.push(d));
        await client2.pubsub(topic).subscribe((d) => recv2.push(d));
        await nexo.pubsub(topic).subscribe((d) => recv3.push(d));

        await nexo.pubsub(topic).publish({ msg: 'broadcast' });

        await waitFor(() => {
            expect(recv1.length).toBe(1);
            expect(recv2.length).toBe(1);
            expect(recv3.length).toBe(1);
        });
        expect(recv1[0].msg).toBe('broadcast');
        expect(recv2[0].msg).toBe('broadcast');
        expect(recv3[0].msg).toBe('broadcast');

        await client1.pubsub(topic).unsubscribe();
        await client2.pubsub(topic).unsubscribe();
        await nexo.pubsub(topic).unsubscribe();
        await client1.disconnect();
        await client2.disconnect();
    });

    it('should clean up subscriber on disconnect without breaking topic', async () => {
        const topic = `disconnect-cleanup-${randomUUID()}`;

        const tempClient = await NexoClient.connect();
        await tempClient.pubsub(topic).subscribe(() => {});
        await new Promise(r => setTimeout(r, 100));

        // Disconnect temp client — server should clean up its subscription
        await tempClient.disconnect();
        await new Promise(r => setTimeout(r, 300));

        // A new subscriber should still receive messages normally
        const received: any[] = [];
        await nexo.pubsub(topic).subscribe((d) => received.push(d));
        await nexo.pubsub(topic).publish({ msg: 'after-disconnect' });

        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0].msg).toBe('after-disconnect');

        await nexo.pubsub(topic).unsubscribe();
    });

    it('should deliver retained messages to wildcard + subscriber', async () => {
        const baseId = randomUUID();

        await nexo.pubsub(`ret-plus-${baseId}/x`).publish('val-x', { retain: true });
        await nexo.pubsub(`ret-plus-${baseId}/y`).publish('val-y', { retain: true });

        const received: string[] = [];
        await nexo.pubsub<string>(`ret-plus-${baseId}/+`).subscribe((d) => received.push(d));

        await waitFor(() => expect(received.length).toBe(2));
        expect(received.sort()).toEqual(['val-x', 'val-y']);

        await nexo.pubsub(`ret-plus-${baseId}/+`).unsubscribe();
        await nexo.pubsub(`ret-plus-${baseId}/x`).clear();
        await nexo.pubsub(`ret-plus-${baseId}/y`).clear();
    });

    it('should deliver retained messages to wildcard # subscriber', async () => {
        const baseId = randomUUID();

        await nexo.pubsub(`ret-hash-${baseId}/a/b/c`).publish('deep', { retain: true });

        const received: string[] = [];
        await nexo.pubsub<string>(`ret-hash-${baseId}/#`).subscribe((d) => received.push(d));

        await waitFor(() => expect(received.length).toBe(1));
        expect(received[0]).toBe('deep');

        await nexo.pubsub(`ret-hash-${baseId}/#`).unsubscribe();
        await nexo.pubsub(`ret-hash-${baseId}/a/b/c`).clear();
    });
});
