import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
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
});
