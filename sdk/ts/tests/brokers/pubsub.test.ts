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
});
