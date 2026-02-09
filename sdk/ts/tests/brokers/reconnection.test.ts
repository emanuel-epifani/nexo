import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('SOCKET RECONNECTION', () => {
    it('PUBSUB: Should auto-resubscribe after connection loss', async () => {
        const topic = `reconnect-pubsub-${randomUUID()}`;
        const received: string[] = [];

        // 1. Subscribe
        await nexo.pubsub(topic).subscribe(m => received.push(m));

        // 2. Simulate Network Failure
        const socket = (nexo as any).conn.socket;
        socket.destroy();

        // Wait for disconnect detection
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(false));

        // 3. Wait for Auto-Reconnect
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

        // 4. Publish NEW message (Server must know about subscription again)
        await nexo.pubsub(topic).publish('after-crash');

        // 5. Verify reception
        await waitFor(() => expect(received).toContain('after-crash'));
    });

    it('QUEUE: Should resume consuming after connection loss', async () => {
        const qName = `reconnect-queue-${randomUUID()}`;
        const q = await nexo.queue(qName).create();
        const received: any[] = [];

        // 1. Subscribe
        await q.subscribe(msg => received.push(msg));

        // 2. Kill Connection
        (nexo as any).conn.socket.destroy();

        // 3. Wait for Reconnect
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

        await new Promise(r => setTimeout(r, 500));

        // 4. Push NEW message (with retry policy)
        await waitFor(async () => {
            try {
                await q.push({ status: 'recovered' });
                return true;
            } catch {
                return false;
            }
        }, { timeout: 5000, interval: 500 });

        // 5. Verify
        await waitFor(() => expect(received).toContainEqual({ status: 'recovered' }));
    });

    it('STREAM: Should resume consuming after connection loss (Rejoin Group)', async () => {
        const topic = `reconnect-stream-${randomUUID()}`;
        const group = 'g-reconnect';
        await nexo.stream(topic).create();
        const received: any[] = [];

        // 1. Subscribe
        await nexo.stream(topic).subscribe(group, m => received.push(m));

        // 2. Kill Connection
        (nexo as any).conn.socket.destroy();

        // 3. Wait for Reconnect
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

        await new Promise(r => setTimeout(r, 500));

        // 4. Publish NEW message (Robust retry)
        await waitFor(async () => {
            try {
                await nexo.stream(topic).publish({ status: 'recovered' });
                return true;
            } catch {
                return false;
            }
        }, { timeout: 5000, interval: 500 });

        // 5. Verify
        await waitFor(() => expect(received).toContainEqual({ status: 'recovered' }));
    });
});
