import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

// Helper: destroy socket and wait for full reconnect cycle
async function destroyAndReconnect() {
    (nexo as any).conn.socket.destroy();
    await waitFor(() => expect((nexo as any).conn.isConnected).toBe(false));
    await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });
}

// Helper: send a command with retry (connection might not be fully ready)
async function sendWithRetry<T>(fn: () => Promise<T>, timeout = 5000): Promise<T> {
    let lastError: any;
    const start = Date.now();
    while (Date.now() - start < timeout) {
        try {
            return await fn();
        } catch (e) {
            lastError = e;
            await new Promise(r => setTimeout(r, 200));
        }
    }
    throw lastError;
}

describe('SOCKET RECONNECTION', () => {

    // =========================================
    // BASIC RECONNECT (happy path per broker)
    // =========================================

    it('PUBSUB: Should auto-resubscribe after connection loss', async () => {
        const topic = `reconnect-pubsub-${randomUUID()}`;
        const received: string[] = [];

        await nexo.pubsub(topic).subscribe(m => received.push(m));

        await destroyAndReconnect();

        // PubSub re-subscribe happens in 'reconnect' event handler.
        // Publish after reconnect — server must know about subscription again.
        await sendWithRetry(() => nexo.pubsub(topic).publish('after-crash'));

        await waitFor(() => expect(received).toContain('after-crash'));
    });

    it('QUEUE: Should resume consuming after connection loss', async () => {
        const qName = `reconnect-queue-${randomUUID()}`;
        const q = await nexo.queue(qName).create();
        const received: any[] = [];

        await q.subscribe(msg => received.push(msg));

        await destroyAndReconnect();

        await sendWithRetry(() => q.push({ status: 'recovered' }));

        await waitFor(() => expect(received).toContainEqual({ status: 'recovered' }));
    });

    it('STREAM: Should resume consuming after connection loss (Rejoin Group)', async () => {
        const streamName = `reconnect-stream-${randomUUID()}`;
        const group = `g-${randomUUID()}`;
        await nexo.stream(streamName).create();
        const received: any[] = [];

        await nexo.stream(streamName).subscribe(group, m => received.push(m));

        await destroyAndReconnect();

        await sendWithRetry(() => nexo.stream(streamName).publish({ status: 'recovered' }));

        await waitFor(() => expect(received).toContainEqual({ status: 'recovered' }), { timeout: 10000 });
    });

    // =========================================
    // IN-FLIGHT MESSAGES (at-least-once)
    // =========================================

    it('QUEUE: In-flight message should be redelivered after crash (at-least-once)', async () => {
        const qName = `reconnect-inflight-${randomUUID()}`;
        const q = await nexo.queue(qName).create({ visibilityTimeoutMs: 2000 });

        // Push a message BEFORE subscribing
        await q.push({ important: true });

        const received: any[] = [];
        let firstDelivery = true;

        await q.subscribe(async (msg) => {
            received.push(msg);
            if (firstDelivery) {
                firstDelivery = false;
                // Kill connection BEFORE acking — message is in-flight
                (nexo as any).conn.socket.destroy();
                // Throw to prevent ack (loop will catch ConnectionClosedError anyway)
                throw new Error('crash before ack');
            }
            // Second delivery: process normally (auto-ack)
        });

        // Wait for redelivery: visibility timeout expires → message becomes Ready → redelivered
        await waitFor(
            () => expect(received.length).toBeGreaterThanOrEqual(2),
            { timeout: 10000 }
        );

        expect(received[0]).toEqual({ important: true });
        expect(received[1]).toEqual({ important: true });
    });

    // =========================================
    // OPERATIONS DURING DISCONNECT
    // =========================================

    it('QUEUE: push() during disconnect should fail with predictable error', async () => {
        const qName = `reconnect-push-err-${randomUUID()}`;
        const q = await nexo.queue(qName).create();

        (nexo as any).conn.socket.destroy();
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(false));

        // push() while disconnected must throw, not hang or cause unhandled rejection
        await expect(q.push({ test: true })).rejects.toThrow();

        // Wait for reconnect, then verify push works again
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });
        await sendWithRetry(() => q.push({ test: 'after-reconnect' }));
    });

    // =========================================
    // DOUBLE CRASH (no loop duplication)
    // =========================================

    it('QUEUE: Should survive double crash without duplicating consumer loops', async () => {
        const qName = `reconnect-double-${randomUUID()}`;
        const q = await nexo.queue(qName).create();
        const received: any[] = [];

        await q.subscribe(msg => received.push(msg));

        // First crash + reconnect
        await destroyAndReconnect();

        // Second crash + reconnect (back-to-back)
        await destroyAndReconnect();

        // Push a single message
        await sendWithRetry(() => q.push({ value: 'once' }));

        // Should receive exactly once (no duplicate loops)
        await waitFor(() => expect(received).toContainEqual({ value: 'once' }));

        // Small wait to check no duplicates arrive
        await new Promise(r => setTimeout(r, 500));
        const matchCount = received.filter(m => m.value === 'once').length;
        expect(matchCount).toBe(1);
    });

    // =========================================
    // stop() DURING DISCONNECT
    // =========================================

    it('QUEUE: stop() during disconnect should prevent loop from resuming', async () => {
        const qName = `reconnect-stop-${randomUUID()}`;
        const q = await nexo.queue(qName).create();
        const received: any[] = [];

        const sub = await q.subscribe(msg => received.push(msg));

        // Kill connection
        (nexo as any).conn.socket.destroy();
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(false));

        // Stop consumer WHILE disconnected
        sub.stop();

        // Wait for reconnect
        await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

        // Push a message — consumer should NOT pick it up (it was stopped)
        await sendWithRetry(() => q.push({ shouldNotArrive: true }));

        // Wait a reasonable time and verify nothing was received
        await new Promise(r => setTimeout(r, 1000));
        expect(received).toEqual([]);
    });

    // =========================================
    // PUBSUB: re-subscribe failure resilience
    // =========================================

    it('PUBSUB: Should receive messages on topics subscribed before AND after crash', async () => {
        const topicBefore = `reconnect-ps-before-${randomUUID()}`;
        const topicAfter = `reconnect-ps-after-${randomUUID()}`;
        const receivedBefore: string[] = [];
        const receivedAfter: string[] = [];

        // Subscribe to first topic before crash
        await nexo.pubsub(topicBefore).subscribe(m => receivedBefore.push(m));

        await destroyAndReconnect();

        // Subscribe to second topic after crash
        await nexo.pubsub(topicAfter).subscribe(m => receivedAfter.push(m));

        // Publish to both
        await sendWithRetry(() => nexo.pubsub(topicBefore).publish('msg-before'));
        await nexo.pubsub(topicAfter).publish('msg-after');

        await waitFor(() => expect(receivedBefore).toContain('msg-before'));
        await waitFor(() => expect(receivedAfter).toContain('msg-after'));
    });
});
