import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

const SERVER_PORT = parseInt(process.env.NEXO_PORT!);

describe('Stream Manager', () => {
    let clientA: NexoClient;
    let clientB: NexoClient;
    let clientProducer: NexoClient;

    beforeEach(async () => {
        clientA = await NexoClient.connect({ port: SERVER_PORT });
        clientB = await NexoClient.connect({ port: SERVER_PORT });
        clientProducer = await NexoClient.connect({ port: SERVER_PORT });
    });

    afterEach(async () => {
        try { clientA?.disconnect(); } catch {}
        try { clientB?.disconnect(); } catch {}
        try { clientProducer?.disconnect(); } catch {}
    });


    describe('Basic Operations', () => {
        it('should publish and consume messages in order', async () => {
            const topic = `basic-order-${randomUUID()}`;
            const group = 'g1';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            await producer.publish({ seq: 1 });
            await producer.publish({ seq: 2 });
            await producer.publish({ seq: 3 });

            const received: any[] = [];
            const consumer = clientA.stream(topic, group);
            await consumer.subscribe(data => received.push(data));

            await waitFor(() => expect(received.length).toBe(3));
            expect(received).toMatchObject([{ seq: 1 }, { seq: 2 }, { seq: 3 }]);

            consumer.stop();
        });

        it('should handle empty topic without blocking', async () => {
            const topic = `empty-${randomUUID()}`;
            const group = 'g1';

            await clientA.stream(topic, group).create();

            const received: any[] = [];
            const consumer = clientA.stream(topic, group);
            await consumer.subscribe(data => received.push(data));

            // Wait a bit, should not block
            await new Promise(r => setTimeout(r, 200));
            expect(received.length).toBe(0);

            // Now publish
            await clientProducer.stream(topic).publish({ late: true });
            await waitFor(() => expect(received.length).toBe(1));

            consumer.stop();
        });

        it('should distribute messages across partitions', async () => {
            const topic = `multi-partition-${randomUUID()}`;
            const group = 'g1';

            await clientA.stream(topic, group).create();

            // Publish many messages (server has 4 partitions by default)
            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 20; i++) {
                await producer.publish({ i });
            }

            const received: any[] = [];
            const consumer = clientA.stream(topic, group);
            await consumer.subscribe(data => received.push(data));

            await waitFor(() => expect(received.length).toBe(20), { timeout: 5000 });

            consumer.stop();
        });
    });


    describe('Offset Management', () => {
        it('should resume from last committed offset after disconnect', async () => {
            const topic = `resume-${randomUUID()}`;
            const group = 'g-resume';

            await clientA.stream(topic, group).create();

            // Publish 20 messages
            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 20; i++) await producer.publish({ i });

            // Consumer A: process 10, then disconnect
            const receivedA: any[] = [];
            const consumerA = clientA.stream(topic, group);
            await consumerA.subscribe(async data => {
                receivedA.push(data);
                if (receivedA.length === 10) {
                    clientA.disconnect();
                }
            }, { batchSize: 1 });

            await waitFor(() => expect(receivedA.length).toBe(10), { timeout: 10000 });
            await new Promise(r => setTimeout(r, 500)); // Wait for rebalance

            // Consumer B: should get remaining messages
            const receivedB: any[] = [];
            const consumerB = clientB.stream(topic, group);
            await consumerB.subscribe(data => receivedB.push(data));

            // At-least-once: total >= 20 (may have duplicates during rebalance)
            // All 20 unique messages must be received
            await waitFor(() => {
                const total = receivedA.length + receivedB.length;
                expect(total).toBeGreaterThanOrEqual(20);
            }, { timeout: 10000 });

            // Verify all messages were received (dedup by 'i')
            const allIds = new Set([...receivedA, ...receivedB].map(m => m.i));
            expect(allIds.size).toBe(20);

            consumerB.stop();
        });

        it('should not regress committed offset (monotonic)', async () => {
            const topic = `monotonic-${randomUUID()}`;
            const group = 'g-mono';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 10; i++) await producer.publish({ i });

            // Consume all
            const received: any[] = [];
            const consumer = clientA.stream(topic, group);
            await consumer.subscribe(data => received.push(data));

            await waitFor(() => expect(received.length).toBe(10));
            consumer.stop();
            clientA.disconnect();

            await new Promise(r => setTimeout(r, 300));

            // Reconnect, should not re-receive
            const clientC = await NexoClient.connect({ port: SERVER_PORT });
            const receivedC: any[] = [];
            const consumerC = clientC.stream(topic, group);
            await consumerC.subscribe(data => receivedC.push(data));

            // Publish new message
            await producer.publish({ i: 10 });

            await waitFor(() => expect(receivedC.length).toBe(1));
            expect(receivedC[0].i).toBe(10); // Only the new one

            consumerC.stop();
            clientC.disconnect();
        });
    });


    describe('Consumer Groups - Rebalancing', () => {
        it('should rebalance when new consumer joins (scale up)', async () => {
            const topic = `scale-up-${randomUUID()}`;
            const group = 'g-scale';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            // Initial batch
            for (let i = 0; i < 20; i++) await producer.publish({ batch: 1, i });

            const receivedA: any[] = [];
            const receivedB: any[] = [];

            // Start A
            const streamA = clientA.stream(topic, group);
            await streamA.subscribe(data => receivedA.push(data));

            await new Promise(r => setTimeout(r, 300)); // Let A start

            // Start B (triggers rebalance)
            const streamB = clientB.stream(topic, group);
            await streamB.subscribe(data => receivedB.push(data));

            // Publish more while both active
            for (let i = 0; i < 30; i++) await producer.publish({ batch: 2, i });

            await waitFor(() => {
                const total = receivedA.length + receivedB.length;
                expect(total).toBe(50);
                expect(receivedA.length).toBeGreaterThan(0);
                expect(receivedB.length).toBeGreaterThan(0);
            }, { timeout: 15000 });

            streamA.stop();
            streamB.stop();
        });

        it('should reassign partitions when consumer leaves (scale down)', async () => {
            const topic = `scale-down-${randomUUID()}`;
            const group = 'g-fault';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            const received: any[] = [];

            // Two consumers
            const streamA = clientA.stream(topic, group);
            const streamB = clientB.stream(topic, group);

            await streamA.subscribe(data => received.push(data));
            await streamB.subscribe(data => received.push(data));

            await new Promise(r => setTimeout(r, 500)); // Stabilize

            // Batch 1
            for (let i = 0; i < 40; i++) await producer.publish({ batch: 1, i });
            await waitFor(() => expect(received.length).toBe(40), { timeout: 15000 });

            // Kill A - wait for server to detect disconnect and rebalance
            clientA.disconnect();
            await new Promise(r => setTimeout(r, 3000));

            // Batch 2 - B should handle all partitions now
            for (let i = 0; i < 40; i++) await producer.publish({ batch: 2, i });

            await waitFor(() => expect(received.length).toBe(80), { timeout: 20000 });

            streamB.stop();
        });

        it('should handle rapid join/leave cycles', async () => {
            const topic = `rapid-${randomUUID()}`;
            const group = 'g-rapid';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 30; i++) await producer.publish({ i });

            const received: any[] = [];

            // A joins
            const streamA = clientA.stream(topic, group);
            await streamA.subscribe(data => received.push(data));

            await new Promise(r => setTimeout(r, 200));

            // B joins
            const streamB = clientB.stream(topic, group);
            await streamB.subscribe(data => received.push(data));

            await new Promise(r => setTimeout(r, 200));

            // A leaves
            clientA.disconnect();

            await new Promise(r => setTimeout(r, 500));

            // Publish more
            for (let i = 0; i < 20; i++) await producer.publish({ i: i + 30 });

            // B should eventually get all 50
            await waitFor(() => expect(received.length).toBe(50), { timeout: 15000 });

            streamB.stop();
        });

        it('should handle consumer rejoin to same group', async () => {
            const topic = `rejoin-${randomUUID()}`;
            const group = 'g-rejoin';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 10; i++) await producer.publish({ i });

            // First join
            const received1: any[] = [];
            const stream1 = clientA.stream(topic, group);
            await stream1.subscribe(data => received1.push(data));

            await waitFor(() => expect(received1.length).toBe(10));
            stream1.stop();
            clientA.disconnect();

            await new Promise(r => setTimeout(r, 500));

            // Rejoin with new client
            const clientA2 = await NexoClient.connect({ port: SERVER_PORT });
            const received2: any[] = [];

            // Publish more
            for (let i = 10; i < 20; i++) await producer.publish({ i });

            const stream2 = clientA2.stream(topic, group);
            await stream2.subscribe(data => received2.push(data));

            await waitFor(() => expect(received2.length).toBe(10));

            // Should only get new messages (10-19)
            expect(received2.every(m => m.i >= 10)).toBe(true);

            stream2.stop();
            clientA2.disconnect();
        });
    });


    describe('GenerationdId - Epoch Fencing', () => {
        it('should reject operations with stale generation id', async () => {
            const topic = `epoch-${randomUUID()}`;
            const group = 'g-epoch';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 50; i++) await producer.publish({ i });

            const receivedA: any[] = [];
            const receivedB: any[] = [];

            // A starts consuming
            const streamA = clientA.stream(topic, group);
            await streamA.subscribe(data => receivedA.push(data));

            await new Promise(r => setTimeout(r, 300));

            // B joins - triggers rebalance, A's gen_id becomes stale
            const streamB = clientB.stream(topic, group);
            await streamB.subscribe(data => receivedB.push(data));

            // Both should eventually process all messages
            // A should recover from REBALANCE_NEEDED and rejoin
            await waitFor(() => {
                const total = receivedA.length + receivedB.length;
                expect(total).toBe(50);
            }, { timeout: 15000 });

            streamA.stop();
            streamB.stop();
        });
    });


    describe('Message Integrity', () => {
        it('should not lose messages during rebalance', async () => {
            const topic = `integrity-${randomUUID()}`;
            const group = 'g-integrity';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            const allReceived = new Set<number>();

            const streamA = clientA.stream(topic, group);
            const streamB = clientB.stream(topic, group);

            await streamA.subscribe(data => allReceived.add(data.i));

            // Publish while A is alone
            for (let i = 0; i < 25; i++) await producer.publish({ i });

            await new Promise(r => setTimeout(r, 200));

            // B joins mid-stream
            await streamB.subscribe(data => allReceived.add(data.i));

            // Publish more
            for (let i = 25; i < 50; i++) await producer.publish({ i });

            await waitFor(() => expect(allReceived.size).toBe(50), { timeout: 15000 });

            // Verify all messages received (0-49)
            for (let i = 0; i < 50; i++) {
                expect(allReceived.has(i)).toBe(true);
            }

            streamA.stop();
            streamB.stop();
        });

        it('should handle at-least-once delivery (duplicates possible on crash)', async () => {
            const topic = `atleast-${randomUUID()}`;
            const group = 'g-atleast';

            await clientA.stream(topic, group).create();

            const producer = clientProducer.stream(topic);
            for (let i = 0; i < 20; i++) await producer.publish({ i });

            const received: number[] = [];
            let processedCount = 0;

            const stream = clientA.stream(topic, group);
            await stream.subscribe(async data => {
                received.push(data.i);
                processedCount++;
                
                // Simulate crash after 10 messages (before commit completes)
                if (processedCount === 10) {
                    clientA.disconnect();
                }
            }, { batchSize: 100 }); // Large batch to test partial processing

            await waitFor(() => expect(received.length).toBeGreaterThanOrEqual(10), { timeout: 5000 });

            await new Promise(r => setTimeout(r, 500));

            // New consumer should get remaining (possibly with some duplicates)
            const receivedB: number[] = [];
            const streamB = clientB.stream(topic, group);
            await streamB.subscribe(data => receivedB.push(data.i));

            await waitFor(() => {
                // Total should be at least 20 (might be more due to duplicates)
                expect(received.length + receivedB.length).toBeGreaterThanOrEqual(20);
            }, { timeout: 10000 });

            // All original messages should be covered
            const allIds = new Set([...received, ...receivedB]);
            for (let i = 0; i < 20; i++) {
                expect(allIds.has(i)).toBe(true);
            }

            streamB.stop();
        });
    });


    describe('Multi-Topic', () => {
        it('should handle same group on different topics independently', async () => {
            const topicX = `multi-x-${randomUUID()}`;
            const topicY = `multi-y-${randomUUID()}`;
            const group = 'shared-group';

            await clientA.stream(topicX, group).create();
            await clientA.stream(topicY, group).create();

            const producerX = clientProducer.stream(topicX);
            const producerY = clientProducer.stream(topicY);

            for (let i = 0; i < 10; i++) await producerX.publish({ topic: 'X', i });
            for (let i = 0; i < 10; i++) await producerY.publish({ topic: 'Y', i });

            const receivedX: any[] = [];
            const receivedY: any[] = [];

            const streamX = clientA.stream(topicX, group);
            const streamY = clientB.stream(topicY, group);

            await streamX.subscribe(data => receivedX.push(data));
            await streamY.subscribe(data => receivedY.push(data));

            await waitFor(() => {
                expect(receivedX.length).toBe(10);
                expect(receivedY.length).toBe(10);
            }, { timeout: 10000 });

            expect(receivedX.every(m => m.topic === 'X')).toBe(true);
            expect(receivedY.every(m => m.topic === 'Y')).toBe(true);

            streamX.stop();
            streamY.stop();
        });
    });
});
