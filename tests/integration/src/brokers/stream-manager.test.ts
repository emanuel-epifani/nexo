import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

const PORT = 8080;

describe('Stream Manager & Rebalancing Integration', () => {
    let clientA: NexoClient;
    let clientB: NexoClient;
    let clientProducer: NexoClient;

    beforeEach(async () => {
        clientA = await NexoClient.connect({ port: PORT });
        clientB = await NexoClient.connect({ port: PORT });
        clientProducer = await NexoClient.connect({ port: PORT });
    });

    afterEach(async () => {
        try { if (clientA?.connected) clientA.disconnect(); } catch {}
        try { if (clientB?.connected) clientB.disconnect(); } catch {}
        try { if (clientProducer?.connected) clientProducer.disconnect(); } catch {}
    });

    // 1. BASE: Pub/Sub Sequenziale
    it('should publish and consume messages in order within a partition', async () => {
        const topicName = `test-topic-${randomUUID()}`;
        const group = 'group-1';
        
        // Setup: Create Stream
        await clientA.stream(topicName, group).create();

        // Publish
        const producer = clientProducer.stream(topicName);
        await producer.publish({ msg: 1 });
        await producer.publish({ msg: 2 });
        await producer.publish({ msg: 3 });

        // Consume
        const received: any[] = [];
        const consumer = clientA.stream(topicName, group);
        
        await consumer.subscribe(async (data) => {
            received.push(data);
        });

        // Verify
        await waitFor(() => {
            expect(received.length).toBe(3);
            expect(received).toMatchObject([{ msg: 1 }, { msg: 2 }, { msg: 3 }]);
        });
        
        consumer.stop();
    });

    // 2. RESUME: Offset Consistency
    it('should resume from last committed offset after restart', async () => {
        const topicName = `test-resume-${randomUUID()}`;
        const group = 'group-persistence';

        await clientA.stream(topicName, group).create();
        const producer = clientProducer.stream(topicName);

        // Publish 10 messages
        for (let i = 0; i < 10; i++) await producer.publish({ i });

        // CONSUMER A: Read only 5 and stop
        const receivedA: any[] = [];
        const consumerA = clientA.stream(topicName, group);
        
        await consumerA.subscribe(async (data) => {
            receivedA.push(data);
            if (receivedA.length === 5) {
                consumerA.stop(); // Stop -> Trigger Commit of last processed
            }
        });

        await waitFor(() => expect(receivedA.length).toBe(5));
        
        // Wait briefly for commit to reach server
        await new Promise(r => setTimeout(r, 200));

        // CONSUMER B (New instance, same group): Should read from 6 to 10
        const receivedB: any[] = [];
        const consumerB = clientB.stream(topicName, group);
        
        await consumerB.subscribe(async (data) => {
            receivedB.push(data);
        });

        // Should receive the remaining 5
        await waitFor(() => {
            expect(receivedB.length).toBe(5);
            expect(receivedB[0].i).toBe(5); // Index 5 = 6th message
            expect(receivedB[4].i).toBe(9); // Index 9 = 10th message
        });
        
        consumerB.stop();
    });

    // 3. SCALE UP: Rebalancing from 1 to 2 Consumers
    it('should rebalance partitions when a new consumer joins', async () => {
        const topicName = `test-scale-up-${randomUUID()}`;
        const group = 'group-scale';

        // Create topic (Server creates default partitions)
        await clientA.stream(topicName, group).create();
        
        const producer = clientProducer.stream(topicName);
        // Publish enough messages to saturate partitions
        for (let i = 0; i < 50; i++) await producer.publish({ id: i });

        const receivedA: any[] = [];
        const receivedB: any[] = [];

        // 1. Start Consumer A
        const streamA = clientA.stream(topicName, group);
        await streamA.subscribe((data) => {
            receivedA.push(data)
        });

        // Let A start working
        await new Promise(r => setTimeout(r, 500));
        
        // 2. Start Consumer B (Join -> Trigger Rebalance -> A gets "Rebalance needed" -> A re-joins)
        const streamB = clientB.stream(topicName, group);
        await streamB.subscribe((data) => {
            receivedB.push(data)
        });

        // Eventually, both should have processed everything together
        await waitFor(() => {
            const total = receivedA.length + receivedB.length;
            expect(total).toBe(50);
            
            // Check that load was distributed (both did work)
            expect(receivedA.length).toBeGreaterThan(0);
            expect(receivedB.length).toBeGreaterThan(0);
        }, { timeout: 8000 });

        console.log(`Split stats -> A: ${receivedA.length}, B: ${receivedB.length}`);

        streamA.stop();
        streamB.stop();
    });

    // 4. SCALE DOWN: Rebalancing from 2 to 1 (Fault Tolerance)
    it('should reassign orphaned partitions when a consumer leaves', async () => {
        const topicName = `test-scale-down-${randomUUID()}`;
        const group = 'group-fault';

        await clientA.stream(topicName, group).create();
        const producer = clientProducer.stream(topicName);

        const receivedTotal: any[] = [];
        const collect = (d: any) => receivedTotal.push(d);

        // Two active consumers
        const streamA = clientA.stream(topicName, group);
        const streamB = clientB.stream(topicName, group);
        
        await streamA.subscribe(collect);
        await streamB.subscribe(collect);

        // Wait for stabilization
        await new Promise(r => setTimeout(r, 500));

        // Batch 1
        for (let i = 0; i < 20; i++) await producer.publish({ batch: 1, id: i });
        await waitFor(() => expect(receivedTotal.length).toBe(20));

        // KILL CONSUMER A (Simulate brutal crash/disconnect)
        clientA.disconnect(); 

        // Batch 2 (Published while A is dead)
        // Server should detect A is gone and reassign its partitions to B
        for (let i = 0; i < 20; i++) await producer.publish({ batch: 2, id: i });

        // B should receive all of batch 2 (including partitions previously owned by A)
        await waitFor(() => expect(receivedTotal.length).toBe(40), { timeout: 10000 });
        
        streamB.stop();
    });
});
