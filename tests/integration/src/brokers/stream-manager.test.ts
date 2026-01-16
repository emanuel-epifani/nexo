import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { waitFor } from '../utils/wait-for';
import { v4 as uuidv4 } from 'uuid';

const PORT = 8080;

describe('Stream Manager & Rebalancing Integration', () => {
    let clientA: NexoClient;
    let clientB: NexoClient;

    beforeEach(async () => {
        clientA = await NexoClient.connect({ port: PORT });
        clientB = await NexoClient.connect({ port: PORT });
    });

    afterEach(async () => {
        // Pulizia sicura
        try { if (clientA?.connected) clientA.disconnect(); } catch {}
        try { if (clientB?.connected) clientB.disconnect(); } catch {}
    });

    // 1. BASE: Pub/Sub Sequenziale
    it('should publish and consume messages in order within a partition', async () => {
        const topicName = `test-topic-${uuidv4()}`;
        const group = 'group-1';

        await clientA.stream(topicName, group).create();

        const producer = clientA.stream(topicName);
        await producer.publish({ msg: 1 });
        await producer.publish({ msg: 2 });
        await producer.publish({ msg: 3 });

        const received: any[] = [];
        const consumer = clientA.stream(topicName, group);

        // CORREZIONE QUI: Salviamo l'handle restituito dalla subscribe
        const subHandle = await consumer.subscribe(async (data) => {
            received.push(data);
        });

        await waitFor(() => expect(received.length).toBe(3));
        expect(received).toMatchObject([{ msg: 1 }, { msg: 2 }, { msg: 3 }]);

        // CORREZIONE QUI: Chiamiamo stop sull'handle
        subHandle.stop();
    });

    // 2. RESUME: Offset Consistency
    it('should resume from last committed offset after restart', async () => {
        const topicName = `test-resume-${uuidv4()}`;
        const group = 'group-persistence';

        await clientA.stream(topicName, group).create();
        const producer = clientA.stream(topicName);

        for (let i = 0; i < 10; i++) await producer.publish({ i });

        const receivedA: any[] = [];
        const consumerA = clientA.stream(topicName, group);

        const subA = await consumerA.subscribe(async (data) => {
            receivedA.push(data);
            if (receivedA.length === 5) {
                // Stop controllato dopo 5 messaggi
                subA.stop();
            }
        });

        await waitFor(() => expect(receivedA.length).toBe(5));
        await new Promise(r => setTimeout(r, 200)); // Wait for commit

        // CONSUMER B (Nuova istanza)
        const receivedB: any[] = [];
        const consumerB = clientB.stream(topicName, group);

        const subB = await consumerB.subscribe(async (data) => {
            receivedB.push(data);
        });

        await waitFor(() => expect(receivedB.length).toBe(5));
        expect(receivedB[0].i).toBe(5);
        expect(receivedB[4].i).toBe(9);

        subB.stop();
    });

    // 3. SCALE UP: Rebalancing da 1 a 2 Consumer
    it('should rebalance partitions when a new consumer joins', async () => {
        const topicName = `test-scale-up-${uuidv4()}`;
        const group = 'group-scale';

        await clientA.stream(topicName, group).create();
        const producer = clientA.stream(topicName);

        for (let i = 0; i < 50; i++) await producer.publish({ id: i });

        const receivedA: any[] = [];
        const receivedB: any[] = [];

        // Consumer A parte
        const subA = await clientA.stream(topicName, group).subscribe((data) => receivedA.push(data));

        await new Promise(r => setTimeout(r, 500));

        // Consumer B parte -> Trigger Rebalance
        const subB = await clientB.stream(topicName, group).subscribe((data) => receivedB.push(data));

        await waitFor(() => expect(receivedA.length + receivedB.length).toBe(50), { timeout: 8000 });

        console.log(`Split stats -> A: ${receivedA.length}, B: ${receivedB.length}`);

        expect(receivedA.length).toBeGreaterThan(0);
        expect(receivedB.length).toBeGreaterThan(0);

        subA.stop();
        subB.stop();
    });

    // 4. SCALE DOWN: Rebalancing da 2 a 1 (Fault Tolerance)
    it('should reassign orphaned partitions when a consumer leaves', async () => {
        const topicName = `test-scale-down-${uuidv4()}`;
        const group = 'group-fault';

        await clientA.stream(topicName, group).create();
        const producer = clientA.stream(topicName);

        const receivedTotal: any[] = [];
        const collect = (d: any) => receivedTotal.push(d);

        // Due consumer attivi
        // Nota: non salvo subA perché simulerò un crash brutale del client
        await clientA.stream(topicName, group).subscribe(collect);
        const subB = await clientB.stream(topicName, group).subscribe(collect);

        await new Promise(r => setTimeout(r, 500));

        // Batch 1: consumato da entrambi
        for (let i = 0; i < 20; i++) await producer.publish({ batch: 1, id: i });
        await waitFor(() => expect(receivedTotal.length).toBe(20));

        // CRASH CONSUMER A
        clientA.disconnect();

        // Batch 2: dovrebbe essere consumato tutto da B (dopo che il server rileva il crash)
        for (let i = 0; i < 20; i++) await producer.publish({ batch: 2, id: i });

        await waitFor(() => expect(receivedTotal.length).toBe(40), { timeout: 10000 });

        subB.stop();
    });
});