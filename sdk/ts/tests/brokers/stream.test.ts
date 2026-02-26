import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { NexoClient } from '../../src/client';
import { nexo } from '../nexo';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';

describe('STREAM', () => {
    let clientA: NexoClient;
    let clientB: NexoClient;
    const SERVER_PORT = parseInt(process.env.SERVER_PORT!);
    const SERVER_HOST = process.env.SERVER_HOST!;

    beforeAll(async () => {
        clientA = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });
        clientB = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });
    });

    afterAll(async () => {
        await Promise.allSettled([
            clientA?.disconnect(),
            clientB?.disconnect()
        ]);
    });

    it('should support Happy Path (Publish/Subscribe)', async () => {
        const topic = `stream-basic-${randomUUID()}`;
        await nexo.stream(topic).create();

        const received: any[] = [];
        const sub = await clientA.stream(topic).subscribe('g1', (data) => received.push(data));

        await nexo.stream(topic).publish({ id: 1 });
        await nexo.stream(topic).publish({ id: 2 });

        await waitFor(() => expect(received.length).toBe(2));
        sub.stop();
    });

    it('Independent CONSUMER GROUPS => should deliver all messages to each group', async () => {
        const topic = `stream-groups-${randomUUID()}`;
        await nexo.stream(topic).create();

        const recvA: any[] = [];
        const recvB: any[] = [];

        const subA = await clientA.stream(topic).subscribe('group_A', (d) => recvA.push(d));
        const subB = await clientB.stream(topic).subscribe('group_B', (d) => recvB.push(d));

        await nexo.stream(topic).publish({ msg: 'hello' });

        await waitFor(() => {
            expect(recvA.length).toBe(1);
            expect(recvB.length).toBe(1);
        });

        subA.stop();
        subB.stop();
    });

    it('Same CONSUMER GROUP => should distribute messages without duplicates', async () => {
        const topic = `parallel-consumers-${randomUUID()}`;
        const group = 'parallel_group';
        await nexo.stream(topic).create();

        const receivedA = new Set<number>();
        const receivedB = new Set<number>();

        // Start 2 consumers in same group
        const subA = await clientA.stream(topic).subscribe(group, (d) => {
            if (receivedA.has(d.id)) throw new Error(`Duplicate in A: ${d.id}`);
            receivedA.add(d.id);
        });

        const subB = await clientB.stream(topic).subscribe(group, (d) => {
            if (receivedB.has(d.id)) throw new Error(`Duplicate in B: ${d.id}`);
            receivedB.add(d.id);
        });

        // Wait for join
        await new Promise(r => setTimeout(r, 500));

        // Publish messages
        for (let i = 0; i < 100; i++) {
            await nexo.stream(topic).publish({ id: i });
        }

        await waitFor(() => expect(receivedA.size + receivedB.size).toBe(100));

        // Verify NO overlap
        const overlap = [...receivedA].filter(id => receivedB.has(id));
        expect(overlap.length).toBe(0);

        subA.stop();
        subB.stop();
    });

    it('should handle consumer disconnect with zero data loss', async () => {
        const tempClientA = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });
        const tempClientB = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });

        const topic = `stream-disconnect-${randomUUID()}`;
        const group = 'group_disconnect';
        await nexo.stream(topic).create();

        const producer = nexo.stream(topic);
        const allReceivedIds = new Set<number>();
        const track = (d: any) => allReceivedIds.add(d.i);

        // 1. Start A & B 
        const subA = await tempClientA.stream(topic).subscribe(group, track);
        const subB = await tempClientB.stream(topic).subscribe(group, track);

        // 2. Warm up (0-19)
        for (let i = 0; i < 20; i++) await producer.publish({ i });
        await waitFor(() => expect(allReceivedIds.size).toBe(20));

        // 3. Disconnect Client A
        await tempClientA.disconnect();

        // 4. Wait for pending messages to be redelivered
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

        subB.stop();
        await tempClientB.disconnect();
    });
});
