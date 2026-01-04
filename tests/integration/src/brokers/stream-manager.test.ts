import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '@nexo/client';




describe('Stream Broker', () => {

    it('Should FAIL to publish to a non-existent topic', async () => {
        const topicName = 'ghost-topic';
        const stream = nexo.stream(topicName);
        await expect(stream.publish({ val: 1 })).rejects.toThrow('Topic \'ghost-topic\' not found');
    });

    it('Should FAIL to subscribe to a non-existent topic', async () => {
        const topicName = 'ghost-topic-sub';
        const stream = nexo.stream(topicName, 'group-1');
        await expect(stream.subscribe(() => { })).rejects.toThrow('Topic \'ghost-topic-sub\' not found');
    });

    it('Should create a topic explicitly', async () => {
        const topicName = 'orders-v1';
        const stream = await nexo.stream(topicName, 'test-group').create({ partitions: 4 });
        expect(stream).toBeDefined();
    });

    it('Should publish and consume messages (FIFO within partition)', async () => {
        const topicName = 'orders-fifo';
        await nexo.stream(topicName).create({ partitions: 4 });

        const streamPub = nexo.stream(topicName);
        const streamSub = nexo.stream(topicName, 'billing-service');

        const messages = [
            { id: 1, val: 'A' },
            { id: 2, val: 'B' },
            { id: 3, val: 'C' }
        ];

        const received: any[] = [];
        await streamSub.subscribe((data) => {
            received.push(data);
        });

        await new Promise(r => setTimeout(r, 100));

        for (const msg of messages) {
            await streamPub.publish(msg, { key: 'user-1' });
        }

        // Wait until we receive all 3 messages or timeout
        for (let i = 0; i < 20; i++) {
            if (received.length >= 3) break;
            await new Promise(r => setTimeout(r, 100));
        }

        expect(received).toHaveLength(3);
        expect(received).toEqual(messages);
    });

    it('Should distribute messages across partitions', async () => {
        const topicName = 'logs-dist';
        const stream = nexo.stream(topicName, 'logger');
        await stream.create({ partitions: 8 });

        const TOTAL = 50;
        let receivedCount = 0;

        await stream.subscribe(() => {
            receivedCount++;
        });

        await new Promise(r => setTimeout(r, 100));

        const promises = [];
        for (let i = 0; i < TOTAL; i++) {
            promises.push(stream.publish({ i }, { key: `k-${i}` }));
        }
        await Promise.all(promises);

        for (let i = 0; i < 20; i++) {
            if (receivedCount >= TOTAL) break;
            await new Promise(r => setTimeout(r, 100));
        }
        expect(receivedCount).toBe(TOTAL);
    });

    describe('Consumer Groups Logic', () => {

        it('Fan-Out: 2 consumers in DIFFERENT groups should BOTH receive ALL messages', async () => {
            const topicName = 'broadcast-news';
            await nexo.stream(topicName).create({ partitions: 4 });

            const pub = nexo.stream(topicName);
            const subGroupA = nexo.stream(topicName, 'analytics-service');
            const subGroupB = nexo.stream(topicName, 'notification-service');

            const receivedA: any[] = [];
            const receivedB: any[] = [];

            await subGroupA.subscribe(msg => { receivedA.push(msg); });
            await subGroupB.subscribe(msg => { receivedB.push(msg); });

            await new Promise(r => setTimeout(r, 100));

            for (let i = 0; i < 5; i++) {
                await pub.publish({ msg: `news-${i}` });
            }

            await new Promise(r => setTimeout(r, 500));

            expect(receivedA.length).toBe(5);
            expect(receivedB.length).toBe(5);
        });

        it('Competing Consumers: 2 consumers in SAME group (Current V1 Behavior: Duplication)', async () => {
            const topicName = 'work-queue';
            await nexo.stream(topicName).create({ partitions: 4 });

            const pub = nexo.stream(topicName);

            // Client 2 connection
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });

            const sub1 = nexo.stream(topicName, 'workers');
            const sub2 = client2.stream(topicName, 'workers');

            const received1: any[] = [];
            const received2: any[] = [];

            await sub1.subscribe(msg => { received1.push(msg); });
            await sub2.subscribe(msg => { received2.push(msg); });

            await new Promise(r => setTimeout(r, 100));

            for (let i = 0; i < 10; i++) {
                await pub.publish({ task: i });
            }

            await new Promise(r => setTimeout(r, 800));

            // V1 LIMITATION: Duplication
            expect(received1.length).toBe(10);
            expect(received2.length).toBe(10);

            client2.disconnect();
        });

        it('Partition Stickiness: Same Key -> Same Consumer (assuming strict partition assignment)', async () => {
            const topicName = 'sticky-session';
            await nexo.stream(topicName).create({ partitions: 4 });

            const pub = nexo.stream(topicName);
            const sub = nexo.stream(topicName, 'session-handler');

            const receivedKeys: string[] = [];

            await sub.subscribe((msg: any) => {
                receivedKeys.push(msg.k);
            });

            await new Promise(r => setTimeout(r, 100));

            await pub.publish({ k: 'A1' }, { key: 'user-A' });
            await pub.publish({ k: 'A2' }, { key: 'user-A' });
            await pub.publish({ k: 'A3' }, { key: 'user-A' });

            await new Promise(r => setTimeout(r, 500));

            expect(receivedKeys).toEqual(['A1', 'A2', 'A3']);
        });

    });

});