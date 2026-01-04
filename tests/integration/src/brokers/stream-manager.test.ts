import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '@nexo/client';

describe('Stream Broker', () => {

    it('Should FAIL to publish to a non-existent topic', async () => {
        const topicName = 'ghost-topic';
        const stream = nexo.stream(topicName);
        await expect(stream.publish({ val: 1 })).rejects.toThrow();
    });

    it('Should FAIL to subscribe to a non-existent topic', async () => {
        const topicName = 'ghost-topic-sub';
        const stream = nexo.stream(topicName, 'group-1');
        await expect(stream.subscribe(() => { })).rejects.toThrow();
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
        // We can't guarantee global order across partitions, but with same key 'user-1' 
        // they should land in same partition and be ordered.
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

        for (let i = 0; i < 30; i++) {
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

            await new Promise(r => setTimeout(r, 200));

            for (let i = 0; i < 5; i++) {
                await pub.publish({ msg: `news-${i}` });
            }

            await new Promise(r => setTimeout(r, 1000));

            expect(receivedA.length).toBe(5);
            expect(receivedB.length).toBe(5);
        });

        it('Load Balancing: 2 consumers in SAME group should SPLIT the load', async () => {
            const topicName = 'load-balance-test';
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

            // Wait for rebalancing
            await new Promise(r => setTimeout(r, 500));

            // Publish 20 messages with random keys to hit all partitions
            for (let i = 0; i < 20; i++) {
                await pub.publish({ task: i }, { key: `k-${i}` });
            }

            await new Promise(r => setTimeout(r, 1500));

            // Logic: Total received must be 20. Neither should be 0 (statistically unlikely with 4 partitions and 20 random keys)
            console.log(`Load Balanced: Client1=${received1.length}, Client2=${received2.length}`);
            
            expect(received1.length + received2.length).toBe(20);
            expect(received1.length).toBeGreaterThan(0);
            expect(received2.length).toBeGreaterThan(0);

            client2.disconnect();
        });

        it('Failover: When a consumer leaves, the other takes over', async () => {
            const topicName = 'failover-test';
            await nexo.stream(topicName).create({ partitions: 2 }); // 2 partitions, easier to test

            const pub = nexo.stream(topicName);
            
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });

            const sub1 = nexo.stream(topicName, 'failover-group');
            const sub2 = client2.stream(topicName, 'failover-group');

            const received1: any[] = [];
            const received2: any[] = [];

            await sub1.subscribe(msg => { received1.push(msg); });
            await sub2.subscribe(msg => { received2.push(msg); });

            await new Promise(r => setTimeout(r, 500));

            // Kill Client 2
            client2.disconnect();
            
            // Wait for server to detect disconnect and rebalance
            await new Promise(r => setTimeout(r, 500));

            // Publish messages
            for (let i = 0; i < 10; i++) {
                await pub.publish({ i });
            }
            
            await new Promise(r => setTimeout(r, 1000));

            // Client 1 should have received ALL messages because Client 2 is dead
            expect(received1.length).toBe(10);
            expect(received2.length).toBe(0);
        });

        it('Smart Resume: Client should restart from last committed batch', async () => {
            console.log("Starting Smart Resume Test...");
            const topicName = 'resume-test';
            await nexo.stream(topicName).create({ partitions: 1 }); 

            const pub = nexo.stream(topicName);
            const groupName = 'resume-group';

            console.log("Publishing 50 messages...");
            for (let i = 0; i < 50; i++) {
                await pub.publish({ val: i });
            }

            console.log("Starting Client 1...");
            const client1 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const sub1 = client1.stream(topicName, groupName);
            
            let count1 = 0;
            
            await new Promise<void>((resolve, reject) => {
                const timeout = setTimeout(() => reject(new Error("Client 1 Timeout")), 5000);
                
                sub1.subscribe(async (msg) => {
                    count1++;
                    if (count1 === 20) {
                        console.log("Client 1 reached 20 messages. Stopping...");
                        clearTimeout(timeout);
                        // Give PLENTY of time for auto-commit to happen and be ACKed by server
                        // The loop in SDK needs to finish processing, then send COMMIT, then receive OK.
                        setTimeout(resolve, 2000);
                    }
                }, { batchSize: 20 });
            });

            console.log("Disconnecting Client 1...");
            client1.disconnect(); 
            
            // CRITICAL: Wait for Server to detect disconnect, remove member, and be ready for C2.
            // If C2 joins while C1 is still considered "active" (race condition), C2 might get 0 partitions 
            // because C1 still holds the only partition.
            await new Promise(r => setTimeout(r, 1000));

            console.log("Starting Client 2...");
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const sub2 = client2.stream(topicName, groupName);
            
            const received2: any[] = [];
            
            // Wait for Client 2 to consume remaining 30 messages
            await new Promise<void>((resolve, reject) => {
                 const timeout = setTimeout(() => reject(new Error("Client 2 Timeout")), 5000);
                 
                 sub2.subscribe((msg) => {
                    received2.push(msg);
                    if (received2.length === 30) {
                        console.log("Client 2 reached 30 messages. Success.");
                        clearTimeout(timeout);
                        resolve();
                    }
                }, { batchSize: 100 });
            });

            console.log(`Smart Resume Result: Client1=${count1}, Client2=${received2.length}`);
            
            expect(count1).toBe(20); // Should verify we stopped exactly at 20 (might be slightly more if race condition, but let's see)
            expect(received2.length).toBe(30);
            expect(received2[0].val).toBe(20); 
            expect(received2[29].val).toBe(49); 

            client2.disconnect();
        }, 15000); // Increased test timeout
    });

});
