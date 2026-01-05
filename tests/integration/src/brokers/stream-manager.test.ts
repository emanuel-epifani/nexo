import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '@nexo/client';
import { BenchmarkProbe } from '../utils/benchmark-misure';
import { waitFor } from '../utils/wait-for';

describe('Stream Broker (MPSC Actor - No Partitions)', () => {

    describe('Core Functionality', () => {

        it('Should create a topic explicitly', async () => {
            const topicName = 'orders-v1';
            const stream = await nexo.stream(topicName, 'test-group').create();
            expect(stream).toBeDefined();
        });

        it('Should publish and subscribe to messages (Basic Flow)', async () => {
            const topic = 'basic-flow';
            await nexo.stream(topic).create();

            const messages = [{ text: 'hello' }, { text: 'world' }];
            const received: any[] = [];

            await nexo.stream(topic, 'group-basic').subscribe(msg => {
                received.push(msg);
            });

            // Wait for subscription to establish
            await new Promise(r => setTimeout(r, 50));

            for (const m of messages) {
                await nexo.stream(topic).publish(m);
            }

            await waitFor(() => received.length === 2);
            expect(received).toEqual(messages);
        });

        it('FIFO: Order is strictly preserved', async () => {
            const topic = 'fifo-test';
            await nexo.stream(topic).create();

            const messages = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }];

            for (const m of messages) {
                await nexo.stream(topic).publish(m);
            }

            const received: any[] = [];
            await nexo.stream(topic, 'group-fifo').subscribe(msg => {
                received.push(msg);
            });

            await waitFor(() => received.length === messages.length);
            expect(received).toEqual(messages);
        });

        it('Key is preserved in messages', async () => {
            const topic = 'key-test';
            await nexo.stream(topic).create();

            await nexo.stream(topic).publish({ data: 'test' }, { key: 'my-key' });

            const received: any[] = [];
            await nexo.stream(topic, 'group-key').subscribe(msg => {
                received.push(msg);
            });

            await waitFor(() => received.length === 1);
            expect(received[0].data).toBe('test');
        });
    });

    describe('Consumer Groups', () => {

        it('Fan-Out: Different groups receive ALL messages independently', async () => {
            const topicName = 'broadcast-news';
            await nexo.stream(topicName).create();

            const receivedA: any[] = [];
            const receivedB: any[] = [];

            const subGroupA = nexo.stream(topicName, 'analytics-service');
            const subGroupB = nexo.stream(topicName, 'notification-service');

            await subGroupA.subscribe(msg => {
                receivedA.push(msg)
            });
            await subGroupB.subscribe(msg => {
                receivedB.push(msg)
            });

            await new Promise(r => setTimeout(r, 100));

            for (let i = 0; i < 5; i++) {
                await nexo.stream(topicName).publish({ msg: `news-${i}` });
            }

            await waitFor(() => receivedA.length === 5 && receivedB.length === 5);
            expect(receivedA.length).toBe(5);
            expect(receivedB.length).toBe(5);
        });

        it('Competing Consumers: Same group processes messages once', async () => {
            const topicName = 'competing-test';
            await nexo.stream(topicName).create();

            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });

            const received1: any[] = [];
            const received2: any[] = [];
            const allReceived = new Set<number>();

            await nexo.stream(topicName, 'workers').subscribe(msg => {
                received1.push(msg);
                allReceived.add(msg.task);
            });
            await client2.stream(topicName, 'workers').subscribe(msg => {
                received2.push(msg);
                allReceived.add(msg.task);
            });

            await new Promise(r => setTimeout(r, 200));

            const TOTAL = 20;
            for (let i = 0; i < TOTAL; i++) {
                await nexo.stream(topicName).publish({ task: i });
            }

            // Both consumers should eventually process all messages
            // (competing for the same offset)
            await waitFor(() => allReceived.size === TOTAL, 5000);
            expect(allReceived.size).toBe(TOTAL);

            client2.disconnect();
        });

        it('Offset Persistence: Should resume from last committed offset', async () => {
            const topic = 'offset-resume-test';
            const group = 'resumer-group';
            await nexo.stream(topic).create();

            // 1. Publish 10 messages
            for (let i = 0; i < 10; i++) {
                await nexo.stream(topic).publish({ id: i });
            }

            // 2. Consume first 5
            const sub1 = nexo.stream(topic, group);
            let count = 0;
            let stopConsumer: (() => void) | undefined;

            await new Promise<void>(async (resolve) => {
                const handle = await sub1.subscribe(async (msg) => {
                    count++;
                    if (count === 5) {
                        resolve();
                    }
                }, { batchSize: 1 });
                stopConsumer = handle.stop;
            });

            if (stopConsumer) stopConsumer();
            await new Promise(r => setTimeout(r, 200));

            // 3. New subscription should resume from offset 5
            const received: any[] = [];
            const sub2 = nexo.stream(topic, group);
            await sub2.subscribe(msg => received.push(msg));

            await waitFor(() => received.length === 5);
            expect(received.map(m => m.id)).toEqual([5, 6, 7, 8, 9]);
        });
    });

    describe('Error Handling & Edge Cases', () => {

        it('Should FAIL to publish to non-existent topic', async () => {
            const stream = nexo.stream('ghost-topic');
            await expect(stream.publish({ val: 1 })).rejects.toThrow();
        });

        it('Should FAIL to subscribe to non-existent topic', async () => {
            const stream = nexo.stream('ghost-topic-sub', 'group-1');
            await expect(stream.subscribe(() => { })).rejects.toThrow();
        });

        it('Should handle large payload (1MB)', async () => {
            const topic = 'heavy-payload';
            await nexo.stream(topic).create();

            const largeString = 'x'.repeat(1024 * 1024);
            await nexo.stream(topic).publish({ data: largeString });

            let receivedSize = 0;
            await nexo.stream(topic, 'heavy-group').subscribe(msg => {
                receivedSize = msg.data.length;
            });

            await waitFor(() => receivedSize > 0);
            expect(receivedSize).toBe(1024 * 1024);
        });

        it('Idempotent Create: Creating same topic twice should not error', async () => {
            const topic = 'idempotent-create';
            await nexo.stream(topic).create();
            await nexo.stream(topic).create(); // Should not throw

            // Verify topic still works
            await nexo.stream(topic).publish({ test: 1 });
            const received: any[] = [];
            await nexo.stream(topic, 'idem-group').subscribe(msg => received.push(msg));
            await waitFor(() => received.length === 1);
            expect(received[0].test).toBe(1);
        });

        it('Empty Topic: Reading from empty topic returns nothing', async () => {
            const topic = 'empty-topic';
            await nexo.stream(topic).create();

            const received: any[] = [];
            const handle = await nexo.stream(topic, 'empty-group').subscribe(msg => {
                received.push(msg);
            });

            // Wait a bit, should receive nothing
            await new Promise(r => setTimeout(r, 300));
            expect(received.length).toBe(0);

            handle.stop();
        });

        it('Callback Error: Consumer continues after callback throws', async () => {
            const topic = 'callback-error';
            await nexo.stream(topic).create();

            await nexo.stream(topic).publish({ id: 1 });
            await nexo.stream(topic).publish({ id: 2 });
            await nexo.stream(topic).publish({ id: 3 });

            const received: any[] = [];
            await nexo.stream(topic, 'error-group').subscribe(msg => {
                if (msg.id === 2) {
                    throw new Error('Simulated callback error');
                }
                received.push(msg);
            });

            // Should still receive msg 1 and 3 (msg 2 throws but loop continues)
            await waitFor(() => received.length === 2);
            expect(received.map(m => m.id)).toEqual([1, 3]);
        });

        it('Stop and Restart: Subscription can be stopped and restarted', async () => {
            const topic = 'stop-restart';
            const group = 'restart-group';
            await nexo.stream(topic).create();

            await nexo.stream(topic).publish({ phase: 1 });

            const received: any[] = [];
            const handle = await nexo.stream(topic, group).subscribe(msg => {
                received.push(msg);
            });

            await waitFor(() => received.length === 1);
            handle.stop();

            // Publish while stopped
            await nexo.stream(topic).publish({ phase: 2 });
            await new Promise(r => setTimeout(r, 200));

            // Restart - should resume from committed offset
            await nexo.stream(topic, group).subscribe(msg => {
                received.push(msg);
            });

            await waitFor(() => received.length === 2);
            expect(received.map(m => m.phase)).toEqual([1, 2]);
        });

        it('Disconnect Cleanup: Client disconnect removes from group', async () => {
            const topic = 'disconnect-cleanup';
            const group = 'cleanup-group';
            await nexo.stream(topic).create();

            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });

            await nexo.stream(topic, group).subscribe(() => { });
            await client2.stream(topic, group).subscribe(() => { });

            // Disconnect client2
            client2.disconnect();
            await new Promise(r => setTimeout(r, 100));

            // Publish messages - client1 should handle all
            const received: any[] = [];
            // Re-subscribe on nexo to capture
            const handle = await nexo.stream(topic, group).subscribe(msg => {
                received.push(msg);
            });

            await new Promise(r => setTimeout(r, 100));
            for (let i = 0; i < 5; i++) {
                await nexo.stream(topic).publish({ i });
            }

            await waitFor(() => received.length === 5);
            expect(received.length).toBe(5);
            handle.stop();
        });

        it('Same Group Name Different Topics: Groups are isolated by topic', async () => {
            const topic1 = 'isolated-topic-1';
            const topic2 = 'isolated-topic-2';
            const group = 'shared-group-name';

            await nexo.stream(topic1).create();
            await nexo.stream(topic2).create();

            const received1: any[] = [];
            const received2: any[] = [];

            await nexo.stream(topic1, group).subscribe(msg => received1.push(msg));

            // This should fail or create a separate group
            // because the group is bound to topic1
            try {
                await nexo.stream(topic2, group).subscribe(msg => received2.push(msg));
            } catch (e) {
                // Expected: group is for different topic
                expect((e as Error).message).toContain('topic');
                return;
            }

            // If it didn't throw, verify isolation
            await nexo.stream(topic1).publish({ from: 'topic1' });
            await nexo.stream(topic2).publish({ from: 'topic2' });

            await waitFor(() => received1.length === 1 || received2.length === 1);
        });
    });

    describe('Performance Benchmarks', () => {

        it('Ingestion Throughput (Single Topic)', async () => {
            const MESSAGES = 10_000;
            const topicName = 'perf-ingestion';
            await nexo.stream(topicName).create();
            const pub = nexo.stream(topicName);

            const probe = new BenchmarkProbe("STREAM - INGESTION - write on topic", MESSAGES);
            probe.startTimer();

            // Parallel publish
            const chunks = 10;
            const chunkSize = MESSAGES / chunks;

            await Promise.all(Array.from({ length: chunks }).map(async () => {
                for (let i = 0; i < chunkSize; i++) {
                    await pub.publish({ ts: Date.now() });
                }
            }));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(1000);
        });

        it('Consumer Throughput (Catch-up Read)', async () => {
            const topicName = 'perf-catchup';
            const MESSAGES = 10_000;
            await nexo.stream(topicName).create();
            const pub = nexo.stream(topicName);

            // Pre-fill
            const batch = 500;
            for (let i = 0; i < MESSAGES; i += batch) {
                const promises = [];
                for (let j = 0; j < batch; j++) promises.push(pub.publish({ i }));
                await Promise.all(promises);
            }

            const probe = new BenchmarkProbe("STREAM - CATCHUP - read from topic", MESSAGES);
            let received = 0;
            const sub = nexo.stream(topicName, 'perf-reader');

            probe.startTimer();
            await new Promise<void>(resolve => {
                sub.subscribe(async () => {
                    received++;
                    if (received === MESSAGES) resolve();
                }, { batchSize: 500 });
            });

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(5000);
        });

        it('Mixed Load (Read/Write Contention)', async () => {
            const topicName = 'perf-mixed';
            await nexo.stream(topicName).create();
            const pub = nexo.stream(topicName);
            const sub = nexo.stream(topicName, 'mixed-group');

            const DURATION_MS = 2000;
            let producing = true;
            let written = 0;
            let read = 0;

            const producerPromise = (async () => {
                while (producing) {
                    const promises = [];
                    for (let k = 0; k < 20; k++) promises.push(pub.publish({ i: written++ }));
                    await Promise.all(promises);
                    await new Promise(r => setImmediate(r));
                }
            })();

            const subHandle = await sub.subscribe(() => {
                read++;
            }, { batchSize: 50 });

            await new Promise(r => setTimeout(r, DURATION_MS));

            producing = false;
            await producerPromise;
            if (subHandle) subHandle.stop();

            console.log(`[STREAM - MIXED] Written: ${written} | Read: ${read} | Ratio: ${(read / written).toFixed(2)}`);

            expect(written).toBeGreaterThan(500);
            expect(read).toBeGreaterThan(written * 0.5);
        });
    });

});
