import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '@nexo/client';
import { BenchmarkProbe } from '../utils/benchmark-misure';
import { waitFor } from '../utils/wait-for';

describe('Stream Broker (In-Memory)', () => {

    describe('Core Functionality (Happy Path)', () => {

        it('Should create a topic explicitly', async () => {
            const topicName = 'orders-v1';
            const stream = await nexo.stream(topicName, 'test-group').create({ partitions: 4 });
            expect(stream).toBeDefined();
        });

        it('Should publish and subscribe to messages (Basic Flow)', async () => {
            const topic = 'basic-flow';
            await nexo.stream(topic).create({ partitions: 1 });

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

        it('FIFO: With same key, order is strictly preserved', async () => {
            const topic = 'fifo-test';
            await nexo.stream(topic).create({ partitions: 2 });

            const messages = [{ id: 1 }, { id: 2 }, { id: 3 }];
            // Use same key to force same partition
            for (const m of messages) {
                await nexo.stream(topic).publish(m, { key: 'user-1' });
            }

            const received: any[] = [];
            await nexo.stream(topic, 'group-fifo').subscribe(msg => {
                received.push(msg);
            });

            await waitFor(() => received.length === messages.length);
            expect(received).toEqual(messages);
        });
    });

    describe('Partitioning & Scaling', () => {

        it('Should distribute messages across partitions (Round Robin without key)', async () => {
            const topic = 'parallel-test';
            await nexo.stream(topic).create({ partitions: 2 });

            // Publish 10 messages without key -> Round Robin
            for (let i = 0; i < 10; i++) await nexo.stream(topic).publish({ i });

            const received: any[] = [];
            await nexo.stream(topic, 'group-rr').subscribe(msg => {
                received.push(msg);
            });

            await waitFor(() => received.length === 10);
            expect(received).toHaveLength(10);
        });

        it('Isolation: Slow consumer on Partition 0 should NOT block Partition 1', async () => {
            const topic = 'isolation-test';
            await nexo.stream(topic).create({ partitions: 2 });
            const group = 'isolation-group';

            // Force messages to different partitions using keys
            // Assumes hashing distributes 'k1' and 'k2' differently (or we use enough keys to hit both)
            await nexo.stream(topic).publish({ target: 'slow' }, { key: 'k1' });
            await nexo.stream(topic).publish({ target: 'fast' }, { key: 'k2' });

            let fastReceived = false;

            // Single client subscribing to both, but processing one slowly?
            // Actually JS is single threaded, so we simulate logical blocking or just order of processing.
            // Better test: Ensure 'fast' message arrives even if 'slow' is "being processed".
            // Since the handler is async, we can simulate delay.

            nexo.stream(topic, group).subscribe(async (msg) => {
                if (msg.target === 'slow') {
                    await new Promise(r => setTimeout(r, 500)); // Simulate work
                } else if (msg.target === 'fast') {
                    fastReceived = true;
                }
            });

            // 'fast' should arrive quickly, even if 'slow' is stuck in the handler (if parallel processing works)
            // Note: The JS SDK loop implementation matters here. If it awaits callback before next fetch, it blocks.
            // If it launches callback without await (or Promise.all), it's parallel. 
            // The current SDK implementation awaits callback. So this tests if Partitions are fetched independently.

            await waitFor(() => fastReceived, 1000);
            expect(fastReceived).toBe(true);
        });
    });

    describe('Consumer Groups & Rebalancing', () => {

        it('Fan-Out: 2 consumers in DIFFERENT groups should BOTH receive ALL messages', async () => {
            const topicName = 'broadcast-news';
            await nexo.stream(topicName).create({ partitions: 4 });

            const receivedA: any[] = [];
            const receivedB: any[] = [];

            const subGroupA = nexo.stream(topicName, 'analytics-service');
            const subGroupB = nexo.stream(topicName, 'notification-service');

            await subGroupA.subscribe(msg => receivedA.push(msg));
            await subGroupB.subscribe(msg => receivedB.push(msg));

            // Wait slightly for subscriptions to register on server
            await new Promise(r => setTimeout(r, 100));

            for (let i = 0; i < 5; i++) {
                await nexo.stream(topicName).publish({ msg: `news-${i}` });
            }

            await waitFor(() => receivedA.length === 5 && receivedB.length === 5);
            expect(receivedA.length).toBe(5);
            expect(receivedB.length).toBe(5);
        });

        it('Load Balancing: 2 consumers in SAME group should SPLIT the load', async () => {
            const topicName = 'load-balance-test';
            await nexo.stream(topicName).create({ partitions: 4 });

            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });

            const received1: any[] = [];
            const received2: any[] = [];

            await nexo.stream(topicName, 'workers').subscribe(msg => received1.push(msg));
            await client2.stream(topicName, 'workers').subscribe(msg => received2.push(msg));

            // Wait for rebalancing (server side)
            // We can't query rebalance status easily, so we wait or just rely on eventual consistency
            await new Promise(r => setTimeout(r, 200));

            // Publish enough messages to ensure distribution
            const TOTAL = 20;
            for (let i = 0; i < TOTAL; i++) {
                await nexo.stream(topicName).publish({ task: i }, { key: `k-${i}` });
            }

            // Allow for duplicates (At-Least-Once due to rebalancing)
            await waitFor(() => (received1.length + received2.length) >= TOTAL);

            // Check uniqueness to confirm we got everything
            const allIds = new Set([...received1, ...received2].map(m => m.task));
            expect(allIds.size).toBe(TOTAL);

            expect(received1.length).toBeGreaterThan(0);
            expect(received2.length).toBeGreaterThan(0);

            client2.disconnect();
        });

        it('Failover: When a consumer leaves, the other takes over', async () => {
            const topicName = 'failover-test';
            await nexo.stream(topicName).create({ partitions: 2 });
            const group = 'failover-group';

            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST!,
                port: parseInt(process.env.NEXO_PORT!)
            });

            const received1: any[] = [];

            // Client 1 tracks messages
            await nexo.stream(topicName, group).subscribe(msg => received1.push(msg));
            // Client 2 is just a sink to occupy partitions
            await client2.stream(topicName, group).subscribe(() => { });

            await new Promise(r => setTimeout(r, 200)); // Allow join/rebalance

            // Kill client 2
            client2.disconnect();

            // Wait for server to detect disconnect/rebalance (implicit)
            // Publish messages
            for (let i = 0; i < 10; i++) {
                await nexo.stream(topicName).publish({ i });
            }

            // Client 1 should eventually pick up ALL partitions and get all messages
            // Allow duplicates
            await waitFor(() => received1.length >= 10, 5000);

            const unique = new Set(received1.map(m => m.i));
            expect(unique.size).toBe(10);
        });


        it('Offset Persistence: Should resume from last committed offset after restart', async () => {
            const topic = 'offset-resume-test';
            const group = 'resumer-group';
            await nexo.stream(topic).create({ partitions: 1 });

            // 1. Publish 10 messages: 0..9
            for (let i = 0; i < 10; i++) await nexo.stream(topic).publish({ id: i });

            // 2. Consume only first 5
            const sub1 = nexo.stream(topic, group);
            let count = 0;

            let stopConsumer: (() => void) | undefined;

            await new Promise<void>(async (resolve) => {
                const handle = await sub1.subscribe(async (msg) => {
                    count++;
                    if (count === 5) {
                        resolve();
                    }
                }, { batchSize: 1 }); // Important: fetch 1 by 1 to stop precisely
                stopConsumer = handle.stop;
            });

            if (stopConsumer) stopConsumer();

            // Allow async commit to happen on server
            await new Promise(r => setTimeout(r, 200));

            // 3. New Client (simulating restart) joins same group
            const received: any[] = [];
            const sub2 = nexo.stream(topic, group);
            await sub2.subscribe(msg => received.push(msg));

            // 4. Should receive only 5..9
            await waitFor(() => received.length === 5);
            expect(received.map(m => m.id)).toEqual([5, 6, 7, 8, 9]);
        });
    });

    describe('Advanced Logic & Edge Cases', () => {

        it('Oversubscription: Surplus consumers should remain idle', async () => {
            const topic = 'oversubscription-test';
            // Creiamo SOLO 2 partizioni
            await nexo.stream(topic).create({ partitions: 2 });
            const group = 'overflow-group';

            const receivedA: any[] = [];
            const receivedB: any[] = [];
            const receivedC: any[] = []; // Questo dovrebbe rimanere vuoto o quasi

            // Client 1
            await nexo.stream(topic, group).subscribe(m => receivedA.push(m));

            // Client 2 (Nuova connessione)
            const client2 = await NexoClient.connect({ host: process.env.NEXO_HOST!, port: parseInt(process.env.NEXO_PORT!) });
            await client2.stream(topic, group).subscribe(m => receivedB.push(m));

            // Client 3 (Nuova connessione - dovrebbe essere di troppo)
            const client3 = await NexoClient.connect({ host: process.env.NEXO_HOST!, port: parseInt(process.env.NEXO_PORT!) });
            await client3.stream(topic, group).subscribe(m => receivedC.push(m));

            await new Promise(r => setTimeout(r, 200)); // Attesa rebalance

            // Pubblichiamo 30 messaggi
            for (let i = 0; i < 30; i++) {
                await nexo.stream(topic).publish({ i }, { key: `k-${i}` });
            }

            await waitFor(() => (receivedA.length + receivedB.length + receivedC.length) === 30);

            // VERIFICA: Solo 2 client dovrebbero aver lavorato attivamente.
            // Nota: Il rebalance potrebbe aver spostato partizioni, ma alla fine stazionaria solo 2 lavorano.
            // Contiamo quanti client hanno ricevuto "significativamente" dati (>0)
            const activeClients = [receivedA.length, receivedB.length, receivedC.length].filter(l => l > 0).length;

            expect(activeClients).toBeLessThanOrEqual(2);

            client2.disconnect();
            client3.disconnect();
        });

        it('Zombie Commit: Should Reject Commit from Fenced (Old Epoch) Client', async () => {
            // 1. Setup
            const topic = 'zombie-fencing';
            const group = 'zombie-check';
            await nexo.stream(topic).create({ partitions: 1 });

            // Publish 1 message
            await nexo.stream(topic).publish({ val: 'critical-data' });

            // 2. Client A (Victim)
            const clientA = await NexoClient.connect({ host: process.env.NEXO_HOST!, port: parseInt(process.env.NEXO_PORT!) });
            const receivedA: any[] = [];

            // Hook to pause A before commit? We simulate "slow processing"
            // Using batchSize: 1 to ensure fetch-process-commit cycle is tight
            let resolveProcessingA: () => void;
            const processingA = new Promise<void>(r => resolveProcessingA = r);

            await clientA.stream(topic, group).subscribe(async (msg) => {
                receivedA.push(msg);
                // Client A hangs here! holding the old Epoch (e.g., 1)
                // Only hang on the FIRST message
                if (receivedA.length === 1) {
                    await processingA;
                }
            }, { batchSize: 1 });

            // Wait for A to fetch the message but NOT commit yet (it's stuck in await processingA)
            await waitFor(() => receivedA.length === 1);

            // 3. Client B (Usurper) joins -> Triggers Rebalance -> Epoch 2
            const clientB = await NexoClient.connect({ host: process.env.NEXO_HOST!, port: parseInt(process.env.NEXO_PORT!) });
            const receivedB: any[] = [];
            await clientB.stream(topic, group).subscribe(async (msg) => {
                receivedB.push(msg);
            });

            // Wait for Rebalance to complete on server
            await new Promise(r => setTimeout(r, 500));

            // 4. Resume A -> It will try to S_COMMIT with Epoch 1 -> FAIL -> REJOIN
            resolveProcessingA!();

            // Wait a bit for commit to fly, fail, and rejoin logic to kick in
            await new Promise(r => setTimeout(r, 500));

            // 5. Verification
            // The message 'critical-data' was NEVER committed.
            // So either A (after rejoin) or B (new owner) MUST receive it again.
            // receivedA would become 2 (duplicate delivery) OR receivedB would become 1.

            await waitFor(() => receivedA.length === 2 || receivedB.length === 1);

            if (receivedB.length === 1) {
                expect(receivedB[0].val).toBe('critical-data');
            } else {
                expect(receivedA[1].val).toBe('critical-data');
            }

            clientA.disconnect();
            clientB.disconnect();
        });
    });

    describe('Error Handling & Edge Cases', () => {

        it('Should FAIL to publish to a non-existent topic', async () => {
            const topicName = 'ghost-topic';
            const stream = nexo.stream(topicName);
            // Expect promise to reject
            await expect(stream.publish({ val: 1 })).rejects.toThrow();
        });

        it('Should FAIL to subscribe to a non-existent topic', async () => {
            const topicName = 'ghost-topic-sub';
            const stream = nexo.stream(topicName, 'group-1');
            await expect(stream.subscribe(() => { })).rejects.toThrow();
        });

        it('Should handle massive payload (boundary check)', async () => {
            const topic = 'heavy-payload';
            await nexo.stream(topic).create({ partitions: 1 });

            // 1MB string
            const largeString = 'x'.repeat(1024 * 1024);
            await nexo.stream(topic).publish({ data: largeString });

            let receivedSize = 0;
            await nexo.stream(topic, 'heavy-group').subscribe(msg => {
                receivedSize = msg.data.length;
            });

            await waitFor(() => receivedSize > 0);
            expect(receivedSize).toBe(1024 * 1024);
        });
    });

    describe('Performance Benchmarks', () => {

        it('Ingestion Throughput (1 Producer -> 4 Partitions)', async () => {
            const MESSAGES = 10_000; // Reduced for CI speed, scale up for real bench
            const topicName = 'perf-ingestion';
            await nexo.stream(topicName).create({ partitions: 4 });
            const pub = nexo.stream(topicName);

            const probe = new BenchmarkProbe("STREAM - INGESTION", MESSAGES);
            probe.startTimer();

            const chunks = 10;
            const chunkSize = MESSAGES / chunks;

            await Promise.all(Array.from({ length: chunks }).map(async () => {
                for (let i = 0; i < chunkSize; i++) {
                    await pub.publish({ ts: Date.now() });
                }
            }));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(1000); // Conservative check
        });

        it('Consumer Throughput (Catch-up Read)', async () => {
            const topicName = 'perf-catchup';
            const MESSAGES = 10_000;
            await nexo.stream(topicName).create({ partitions: 1 });
            const pub = nexo.stream(topicName);

            // Pre-fill
            const batch = 500;
            for (let i = 0; i < MESSAGES; i += batch) {
                const promises = [];
                for (let j = 0; j < batch; j++) promises.push(pub.publish({ i }));
                await Promise.all(promises);
            }

            const probe = new BenchmarkProbe("STREAM - CATCHUP", MESSAGES);
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

        it('Mixed Load Stress (Read/Write Contention)', async () => {
            const topicName = 'perf-mixed';
            await nexo.stream(topicName).create({ partitions: 4 });
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
