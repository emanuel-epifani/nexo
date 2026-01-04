import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { NexoClient } from '@nexo/client';
import { BenchmarkProbe } from '../utils/benchmark-misure';

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
            let subHandle1: { stop: () => void } | undefined;

            await new Promise<void>(async (resolve, reject) => {
                const timeout = setTimeout(() => reject(new Error("Client 1 Timeout")), 5000);

                subHandle1 = await sub1.subscribe(async (msg) => {
                    count1++;
                    if (count1 === 20) {
                        console.log("Client 1 reached 20 messages. Stopping SDK loop...");
                        clearTimeout(timeout);
                        if (subHandle1) subHandle1.stop();
                        // Wait for loop to exit and auto-commit to happen
                        setTimeout(resolve, 500);
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

    describe('Performance Benchmarks', () => {

        /**
         * SCENARIO 1: Ingestion Burst (Write-Heavy)
         * Question: "Can the system handle a traffic spike (e.g. Black Friday)?"
         * Focus: Server write throughput, Locking efficiency, Memory allocation speed.
         * Setup: Multiple parallel producers, NO consumers.
         */
        it('Ingestion Throughput (1 Producer -> 4 Partitions)', async () => {
            const MESSAGES = 50_000;
            const topicName = 'perf-ingestion';
            await nexo.stream(topicName).create({ partitions: 4 });
            const pub = nexo.stream(topicName);

            const probe = new BenchmarkProbe("STREAM - INGESTION", MESSAGES);
            probe.startTimer();

            // Parallel writes simulating high load
            const chunks = 50;
            const chunkSize = MESSAGES / chunks;

            await Promise.all(Array.from({ length: chunks }).map(async (_, c) => {
                for (let i = 0; i < chunkSize; i++) {
                    // Use round-robin (no key) for max speed
                    await pub.publish({ ts: Date.now() });
                }
            }));

            const stats = probe.printResult();
            // Expectation: Rust in-memory should easily handle > 100k/sec locally
            // Lowered expectation slightly for CI/CD or slower envs, but should be fast
            expect(stats.throughput).toBeGreaterThan(10_000);
        });

        /**
         * SCENARIO 2: Consumer Lag Recovery (Read-Heavy)
         * Question: "If a worker crashes for an hour, how fast can it catch up?"
         * Focus: Sequential Read speed, Batch Serialization, Network bandwidth.
         * Setup: Pre-filled topic, Single consumer reading everything in large batches.
         */
        it('Consumer Throughput (Catch-up Read 50k msgs)', async () => {
            // Setup: topic with data already inside
            const topicName = 'perf-catchup';
            const MESSAGES = 50_000;
            await nexo.stream(topicName).create({ partitions: 1 }); // 1 partition to test single-thread raw read speed
            const pub = nexo.stream(topicName);

            // Pre-fill
            const promises = [];
            for (let i = 0; i < MESSAGES; i += 1000) { // Batch publish for setup speed
                promises.push((async () => {
                    for (let j = 0; j < 1000; j++) await pub.publish({ i });
                })());
            }
            await Promise.all(promises);

            // Start Consume
            const probe = new BenchmarkProbe("STREAM - CATCHUP", MESSAGES);
            const sub = nexo.stream(topicName, 'perf-reader');

            let received = 0;
            probe.startTimer();

            await new Promise<void>(resolve => {
                sub.subscribe(async () => {
                    received++;
                    if (received === MESSAGES) resolve();
                }, { batchSize: 1000 }); // Large batch for max throughput
            });

            const stats = probe.printResult();
            // Reading from RAM should be insanely fast
            expect(stats.throughput).toBeGreaterThan(20_000);
        });

        /**
         * SCENARIO 3: End-to-End Latency (Realtime)
         * Question: "What is the delay between Event Occurred -> Event Processed?"
         * Focus: Polling interval, Network Roundtrip, Processing Overhead.
         * Setup: 1 Producer sending slowly, 1 Consumer reading immediately.
         */
        it('End-to-End Latency (Ping-Pong)', async () => {
            const topicName = 'perf-latency';
            const MESSAGES = 2000;
            await nexo.stream(topicName).create({ partitions: 1 });

            const pub = nexo.stream(topicName);
            const sub = nexo.stream(topicName, 'latency-group');

            const probe = new BenchmarkProbe("STREAM - E2E LATENCY", MESSAGES);

            let received = 0;
            const done = new Promise<void>(resolve => {
                sub.subscribe((msg: any) => {
                    received++;
                    probe.recordLatency(msg.ts);
                    if (received === MESSAGES) resolve();
                }, { batchSize: 1 }); // Batch 1 for lowest latency (aggressive polling)
            });

            // Wait for subscription to be active
            await new Promise(r => setTimeout(r, 500));

            probe.startTimer();
            for (let i = 0; i < MESSAGES; i++) {
                await pub.publish({ ts: Date.now() });
                // Small delay to simulate real traffic, not batch dump
                if (i % 100 === 0) await new Promise(r => setTimeout(r, 1));
            }

            await done;
            const stats = probe.printResult();

            // Latency should be decent
            // expect(stats.avg).toBeLessThan(50); // < 50ms average
        });

        /**
         * SCENARIO 4: Mixed Load (Read + Write Contention)
         * Question: "Does the system lock up when reading and writing simultaneously?"
         * Focus: RwLock contention (Writer vs Reader starvation).
         * Setup: Continuous Write + Continuous Read for fixed duration.
         */
        it('Mixed Load Stress (Read/Write Contention)', async () => {
            const topicName = 'perf-mixed';
            await nexo.stream(topicName).create({ partitions: 4 });

            const pub = nexo.stream(topicName);
            const sub = nexo.stream(topicName, 'mixed-group');

            const DURATION_MS = 2000;
            let producing = true;
            let written = 0;
            let read = 0;

            // 1. Producer Loop (Flood)
            const producerPromise = (async () => {
                while (producing) {
                    // Send small batches to avoid flooding event loop too much
                    const promises = [];
                    for (let k = 0; k < 50; k++) promises.push(pub.publish({ i: written++ }));
                    await Promise.all(promises);
                    // Small yield to let reader breathe in JS runtime
                    await new Promise(r => setImmediate(r));
                }
            })();

            // 2. Consumer Loop
            const subHandle = await sub.subscribe(() => {
                read++;
            }, { batchSize: 100 });

            // Run for X seconds
            await new Promise(r => setTimeout(r, DURATION_MS));

            producing = false;
            await producerPromise;
            if (subHandle) subHandle.stop();

            console.log(`[STREAM - MIXED] Written: ${written} | Read: ${read} | Ratio: ${(read / written).toFixed(2)}`);

            // Expectations:
            // 1. Should have written A LOT (throughput check)
            expect(written).toBeGreaterThan(1000);

            // 2. Should have read A LOT (contention check)
            // Ideally Read should be close to Written (minus lag at end). 
            // If Read is 0 or very low, Readers are starved by Writers.
            expect(read).toBeGreaterThan(written * 0.5);
        });
    });

});
