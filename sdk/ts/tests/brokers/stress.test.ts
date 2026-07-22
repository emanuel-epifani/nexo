/**
 * Nexo TS SDK — Stress / Benchmark Suite
 *
 * Last run: 2026-07-22 | MacBook Pro M4 Pro | Battery High Performance
 *
 * THROUGHPUT (50k ops, 50 concurrent workers):
 *   STORE SET            190,972 ops/sec | p50: 0.21ms | p99: 1.56ms | MAX: 4.05ms
 *   STORE GET            100,133 ops/sec | p50: 0.32ms | p99: 3.77ms | MAX: 12.51ms
 *   QUEUE PUSH           172,822 ops/sec | p50: 0.23ms | p99: 1.14ms | MAX: 2.27ms
 *   QUEUE PUSH BATCH   1,214,513 ops/sec | p50: 0.04ms | p99: 0.07ms | MAX: 0.08ms
 *   STREAM PUBLISH       202,517 ops/sec | p50: 0.22ms | p99: 1.15ms | MAX: 2.47ms
 *   STREAM PUB BATCH   1,969,166 ops/sec | p50: 0.02ms | p99: 0.04ms | MAX: 0.04ms
 *   PUBSUB PUBLISH       205,926 ops/sec | p50: 0.23ms | p99: 0.98ms | MAX: 2.00ms
 *   QUEUE CONSUME+ACK    165,630 ops/sec
 *   STREAM SUB+ACK        34,295 ops/sec
 *
 * LATENCY (100k sequential ops):
 *   STORE SET             29,712 ops/sec | p50: 0.02ms | p99: 0.13ms | MAX: 5.59ms
 *   STORE GET             30,223 ops/sec | p50: 0.02ms | p99: 0.12ms | MAX: 12.45ms
 *   QUEUE PUSH            25,939 ops/sec | p50: 0.03ms | p99: 0.14ms | MAX: 24.27ms
 *   STREAM PUBLISH        20,187 ops/sec | p50: 0.04ms | p99: 0.21ms | MAX: 11.34ms
 *   PUBSUB PUBLISH        30,390 ops/sec | p50: 0.02ms | p99: 0.12ms | MAX: 14.49ms
 *
 * UTILS:
 *   runConcurrent     21,883,231 ops/sec
 */
import {describe, expect, it} from "vitest";
import {BenchmarkProbe} from "../utils/benchmark-misure";
import {nexo} from "../nexo";
import {runConcurrent} from "../../src/utils/concurrent";


describe('Stress test', () => {

    describe('THROUGHPUT', () => {
        it('STORE - SET - concurrent workers', async () => {
            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;

            const probe = new BenchmarkProbe('STORE SET', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await nexo.store.map.set(`bench-${workerId}-${i}`, `value-${i}`);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(30_000);
        });
        it('STORE - GET - concurrent workers', async () => {
            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;

            // Pre-populate keys
            for (let w = 0; w < WORKERS; w++)
                for (let i = 0; i < OPS_PER_WORKER; i++)
                    await nexo.store.map.set(`get-${w}-${i}`, `value-${i}`);

            const probe = new BenchmarkProbe('STORE GET', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await nexo.store.map.get(`get-${workerId}-${i}`);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            probe.printResult();
        });
        it('QUEUE - PUSH - concurrent workers', async () => {
            const q = nexo.queue('bench-queue-throughput');
            await q.create();

            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;
            const payload = {op: 'job', data: 'x', t: Date.now()};

            const probe = new BenchmarkProbe('QUEUE PUSH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await q.push(payload);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            probe.printResult();

            await q.delete();
        });
        it('QUEUE - PUSH BATCH - concurrent workers', async () => {
            const q = nexo.queue('bench-queue-batch-throughput');
            await q.create();

            const TOTAL = 50_000;
            const WORKERS = 50;
            const BATCH_SIZE = 100;
            const BATCHES_PER_WORKER = TOTAL / WORKERS / BATCH_SIZE;
            const payload = {op: 'job', data: 'x', t: Date.now()};

            const probe = new BenchmarkProbe('QUEUE PUSH BATCH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                const batch = Array.from({length: BATCH_SIZE}, () => ({data: payload}));
                for (let i = 0; i < BATCHES_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await q.pushBatch(batch);
                    probe.recordBatch(BATCH_SIZE, performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            probe.printResult();

            await q.delete();
        });
        it('STREAM - PUBLISH - concurrent workers', async () => {
            const topic = 'bench-stream-throughput';
            await nexo.stream(topic).create();

            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;
            const payload = {op: 'event', data: 'x', t: Date.now()};

            const probe = new BenchmarkProbe('STREAM PUBLISH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await nexo.stream(topic).publish(payload);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            probe.printResult();

            await nexo.stream(topic).delete();
        });
        it('STREAM - PUBLISH BATCH - concurrent workers', async () => {
            const topic = 'bench-stream-batch-throughput';
            await nexo.stream(topic).create();

            const TOTAL = 50_000;
            const WORKERS = 50;
            const BATCH_SIZE = 100;
            const BATCHES_PER_WORKER = TOTAL / WORKERS / BATCH_SIZE;
            const payload = {op: 'event', data: 'x', t: Date.now()};

            const probe = new BenchmarkProbe('STREAM PUBLISH BATCH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                const batch = Array.from({length: BATCH_SIZE}, () => ({data: payload}));
                for (let i = 0; i < BATCHES_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await nexo.stream(topic).publishBatch(batch);
                    probe.recordBatch(BATCH_SIZE, performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            probe.printResult();

            await nexo.stream(topic).delete();
        });
        it('PUBSUB - PUBLISH - concurrent workers', async () => {
            const topic = nexo.pubsub('bench/pubsub-throughput');
            const payload = {op: 'ping', data: 'x', t: Date.now()};

            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;

            const probe = new BenchmarkProbe('PUBSUB PUBLISH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await topic.publish(payload);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({length: WORKERS}, (_, i) => worker(i)));
            probe.printResult();
        });
        it('QUEUE - CONSUME+ACK - subscriber throughput', async () => {
            const q = nexo.queue('bench-queue-consume');
            await q.create();

            const TOTAL = 50_000;
            const payload = {op: 'job', data: 'x', t: Date.now()};

            // Pre-fill queue
            for (let i = 0; i < TOTAL; i++) await q.push(payload);

            const probe = new BenchmarkProbe('QUEUE CONSUME+ACK', TOTAL);
            let consumed = 0;
            probe.startTimer();

            const sub = await q.subscribe(async () => {
                consumed++;
            }, {batchSize: 50, waitMs: 100, concurrency: 10});

            while (consumed < TOTAL) await new Promise(r => setTimeout(r, 50));
            await sub.stop();
            probe.printResult();

            await q.delete();
        });
        it('STREAM - SUBSCRIBE+ACK - subscriber throughput', async () => {
            const topic = 'bench-stream-subscribe';
            await nexo.stream(topic).create();

            const TOTAL = 50_000;
            const payload = {op: 'event', data: 'x', t: Date.now()};

            // Pre-fill stream
            for (let i = 0; i < TOTAL; i++) await nexo.stream(topic).publish(payload);

            const probe = new BenchmarkProbe('STREAM SUBSCRIBE+ACK', TOTAL);
            let consumed = 0;
            probe.startTimer();

            const sub = await nexo.stream(topic).subscribe('bench-group', async () => {
                consumed++;
            }, {batchSize: 100, waitMs: 100, concurrency: 10});

            while (consumed < TOTAL) await new Promise(r => setTimeout(r, 50));
            await sub.stop();
            probe.printResult();

            await nexo.stream(topic).delete();
        });
    })

    describe('LATENCY', () => {
        it('STORE - SET - sequential', async () => {
            const ITERATIONS = 100_000;
            const probe = new BenchmarkProbe('STORE SET LATENCY', ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await nexo.store.map.set(`latency-${i}`, 'v');
                probe.record(performance.now() - t0);
            }

            probe.printResult();
        });
        it('STORE - GET - sequential', async () => {
            const ITERATIONS = 100_000;

            for (let i = 0; i < ITERATIONS; i++) await nexo.store.map.set(`lat-get-${i}`, `val-${i}`);

            const probe = new BenchmarkProbe('STORE GET LATENCY', ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await nexo.store.map.get(`lat-get-${i}`);
                probe.record(performance.now() - t0);
            }

            probe.printResult();
        });
        it('QUEUE - PUSH - sequential', async () => {
            const q = nexo.queue('bench-queue-latency');
            await q.create();

            const ITERATIONS = 100_000;
            const payload = { op: 'job', data: 'x', t: Date.now() };
            const probe = new BenchmarkProbe('QUEUE PUSH LATENCY', ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await q.push(payload);
                probe.record(performance.now() - t0);
            }

            probe.printResult();

            await q.delete();
        });
        it('STREAM - PUBLISH - sequential', async () => {
            const topic = 'bench-stream-latency';
            await nexo.stream(topic).create();

            const ITERATIONS = 100_000;
            const payload = { op: 'event', data: 'x', t: Date.now() };
            const probe = new BenchmarkProbe('STREAM PUBLISH LATENCY', ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await nexo.stream(topic).publish(payload);
                probe.record(performance.now() - t0);
            }

            probe.printResult();

            await nexo.stream(topic).delete();
        });
        it('PUBSUB -PUBLISH - sequential', async () => {
            const topic = nexo.pubsub('bench/pubsub-latency');
            const payload = { op: 'ping', data: 'x', t: Date.now() };

            const ITERATIONS = 100_000;
            const probe = new BenchmarkProbe('PUBSUB PUBLISH LATENCY', ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await topic.publish(payload);
                probe.record(performance.now() - t0);
            }

            probe.printResult();
        });
    })

    describe('UTILS', () => {
        it('runConcurrent - correctness (all items processed, no duplicates)', async () => {
            const items = Array.from({length: 1000}, (_, i) => i);
            const processed: number[] = [];
            let maxConcurrent = 0;
            let current = 0;

            await runConcurrent(items, 10, async (item) => {
                current++;
                maxConcurrent = Math.max(maxConcurrent, current);
                await new Promise(r => setTimeout(r, 0));
                processed.push(item);
                current--;
            });

            expect(processed.length).toBe(1000);
            expect(new Set(processed).size).toBe(1000);
            expect(maxConcurrent).toBeLessThanOrEqual(10);
        });

        it('runConcurrent - performance (100k items, concurrency 10)', async () => {
            const items = Array.from({length: 100_000}, (_, i) => i);
            const probe = new BenchmarkProbe('runConcurrent', items.length);
            probe.startTimer();

            await runConcurrent(items, 10, async () => {
                probe.record(0);
            });

            probe.printResult();
        });
    })

});