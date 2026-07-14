import {describe, expect, it} from "vitest";
import {BenchmarkProbe} from "../utils/benchmark-misure";
import {nexo} from "../nexo";


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

            await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i)));
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

            await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i)));
            probe.printResult();
        });
        it('QUEUE - PUSH - concurrent workers', async () => {
            const q = nexo.queue('bench-queue-throughput');
            await q.create();

            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;
            const payload = { op: 'job', data: 'x', t: Date.now() };

            const probe = new BenchmarkProbe('QUEUE PUSH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await q.push(payload);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i)));
            probe.printResult();

            await q.delete();
        });
        it('STREAM - PUBLISH - concurrent workers', async () => {
            const topic = 'bench-stream-throughput';
            await nexo.stream(topic).create();

            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;
            const payload = { op: 'event', data: 'x', t: Date.now() };

            const probe = new BenchmarkProbe('STREAM PUBLISH', TOTAL);
            probe.startTimer();

            const worker = async (workerId: number) => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await nexo.stream(topic).publish(payload);
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i)));
            probe.printResult();

            await nexo.stream(topic).delete();
        });
        it('PUBSUB - PUBLISH - concurrent workers', async () => {
            const topic = nexo.pubsub('bench/pubsub-throughput');
            const payload = { op: 'ping', data: 'x', t: Date.now() };

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

            await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i)));
            probe.printResult();
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

});
/* PRIMA

[STORE SET]
 🚀 Throughput:  217,166 ops/sec
 ⏱️  Latency:     p50: 0.22ms | p99: 0.96ms | MAX: 1.95ms (samples: 50000)

[STORE GET]
 🚀 Throughput:  235,981 ops/sec
 ⏱️  Latency:     p50: 0.21ms | p99: 0.86ms | MAX: 1.58ms (samples: 50000)

[QUEUE PUSH]
 🚀 Throughput:  155,862 ops/sec
 ⏱️  Latency:     p50: 0.27ms | p99: 1.07ms | MAX: 3.68ms (samples: 50000)

[STREAM PUBLISH]
 🚀 Throughput:  161,910 ops/sec
 ⏱️  Latency:     p50: 0.24ms | p99: 1.27ms | MAX: 9.31ms (samples: 50000)

[PUBSUB PUBLISH]
 🚀 Throughput:  209,122 ops/sec
 ⏱️  Latency:     p50: 0.24ms | p99: 0.97ms | MAX: 1.63ms (samples: 50000)

[STORE SET LATENCY]
 🚀 Throughput:  44,699 ops/sec
 ⏱️  Latency:     p50: 0.02ms | p99: 0.05ms | MAX: 1.41ms (samples: 100000)

[STORE GET LATENCY]
 🚀 Throughput:  43,625 ops/sec
 ⏱️  Latency:     p50: 0.02ms | p99: 0.05ms | MAX: 0.89ms (samples: 100000)
2026-07-14T10:15:23.475584Z  INFO Queue Persistence Writer stopped for "./data/queues/bench-queue-throughput.db"
2026-07-14T10:15:23.484637Z  INFO [StreamManager] Creating topic 'bench-stream-throughput'

[QUEUE PUSH LATENCY]
 🚀 Throughput:  38,906 ops/sec
 ⏱️  Latency:     p50: 0.02ms | p99: 0.05ms | MAX: 5.81ms (samples: 100000)
2026-07-14T10:15:33.515130Z  INFO Queue Persistence Writer stopped for "./data/queues/bench-queue-latency.db"
2026-07-14T10:15:33.519617Z  INFO [StreamManager] Creating topic 'bench-stream-latency'
2026-07-14T10:15:38.284615Z  INFO [StreamManager] Disconnecting client: 618341ed-0371-4529-899d-be6c178d4bfc

[STREAM PUBLISH LATENCY]
 🚀 Throughput:  41,957 ops/sec
 ⏱️  Latency:     p50: 0.02ms | p99: 0.05ms | MAX: 0.88ms (samples: 100000)

[PUBSUB PUBLISH LATENCY]
 🚀 Throughput:  42,915 ops/sec
 ⏱️  Latency:     p50: 0.02ms | p99: 0.05ms | MAX: 0.61ms (samples: 100000)

 */
/*DOPO

 */