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
        it('STORE - PUSH - concurrent workers', async () => {
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
        it('QUEUE - SET - sequential', async () => {
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