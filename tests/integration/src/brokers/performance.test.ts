import { describe, it, expect } from 'vitest';
import { NexoClient } from "@nexo/client/dist/client";
import { nexo } from "../nexo";

const SERVER_PORT = parseInt(process.env.NEXO_PORT || "8080");

class BenchmarkProbe {
    private latencies: number[] = [];
    private start: number = 0;
    private currentIndex = 0;

    constructor(private name: string, private totalOps: number) {
        this.latencies = new Array(totalOps);
    }

    startTimer() { this.start = performance.now(); }

    record(val: number) {
        if (this.currentIndex < this.totalOps) {
            this.latencies[this.currentIndex++] = val;
        }
    }

    // Per misurare latenza End-to-End (da timestamp nel payload)
    recordLatency(startMs: number) {
        this.record(Date.now() - startMs);
    }

    printResult() {
        const durationSec = (performance.now() - this.start) / 1000;
        const throughput = Math.floor(this.totalOps / durationSec);

        const validLatencies = this.latencies.slice(0, this.currentIndex).sort((a, b) => a - b);
        const count = validLatencies.length;

        const p50 = validLatencies[Math.floor(count * 0.50)] || 0;
        const p99 = validLatencies[Math.floor(count * 0.99)] || 0;
        const max = validLatencies[count - 1] || 0;

        console.log(`\n\x1b[36m[${this.name}]\x1b[0m`);
        console.log(` 🚀 Throughput:  \x1b[32m${throughput.toLocaleString()} ops/sec\x1b[0m`);

        if (count > 0) {
            const colorMax = max > 100 ? "\x1b[31m" : "\x1b[33m";
            console.log(` ⏱️  Latency:     p50: ${p50.toFixed(2)}ms | p99: ${p99.toFixed(2)}ms | ${colorMax}MAX: ${max.toFixed(2)}ms\x1b[0m (samples: ${count})`);
        } else {
            console.log(` ⏱️  Latency:     (no samples recorded)`);
        }

        return { throughput, p99, max };
    }
}

describe('Performance & Stress Tests', function () {

    // --- SOCKET CORE ---
    describe('1. Socket Core (Hot Path)', async () => {
        it('Small Payload Throughput', async () => {
            const TOTAL = 50_000;
            const CONCURRENCY = 500;
            const payload = { op: "ping" };

            const probe = new BenchmarkProbe("SOCKET - SMALL", TOTAL);
            probe.startTimer();

            const worker = async () => {
                const opsPerWorker = TOTAL / CONCURRENCY;
                for (let i = 0; i < opsPerWorker; i++) {
                    const t0 = performance.now();
                    await (nexo as any).debug.echo(payload);
                    probe.record(performance.now() - t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, worker));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(680_000); // Strict Baseline M4 Pro
            expect(stats.p99).toBeLessThan(3);
            expect(stats.max).toBeLessThan(5);
        });

        it('Large Payload (10KB) Bandwidth', async () => {
            const TOTAL = 5_000;
            const CONCURRENCY = 50;
            const payload = { data: "x".repeat(1024 * 10) };

            const probe = new BenchmarkProbe("SOCKET - 10KB", TOTAL);
            probe.startTimer();

            const worker = async () => {
                const opsPerWorker = TOTAL / CONCURRENCY;
                for (let i = 0; i < opsPerWorker; i++) {
                    const t0 = performance.now();
                    await (nexo as any).debug.echo(payload);
                    probe.record(performance.now() - t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, worker));

            const stats = probe.printResult();
            const mbs = (stats.throughput * 10) / 1024;
            console.log(` 📦 Bandwidth:   ~${mbs.toFixed(1)} MB/s`);

            expect(stats.throughput).toBeGreaterThan(28_000); // Strict Baseline M4 Pro
            expect(stats.p99).toBeLessThan(2.5);
            expect(stats.max).toBeLessThan(3);
        });
    });

    // --- STORE ---
    describe('2. STORE Broker', function () {
        it('Write Throughput', async () => {
            const TOTAL = 100_000;
            const CONCURRENCY = 200;

            const probe = new BenchmarkProbe("STORE - WRITE", TOTAL);
            probe.startTimer();

            const worker = async (id: number) => {
                const opsPerWorker = TOTAL / CONCURRENCY;
                for (let i = 0; i < opsPerWorker; i++) {
                    const t0 = performance.now();
                    await nexo.store.kv.set(`stress:${id}:${i}`, "val");
                    probe.record(performance.now() - t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(790_000); // Strict Baseline M4 Pro
            expect(stats.p99).toBeLessThan(1);
            expect(stats.max).toBeLessThan(3);
        });

        it('Read Throughput', async () => {
            const PREFILL = 100_000;
            const READ_OPS = 50_000;
            const CONCURRENCY = 100;

            const batchSize = 1000;
            for (let i = 0; i < PREFILL; i += batchSize) {
                const batch = [];
                for (let j = 0; j < batchSize; j++) batch.push(nexo.store.kv.set(`k:${i + j}`, "static"));
                await Promise.all(batch);
            }

            const probe = new BenchmarkProbe("STORE - READ", READ_OPS);
            probe.startTimer();

            const worker = async () => {
                const opsPerWorker = READ_OPS / CONCURRENCY;
                for (let i = 0; i < opsPerWorker; i++) {
                    const key = `k:${Math.floor(Math.random() * PREFILL)}`;
                    const t0 = performance.now();
                    await nexo.store.kv.get(key);
                    probe.record(performance.now() - t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, worker));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(690_000); // Strict Baseline M4 Pro
            expect(stats.p99).toBeLessThan(1);
            expect(stats.max).toBeLessThan(3);
        });
    });

    // --- PUBSUB ---
    describe('3. PUBSUB Broker', function () {
        it('Fan-Out (1 Pub -> 100 Subs)', async () => {
            const SUBSCRIBERS = 100;
            const MESSAGES = 100;
            const TOTAL_EVENTS = MESSAGES * SUBSCRIBERS;

            const clients: NexoClient[] = [];
            let received = 0;
            const probe = new BenchmarkProbe("PUBSUB - FANOUT", TOTAL_EVENTS);

            for (let i = 0; i < SUBSCRIBERS; i++) {
                const c = await NexoClient.connect({ port: SERVER_PORT });
                await c.pubsub.subscribe('perf/fanout', (msg: any) => {
                    received++;
                    if (msg.ts) probe.recordLatency(msg.ts);
                });
                clients.push(c);
            }

            probe.startTimer();
            await Promise.all(Array.from({ length: MESSAGES }).map(() => nexo.pubsub.publish('perf/fanout', { ts: Date.now() })));

            while (received < TOTAL_EVENTS) {
                await new Promise(r => setTimeout(r, 10));
                if ((performance.now() - probe['start']) > 5000) break;
            }

            const stats = probe.printResult();
            clients.forEach(c => c.disconnect());
            expect(received).toBe(TOTAL_EVENTS);
            expect(stats.throughput).toBeGreaterThan(200_000); // Strict Baseline M4 Pro
        });

        it('Fan-In (50 Pubs -> 1 Sub)', async () => {
            const PUBLISHERS = 50;
            const MSGS_PER_PUB = 50;
            const TOTAL_EXPECTED = PUBLISHERS * MSGS_PER_PUB;
            const clients: NexoClient[] = [];
            let received = 0;

            const probe = new BenchmarkProbe("PUBSUB - FANIN", TOTAL_EXPECTED);

            await nexo.pubsub.subscribe('sensors/+', (msg: any) => {
                received++;
                if (msg.ts) probe.recordLatency(msg.ts);
            });

            for (let i = 0; i < PUBLISHERS; i++) {
                clients.push(await NexoClient.connect({ port: SERVER_PORT }));
            }

            probe.startTimer();
            await Promise.all(clients.map((c, i) => {
                const promises = [];
                for (let k = 0; k < MSGS_PER_PUB; k++) {
                    promises.push(c.pubsub.publish(`sensors/d${i}`, { ts: Date.now() }));
                }
                return Promise.all(promises);
            }));

            while (received < TOTAL_EXPECTED) {
                await new Promise(r => setTimeout(r, 10));
                if ((performance.now() - probe['start']) > 5000) break;
            }

            probe.printResult();
            clients.forEach(c => c.disconnect());
            expect(received).toBe(TOTAL_EXPECTED);
        });

        it('Wildcard Routing Stress', async () => {
            const OPS = 10_000;
            let received = 0;
            await nexo.pubsub.subscribe('infra/+/+/cpu', () => { received++; });

            const probe = new BenchmarkProbe("PUBSUB - WILDCARD", OPS);
            probe.startTimer();

            const worker = async () => {
                const opsPerWorker = OPS / 10;
                for (let i = 0; i < opsPerWorker; i++) {
                    const t0 = performance.now();
                    await nexo.pubsub.publish('infra/us-east/server-1/cpu', { u: 90 });
                    probe.record(performance.now() - t0);
                }
            };
            await Promise.all(Array.from({ length: 10 }, worker));

            while (received < OPS) await new Promise(r => setTimeout(r, 10));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(130_000); // Strict Baseline M4 Pro
            expect(stats.p99).toBeLessThan(5);
        });
    });

    // --- QUEUES ---
    describe('4. QUEUE Broker', function () {
        it('Producer (Push) Throughput', async () => {
            const TOTAL = 20_000;
            const CONCURRENCY = 50;
            const q = nexo.queue("bench-push");

            const probe = new BenchmarkProbe("QUEUE - PUSH", TOTAL);
            probe.startTimer();

            const producer = async () => {
                const opsPerWorker = TOTAL / CONCURRENCY;
                for (let i = 0; i < opsPerWorker; i++) {
                    const t0 = performance.now();
                    await q.push({ job: i });
                    probe.record(performance.now() - t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, producer));

            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(200_000); // Strict Baseline M4 Pro
            expect(stats.p99).toBeLessThan(5);
        });

        it('Consumer (Subscribe) Throughput', async () => {
            const TOTAL = 10_000;
            const CONCURRENCY_PUSH = 10;
            const q = nexo.queue("bench-concurrent");

            let consumed = 0;
            const probe = new BenchmarkProbe("QUEUE - CONSUME", TOTAL);

            // 1. Consumer ready
            const consumerPromise = new Promise<void>((resolve) => {
                const sub = q.subscribe(async (msg: any) => {
                    consumed++;
                    if (msg.ts) probe.recordLatency(msg.ts);

                    if (consumed >= TOTAL) {
                        sub.stop();
                        const stats = probe.printResult();
                        expect(stats.throughput).toBeGreaterThan(15_000); // Baseline concurrent
                        expect(stats.p99).toBeLessThan(10); // Should be fast in steady state
                        resolve();
                    }
                });
            });

            probe.startTimer();

            // 2. Producer start
            const producer = async () => {
                const opsPerWorker = TOTAL / CONCURRENCY_PUSH;
                for (let i = 0; i < opsPerWorker; i++) {
                    await q.push({ ts: Date.now() });
                }
            };
            const producerPromise = Promise.all(Array.from({ length: CONCURRENCY_PUSH }, producer));

            await Promise.all([producerPromise, consumerPromise]);
        });
    });
});
