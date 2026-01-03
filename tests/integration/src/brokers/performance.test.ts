import { describe, it, expect } from 'vitest';
import { NexoClient } from "@nexo/client/dist/client";
import { nexo } from "../nexo";

const SERVER_PORT = parseInt(process.env.NEXO_PORT || "8080");

class BenchmarkProbe {
    private latencies: number[] = [];
    private start: number = 0;

    constructor(private name: string, private totalOps: number) {
        // Pre-allocate memory to avoid resizing overhead during test
        // This makes 100% sampling much cheaper!
        this.latencies = new Array(totalOps);
    }

    private currentIndex = 0;

    startTimer() { this.start = performance.now(); }

    // Record every single operation (100% Sampling - Audit Grade)
    record(startMs: number) {
        if (this.currentIndex < this.totalOps) {
            this.latencies[this.currentIndex++] = performance.now() - startMs;
        }
    }

    printResult() {
        const durationSec = (performance.now() - this.start) / 1000;
        const throughput = Math.floor(this.totalOps / durationSec);

        // Filter out empty slots if test didn't complete exactly totalOps
        const validLatencies = this.latencies.slice(0, this.currentIndex).sort((a, b) => a - b);
        const count = validLatencies.length;

        // LE METRICHE PRAGMATICHE (AUDIT GRADE)
        const p50 = validLatencies[Math.floor(count * 0.50)] || 0;
        const p99 = validLatencies[Math.floor(count * 0.99)] || 0;
        const max = validLatencies[count - 1] || 0;

        console.log(`\n\x1b[36m[${this.name}]\x1b[0m`);
        console.log(` 🚀 Throughput:  \x1b[32m${throughput.toLocaleString()} ops/sec\x1b[0m`);

        if (count > 0) {
            // Regola del pollice: se MAX > 100ms su localhost, c'è un problema.
            const colorMax = max > 100 ? "\x1b[31m" : "\x1b[33m"; // Rosso se > 100ms
            console.log(` ⏱️  Latency:     p50: ${p50.toFixed(2)}ms | p99: ${p99.toFixed(2)}ms | ${colorMax}MAX: ${max.toFixed(2)}ms\x1b[0m (100% Audit: ${count} samples)`);
        } else {
            console.log(` ⏱️  Latency:     (no samples recorded)`);
        }
        return throughput;
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
                    probe.record(t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, worker));

            const throughput = probe.printResult();
            expect(throughput).toBeGreaterThan(50_000);
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
                    probe.record(t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, worker));

            const throughput = probe.printResult();
            const mbs = (throughput * 10) / 1024;
            console.log(` 📦 Bandwidth:   ~${mbs.toFixed(1)} MB/s`);
            expect(throughput).toBeGreaterThan(1000);
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
                    probe.record(t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

            const throughput = probe.printResult();
            expect(throughput).toBeGreaterThan(50_000);
        });

        it('Read Throughput (1M Keys)', async () => {
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
                    probe.record(t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, worker));

            const throughput = probe.printResult();
            expect(throughput).toBeGreaterThan(50_000);
        });
    });

    // --- PUBSUB ---
    describe('3. PUBSUB Broker', function () {
        it('Fan-Out (Broadcasting)', async () => {
            const SUBSCRIBERS = 100;
            const MESSAGES = 100;
            const TOTAL_EVENTS = MESSAGES * SUBSCRIBERS;

            const clients: NexoClient[] = [];
            let received = 0;

            for (let i = 0; i < SUBSCRIBERS; i++) {
                const c = await NexoClient.connect({ port: SERVER_PORT });
                await c.pubsub.subscribe('perf/fanout', () => { received++; });
                clients.push(c);
            }

            // Fan-out is asynchronous, hard to measure per-message latency reliably without complex coordination
            // Keeping throughput only for this specific scenario
            const probe = new BenchmarkProbe("PUBSUB - FANOUT (1 Pub -> ${SUBSCRIBERS} Subs)", TOTAL_EVENTS);
            probe.startTimer();

            await Promise.all(Array.from({ length: MESSAGES }).map(() => nexo.pubsub.publish('perf/fanout', { t: 1 })));

            while (received < TOTAL_EVENTS) {
                await new Promise(r => setTimeout(r, 10));
                if ((performance.now() - probe['start']) > 5000) break;
            }

            const throughput = probe.printResult();
            clients.forEach(c => c.disconnect());
            expect(received).toBe(TOTAL_EVENTS);
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
                    probe.record(t0);
                }
            };
            await Promise.all(Array.from({ length: 10 }, worker));

            while (received < OPS) await new Promise(r => setTimeout(r, 10));

            probe.printResult();
        });
    });

    // --- QUEUES ---
    describe('4. QUEUE Broker', function () {
        it('Push Throughput', async () => {
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
                    probe.record(t0);
                }
            };
            await Promise.all(Array.from({ length: CONCURRENCY }, producer));

            probe.printResult();
        });

        it('Pop (Subscribe) Throughput', async () => {
            const TOTAL = 5_000;
            const q = nexo.queue("bench-pop");
            for (let i = 0; i < TOTAL; i++) await q.push({ j: i });

            let consumed = 0;
            const probe = new BenchmarkProbe("QUEUE - POP", TOTAL);
            probe.startTimer();

            return new Promise<void>((resolve) => {
                const sub = q.subscribe(async (msg) => {
                    consumed++;
                    if (consumed >= TOTAL) {
                        sub.stop();
                        probe.printResult();
                        resolve();
                    }
                });
            });
        });
    });
});
