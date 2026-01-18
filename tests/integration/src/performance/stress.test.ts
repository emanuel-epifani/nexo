import {expect, it} from "vitest";
import {BenchmarkProbe} from "../utils/benchmark-misure";
import {nexo} from "../nexo";


describe('Stress test', async () => {

    describe('SOCKET', async () => {
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
            expect(stats.throughput).toBeGreaterThan(100_000); // Adjusted for stability
            expect(stats.p99).toBeLessThan(10);
        });

        it('Large Payload Throughput', async () => {
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

            expect(stats.throughput).toBeGreaterThan(10_000); // Adjusted for stability
            expect(stats.p99).toBeLessThan(10);
        });
    })

    describe('STORE', async () => {
        //todo: to implement
    })

    describe('QUEUE', async () => {
        //todo: to implement
    })

    describe('PUBSUB', async () => {
        //todo: to implement
    })

    describe('STREAM', async () => {
        //todo: to implement
    })

})