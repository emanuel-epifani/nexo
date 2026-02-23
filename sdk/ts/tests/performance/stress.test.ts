import {describe, expect, it} from "vitest";
import {BenchmarkProbe} from "../utils/benchmark-misure";
import {nexo} from "../nexo";


describe('Stress test', () => {

    describe('THROUGHPUT', () => {

        it('Store SET - sustained throughput', async () => {
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

        it('Store GET - sustained throughput', async () => {
            // Pre-populate
            await nexo.store.map.set('bench-read-key', 'bench-value');

            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;

            const probe = new BenchmarkProbe('STORE GET', TOTAL);
            probe.startTimer();

            const worker = async () => {
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const t0 = performance.now();
                    await nexo.store.map.get('bench-read-key');
                    probe.record(performance.now() - t0);
                }
            };

            await Promise.all(Array.from({ length: WORKERS }, worker));
            const stats = probe.printResult();
            expect(stats.throughput).toBeGreaterThan(30_000);
        });

        it('Different payload - Serialization Overhead', async () => {
            const TOTAL = 50_000;
            const WORKERS = 50;
            const OPS_PER_WORKER = TOTAL / WORKERS;

            const testCases = [
                {
                    name: "JSON Object",
                    payload: { op: "ping", data: "test", timestamp: Date.now() },
                },
                {
                    name: "String",
                    payload: "ping-test-string-with-some-data",
                },
                {
                    name: "Raw Buffer",
                    payload: Buffer.from("raw-buffer-data", 'utf8'),
                }
            ];

            for (const testCase of testCases) {
                const probe = new BenchmarkProbe(`SERIALIZATION - ${testCase.name}`, TOTAL);
                probe.startTimer();

                const worker = async (workerId: number) => {
                    for (let i = 0; i < OPS_PER_WORKER; i++) {
                        const t0 = performance.now();
                        await nexo.store.map.set(`ser-${workerId}-${i}`, testCase.payload);
                        probe.record(performance.now() - t0);
                    }
                };

                await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i)));
                probe.printResult();
            }
        });

    });

    describe('LATENCY', () => {

        it('Single request latency (sequential)', async () => {
            const ITERATIONS = 10_000;
            const probe = new BenchmarkProbe('SINGLE REQ LATENCY', ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await nexo.store.map.set(`latency-${i}`, 'v');
                probe.record(performance.now() - t0);
            }

            probe.printResult();
        });

    });

});