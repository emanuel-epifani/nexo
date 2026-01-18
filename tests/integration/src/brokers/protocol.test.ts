import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";
import { NexoClient } from "@nexo/client";
import { BenchmarkProbe } from "../utils/benchmark-misure";

const SERVER_PORT = parseInt(process.env.SERVER_PORT!);

describe('Nexo Protocol & Socket', () => {

    describe('Serialization & Data Types', () => {
        it('JSON Roundtrip - Complex nested object with special chars', async () => {
            const complexData = {
                string: "Nexo Engine",
                number: 42.5,
                boolean: true,
                nullValue: null,
                array: [1, "due", { tre: 3 }],
                nested: {
                    id: "abc-123",
                    tags: ["rust", "typescript", "fast"],
                    meta: { active: true, version: 0.2, deep: { value: "ok" } }
                },
                specialChars: "🚀🔥 €$ £",
                unicode: "こんにちは",
                longString: "A".repeat(1000),
                base64: Buffer.from("hello world").toString('base64')
            };

            const response = await (nexo as any).debug.echo(complexData);
            expect(response).toEqual(complexData);
        });

        it('JSON Roundtrip - Empty object', async () => {
            const result = await (nexo as any).debug.echo({});
            expect(result).toEqual({});
        });

        it('JSON Roundtrip - Null and undefined values', async () => {
            const result = await (nexo as any).debug.echo({ a: null, b: undefined });
            expect(result).toEqual({ a: null }); // undefined stripped by JSON
        });

        it('DataType - Empty string vs null distinction', async () => {
            await nexo.store.kv.set('proto:empty-str', '');
            await nexo.store.kv.set('proto:null-val', null);

            expect(await nexo.store.kv.get('proto:empty-str')).toBe('');
            expect(await nexo.store.kv.get('proto:null-val')).toBeNull();
        });

        it('DataType - Binary-like data via base64', async () => {
            const binaryData = Buffer.from([0x00, 0x01, 0xFF, 0xFE]).toString('base64');
            const result = await (nexo as any).debug.echo({ bin: binaryData });
            expect(result.bin).toBe(binaryData);
        });
    });

    describe('Performance & Benchmarks', () => {
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

            expect(stats.throughput).toBeGreaterThan(10_000); // Adjusted for stability
            expect(stats.p99).toBeLessThan(10);
        });
    });
});
