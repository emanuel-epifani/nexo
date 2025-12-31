import { describe, it, expect } from 'vitest';
import { nexo } from "./nexo";

describe('Nexo Protocol & Socket', () => {

    it('Protocol -> Complex Object Roundtrip (JSON Serialization & Binary Framing)', async () => {
        const complexData = {
            string: "Nexo Engine",
            number: 42.5,
            boolean: true,
            nullValue: null,
            array: [1, "due", { tre: 3 }],
            nested: {
                id: "abc-123",
                tags: ["rust", "typescript", "fast"],
                meta: {
                    active: true,
                    version: 0.2,
                    nestedAgain: {
                        deep: "value"
                    }
                }
            },
            specialChars: "🚀🔥 €$ £",
            unicode: "こんにちは",
            longString: "A".repeat(1000), // Test larger payloads
            binarySim: Buffer.from("hello world").toString('base64') // JSON handles buffers as strings usually, but let's test typical JS usage
        };

        // Test the internal request/send mechanism via the debug namespace
        // This validates: 
        // 1. DataCodec (JSON.stringify)
        // 2. RequestBuilder (Binary framing, Opcode, FrameType, Lengths)
        // 3. Socket Transmission (TCP Fragmentation/Merging)
        // 4. Rust Router (Jump table, Payload parsing)
        // 5. Rust Response encoding
        // 6. RingDecoder (Frame extraction from stream)
        // 7. DataCodec (JSON.parse)
        const response = await (nexo as any).debug.echo(complexData);
        
        expect(response).toEqual(complexData);
    });

    it('Protocol -> Multiple rapid requests (Concurrency & Frame ID matching)', async () => {
        const requests = Array.from({ length: 50 }, (_, i) => ({
            id: i,
            token: Math.random().toString(36),
            timestamp: Date.now()
        }));

        const results = await Promise.all(
            requests.map(data => (nexo as any).debug.echo(data))
        );

        expect(results).toEqual(requests);
    });

    it('Performance -> Throughput Benchmark (FULL PIPE STRESS)', async () => {
        const TOTAL_OPERATIONS = 1_000_000;
        const CONCURRENCY = 1000; // Saturiamo il batch buffer da 64KB
        const payload = {
            id: "bench-123",
            timestamp: Date.now(),
            data: "stressing-the-pipe-with-some-bytes-to-parse-and-decode"
        };

        const opsPerWorker = TOTAL_OPERATIONS / CONCURRENCY;
        const start = performance.now();

        const worker = async () => {
            for (let i = 0; i < opsPerWorker; i++) {
                await (nexo as any).debug.echo(payload);
            }
        };

        await Promise.all(Array.from({ length: CONCURRENCY }, worker));

        const end = performance.now();
        const durationSeconds = (end - start) / 1000;
        const throughput = Math.floor(TOTAL_OPERATIONS / durationSeconds);

        console.log(`\n\x1b[32m[PERF] Full Pipe Throughput: ${throughput.toLocaleString()} ops/sec\x1b[0m`);

        expect(throughput).toBeGreaterThan(800_000);
    });

    it('Performance -> AVG Throughput (ops/sec) with 10 Million existing keys', async () => {
        const PREFILL_COUNT = 5_000_000;
        const CONCURRENCY = 1000;
        const DURATION_MS = 2000;

        console.log(`📦 Fase 1: Pre-fill di ${PREFILL_COUNT.toLocaleString()} chiavi...`);

        // Riempiamo la memoria (usiamo i batch per fare in fretta)
        for (let i = 0; i < PREFILL_COUNT; i += 1000) {
            const batch = [];
            for (let j = 0; j < 1000; j++) {
                batch.push(nexo.store.kv.set(`dummy:${i + j}`, "payload", 3)); // Scadono tra 3 secondi
            }
            await Promise.all(batch);
        }

        console.log(`🚀 Fase 2: Avvio Benchmark su ${CONCURRENCY} workers...`);

        let operations = 0;
        let isRunning = true;
        const start = performance.now();

        const worker = async () => {
            while (isRunning) {
                await nexo.store.kv.set(`bench:${operations++}`, "x".repeat(256));
            }
        };

        const workers = Array(CONCURRENCY).fill(null).map(() => worker());

        await new Promise(resolve => setTimeout(resolve, DURATION_MS));
        isRunning = false;
        await Promise.all(workers);

        const totalTime = (performance.now() - start) / 1000;
        const opsPerSec = Math.floor(operations / totalTime);

        console.log(`\n--- STRESS RESULT (with ${PREFILL_COUNT.toLocaleString()} keys) ---`);
        console.log(`Throughput: ${opsPerSec.toLocaleString()} ops/sec`);
        console.log(`------------------------------------\n`);
        expect(opsPerSec).toBeGreaterThan(1_000_000);

    });



});


