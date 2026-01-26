import {expect, it} from "vitest";
import {BenchmarkProbe} from "../utils/benchmark-misure";
import {nexo} from "../nexo";
import {Cursor, FrameCodec} from "../../../../sdk/ts/src/codec";


describe('Stress test', async () => {

    describe('SOCKET', async () => {

        it('Serialization Overhead Analysis', async () => {
            const TOTAL = 50_000;

            // Test diversi tipi di serializzazione
            const testCases = [
                {
                    name: "JSON Object",
                    payload: { op: "ping", data: "test", timestamp: Date.now() },
                    expectedThroughput: 150_000
                },
                {
                    name: "String",
                    payload: "ping-test-string-with-some-data",
                    expectedThroughput: 40_000
                },
                {
                    name: "Raw Buffer",
                    payload: Buffer.from("raw-buffer-data", 'utf8'),
                    expectedThroughput: 60_000
                }
            ];

            for (const testCase of testCases) {
                const probe = new BenchmarkProbe(`SERIALIZATION - ${testCase.name}`, TOTAL);
                probe.startTimer();

                const worker = async () => {
                    const opsPerWorker = TOTAL / 100; // 100 concurrent
                    for (let i = 0; i < opsPerWorker; i++) {
                        const t0 = performance.now();
                        await (nexo as any).debug.echo(testCase.payload);
                        probe.record(performance.now() - t0);
                    }
                };
                await Promise.all(Array.from({ length: 100 }, worker));

                const stats = probe.printResult();
                expect(stats.throughput).toBeGreaterThan(testCase.expectedThroughput);
            }
        });

        it('Codec Performance (encoding/decoding) Only', async () => {
            const TOTAL = 1_000_000;
            const payload = { op: "ping", data: "test" };

            // Test solo encoding
            const encodeProbe = new BenchmarkProbe("CODEC - ENCODE ONLY", TOTAL);
            encodeProbe.startTimer();

            for (let i = 0; i < TOTAL; i++) {
                const t0 = performance.now();
                FrameCodec.any(payload); // Solo encoding, no network
                encodeProbe.record(performance.now() - t0);
            }

            const encodeStats = encodeProbe.printResult();

            // Test solo decoding
            const encoded = FrameCodec.any(payload);
            const decodeProbe = new BenchmarkProbe("CODEC - DECODE ONLY", TOTAL);
            decodeProbe.startTimer();

            for (let i = 0; i < TOTAL; i++) {
                const t0 = performance.now();
                FrameCodec.decodeAny(new Cursor(encoded));
                decodeProbe.record(performance.now() - t0);
            }

            const decodeStats = decodeProbe.printResult();

            // Verifica che codec non sia il bottleneck
            expect(encodeStats.throughput).toBeGreaterThan(1_000_000);
            expect(decodeStats.throughput).toBeGreaterThan(1_000_000);
        });

        it('Single Request Latency - no concurrency noise', async () => {
            const ITERATIONS = 10_000;
            const payload = { op: "ping" };
            const probe = new BenchmarkProbe("SINGLE REQ LATENCY", ITERATIONS);
            probe.startTimer();

            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await (nexo as any).debug.echo(payload);
                probe.record(performance.now() - t0);
            }
            probe.printResult();
        });

        it('Encoding vs Decoding vs Network Ratio', async () => {
            const payload = { data: "x".repeat(512) };
            const ITERATIONS = 10_000;

            // 1. Misura solo Encoding
            let encodeTime = 0;
            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                FrameCodec.any(payload);
                encodeTime += performance.now() - t0;
            }

            // 2. Misura solo Decoding (Simulato)
            const encodedBuffer = FrameCodec.any(payload); // Prepariamo un buffer
            const cursor = new Cursor(encodedBuffer); // Creiamo il cursore
            // Nota: dobbiamo resettare il cursore ad ogni giro o ricrearlo

            let decodeTime = 0;
            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                // Simuliamo l'overhead di creazione del cursore che avviene nella connection
                const c = new Cursor(encodedBuffer);
                FrameCodec.decodeAny(c);
                decodeTime += performance.now() - t0;
            }

            // 3. Misura Round-Trip Completo (Reale)
            let totalTime = 0;
            for (let i = 0; i < ITERATIONS; i++) {
                const t0 = performance.now();
                await (nexo as any).debug.echo(payload);
                totalTime += performance.now() - t0;
            }

            // 4. Calcolo differenziale
            // Network = Totale - (Encoding + Decoding)
            const networkTime = totalTime - encodeTime - decodeTime;

            console.log(`\n--- ANALYSIS (${ITERATIONS} ops) ---`);
            console.log(`Total:       ${totalTime.toFixed(2)}ms`);
            console.log(`Encoding:    ${encodeTime.toFixed(2)}ms (${((encodeTime/totalTime)*100).toFixed(1)}%)`);
            console.log(`Decoding:    ${decodeTime.toFixed(2)}ms (${((decodeTime/totalTime)*100).toFixed(1)}%)`);
            console.log(`Network/IO:  ${networkTime.toFixed(2)}ms (${((networkTime/totalTime)*100).toFixed(1)}%)`);
        });

    });

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