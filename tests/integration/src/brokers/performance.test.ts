import { describe, it, expect } from 'vitest';
import {NexoClient} from "@nexo/client/dist/client";
import {nexo} from "../nexo";

const SERVER_PORT = parseInt(process.env.NEXO_PORT!);


describe('Performance', function() {
	it('should be fast', async function() {})

    describe('PUB-SUB', function() {
        it('Perf: Fan-Out (Broadcasting)', async () => {
            // Scenario: 1 Publisher -> 100 Subscribers
            const subscribers = 100;
            const clients: NexoClient[] = [];
            let receivedCount = 0;

            for (let i = 0; i < subscribers; i++) {
                const c = await NexoClient.connect({ port: SERVER_PORT });
                await c.pubsub.subscribe('news/global', () => { receivedCount++; });
                clients.push(c);
            }

            await nexo.pubsub.publish('news/global', 'Breaking News');

            await new Promise(r => setTimeout(r, 500));
            expect(receivedCount).toBe(subscribers);

            clients.forEach(c => c.disconnect());
        });

        it('Perf: Fan-In (Telemetry)', async () => {
            // Scenario: 10 Publishers -> 1 Subscriber
            const publishers = 10;
            const clients: NexoClient[] = [];
            const payload = { temp: 20 };
            let receivedCount = 0;

            await nexo.pubsub.subscribe('sensors/+', () => { receivedCount++; });

            for (let i = 0; i < publishers; i++) {
                const c = await NexoClient.connect({ port: SERVER_PORT });
                clients.push(c);
            }

            // Parallel publish
            await Promise.all(clients.map((c, i) => c.pubsub.publish(`sensors/dev${i}`, payload)));

            await new Promise(r => setTimeout(r, 500));
            expect(receivedCount).toBe(publishers);

            clients.forEach(c => c.disconnect());
        });

        it('Perf: Traffic Mesh (Chat)', async () => {
            // Scenario: 10 Clients, everyone subscribes to "room1", everyone publishes 1 msg
            const count = 10;
            const clients: NexoClient[] = [];
            let totalDeliveries = 0;

            for (let i = 0; i < count; i++) {
                const c = await NexoClient.connect({ port: SERVER_PORT });
                await c.pubsub.subscribe('room/1', () => { totalDeliveries++; });
                clients.push(c);
            }

            await Promise.all(clients.map(c => c.pubsub.publish('room/1', 'Hello')));

            await new Promise(r => setTimeout(r, 500));
            expect(totalDeliveries).toBe(count * count);

            clients.forEach(c => c.disconnect());
        });

        it('Performance -> Throughput Benchmark (FULL PIPE STRESS)', async () => {
            const TOTAL_OPERATIONS = 100_000;
            const CONCURRENCY = 100;

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

            console.log(`\n\x1b[32m[PERF VITEST] Throughput: ${throughput.toLocaleString()} deliveries/sec\x1b[0m`);

            expect(throughput).toBeGreaterThan(100_000);
        });
    });

    describe('STORE', function() {

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

            await Promise.all(Array.from({length: CONCURRENCY}, worker));

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
                    await nexo.store.kv.set(`bench:${operations++}`, "x");
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


    })



});