import {describe, expect, it} from "vitest";
import {BenchmarkProbe} from "../utils/benchmark-misure";
import {nexo} from "../nexo";
import {FrameWriter} from "../../src/codec";


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


    describe('PIPELINED (codec-sensitive)', () => {

        // Pipelined sends saturate the SDK CPU path (codec packing) on a single
        // connection, surfacing per-op encoding cost that round-trip-bound
        // workers hide. Ideal probe to detect codec-level regressions/wins.

        it('Store SET pipelined', async () => {
            const TOTAL = 100_000;
            const PIPELINE = 100;

            const probe = new BenchmarkProbe('STORE SET PIPELINED', TOTAL);
            probe.startTimer();

            for (let i = 0; i < TOTAL; i += PIPELINE) {
                const promises: Promise<void>[] = [];
                for (let j = 0; j < PIPELINE; j++) {
                    promises.push(nexo.store.map.set(`pipe-${i + j}`, 'v'));
                }
                await Promise.all(promises);
            }

            probe.printResult();
        });

        it('PubSub publish pipelined (small JSON)', async () => {
            const TOTAL = 100_000;
            const PIPELINE = 100;
            const topic = nexo.pubsub('bench/pipeline');
            const payload = { op: 'ping', data: 'x', t: Date.now() };

            const probe = new BenchmarkProbe('PUBSUB PUB PIPELINED', TOTAL);
            probe.startTimer();

            for (let i = 0; i < TOTAL; i += PIPELINE) {
                const promises: Promise<void>[] = [];
                for (let j = 0; j < PIPELINE; j++) {
                    promises.push(topic.publish(payload));
                }
                await Promise.all(promises);
            }

            probe.printResult();
        });

        it('Queue push pipelined', async () => {
            const q = nexo.queue('bench-pipe-queue');
            await q.create();

            const TOTAL = 100_000;
            const PIPELINE = 100;
            const payload = { op: 'job', data: 'x', t: Date.now() };

            const probe = new BenchmarkProbe('QUEUE PUSH PIPELINED', TOTAL);
            probe.startTimer();

            for (let i = 0; i < TOTAL; i += PIPELINE) {
                const promises: Promise<void>[] = [];
                for (let j = 0; j < PIPELINE; j++) {
                    promises.push(q.push(payload));
                }
                await Promise.all(promises);
            }

            probe.printResult();

            await q.delete();
        });

    });

    describe('CODEC PACK (no server, pure CPU)', () => {

        // Misura il tempo di "impacchettamento" del messaggio in nanosecondi.
        // Non tocca il server: chiama solo FrameWriter in memoria.
        // Serve a vedere il guadagno del codec senza che il tempo di rete
        // (anche solo loopback locale) lo nasconda.

        const writer = new FrameWriter();
        const ITERS = 1_000_000;
        const WARMUP = 50_000;

        const PAYLOAD_JSON = { op: 'ping', data: 'test', timestamp: 1700000000000, userId: 42 };
        const PAYLOAD_STRING = 'ping-test-string-with-some-data';
        const PAYLOAD_BUFFER = Buffer.from('raw-buffer-data-payload-bytes', 'utf8');
        const UUID_HEX = '550e8400-e29b-41d4-a716-446655440000';

        // Opcodes (qualsiasi valore va bene: misuriamo il pack, non il dispatch lato server)
        const OP_STORE_SET = 0x02;
        const OP_PUBSUB_PUB = 0x21;
        const OP_QUEUE_PUSH = 0x11;
        const OP_QUEUE_ACK = 0x13;
        const OP_STREAM_FETCH = 0x32;
        const OP_STREAM_PUB = 0x31;
        const OP_STREAM_ACK = 0x34;

        const scenarios: { name: string; pack: () => Buffer }[] = [
            {
                name: 'store.set (str+str+any-json)',
                pack: () => writer.begin().string('bench-key-1234').string('{}').any(PAYLOAD_JSON).finish(1, OP_STORE_SET),
            },
            {
                name: 'store.set (str+str+any-str)',
                pack: () => writer.begin().string('bench-key-1234').string('{}').any(PAYLOAD_STRING).finish(1, OP_STORE_SET),
            },
            {
                name: 'store.set (str+str+any-buf)',
                pack: () => writer.begin().string('bench-key-1234').string('{}').any(PAYLOAD_BUFFER).finish(1, OP_STORE_SET),
            },
            {
                name: 'pubsub.publish',
                pack: () => writer.begin().string('events/orders/created').string('{}').any(PAYLOAD_JSON).finish(1, OP_PUBSUB_PUB),
            },
            {
                name: 'queue.push',
                pack: () => writer.begin().string('jobs-queue').string('{}').any(PAYLOAD_JSON).finish(1, OP_QUEUE_PUSH),
            },
            {
                name: 'queue.ack (uuid+str)',
                pack: () => writer.begin().uuid(UUID_HEX).string('jobs-queue').finish(1, OP_QUEUE_ACK),
            },
            {
                name: 'stream.fetch (str+str+u32+u32)',
                pack: () => writer.begin().string('events-stream').string('worker-group').u32(100).u32(500).finish(1, OP_STREAM_FETCH),
            },
            {
                name: 'stream.publish',
                pack: () => writer.begin().string('events-stream').any(PAYLOAD_JSON).finish(1, OP_STREAM_PUB),
            },
            {
                name: 'stream.ack (str+str+u64)',
                pack: () => writer.begin().string('events-stream').string('worker-group').u64(1234567890n).finish(1, OP_STREAM_ACK),
            },
        ];

        it('Pack throughput per ogni comando', () => {
            console.log('\n=== Codec pack throughput (1M iter, 50K warmup) ===');
            for (const s of scenarios) {
                // Warmup: lascia che il JIT di V8 ottimizzi il codice caldo
                for (let i = 0; i < WARMUP; i++) s.pack();

                // Misura
                const t0 = process.hrtime.bigint();
                for (let i = 0; i < ITERS; i++) s.pack();
                const t1 = process.hrtime.bigint();

                const nsPerOp = Number(t1 - t0) / ITERS;
                const opsPerSec = Math.floor(1e9 / nsPerOp);
                const sample = s.pack();

                console.log(
                    `[${s.name.padEnd(32)}] ${nsPerOp.toFixed(0).padStart(5)} ns/op  | ` +
                    `${opsPerSec.toLocaleString().padStart(12)} ops/sec  | ` +
                    `frame=${sample.length}B`
                );
            }
            console.log('====================================================\n');
        });
    });

});