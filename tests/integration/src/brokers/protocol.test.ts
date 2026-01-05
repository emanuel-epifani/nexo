import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";
import { NexoClient } from "@nexo/client/dist/client";
import { BenchmarkProbe } from "../utils/benchmark-misure";

const SERVER_PORT = parseInt(process.env.NEXO_PORT!);

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

    describe('Concurrency & Framing', () => {
        it('Frame ID Matching - 50 concurrent requests return correct responses', async () => {
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

        it('Frame ID Matching - Burst followed by burst', async () => {
            const burst1 = await Promise.all(
                Array.from({ length: 100 }, (_, i) => (nexo as any).debug.echo({ burst: 1, i }))
            );
            const burst2 = await Promise.all(
                Array.from({ length: 100 }, (_, i) => (nexo as any).debug.echo({ burst: 2, i }))
            );

            expect(burst1.every((r, i) => r.burst === 1 && r.i === i)).toBe(true);
            expect(burst2.every((r, i) => r.burst === 2 && r.i === i)).toBe(true);
        });

        it('Stress - 10k Pipelined requests', async () => {
            const COUNT = 10000;
            const requests = Array.from({ length: COUNT }, (_, i) => ({ id: i, val: `req-${i}` }));

            const results = await Promise.all(
                requests.map(data => (nexo as any).debug.echo(data))
            );

            expect(results).toHaveLength(COUNT);
            expect(results[0]).toEqual(requests[0]);
            expect(results[COUNT - 1]).toEqual(requests[COUNT - 1]);
        }, 30000);
    });

    describe('Buffer & Networking', () => {
        it('Buffer Boundary - Payload at 64KB (writeBuf size)', async () => {
            const size = 64 * 1024 - 100;
            const payload = { data: 'x'.repeat(size) };
            const result = await (nexo as any).debug.echo(payload);
            expect(result.data.length).toBe(size);
        });

        it('Buffer Boundary - Payload just over 64KB forces flush', async () => {
            const size = 64 * 1024 + 100;
            const payload = { data: 'x'.repeat(size) };
            const result = await (nexo as any).debug.echo(payload);
            expect(result.data.length).toBe(size);
        });

        it('Buffer Boundary - Large then small payloads', async () => {
            const largePayload = { big: 'x'.repeat(100_000) };
            const smallPayloads = Array.from({ length: 50 }, (_, i) => ({ id: i }));

            const bigResult = await (nexo as any).debug.echo(largePayload);
            expect(bigResult.big.length).toBe(100_000);

            const smallResults = await Promise.all(
                smallPayloads.map(p => (nexo as any).debug.echo(p))
            );
            expect(smallResults).toEqual(smallPayloads);
        });

        it('Stress - 10MB Payload (RingDecoder resize + TCP fragmentation)', async () => {
            const size = 10 * 1024 * 1024;
            const largeString = "A".repeat(size);
            const payload = { data: largeString, meta: "huge-payload" };

            const response = await (nexo as any).debug.echo(payload);

            expect(response.data.length).toBe(size);
            expect(response.data).toBe(largeString);
        }, 30000);

        it('Stress - Random payload sizes (1B to 100KB)', async () => {
            const COUNT = 1000;
            const requests = Array.from({ length: COUNT }, (_, i) => {
                const size = Math.floor(Math.random() * 100000) + 1;
                return { idx: i, blob: Buffer.allocUnsafe(size).toString('hex') };
            });

            const results = await Promise.all(
                requests.map(data => (nexo as any).debug.echo(data))
            );

            expect(results).toHaveLength(COUNT);
            expect(results[50]).toEqual(requests[50]);
        }, 30000);
    });

    describe('PubSub Integrity', () => {
        it('PUSH Integrity - Rapid fire messages should not corrupt', async () => {
            const MESSAGES = 1000;
            const received: any[] = [];
            const topic = nexo.pubsub('proto/rapid');

            await topic.subscribe((msg: any) => {
                received.push(msg);
            });

            for (let i = 0; i < MESSAGES; i++) {
                await topic.publish({
                    seq: i,
                    data: `payload-${i}-${'x'.repeat(100)}`
                });
            }

            // Polling for completion
            for (let i = 0; i < 10; i++) {
                if (received.length >= MESSAGES) break;
                await new Promise(r => setTimeout(r, 100));
            }

            expect(received.length).toBe(MESSAGES);
            for (const msg of received) {
                expect(msg).toHaveProperty('seq');
                expect(msg).toHaveProperty('data');
                expect(typeof msg.seq).toBe('number');
                expect(msg.data).toContain(`payload-${msg.seq}-`);
            }
        });

        it('PUSH Integrity - Interleaved REQUEST + PUSH', async () => {
            const pushMessages: any[] = [];
            const topic = nexo.pubsub('proto/interleave');

            await topic.subscribe((msg) => {
                pushMessages.push(msg);
            });

            const requests = Array.from({ length: 100 }, (_, i) =>
                (nexo as any).debug.echo({ reqId: i })
            );
            const publishes = Array.from({ length: 100 }, (_, i) =>
                topic.publish({ pushId: i })
            );

            const [responses] = await Promise.all([
                Promise.all(requests),
                Promise.all(publishes)
            ]);

            for (let i = 0; i < 10; i++) {
                if (pushMessages.length >= 100) break;
                await new Promise(r => setTimeout(r, 100));
            }

            expect(responses.every((r, i) => r.reqId === i)).toBe(true);
            expect(pushMessages.length).toBe(100);
            expect(pushMessages.every(m => typeof m.pushId === 'number')).toBe(true);
        });

        it('PUSH Integrity - Subscribe then rapid publish from another client', async () => {
            const received: any[] = [];
            const topicName = 'proto/cross-client';

            const subscriber = await NexoClient.connect({ port: SERVER_PORT });
            await subscriber.pubsub(topicName).subscribe((msg) => {
                received.push(msg);
            });

            await new Promise(r => setTimeout(r, 50));

            const MSGS = 500;
            const publisher = nexo.pubsub(topicName);
            await Promise.all(
                Array.from({ length: MSGS }, (_, i) =>
                    publisher.publish({ seq: i })
                )
            );

            for (let i = 0; i < 10; i++) {
                if (received.length >= MSGS) break;
                await new Promise(r => setTimeout(r, 100));
            }

            expect(received.length).toBe(MSGS);
            expect(new Set(received.map(m => m.seq)).size).toBe(MSGS);

            subscriber.disconnect();
        });
    });

    describe('Queue Integrity', () => {
        it('Queue Integrity - Rapid messages preserve payload', async () => {
            const TOTAL = 500;
            const q = await nexo.queue("proto-integrity").create();
            const received: any[] = [];

            await Promise.all(
                Array.from({ length: TOTAL }, (_, i) =>
                    q.push({ idx: i, check: `val-${i}` })
                )
            );

            await new Promise<void>((resolve) => {
                const sub = q.subscribe((msg: any) => {
                    received.push(msg);
                    if (received.length >= TOTAL) {
                        sub.stop();
                        resolve();
                    }
                });
            });

            for (const msg of received) {
                expect(msg).toHaveProperty('idx');
                expect(msg).toHaveProperty('check');
                expect(msg.check).toBe(`val-${msg.idx}`);
            }
        });
    });

    describe('System Isolation & Errors', () => {
        it('Multi-Client - 10 clients x 100 requests each, responses isolated', async () => {
            const clients = await Promise.all(
                Array.from({ length: 10 }, () => NexoClient.connect({ port: SERVER_PORT }))
            );

            const allResults = await Promise.all(
                clients.map((client, clientIdx) =>
                    Promise.all(
                        Array.from({ length: 100 }, (_, reqIdx) =>
                            (client as any).debug.echo({ clientIdx, reqIdx })
                        )
                    )
                )
            );

            for (let c = 0; c < 10; c++) {
                for (let r = 0; r < 100; r++) {
                    expect(allResults[c][r]).toEqual({ clientIdx: c, reqIdx: r });
                }
            }

            clients.forEach(c => c.disconnect());
        });

        it('Error Handling - Non-existent key returns null', async () => {
            const result = await nexo.store.kv.get('proto:nonexistent-key-12345');
            expect(result).toBeNull();
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
