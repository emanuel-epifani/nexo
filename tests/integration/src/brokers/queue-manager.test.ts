import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";
import { NexoClient, Opcode } from '@nexo/client';
import { BenchmarkProbe } from "../utils/benchmark-misure";

describe('QUEUE broker', () => {

    describe('Core API (Handle-based)', () => {
        it('Producer PUSH message -> Consumer receives it', async () => {
            const q = await nexo.queue('test_q_base_1').create();

            const payload = { msg: 'hello nexo' };

            let received: any = null;
            const sub = await q.subscribe(async (data) => {
                received = data;
            });

            await q.push(payload);

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();

            expect(received).toEqual(payload);
        });

        it('Should FAIL to push to a non-existent queue', async () => {
            const q = nexo.queue('ghost-queue');
            await expect(q.push('fail')).rejects.toThrow('Queue \'ghost-queue\' not found');
        });

        it('Should FAIL to subscribe to a non-existent queue', async () => {
            const q = nexo.queue('ghost-queue');
            await expect(q.subscribe(async () => { })).rejects.toThrow('Queue \'ghost-queue\' not found');
        });

        it('Producer PUSH 1 message -> only ONE of 2 competing consumers receives it', async () => {
            const qName = 'test_q_competing';
            const q1 = await nexo.queue(qName).create();

            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const q2 = client2.queue(qName);

            let count = 0;
            const sub1 = await q1.subscribe(async () => { count++; });
            const sub2 = await q2.subscribe(async () => { count++; });

            await new Promise(r => setTimeout(r, 100));
            await q1.push('competing-test');

            await new Promise(r => setTimeout(r, 500));
            sub1.stop();
            sub2.stop();

            expect(count).toBe(1);
            client2.disconnect();
        });

        it('Producer PUSH 2 messages -> 2 consumers receive ONE message each (Fair Distribution)', async () => {
            const qName = 'test_q_fair';
            const q1 = await nexo.queue(qName).create();

            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const q2 = client2.queue(qName);

            const received1: any[] = [];
            const received2: any[] = [];

            const sub1 = await q1.subscribe(async (msg) => { received1.push(msg); }, { batchSize: 1 });
            const sub2 = await q2.subscribe(async (msg) => { received2.push(msg); }, { batchSize: 1 });

            await new Promise(r => setTimeout(r, 100));
            const msg1 = 'msg1'
            const msg2 = 'msg2'
            await q1.push(msg1);
            await q1.push(msg2);

            await new Promise(r => setTimeout(r, 500));
            sub1.stop();
            sub2.stop();

            expect(received1.length).toBe(1);
            expect(received2.length).toBe(1);
            expect(received1[0]).not.toEqual(received2[0]);

            const allReceived = [...received1, ...received2].sort();
            expect(allReceived).toEqual([msg1, msg2]);

            client2.disconnect();
        });

        it('Producer PUSH 3 messages -> Consumer receives them in FIFO order', async () => {
            const q = await nexo.queue('test_q_fifo').create();

            const messages = ['first', 'second', 'third'];

            for (const m of messages) await q.push(m);

            const received: string[] = [];
            const sub = await q.subscribe(async (msg) => {
                received.push(msg);
            });

            for (let i = 0; i < 20; i++) {
                if (received.length === 3) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            expect(received).toEqual(messages);
        });
    });

    describe('Message Priority', () => {
        it('Producer PUSH high/low priority -> Consumer receives HIGH priority first', async () => {
            const q = await nexo.queue('test_q_priority_1').create();

            const msg_low_priority = 'low-priority'
            const msg_medium_priority = 'medium-priority'
            const msg_high_priority = 'high-priority'

            await q.push(msg_low_priority, { priority: 0 });
            await q.push(msg_medium_priority, { priority: 10 });
            await q.push(msg_high_priority, { priority: 255 });

            const received: string[] = [];
            const sub = await q.subscribe(async (msg) => {
                received.push(msg);
            });

            for (let i = 0; i < 20; i++) {
                if (received.length === 3) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            expect(received[0]).toBe(msg_high_priority);
            expect(received[1]).toBe(msg_medium_priority);
            expect(received[2]).toBe(msg_low_priority);
        });

        it('Producer PUSH mixed priorities -> Consumer receives in Priority-then-FIFO order', async () => {
            const q = await nexo.queue('test_q_priority_fifo').create();

            const h1 = 'high-1';
            const h2 = 'high-2';
            const l1 = 'low-1';
            const l2 = 'low-2';

            await q.push(h1, { priority: 10 });
            await q.push(l1, { priority: 5 });
            await q.push(h2, { priority: 10 });
            await q.push(l2, { priority: 5 });

            const received: string[] = [];
            const sub = await q.subscribe(async (msg) => {
                received.push(msg);
            }, { batchSize: 1 });

            for (let i = 0; i < 20; i++) {
                if (received.length === 4) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            expect(received).toEqual([h1, h2, l1, l2]);
        });
    });

    describe('Delayed Messages (Scheduling)', () => {
        it('Producer PUSH with delay -> message remains invisible until timer expires', async () => {
            const q = await nexo.queue('test_q_delayed_1').create();

            const payloadSent = 'delayed-msg';
            let msgReceived: string | null = null;
            const delayMs = 400;

            await q.push(payloadSent, { delayMs });

            let received = false;
            const sub = await q.subscribe(async (data) => { received = true; msgReceived = data; });

            await new Promise(r => setTimeout(r, 200));
            expect(received).toBe(false);

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();
            expect(received).toBe(true);
            expect(msgReceived).toBe(payloadSent);
        });

        it('Producer PUSH multiple delayed -> Consumer receives them as they expire (Order)', async () => {
            const q = await nexo.queue('test_q_delayed_order').create();

            const msg_long_delay = 'longer-delay';
            const msg_short_delay = 'shorter-delay';

            await q.push(msg_long_delay, { delayMs: 600 });
            await q.push(msg_short_delay, { delayMs: 300 });

            const received: string[] = [];
            const sub = await q.subscribe(async (msg) => { received.push(msg); });

            await new Promise(r => setTimeout(r, 450));
            expect(received).toEqual([msg_short_delay]);

            for (let i = 0; i < 10; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();
            expect(received).toEqual([msg_short_delay, msg_long_delay]);
        });
    });

    describe('Reliability & Auto-ACK', () => {
        it('Consumer fails callback -> Message becomes VISIBLE again after Visibility Timeout', async () => {
            const q = await nexo.queue('test_q_timeout_custom').create({ visibilityTimeoutMs: 300 });

            await q.push('timeout-msg');

            let receivedCount = 0;
            const sub = await q.subscribe(async () => {
                receivedCount++;
                throw new Error("Force fail to prevent auto ACK");
            });

            for (let i = 0; i < 10; i++) {
                if (receivedCount === 1) break;
                await new Promise(r => setTimeout(r, 50));
            }
            expect(receivedCount).toBe(1);

            await new Promise(r => setTimeout(r, 800));
            sub.stop();
            expect(receivedCount).toBeGreaterThanOrEqual(2);
        });

        it('Consumer repeatedly fails -> Message moves to DLQ after maxRetries', async () => {
            const qName = 'test_q_custom_dlq';
            const q = await nexo.queue(qName).create({ maxRetries: 2, visibilityTimeoutMs: 300 });

            const dlq = await nexo.queue(`${qName}_dlq`).create(); // Create DLQ as well

            await q.push('poison-pill');

            let attempts = 0;
            const sub = await q.subscribe(async () => {
                attempts++;
                throw new Error("fail");
            });

            await new Promise(r => setTimeout(r, 1200));
            sub.stop();
            expect(attempts).toBe(2);

            let dlqReceived = false;
            const dlqSub = await dlq.subscribe(async (data) => {
                if (data === 'poison-pill') dlqReceived = true;
            });

            for (let i = 0; i < 10; i++) {
                if (dlqReceived) break;
                await new Promise(r => setTimeout(r, 100));
            }
            dlqSub.stop();
            expect(dlqReceived).toBe(true);
        }, 10000);

        it('Message age > TTL -> Message is discarded from queue (Retention)', async () => {
            const q = await nexo.queue('test_q_ttl_custom').create({ ttlMs: 300 });

            await q.push("short-lived");

            await new Promise(r => setTimeout(r, 800));

            let received = false;
            const sub = await q.subscribe(async () => { received = true; });

            await new Promise(r => setTimeout(r, 300));
            sub.stop();
            expect(received).toBe(false);
        });

        it('TTL expires while message is waiting for Retry -> Message is discarded (TTL wins over DLQ)', async () => {
            const qName = 'test_ttl_vs_dlq';
            const q = await nexo.queue(qName).create({
                ttlMs: 200,
                maxRetries: 5,
                visibilityTimeoutMs: 100
            });

            const dlq = await nexo.queue(`${qName}_dlq`).create();

            await q.push('msg-destined-to-die');

            let attempts = 0;
            const sub = await q.subscribe(async () => {
                attempts++;
                throw new Error('fail');
            });

            await new Promise(r => setTimeout(r, 400));
            sub.stop();

            let dlqMessage: any = null;
            const dlqSub = await dlq.subscribe(async (msg) => { dlqMessage = msg; });

            await new Promise(r => setTimeout(r, 100));
            dlqSub.stop();

            expect(dlqMessage).toBeNull();

            let zombieMessage: any = null;
            const zombieSub = await q.subscribe(async (msg) => { zombieMessage = msg; });
            await new Promise(r => setTimeout(r, 100));
            zombieSub.stop();

            expect(zombieMessage).toBeNull();
        });

    });

    describe('JSON & DevEx', () => {
        it('Producer PUSH object -> SDK auto-serializes/deserializes JSON data with Generics', async () => {
            interface Order { id: number; meta: { type: string }; list: number[] };
            const q = await nexo.queue<Order>('test_q_json_complex').create();

            const data: Order = { id: 1, meta: { type: 'test' }, list: [1, 2, 3] };
            await q.push(data);

            let received: Order | null = null;
            const sub = await q.subscribe(async (msg) => {
                received = msg;
            });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toEqual(data);
        });

        it('User calls nexo.queue(name) twice -> SDK returns the SAME instance (Idempotency)', () => {
            const q1 = nexo.queue('test_q_idemp');
            const q2 = nexo.queue('test_q_idemp');

            expect(q1).toBe(q2);
            expect(q1.name).toBe('test_q_idemp');
        });
    });

    describe('Robustness & Edge Cases', () => {
        it('Consumer starts BEFORE push -> receives message as soon as it arrives', async () => {
            const q = await nexo.queue('test_q_blocking_new').create();

            let received: any = null;

            const sub = await q.subscribe(async (msg) => { received = msg; });

            await new Promise(r => setTimeout(r, 200));
            expect(received).toBeNull();

            const msgSent = 'instant'
            await q.push(msgSent);

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toBe(msgSent);
        });

        it('User calls ACK twice -> Server handles it without errors (Idempotency)', async () => {
            const q = await nexo.queue('test_q_double_ack_new').create();

            await q.push('msg');

            // Consume via batch API
            const res = await (nexo as any).builder.reset(Opcode.Q_CONSUME)
                .writeU32(1)      // max batch
                .writeU64(5000)   // wait ms
                .writeString(q.name)
                .send();

            // Parse batch response: [Count:4][UUID:16][PayloadLen:4][Payload]
            const count = res.reader.readU32();
            expect(count).toBe(1);
            const msgId = res.reader.readUUID();

            await (q as any).ack(msgId);
            await expect((q as any).ack(msgId)).resolves.toBeUndefined();
        });

        it('User creates queue with special chars -> Broker handles name correctly', async () => {
            const q = await nexo.queue('queues:special/chars@123').create();

            let msgSent = 'special-test'
            await q.push(msgSent);

            let received: any = null;
            const sub = await q.subscribe(async (msg) => { received = msg; });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toBe(msgSent);
        });
    });

    describe('Batch Consume', () => {
        it('Batch size 20 -> High throughput simulation', async () => {
            const q = await nexo.queue('test_q_batch').create();

            const COUNT = 50;
            for (let i = 0; i < COUNT; i++) await q.push(i);

            let receivedCount = 0;
            const start = Date.now();

            const sub = await q.subscribe(async () => {
                receivedCount++;
                await new Promise(r => setTimeout(r, 2));
            }, { batchSize: 20 });

            while (receivedCount < COUNT) {
                await new Promise(r => setTimeout(r, 10));
                if (Date.now() - start > 5000) break;
            }
            sub.stop();

            expect(receivedCount).toBe(COUNT);
        });

        it('Batch size 1 -> Serial processing (Strict Ordering check)', async () => {
            const q = await nexo.queue('test_q_serial').create();

            await q.push(1);
            await q.push(2);
            await q.push(3);

            const received: number[] = [];

            const sub = await q.subscribe(async (val) => {
                received.push(val);
                await new Promise(r => setTimeout(r, 50));
            }, { batchSize: 1 });

            await new Promise(r => setTimeout(r, 300));
            sub.stop();

            expect(received).toEqual([1, 2, 3]);
        });
    });


    // --- PERFORMANCE ---
    describe('Performance test', () => {
        
        it('Throughput: Serial Consumption (Default)', async () => {
            const TOTAL = 5_000;
            const q = await nexo.queue("bench-serial").create();

            // Push massivo
            const producer = async () => {
                for (let i = 0; i < TOTAL; i++) await q.push({ id: i });
            };
            await producer();

            // Consume Seriale
            let consumed = 0;
            const probe = new BenchmarkProbe("QUEUE - SERIAL", TOTAL);
            probe.startTimer();

            await new Promise<void>((resolve) => {
                const sub = q.subscribe(async () => {
                    consumed++;
                    if (consumed >= TOTAL) {
                        sub.then(s => s.stop()); // Use then because subscribe is async
                        probe.printResult();
                        resolve();
                    }
                }, { batchSize: 50 }); // Concurrency default 1
            });
        });

        it('Throughput: Concurrent Consumption (High Performance)', async () => {
            const TOTAL = 10_000;
            const CONCURRENCY = 50; // 50 msg in parallelo
            const q = await nexo.queue("bench-concurrent").create();

            // Push massivo
            const producer = async () => {
                const batch = Array(100).fill(0); // Push a batch simulati lato client per velocità
                for (let i = 0; i < TOTAL / 100; i++) {
                    await Promise.all(batch.map((_, j) => q.push({ id: i * 100 + j })));
                }
            };
            await producer();

            // Consume Parallelo
            let consumed = 0;
            const probe = new BenchmarkProbe("QUEUE - CONCURRENT", TOTAL);
            probe.startTimer();

            await new Promise<void>((resolve) => {
                const sub = q.subscribe(async () => {
                    consumed++;
                    if (consumed >= TOTAL) {
                        sub.then(s => s.stop()); // Use then because subscribe is async
                        const stats = probe.printResult();
                        // Ci aspettiamo throughput molto più alto
                        expect(stats.throughput).toBeGreaterThan(1000); 
                        resolve();
                    }
                }, { 
                    batchSize: 100, 
                    concurrency: CONCURRENCY // <--- Qui la magia
                });
            });
        });
    })


});