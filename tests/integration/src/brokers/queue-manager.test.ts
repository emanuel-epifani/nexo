import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";
import {NexoClient, Opcode} from '@nexo/client';

describe('QUEUE broker', () => {

    describe('Core API (Handle-based)', () => {
        it('Producer PUSH message -> Consumer receives it immediately', async () => {
            const q = await nexo.registerQueue('test_q_base_1');
            const payload = { msg: 'hello nexo' };
            
            let received: any = null;
            const sub = q.subscribe(async (data) => {
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

        it('Producer PUSH 1 message -> only ONE of 2 competing consumers receives it', async () => {
            const qName = 'test_q_competing';
            const q1 = await nexo.registerQueue(qName);
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const q2 = await client2.registerQueue(qName);

            let count = 0;
            const sub1 = q1.subscribe(async () => { count++; });
            const sub2 = q2.subscribe(async () => { count++; });

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
            const q1 = await nexo.registerQueue(qName);
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const q2 = await client2.registerQueue(qName);

            const received1: any[] = [];
            const received2: any[] = [];

            const sub1 = q1.subscribe(async (msg) => { received1.push(msg); });
            const sub2 = q2.subscribe(async (msg) => { received2.push(msg); });

            await new Promise(r => setTimeout(r, 100));
            await q1.push('msg1');
            await q1.push('msg2');

            await new Promise(r => setTimeout(r, 500));
            sub1.stop();
            sub2.stop();

            expect(received1.length).toBe(1);
            expect(received2.length).toBe(1);
            expect(received1[0]).not.toEqual(received2[0]);

            client2.disconnect();
        });

        it('Producer PUSH 3 messages -> Consumer receives them in FIFO order', async () => {
            const q = await nexo.registerQueue('test_q_fifo');
            const messages = ['first', 'second', 'third'];

            for (const m of messages) await q.push(m);

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => {
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
            const q = await nexo.registerQueue('test_q_priority_1');

            await q.push('low-priority', { priority: 0 });
            await q.push('high-priority', { priority: 255 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => {
                received.push(msg);
            });

            for (let i = 0; i < 20; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            expect(received[0]).toBe('high-priority');
            expect(received[1]).toBe('low-priority');
        });

        it('Producer PUSH mixed priorities -> Consumer receives in Priority-then-FIFO order', async () => {
            const q = await nexo.registerQueue('test_q_priority_fifo');

            await q.push('high-1', { priority: 10 });
            await q.push('low-1', { priority: 5 });
            await q.push('high-2', { priority: 10 });
            await q.push('low-2', { priority: 5 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => {
                received.push(msg);
            });

            for (let i = 0; i < 20; i++) {
                if (received.length === 4) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            expect(received).toEqual(['high-1', 'high-2', 'low-1', 'low-2']);
        });
    });

    describe('Delayed Messages (Scheduling)', () => {
        it('Producer PUSH with delay -> message remains invisible until timer expires', async () => {
            const q = await nexo.registerQueue('test_q_delayed_1');
            const payload = 'delayed-msg';
            const delayMs = 1500;

            await q.push(payload, { delayMs });

            let received = false;
            const sub = q.subscribe(async () => { received = true; });

            await new Promise(r => setTimeout(r, 500));
            expect(received).toBe(false);

            for (let i = 0; i < 20; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();
            expect(received).toBe(true);
        });

        it('Producer PUSH multiple delayed -> Consumer receives them as they expire (Order)', async () => {
            const q = await nexo.registerQueue('test_q_delayed_order');

            await q.push('longer-delay', { delayMs: 2000 });
            await q.push('shorter-delay', { delayMs: 1000 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => { received.push(msg); });

            await new Promise(r => setTimeout(r, 1200));
            expect(received).toEqual(['shorter-delay']);

            for (let i = 0; i < 20; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();
            expect(received).toEqual(['shorter-delay', 'longer-delay']);
        });
    });

    describe('Reliability & Custom Timeouts', () => {
        it('Consumer fails callback -> Message becomes VISIBLE again after Visibility Timeout', async () => {
            const q = await nexo.registerQueue('test_q_timeout_custom', { visibilityTimeoutMs: 1000 });
            await q.push('timeout-msg');

            let receivedCount = 0;
            const sub = q.subscribe(async () => {
                receivedCount++;
                throw new Error("Force fail to prevent ACK");
            });

            for (let i = 0; i < 10; i++) {
                if (receivedCount === 1) break;
                await new Promise(r => setTimeout(r, 100));
            }
            expect(receivedCount).toBe(1);

            // Wait for visibility timeout (1s + reaper margin)
            await new Promise(r => setTimeout(r, 2500));
            sub.stop();
            expect(receivedCount).toBeGreaterThanOrEqual(2);
        });

        it('Consumer repeatedly fails -> Message moves to DLQ after maxRetries', async () => {
            const qName = 'test_q_custom_dlq';
            const q = await nexo.registerQueue(qName, { maxRetries: 2, visibilityTimeoutMs: 1000 });
            const dlq = await nexo.registerQueue(`${qName}_dlq`);
            
            await q.push('poison-pill');

            let attempts = 0;
            const sub = q.subscribe(async () => { 
                attempts++; 
                throw new Error("fail");
            });

            // Wait for 2 attempts (~3s)
            await new Promise(r => setTimeout(r, 4000));
            sub.stop();
            expect(attempts).toBe(2);

            let dlqReceived = false;
            const dlqSub = dlq.subscribe(async (data) => {
                if (data === 'poison-pill') dlqReceived = true;
            });

            for (let i = 0; i < 20; i++) {
                if (dlqReceived) break;
                await new Promise(r => setTimeout(r, 100));
            }
            dlqSub.stop();
            expect(dlqReceived).toBe(true);
        }, 15000);

        it('Message age > TTL -> Message is discarded from queue (Retention)', async () => {
            const q = await nexo.registerQueue('test_q_ttl_custom', { ttlMs: 1000 });
            await q.push("short-lived");
            
            // Wait for expiration
            await new Promise(r => setTimeout(r, 2500));
            
            let received = false;
            const sub = q.subscribe(async () => { received = true; });

            await new Promise(r => setTimeout(r, 500));
            sub.stop();
            expect(received).toBe(false);
        });
    });

    describe('JSON & DevEx', () => {
        it('Producer PUSH object -> SDK auto-serializes/deserializes JSON data', async () => {
            const q = await nexo.registerQueue('test_q_json_complex');
            const data = { id: 1, meta: { type: 'test' }, list: [1, 2, 3] };
            
            await q.push(data);
            
            let received: any = null;
            const sub = q.subscribe(async (msg) => { received = msg; });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toEqual(data);
        });

        it('User calls registerQueue twice -> SDK returns the SAME instance (Idempotency)', async () => {
            const q1 = await nexo.registerQueue('test_q_idemp', { maxRetries: 10 });
            const q2 = await nexo.registerQueue('test_q_idemp', { maxRetries: 2 });
            
            expect(q1).toBe(q2);
            expect(q1.name).toBe('test_q_idemp');
        });
    });

    describe('Robustness & Edge Cases', () => {
        it('Consumer starts BEFORE push -> receives message as soon as it arrives', async () => {
            const q = await nexo.registerQueue('test_q_blocking_new');
            let received: any = null;

            const sub = q.subscribe(async (msg) => { received = msg; });

            await new Promise(r => setTimeout(r, 200));
            expect(received).toBeNull();

            await q.push('instant');

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toBe('instant');
        });

        it('User calls ACK twice -> Server handles it without errors (Idempotency)', async () => {
            const q = await nexo.registerQueue('test_q_double_ack_new');
            await q.push('msg');

            const qLen = Buffer.byteLength(q.name, 'utf8');
            const payload = Buffer.allocUnsafe(4 + qLen);
            payload.writeUInt32BE(qLen, 0);
            payload.write(q.name, 4, 'utf8');

            const res = await (nexo as any).send(Opcode.Q_CONSUME, payload);
            const msgId = res.data.subarray(0, 16).toString('hex');

            await q.ack(msgId);
            await expect(q.ack(msgId)).resolves.toBeUndefined();
        });

        it('User creates queue with special chars -> Broker handles name correctly', async () => {
            const q = await nexo.registerQueue('queues:special/chars@123');
            await q.push('special-test');

            let received: any = null;
            const sub = q.subscribe(async (msg) => { received = msg; });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toBe('special-test');
        });
    });
});
