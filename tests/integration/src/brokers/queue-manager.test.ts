import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";
import {NexoClient, Opcode} from '@nexo/client';

describe('QUEUE broker', () => {

    describe('Core API (Handle-based)', () => {
        it('Producer PUSH message -> Consumer receives it', async () => {
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

            const msg_low_priority = 'low-priority'
            const msg_medium_priority = 'medium-priority'
            const msg_high_priority = 'high-priority'

            await q.push(msg_low_priority, { priority: 0 });
            await q.push(msg_medium_priority, { priority: 10 });
            await q.push(msg_high_priority, { priority: 255 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => {
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
            const q = await nexo.registerQueue('test_q_priority_fifo');

            const h1 = 'high-1';
            const h2 = 'high-2';
            const l1 = 'low-1';
            const l2 = 'low-2';

            await q.push(h1, { priority: 10 });
            await q.push(l1, { priority: 5 });
            await q.push(h2, { priority: 10 });
            await q.push(l2, { priority: 5 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => {
                received.push(msg);
            });

            for (let i = 0; i < 20; i++) {
                if (received.length === 4) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();

            // L'ordine atteso è: priorità più alta (10) -> FIFO tra di loro, poi priorità più bassa (5) -> FIFO tra di loro
            expect(received).toEqual([h1, h2, l1, l2]);
        });
    });

    describe('Delayed Messages (Scheduling)', () => {
        it('Producer PUSH with delay -> message remains invisible until timer expires', async () => {
            const q = await nexo.registerQueue('test_q_delayed_1');
            const payloadSent = 'delayed-msg';
            let msgReceived: string | null = null;
            const delayMs = 400;

            await q.push(payloadSent, { delayMs });

            let received = false;
            const sub = q.subscribe(async (data) => { received = true; msgReceived = data; });

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
            const q = await nexo.registerQueue('test_q_delayed_order');

            const msg_long_delay = 'longer-delay';
            const msg_short_delay = 'shorter-delay';

            await q.push(msg_long_delay, { delayMs: 600 });
            await q.push(msg_short_delay, { delayMs: 300 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => { received.push(msg); });

            // Aspettiamo che scada solo il delay corto (300ms + margine)
            await new Promise(r => setTimeout(r, 450));
            expect(received).toEqual([msg_short_delay]);

            // Aspettiamo che scada anche quello lungo (600ms totali)
            for (let i = 0; i < 10; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }
            sub.stop();
            expect(received).toEqual([msg_short_delay, msg_long_delay]);
        });
    });

    describe('Reliability & Custom Timeouts', () => {
        it('Consumer fails callback -> Message becomes VISIBLE again after Visibility Timeout', async () => {
            const q = await nexo.registerQueue('test_q_timeout_custom', { visibilityTimeoutMs: 300 });
            await q.push('timeout-msg');

            let receivedCount = 0;
            const sub = q.subscribe(async () => {
                receivedCount++;
                throw new Error("Force fail to prevent auto ACK");
            });

            for (let i = 0; i < 10; i++) {
                if (receivedCount === 1) break;
                await new Promise(r => setTimeout(r, 50));
            }
            expect(receivedCount).toBe(1);

            // Wait for visibility timeout (300ms + reaper margin)
            await new Promise(r => setTimeout(r, 800));
            sub.stop();
            expect(receivedCount).toBeGreaterThanOrEqual(2);
        });

        it('Consumer repeatedly fails -> Message moves to DLQ after maxRetries', async () => {
            const qName = 'test_q_custom_dlq';
            const q = await nexo.registerQueue(qName, { maxRetries: 2, visibilityTimeoutMs: 300 });
            const dlq = await nexo.registerQueue(`${qName}_dlq`);
            
            await q.push('poison-pill');

            let attempts = 0;
            const sub = q.subscribe(async () => { 
                attempts++; 
                throw new Error("fail");
            });

            // Wait for 2 attempts (~1s)
            await new Promise(r => setTimeout(r, 1200));
            sub.stop();
            expect(attempts).toBe(2);

            let dlqReceived = false;
            const dlqSub = dlq.subscribe(async (data) => {
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
            const q = await nexo.registerQueue('test_q_ttl_custom', { ttlMs: 300 });
            await q.push("short-lived");
            
            // Wait for expiration
            await new Promise(r => setTimeout(r, 800));
            
            let received = false;
            const sub = q.subscribe(async () => { received = true; });

            await new Promise(r => setTimeout(r, 300));
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
            const sub = q.subscribe(async (msg) => {
                received = msg; 
            });

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

            const msgSent = 'instant'

            await q.push(msgSent);

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toBe(msgSent);
        });

        it.skip('User calls ACK twice -> Server handles it without errors (Idempotency)', async () => {
            const q = await nexo.registerQueue('test_q_double_ack_new');
            await q.push('msg');

            // 1. Dobbiamo simulare l'estrazione manuale per fare l'ACK manuale
            // In un caso reale l'utente userebbe q.subscribe, ma qui vogliamo testare l'idempotenza dell'ACK
            // Quindi usiamo il "motore interno" (RequestBuilder) che ora è pubblico (per questo test)
            const res = await (nexo as any).request(Opcode.Q_CONSUME)
                .writeString(q.name)
                .send();

            // Estraiamo l'ID dall'UUID (primi 16 byte del corpo risposta)
            const msgId = res.reader.readUUID();

            // 1° ACK
            await q.ack(msgId);

            // 2° ACK -> Non deve lanciare errori
            await expect(q.ack(msgId)).resolves.toBeUndefined();
        });

        it('User creates queue with special chars -> Broker handles name correctly', async () => {
            const q = await nexo.registerQueue('queues:special/chars@123');
            let msgSent = 'special-test'
            await q.push(msgSent);

            let received: any = null;
            const sub = q.subscribe(async (msg) => { received = msg; });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            sub.stop();
            expect(received).toBe(msgSent);
        });
    });

});
