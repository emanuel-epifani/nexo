import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";
import { NexoClient, Opcode } from '@nexo/client';

describe('QUEUE broker', () => {

    describe('Core API (Handle-based)', () => {
        it('Producer PUSH message -> Consumer receives it', async () => {
            const q = nexo.queue('test_q_base_1');
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
            const q1 = nexo.queue(qName);
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const q2 = client2.queue(qName);

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
            const q1 = nexo.queue(qName);
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });
            const q2 = client2.queue(qName);

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
            const q = nexo.queue('test_q_fifo');
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
            const q = nexo.queue('test_q_priority_1');

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
            const q = nexo.queue('test_q_priority_fifo');

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

            expect(received).toEqual([h1, h2, l1, l2]);
        });
    });

    describe('Delayed Messages (Scheduling)', () => {
        it('Producer PUSH with delay -> message remains invisible until timer expires', async () => {
            const q = nexo.queue('test_q_delayed_1');
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
            const q = nexo.queue('test_q_delayed_order');

            const msg_long_delay = 'longer-delay';
            const msg_short_delay = 'shorter-delay';

            await q.push(msg_long_delay, { delayMs: 600 });
            await q.push(msg_short_delay, { delayMs: 300 });

            const received: string[] = [];
            const sub = q.subscribe(async (msg) => { received.push(msg); });

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
            const q = nexo.queue('test_q_timeout_custom', { visibilityTimeoutMs: 300 });
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

            await new Promise(r => setTimeout(r, 800));
            sub.stop();
            expect(receivedCount).toBeGreaterThanOrEqual(2);
        });

        it('Consumer repeatedly fails -> Message moves to DLQ after maxRetries', async () => {
            const qName = 'test_q_custom_dlq';
            const q = nexo.queue(qName, { maxRetries: 2, visibilityTimeoutMs: 300 });
            const dlq = nexo.queue(`${qName}_dlq`);
            
            await q.push('poison-pill');

            let attempts = 0;
            const sub = q.subscribe(async () => { 
                attempts++; 
                throw new Error("fail");
            });

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
            const q = nexo.queue('test_q_ttl_custom', { ttlMs: 300 });
            await q.push("short-lived");
            
            await new Promise(r => setTimeout(r, 800));
            
            let received = false;
            const sub = q.subscribe(async () => { received = true; });

            await new Promise(r => setTimeout(r, 300));
            sub.stop();
            expect(received).toBe(false);
        });

        it('TTL expires while message is waiting for Retry -> Message is discarded (TTL wins over DLQ)', async () => {
            // 1. SETUP
            // Creiamo una coda con:
            // - TTL molto breve (200ms): il messaggio deve morire presto.
            // - MaxRetries alto (5): avrebbe diritto a molti tentativi.
            // - VisibilityTimeout (100ms): dopo un errore, aspetta 100ms prima di riprovare.
            const qName = 'test_ttl_vs_dlq';
            const q = nexo.queue(qName, {
                ttlMs: 200,
                maxRetries: 5,
                visibilityTimeoutMs: 100
            });

            // Serve anche la DLQ per verificare che NON ci finisca dentro
            const dlq = nexo.queue(`${qName}_dlq`);

            // 2. AZIONE
            // Inviamo il messaggio. Nasce al tempo T=0. Morirà a T=200ms.
            await q.push('msg-destined-to-die');

            // 3. CONSUMO FALLITO (T=~10ms)
            // Consumiamo subito e lanciamo errore.
            // Il messaggio va in "waiting_for_ack" (invisibile).
            // Diventerà visibile di nuovo a T=110ms (10 + 100 visibility).
            let attempts = 0;
            const sub = q.subscribe(async () => {
                attempts++;
                throw new Error('fail');
            });

            // 4. ATTESA STRATEGICA
            // Aspettiamo 400ms. Cosa succede in questo lasso di tempo?
            // - T=110ms: Visibility scade. Il messaggio "potrebbe" essere riprovato.
            // - T=200ms: TTL scade. Il messaggio DEVE morire.
            // - Il reaper gira ogni 100ms.
            //
            // Se il codice è corretto:
            // Il reaper (o il prossimo fetch) si accorge che T > 200ms e lo cancella.
            await new Promise(r => setTimeout(r, 400));
            sub.stop();

            // 5. VERIFICA
            // a. Il messaggio non deve essere stato processato troppe volte (magari 1 o 2, ma poi basta).
            // b. SOPRATTUTTO: La DLQ deve essere VUOTA.
            //    Se il messaggio fosse finito in DLQ, significherebbe che il Retry ha "vinto" sul TTL.

            let dlqMessage: any = null;
            const dlqSub = dlq.subscribe(async (msg) => { dlqMessage = msg; });

            // Diamo tempo per un eventuale dispatch errato verso la DLQ
            await new Promise(r => setTimeout(r, 100));
            dlqSub.stop();

            // EXPECTATIONS
            expect(dlqMessage).toBeNull(); // Non deve essere in DLQ

            // Verifichiamo anche che la coda principale sia vuota (pulizia avvenuta)
            // Proviamo a consumare di nuovo: non deve arrivare nulla.
            let zombieMessage: any = null;
            const zombieSub = q.subscribe(async (msg) => { zombieMessage = msg; });
            await new Promise(r => setTimeout(r, 100));
            zombieSub.stop();

            expect(zombieMessage).toBeNull(); // Non deve essere risorto
        });

    });

    describe('JSON & DevEx', () => {
        it('Producer PUSH object -> SDK auto-serializes/deserializes JSON data with Generics', async () => {
            interface Order { id: number; meta: { type: string }; list: number[] };
            const q = nexo.queue<Order>('test_q_json_complex');
            const data: Order = { id: 1, meta: { type: 'test' }, list: [1, 2, 3] };
            await q.push(data);
            
            let received: Order | null = null;
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

        it('User calls nexo.queue(name) twice -> SDK returns the SAME instance (Idempotency)', () => {
            const q1 = nexo.queue('test_q_idemp');
            const q2 = nexo.queue('test_q_idemp');
            
            expect(q1).toBe(q2);
            expect(q1.name).toBe('test_q_idemp');
        });
    });

    describe('Robustness & Edge Cases', () => {
        it('Consumer starts BEFORE push -> receives message as soon as it arrives', async () => {
            const q = nexo.queue('test_q_blocking_new');
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

        it('User calls ACK twice -> Server handles it without errors (Idempotency)', async () => {
            const q = nexo.queue('test_q_double_ack_new');
            await q.push('msg');

            // 1. Dobbiamo simulare l'estrazione manuale per fare l'ACK manuale
            // In un caso reale l'utente userebbe q.subscribe, ma qui vogliamo testare l'idempotenza dell'ACK
            // Quindi usiamo il "motore interno" (RequestBuilder) che ora è pubblico (per questo test)
            const res = await (nexo as any).builder.reset(Opcode.Q_CONSUME)
                .writeString(q.name)
                .send();

            // Estraiamo l'ID dall'UUID (primi 16 byte del corpo risposta)
            const msgId = res.reader.readUUID();

            // 1° ACK
            await (q as any).ack(msgId);

            // 2° ACK -> Non deve lanciare errori
            await expect((q as any).ack(msgId)).resolves.toBeUndefined();
        });

        it('User creates queue with special chars -> Broker handles name correctly', async () => {
            const q = nexo.queue('queues:special/chars@123');
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

    describe('Prefetch & Concurrency', () => {
        it('Prefetch keeps N requests active -> High throughput simulation', async () => {
            const q = nexo.queue('test_q_prefetch');
            const COUNT = 50;
            // Inviamo 50 messaggi
            for (let i = 0; i < COUNT; i++) await q.push(i);

            let receivedCount = 0;
            const start = Date.now();
            
            // Subscribe con prefetch alto (es. 20)
            const sub = q.subscribe(async () => {
                receivedCount++;
                // Simula leggero processing
                await new Promise(r => setTimeout(r, 2)); 
            }, 20);

            // Aspettiamo che finisca
            while (receivedCount < COUNT) {
                await new Promise(r => setTimeout(r, 10));
                if (Date.now() - start > 5000) break;
            }
            sub.stop();

            expect(receivedCount).toBe(COUNT);
            // Non possiamo testare esattamente quanti "pending" ci sono dal lato client senza mockare socket,
            // ma se finisce in tempi ragionevoli con sleep nel callback, significa che il pipelining funziona.
        });

        it('Prefetch=1 -> Serial processing (Strict Ordering check)', async () => {
            const q = nexo.queue('test_q_serial');
            await q.push(1);
            await q.push(2);
            await q.push(3);

            const received: number[] = [];
            
            // Subscribe con prefetch 1 = Serial
            const sub = q.subscribe(async (val) => {
                received.push(val);
                // Sleep lungo per essere sicuri che se non fosse seriale, ne arriverebbe un altro
                await new Promise(r => setTimeout(r, 50));
            }, 1);

            await new Promise(r => setTimeout(r, 300));
            sub.stop();

            expect(received).toEqual([1, 2, 3]);
        });
    });

});