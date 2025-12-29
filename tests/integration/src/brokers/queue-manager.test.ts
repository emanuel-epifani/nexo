import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { nexo } from "../nexo";
import { NexoClient } from '@nexo/client';

describe('QUEUE broker', () => {

    describe('1. Flussi Base e Concorrenza', () => {
        it('should deliver a message to a single consumer', async () => {
            const queue = 'test_q_base_1';
            const payload = Buffer.from('hello nexo');
            
            let received: any = null;
            // Start consuming in background
            const consumePromise = nexo.queue.consume(queue, async (msg) => {
                received = msg;
                await nexo.queue.ack(queue, msg.id);
            });

            await nexo.queue.push(queue, payload);
            
            // Wait for delivery
            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }

            expect(received).not.toBeNull();
            expect(received.payload).toEqual(payload);
            expect(received.id).toBeDefined();
        });

        it('should handle competing consumers: 1 message, 2 consumers', async () => {
            const queue = 'test_q_competing';
            const payload = Buffer.from('competing-test');
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });

            let count = 0;
            const callback = async (msg: any) => {
                count++;
                // We don't ACK yet to see if the other gets it (it shouldn't)
            };

            nexo.queue.consume(queue, callback);
            client2.queue.consume(queue, callback);

            // Wait a bit to ensure both are registered as waiting_consumers
            await new Promise(r => setTimeout(r, 100));

            await nexo.queue.push(queue, payload);

            // Wait to see if more than one receives it
            await new Promise(r => setTimeout(r, 500));

            expect(count).toBe(1);
            client2.disconnect();
        });

        it('should handle fair distribution: 2 messages, 2 consumers', async () => {
            const queue = 'test_q_fair';
            const client2 = await NexoClient.connect({
                host: process.env.NEXO_HOST,
                port: parseInt(process.env.NEXO_PORT!)
            });

            const received1: any[] = [];
            const received2: any[] = [];

            nexo.queue.consume(queue, async (msg) => {
                received1.push(msg);
                await nexo.queue.ack(queue, msg.id);
            });
            client2.queue.consume(queue, async (msg) => {
                received2.push(msg);
                await client2.queue.ack(queue, msg.id);
            });

            // Ensure both are waiting
            await new Promise(r => setTimeout(r, 100));

            await nexo.queue.push(queue, 'msg1');
            await nexo.queue.push(queue, 'msg2');

            // Wait for distribution
            await new Promise(r => setTimeout(r, 500));

            expect(received1.length).toBe(1);
            expect(received2.length).toBe(1);
            expect(received1[0].payload.toString()).not.toEqual(received2[0].payload.toString());

            client2.disconnect();
        });

        it('should deliver messages in FIFO order within the same priority', async () => {
            const queue = 'test_q_fifo';
            const messages = ['first', 'second', 'third'];

            for (const m of messages) {
                await nexo.queue.push(queue, m);
            }

            const received: string[] = [];
            // We need to consume and ACK sequentially to see them all
            nexo.queue.consume(queue, async (msg) => {
                received.push(msg.payload.toString());
                await nexo.queue.ack(queue, msg.id);
            });

            // Wait for all 3
            for (let i = 0; i < 20; i++) {
                if (received.length === 3) break;
                await new Promise(r => setTimeout(r, 100));
            }

            expect(received).toEqual(messages);
        });
    });

    describe('2. Priorità (Buckets)', () => {
        it('should deliver higher priority messages first', async () => {
            const queue = 'test_q_priority_1';

            let message_high_priority = 'high-priority'
            let message_low_priority = 'low-priority'

            // Inviamo prima un messaggio LOW (0) e poi uno HIGH (255)
            await nexo.queue.push(queue, message_low_priority, { priority: 0 });
            await nexo.queue.push(queue, message_high_priority, { priority: 255 });

            const received: string[] = [];
            nexo.queue.consume(queue, async (msg) => {
                received.push(msg.payload.toString());
                await nexo.queue.ack(queue, msg.id);
            });

            // Aspettiamo la ricezione di entrambi
            for (let i = 0; i < 20; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }

            // Il primo deve essere quello HIGH nonostante sia stato inviato dopo
            expect(received[0]).toBe(message_high_priority);
            expect(received[1]).toBe(message_low_priority);
        });

        it('should respect FIFO within different priority buckets', async () => {
            const queue = 'test_q_priority_fifo';

            // Mix di messaggi: 2 High e 2 Low
            await nexo.queue.push(queue, 'high-1', { priority: 10 });
            await nexo.queue.push(queue, 'low-1', { priority: 5 });
            await nexo.queue.push(queue, 'high-2', { priority: 10 });
            await nexo.queue.push(queue, 'low-2', { priority: 5 });

            const received: string[] = [];
            nexo.queue.consume(queue, async (msg) => {
                received.push(msg.payload.toString());
                await nexo.queue.ack(queue, msg.id);
            });

            for (let i = 0; i < 20; i++) {
                if (received.length === 4) break;
                await new Promise(r => setTimeout(r, 100));
            }

            // L'ordine deve essere: tutti gli High (in ordine FIFO) e poi tutti i Low (in ordine FIFO)
            expect(received).toEqual(['high-1', 'high-2', 'low-1', 'low-2']);
        });
    });

    describe('3. Delayed Jobs (Il tempo)', () => {
        it('should not deliver a delayed message before its time', async () => {
            const queue = 'test_q_delayed_1';
            const payload = 'delayed-msg';
            const delayMs = 1500; // 1.5 secondi

            await nexo.queue.push(queue, payload, { delayMs });

            let received = false;
            nexo.queue.consume(queue, async (msg) => {
                received = true;
                await nexo.queue.ack(queue, msg.id);
            });

            // Dopo 500ms non deve ancora esserci
            await new Promise(r => setTimeout(r, 500));
            expect(received).toBe(false);

            // Dopo altri 1500ms (totale 2000ms) deve essere arrivato
            for (let i = 0; i < 20; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 100));
            }
            expect(received).toBe(true);
        });

        it('should deliver multiple delayed messages in order', async () => {
            const queue = 'test_q_delayed_order';

            await nexo.queue.push(queue, 'longer-delay', { delayMs: 2000 });
            await nexo.queue.push(queue, 'shorter-delay', { delayMs: 1000 });

            const received: string[] = [];
            nexo.queue.consume(queue, async (msg) => {
                received.push(msg.payload.toString());
                await nexo.queue.ack(queue, msg.id);
            });

            // Dopo 1200ms, solo shorter-delay deve essere uscito
            await new Promise(r => setTimeout(r, 1200));
            expect(received).toEqual(['shorter-delay']);

            // Dopo altri 1500ms, entrambi
            for (let i = 0; i < 20; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }
            expect(received).toEqual(['shorter-delay', 'longer-delay']);
        });

        it('should handle multiple messages with the same exact delay', async () => {
            const queue = 'test_q_delayed_same';
            await nexo.queue.push(queue, 'msg1', { delayMs: 1000 });
            await nexo.queue.push(queue, 'msg2', { delayMs: 1000 });

            const received: string[] = [];
            nexo.queue.consume(queue, async (msg) => {
                received.push(msg.payload.toString());
                await nexo.queue.ack(queue, msg.id);
            });

            for (let i = 0; i < 30; i++) {
                if (received.length === 2) break;
                await new Promise(r => setTimeout(r, 100));
            }
            expect(received.length).toBe(2);
        });
    });

    describe('4. Affidabilità e Visibility Timeout', () => {
        it('should make a message visible again if no ACK is received', async () => {
            const queue = 'test_q_timeout';
            await nexo.queue.push(queue, 'timeout-msg');

            let receivedCount = 0;
            const receivedIds: string[] = [];

            nexo.queue.consume(queue, async (msg) => {
                receivedCount++;
                receivedIds.push(msg.id);
                // NON mandiamo l'ACK: il messaggio deve tornare in coda dopo 1s
            });

            for (let i = 0; i < 10; i++) {
                if (receivedCount === 1) break;
                await new Promise(r => setTimeout(r, 100));
            }
            expect(receivedCount).toBe(1);

            // Aspettiamo il timeout (1s + tolleranza reaper)
            await new Promise(r => setTimeout(r, 2500));
            expect(receivedCount).toBeGreaterThanOrEqual(2);
            expect(receivedIds[0]).toBe(receivedIds[1]);
        });
    });

    describe('5. Dead Letter Queue (DLQ)', () => {
        it('should move a message to {name}_dlq after 5 failed attempts', async () => {
            const queue = 'test_q_to_dlq';
            const dlq = `${queue}_dlq`;
            await nexo.queue.push(queue, 'poison-pill');

            let mainCount = 0;
            nexo.queue.consume(queue, async () => { mainCount++; });

            // Aspettiamo 5 fallimenti (~6-7s con timeout a 1s)
            await new Promise(r => setTimeout(r, 8000));
            expect(mainCount).toBe(5);

            let dlqMsg: any = null;
            nexo.queue.consume(dlq, async (msg) => { dlqMsg = msg; });

            for (let i = 0; i < 20; i++) {
                if (dlqMsg) break;
                await new Promise(r => setTimeout(r, 100));
            }
            expect(dlqMsg.payload.toString()).toBe('poison-pill');
        }, 15000);
    });

    describe('6. Edge Cases (DevExperience e Robustezza)', () => {
        it('should handle consumers connecting BEFORE the messages arrive', async () => {
            const queue = 'test_q_blocking';
            let received: any = null;

            // Il consumatore si sottoscrive quando la coda è ancora vuota
            nexo.queue.consume(queue, (msg) => {
                received = msg;
            });

            // Aspettiamo per assicurarci che il server lo metta in waiting_consumers
            await new Promise(r => setTimeout(r, 200));
            expect(received).toBeNull();

            // Inviamo il messaggio ora
            await nexo.queue.push(queue, 'instant-delivery');

            // Il server deve "svegliare" il consumatore e consegnare subito
            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            expect(received.payload.toString()).toBe('instant-delivery');
        });

        it('should be idempotent on double ACKs', async () => {
            const queue = 'test_q_double_ack';
            await nexo.queue.push(queue, 'msg');

            let msgId: string = '';
            nexo.queue.consume(queue, (msg) => {
                msgId = msg.id;
            });

            for (let i = 0; i < 10; i++) {
                if (msgId) break;
                await new Promise(r => setTimeout(r, 50));
            }

            // Primo ACK: il messaggio viene rimosso correttamente
            await nexo.queue.ack(queue, msgId);

            // Secondo ACK sullo stesso UUID: non deve generare errori o crash
            // Il server deve semplicemente ignorarlo o rispondere OK
            await expect(nexo.queue.ack(queue, msgId)).resolves.toBeUndefined();
        });

        it('should handle zero-delay messages as immediate', async () => {
            const queue = 'test_q_zero_delay';
            // Un delay di 0ms deve essere trattato come un push normale
            await nexo.queue.push(queue, 'zero-delay-payload', { delayMs: 0 });

            let received = false;
            nexo.queue.consume(queue, () => {
                received = true;
            });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            expect(received).toBe(true);
        });

        it('should handle special characters in queue names', async () => {
            const queue = 'queue:with/special-chars@123';
            await nexo.queue.push(queue, 'special-name-test');

            let received: any = null;
            nexo.queue.consume(queue, (msg) => {
                received = msg;
            });

            for (let i = 0; i < 10; i++) {
                if (received) break;
                await new Promise(r => setTimeout(r, 50));
            }
            expect(received.payload.toString()).toBe('special-name-test');
        });
    });
});