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
            // Verifica che un messaggio con delay resti "nascosto" e non venga consegnato finché il tempo non è scaduto.
        });

        it('should deliver a delayed message exactly after the delay passes', async () => {
            // Verifica che il Reaper sblocchi correttamente il messaggio una volta passato il delay impostato.
        });

        it('should handle multiple messages with the same exact delay millisecond', async () => {
            // Edge case: verifica che il sistema gestisca correttamente più messaggi che scadono nello stesso identico istante.
        });
    });

    describe('4. Affidabilità e Visibility Timeout', () => {
        it('should make a message visible again if no ACK is received', async () => {
            // Se un worker riceve un messaggio ma crasha (o non manda l'ACK),
            // il messaggio deve tornare disponibile in coda dopo il visibility timeout.
        });

        it('should not re-deliver a message if ACK was received', async () => {
            // Verifica che l'invio dell'ACK cancelli correttamente il timer del Reaper, evitando doppie consegne.
        });

        it('should increment attempts count on each re-delivery', async () => {
            // Verifica che il contatore 'attempts' aumenti ogni volta che il messaggio viene rimesso in coda dal Reaper.
        });
    });

    describe('5. Dead Letter Queue (DLQ)', () => {
        it('should move a message to {name}_dlq after 5 failed attempts', async () => {
            // Verifica che un messaggio "velenoso" che fallisce costantemente venga spostato nella coda DLQ.
        });

        it('should automatically create the DLQ queue if it does not exist', async () => {
            // Verifica che Nexo crei la coda di scarto a runtime senza bisogno di configurazione manuale.
        });

        describe('6. Edge Cases (DevExperience e Robustezza)', () => {
            it('should handle consumers connecting BEFORE the messages arrive', async () => {
                // Testa il Blocking Pop: il client si connette a coda vuota, resta "appeso"
                // e riceve il messaggio istantaneamente appena viene pushato.
            });

            it('should handle consumer disconnect while waiting', async () => {
                // Se un client è in attesa (waiting_consumers) ma si disconnette,
                // Nexo deve gestire l'errore di invio e passare al consumatore successivo.
            });

            it('should be idempotent on double ACKs', async () => {
                // Se un client invia due volte l'ACK per lo stesso messaggio, il server deve rispondere in modo pulito senza crashare.
            });

            it('should handle zero-delay messages as immediate', async () => {
                // Verifica che un delay di 0ms venga trattato esattamente come un PUSH immediato.
            });
        });
    })
});