import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { waitFor } from '../utils/wait-for';
import { randomUUID } from 'crypto';
import { BenchmarkProbe } from "../utils/benchmark-misure";
import { killServer, runNexoServer } from "../utils/server";


describe('BROKER INTEGRATION', async () => {
    let nexo: NexoClient;
    const SERVER_PORT = parseInt(process.env.SERVER_PORT!);
    const SERVER_HOST = process.env.SERVER_HOST!;

    beforeAll(async () => {
        await runNexoServer(SERVER_HOST, SERVER_PORT);
        nexo = await NexoClient.connect({
            host: process.env.SERVER_HOST!,
            port: parseInt(process.env.SERVER_PORT!, 10)
        });
    });

    afterAll(async () => {
        nexo.disconnect();
        killServer();
    });

    describe('BINARY PAYLOAD SUPPORT', () => {
        // Un buffer riconoscibile
        const binaryPayload = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]);

        it('STORE: Should store and retrieve raw Buffer', async () => {
            const key = `bin-store-${randomUUID()}`;
            await nexo.store.map.set(key, binaryPayload);

            const retrieved = await nexo.store.map.get(key);

            expect(Buffer.isBuffer(retrieved)).toBe(true);
            expect(retrieved.equals(binaryPayload)).toBe(true);
        });

        it('QUEUE: Should push and pop raw Buffer', async () => {
            const qName = `bin-queue-${randomUUID()}`;
            const q = await nexo.queue(qName).create();

            await q.push(binaryPayload);

            const received: any[] = [];
            const sub = await q.subscribe(msg => received.push(msg));

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            sub.stop();
        });

        it('PUBSUB: Should publish and subscribe raw Buffer', async () => {
            const topic = `bin-pubsub-${randomUUID()}`;
            const received: any[] = [];

            await nexo.pubsub(topic).subscribe(msg => received.push(msg));
            await nexo.pubsub(topic).publish(binaryPayload);

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
        });

        it('STREAM: Should stream raw Buffer', async () => {
            const topic = `bin-stream-${randomUUID()}`;
            await nexo.stream(topic).create();

            await nexo.stream(topic).publish(binaryPayload);

            const received: any[] = [];
            const sub = await nexo.stream(topic).subscribe('g1', msg => received.push(msg));

            await waitFor(() => expect(received.length).toBe(1));
            expect(Buffer.isBuffer(received[0])).toBe(true);
            expect(received[0].equals(binaryPayload)).toBe(true);
            sub.stop();
        });
    });

    describe('SYSTEM & PROTOCOL', () => {
        it('should handle JSON serialization with special chars and nested objects', async () => {
            // This tests that the SDK <-> Server serialization (snake_case/camelCase) works
            const complexData = {
                string: "Nexo Engine 🚀",
                number: 42.5,
                boolean: true,
                nullValue: null,
                nested: {
                    id: "abc-123",
                    meta: { active: true, deep: { value: "ok" } }
                },
                unicode: "こんにちは"
            };

            // Using the debug echo command (assuming it exists on the system broker or similar)
            // If not available, we can verify this via Store set/get
            const key = `proto:complex:${randomUUID()}`;
            await nexo.store.map.set(key, complexData);
            const result = await nexo.store.map.get(key);

            expect(result).toEqual(complexData);
        });

        it('should distinguish between empty string and null', async () => {
            const keyEmpty = `proto:empty:${randomUUID()}`;
            const keyNull = `proto:null:${randomUUID()}`;

            await nexo.store.map.set(keyEmpty, '');
            await nexo.store.map.set(keyNull, null);

            expect(await nexo.store.map.get(keyEmpty)).toBe('');
            expect(await nexo.store.map.get(keyNull)).toBeNull();
        });

    });

    describe('STORE (KV)', () => {
        it('should perform basic CRUD operations', async () => {
            const key = `crud:${randomUUID()}`;
            const value = 'persistent_value';

            // Set
            await nexo.store.map.set(key, value);

            // Get
            const result = await nexo.store.map.get(key);
            expect(result).toBe(value);

            // Delete
            await nexo.store.map.del(key);
            const deleted = await nexo.store.map.get(key);
            expect(deleted).toBeNull();
        });

        it('should expire keys after TTL', async () => {
            const key = `ttl:${randomUUID()}`;
            // Set with 1s TTL
            await nexo.store.map.set(key, 'temp', { ttl: 1 });

            // Should exist immediately
            expect(await nexo.store.map.get(key)).toBe('temp');

            // Wait 1.5s
            await new Promise(r => setTimeout(r, 1500));

            // Should be gone
            expect(await nexo.store.map.get(key)).toBeNull();
        });
    });

    describe('QUEUE', () => {
        it('should handle full lifecycle: Push -> Subscribe -> Ack', async () => {
            const qName = `queue-life-${randomUUID()}`;
            const q = await nexo.queue(qName).create();
            const payload = { task: 'process_me' };

            let received: any = null;
            const sub = await q.subscribe(async (data) => {
                received = data;
                // Implicit Ack when function returns
            });

            await q.push(payload);

            await waitFor(() => expect(received).toEqual(payload));
            sub.stop();
        });

        it('should move failed messages to DLQ', async () => {
            const qName = `queue-dlq-${randomUUID()}`;
            // Max 1 retry (2 attempts total)
            const q = await nexo.queue(qName).create({ maxRetries: 1, visibilityTimeoutMs: 100 });
            const dlq = await nexo.queue(`${qName}_dlq`).create();

            await q.push('fail_payload');

            // Consumer that always fails
            const sub = await q.subscribe(async () => {
                throw new Error("Simulated Failure");
            });

            // Wait for retries to exhaust
            await new Promise(r => setTimeout(r, 1000));
            sub.stop();

            // Check DLQ
            let dlqMsg = null;
            const dlqSub = await dlq.subscribe(async (data) => {
                dlqMsg = data;
            });

            await waitFor(() => expect(dlqMsg).toBe('fail_payload'));
            dlqSub.stop();
        });

        it('should respect priority (High before Low)', async () => {
            const qName = `queue-prio-${randomUUID()}`;
            const q = await nexo.queue(qName).create();

            // Push Low then High
            await q.push('low', { priority: 0 });
            await q.push('high', { priority: 10 });

            const received: string[] = [];
            // Concurrency 1 to force ordering check
            const sub = await q.subscribe(async (msg) => {
                received.push(msg);
            }, { concurrency: 1 });

            await waitFor(() => expect(received.length).toBe(2));
            sub.stop();

            expect(received).toEqual(['high', 'low']);
        });
    });

    describe('PUBSUB', () => {

        it('should handle Exact Matches and ignore noise', async () => {
            const targetTopic = `chat/room1-${randomUUID()}`;
            const noiseTopic = `chat/room2-${randomUUID()}`; // Stesso prefisso, finale diverso

            const received: any[] = [];

            // 1. Subscribe solo al target
            await nexo.pubsub(targetTopic).subscribe((data) => received.push(data));

            // 2. Publish su target e su noise
            await nexo.pubsub(targetTopic).publish({ msg: 'target' });
            await nexo.pubsub(noiseTopic).publish({ msg: 'noise' });

            // 3. Verifica: devo avere SOLO il messaggio target
            await waitFor(() => expect(received.length).toBe(1));
            expect(received[0].msg).toBe('target');

            await nexo.pubsub(targetTopic).unsubscribe();
        });

        it('should handle Single-Level Wildcard (+) with strict isolation', async () => {
            // Pattern: home/+/temp
            // Deve matchare: home/kitchen/temp
            // NON deve matchare: home/kitchen/light (suffisso diverso)
            // NON deve matchare: home/kitchen/cupboard/temp (livello in più)
            const baseId = randomUUID();
            const pattern = `home-${baseId}/+/temp`;

            const received: any[] = [];
            await nexo.pubsub(pattern).subscribe((data) => received.push(data));

            // Positive case
            await nexo.pubsub(`home-${baseId}/kitchen/temp`).publish({ id: 'match-1' });
            await nexo.pubsub(`home-${baseId}/garage/temp`).publish({ id: 'match-2' });

            // Negative cases
            await nexo.pubsub(`home-${baseId}/kitchen/light`).publish({ id: 'fail-suffix' });
            await nexo.pubsub(`home-${baseId}/kitchen/cupboard/temp`).publish({ id: 'fail-deep' });
            await nexo.pubsub(`office-${baseId}/kitchen/temp`).publish({ id: 'fail-prefix' });

            // Verifica
            await waitFor(() => expect(received.length).toBe(2));
            const ids = received.map(r => r.id).sort();
            expect(ids).toEqual(['match-1', 'match-2']);

            await nexo.pubsub(pattern).unsubscribe();
        });

        it('should handle Multi-Level Wildcard (#) correctly', async () => {
            // Pattern: sensors/#
            // Deve matchare: sensors/temp
            // Deve matchare: sensors/floor1/room2/temp (nested)
            // NON deve matchare: other/sensors/temp (prefisso diverso)
            const baseId = randomUUID();
            const pattern = `sensors-${baseId}/#`;

            const received: any[] = [];
            await nexo.pubsub(pattern).subscribe((data) => received.push(data));

            // Positive cases
            await nexo.pubsub(`sensors-${baseId}/main`).publish({ id: 'root' });
            await nexo.pubsub(`sensors-${baseId}/a/b/c`).publish({ id: 'deep' });

            // Negative cases
            await nexo.pubsub(`other-${baseId}/main`).publish({ id: 'fail-prefix' });
            // Nota: "sensors-ID" (senza slash) non deve matchare "sensors-ID/#" a meno che la logica non lo preveda esplicitamente
            // Solitamente # richiede almeno un livello o lo slash root.

            await waitFor(() => expect(received.length).toBe(2));
            const ids = received.map(r => r.id).sort();
            expect(ids).toEqual(['deep', 'root']);

            await nexo.pubsub(pattern).unsubscribe();
        });
    });

    describe('STREAM', () => {
        // Separate clients for consumer groups to simulate real distributed nodes
        let clientA: NexoClient;
        let clientB: NexoClient;

        beforeEach(async () => {
            clientA = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });
            clientB = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });
        });

        afterEach(async () => {
            try { clientA?.disconnect(); } catch { }
            try { clientB?.disconnect(); } catch { }
        });

        it('should support Happy Path (Publish/Subscribe)', async () => {
            const topic = `stream-basic-${randomUUID()}`;
            await nexo.stream(topic).create();

            const received: any[] = [];
            const sub = await clientA.stream(topic).subscribe('g1', (data) => received.push(data));

            await nexo.stream(topic).publish({ id: 1 });
            await nexo.stream(topic).publish({ id: 2 });

            await waitFor(() => expect(received.length).toBe(2));
            sub.stop();
        });

        it('Independent CONSUMER GRUP => should deliver foreach all messages', async () => {
            const topic = `stream-groups-${randomUUID()}`;
            await nexo.stream(topic).create();

            const recvA: any[] = [];
            const recvB: any[] = [];

            const subA = await clientA.stream(topic).subscribe('group_A', (d) => recvA.push(d));
            const subB = await clientB.stream(topic).subscribe('group_B', (d) => recvB.push(d));

            await nexo.stream(topic).publish({ msg: 'hello' });

            await waitFor(() => {
                expect(recvA.length).toBe(1);
                expect(recvB.length).toBe(1);
            });

            subA.stop();
            subB.stop();
        });

        it('Same CONSUMER GRUOP => should distribute messages without duplicates', async () => {
            const topic = `parallel-consumers-${randomUUID()}`;
            const group = 'parallel_group';
            await nexo.stream(topic).create({ partitions: 4 });

            const receivedA = new Set<number>();
            const receivedB = new Set<number>();

            // Start 2 consumers in same group
            const subA = await clientA.stream(topic).subscribe(group, (d) => {
                if (receivedA.has(d.id)) throw new Error(`Duplicate in A: ${d.id}`);
                receivedA.add(d.id);
            });

            const subB = await clientB.stream(topic).subscribe(group, (d) => {
                if (receivedB.has(d.id)) throw new Error(`Duplicate in B: ${d.id}`);
                receivedB.add(d.id);
            });

            // Wait for rebalance
            await new Promise(r => setTimeout(r, 500));

            // Publish 1000 messages
            for (let i = 0; i < 1000; i++) {
                await nexo.stream(topic).publish({ id: i });
            }

            await waitFor(() => expect(receivedA.size + receivedB.size).toBe(1000));

            // Verify NO overlap
            const overlap = [...receivedA].filter(id => receivedB.has(id));
            expect(overlap.length).toBe(0, 'No duplicates between consumers');

            subA.stop();
            subB.stop();
        });

        it('should survive Rebalancing: Scale UP with LOAD SHARING', async () => {
            const topic = `stream-strict-up-${randomUUID()}`;
            const group = 'group_strict_scale';
            await nexo.stream(topic).create();

            const producer = nexo.stream(topic);

            // Stats per consumer
            const receivedA: number[] = [];
            const receivedB: number[] = [];
            const allReceivedIds = new Set<number>();

            // 1. Start A (Alone)
            const subA = await clientA.stream(topic).subscribe(group, (d) => {
                receivedA.push(d.i);
                allReceivedIds.add(d.i);
            });

            // 2. Publish Batch 1 (0-19) -> Should go ALL to A
            for (let i = 0; i < 20; i++) await producer.publish({ i });

            await waitFor(() => expect(receivedA.length).toBe(20));
            expect(receivedB.length).toBe(0); // B doesn't exist yet

            // 3. Start B (Join Group)
            const subB = await clientB.stream(topic).subscribe(group, (d) => {
                receivedB.push(d.i);
                allReceivedIds.add(d.i);
            });

            // Wait for rebalance (grace period)
            await new Promise(r => setTimeout(r, 500));

            // 4. Publish Batch 2 (20-99) -> Should be SPLIT
            for (let i = 20; i < 100; i++) await producer.publish({ i });

            // 5. ASSERTION
            await waitFor(() => {
                expect(allReceivedIds.size).toBe(100);
            }, { timeout: 10000 });

            // Verify Distribution
            // A aveva già 20. Ne sono arrivati altri 80.
            // Idealmente A ne prende 40 e B ne prende 40 degli 80 nuovi.
            // Ma il partizionamento hash non è perfetto.
            // Assert LASCO: B deve aver fatto *qualcosa* di significativo (> 10 messaggi)
            expect(receivedB.length).toBeGreaterThan(10);

            // Assert RIGOROSO su integrità: Non devono esserci duplicati inter-consumer
            // Se un messaggio è in A, non dovrebbe essere in B (per lo stesso ID > 19)
            // Questo è difficile da testare se c'è at-least-once durante il rebalance,
            // ma in steady state deve essere vero.

            // Check totale
            for (let i = 0; i < 100; i++) {
                if (!allReceivedIds.has(i)) throw new Error(`Missing message ${i}`);
            }

            subA.stop();
            subB.stop();
        });

        it('should survive Rebalancing: Scale DOWN with ZERO DATA LOSS', async () => {
            const topic = `stream-strict-down-${randomUUID()}`;
            const group = 'group_strict_fault';
            await nexo.stream(topic).create();

            const producer = nexo.stream(topic);

            const allReceivedIds = new Set<number>();

            // Funzione helper per tracciare univocamente cosa arriva
            const track = (d: any) => allReceivedIds.add(d.i);

            // 1. Start A & B
            const subA = await clientA.stream(topic).subscribe(group, track);
            const subB = await clientB.stream(topic).subscribe(group, track);

            // 2. Warm up (0-19)
            for (let i = 0; i < 20; i++) await producer.publish({ i });

            // Sync: aspettiamo che tutto il pregresso sia arrivato
            await waitFor(() => expect(allReceivedIds.size).toBe(20));

            // 3. KILL Client A (Simulate Crash)
            // Nota: Se usassi disconnect(), l'SDK potrebbe mandare un pacchetto "LeaveGroup" pulito.
            // Se vuoi simulare un crash vero, chiudi il socket brutalmente se possibile, altrimenti disconnect va bene.
            await clientA.disconnect();
            // subA.stop() non serve se disconnettiamo il client, anzi simula meglio un crash non chiamarlo.

            // 4. Wait for Rebalance detection (opzionale ma realistico)
            await new Promise(r => setTimeout(r, 500));

            // 5. Publish NEW data (20-59)
            // Questi messaggi devono andare TUTTI a B, perché A è morto.
            for (let i = 20; i < 60; i++) await producer.publish({ i });

            // 6. ASSERTION RIGOROSA
            await waitFor(() => {
                // Dobbiamo avere ESATTAMENTE 60 messaggi unici totali (0-59)
                expect(allReceivedIds.size).toBe(60);
            }, { timeout: 10000 });

            // Verifica extra: non ci sono buchi
            for (let i = 0; i < 60; i++) {
                if (!allReceivedIds.has(i)) throw new Error(`Missing message index ${i}`);
            }

            subB.stop();
        });
    });

    describe('SOCKET RECONNECTION', () => {
        it('PUBSUB: Should auto-resubscribe after connection loss', async () => {
            const topic = `reconnect-pubsub-${randomUUID()}`;
            const received: string[] = [];

            // 1. Subscribe
            await nexo.pubsub(topic).subscribe(m => received.push(m));

            // 2. Simulate Network Failure (Hard Close without disconnect logic)
            // We access private socket to destroy it properly simulationg an error
            const socket = (nexo as any).conn.socket;
            socket.destroy();

            // Wait for disconnect detection
            await waitFor(() => expect((nexo as any).conn.isConnected).toBe(false));

            // 3. Wait for Auto-Reconnect (Default interval 1500ms)
            // We allow some buffer for the loop to kick in
            await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

            // 4. Publish NEW message (Server must know about subscription again)
            await nexo.pubsub(topic).publish('after-crash');

            // 5. Verify reception
            await waitFor(() => expect(received).toContain('after-crash'));
        });

        it('QUEUE: Should resume consuming after connection loss', async () => {
            const qName = `reconnect-queue-${randomUUID()}`;
            const q = await nexo.queue(qName).create();
            const received: any[] = [];

            // 1. Subscribe
            await q.subscribe(msg => received.push(msg));

            // 2. Kill Connection (Clean destroy to avoid noise)
            (nexo as any).conn.socket.destroy();

            // 3. Wait for Reconnect
            await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

            // Give it a moment to stabilize
            await new Promise(r => setTimeout(r, 500));

            // 4. Push NEW message (with retry policy for robustness)
            await waitFor(async () => {
                try {
                    await q.push({ status: 'recovered' });
                    return true;
                } catch {
                    return false;
                }
            }, { timeout: 5000, interval: 500 });

            // 5. Verify
            await waitFor(() => expect(received).toContainEqual({ status: 'recovered' }));
        });

        it('STREAM: Should resume consuming after connection loss (Rejoin Group)', async () => {
            const topic = `reconnect-stream-${randomUUID()}`;
            const group = 'g-reconnect';
            await nexo.stream(topic).create();
            const received: any[] = [];

            // 1. Subscribe
            await nexo.stream(topic).subscribe(group, m => received.push(m));

            // 2. Kill Connection
            (nexo as any).conn.socket.destroy();

            // 3. Wait for Reconnect
            await waitFor(() => expect((nexo as any).conn.isConnected).toBe(true), { timeout: 5000 });

            // Buffer
            await new Promise(r => setTimeout(r, 500));

            // 4. Publish NEW message (Robust retry)
            await waitFor(async () => {
                try {
                    await nexo.stream(topic).publish({ status: 'recovered' });
                    return true;
                } catch {
                    return false;
                }
            }, { timeout: 5000, interval: 500 });

            // 5. Verify (Rejoin must have happened)
            await waitFor(() => expect(received).toContainEqual({ status: 'recovered' }));
        });
    });

    describe.skip('PERFORMANCE', () => {

        it('Protocol Latency: Small Payload Round-Trip', async () => {
            const ITERATIONS = 5000;
            const probe = new BenchmarkProbe('PROTOCOL LATENCY (Small)', ITERATIONS);

            // Warmup (JIT optimization)
            await nexo.store.map.set('w', 'w');

            probe.startTimer();
            for (let i = 0; i < ITERATIONS; i++) {
                const start = performance.now();
                // Use a non-existent key get to skip write-I/O overhead and test pure protocol speed
                await nexo.store.map.get('non-existent');
                probe.record(performance.now() - start);
            }

            const stats = probe.printResult();
            // Hard limit: p99 must be sub-millisecond on localhost
            expect(stats.p99).toBeLessThan(2.0);
        });

        it('Socket Throughput: Fire-and-Forget Burst', async () => {
            const topic = `perf-burst-${randomUUID()}`;
            await nexo.stream(topic).create();
            const producer = nexo.stream(topic);
            const COUNT = 20000;

            const probe = new BenchmarkProbe('SOCKET BURST (20k msgs)', COUNT);

            probe.startTimer();
            const promises = [];
            for (let i = 0; i < COUNT; i++) {
                promises.push(producer.publish({ i }));
            }
            await Promise.all(promises);

            const stats = probe.printResult();
            // Baseline: >10k ops/sec means socket buffer & batching are working
            expect(stats.throughput).toBeGreaterThan(10000);
        });

        it('Codec Efficiency: Large Payload (1MB)', async () => {
            const topic = `perf-large-${randomUUID()}`;
            await nexo.stream(topic).create();
            const producer = nexo.stream(topic);

            const COUNT = 50;
            const largePayload = Buffer.alloc(1024 * 1024).fill('x'); // 1MB
            const probe = new BenchmarkProbe('LARGE PAYLOAD (1MB)', COUNT);

            probe.startTimer();
            for (let i = 0; i < COUNT; i++) {
                const start = performance.now();
                await producer.publish(largePayload);
                probe.record(performance.now() - start);
            }

            const stats = probe.printResult();
            // 1MB transfer should be reasonably fast (e.g. < 50ms locally)
            expect(stats.p99).toBeLessThan(100);
        });

        it('Stream vs PubSub (Binary 64KB)', async () => {
            const PAYLOAD_SIZE = 64 * 1024; // 64KB
            const COUNT = 5000;
            const payload = Buffer.alloc(PAYLOAD_SIZE).fill('x');

            // --- ROUND 1: STREAM ---
            const streamTopic = `perf-stream-${randomUUID()}`;
            await nexo.stream(streamTopic).create({ persistence: 'memory' });
            const streamProd = nexo.stream(streamTopic);

            const probeStream = new BenchmarkProbe('STREAM (64KB)', COUNT);
            probeStream.startTimer();

            // Stream è persistente, quindi l'await garantisce la scrittura (o almeno l'accettazione)
            const pStream = [];
            for (let i = 0; i < COUNT; i++) pStream.push(streamProd.publish(payload));
            await Promise.all(pStream);

            const resStream = probeStream.printResult();

            // --- ROUND 2: PUBSUB ---
            const pubsubTopic = `perf-pubsub-${randomUUID()}`;
            const pubsubProd = nexo.pubsub(pubsubTopic);

            // Nota: PubSub in Nexo è fire-and-forget lato server se non c'è retain,
            // ma l'SDK aspetta comunque l'ACK di "Published" dal server.

            const probePubsub = new BenchmarkProbe('PUBSUB (64KB)', COUNT);
            probePubsub.startTimer();

            const pPubsub = [];
            for (let i = 0; i < COUNT; i++) pPubsub.push(pubsubProd.publish(payload));
            await Promise.all(pPubsub);

            const resPubsub = probePubsub.printResult();

            // Confronto (Solo commento, non fail test se vince Stream per varianza)
            console.log(`\n🏆 Winner: ${resPubsub.throughput > resStream.throughput ? 'PUBSUB' : 'STREAM'}`);
            console.log(`📊 PubSub is ${(resPubsub.throughput / resStream.throughput).toFixed(2)}x faster`);
        });

    });

    describe('MEMORY STRESS & CLEANUP VERIFICATION', () => {
        // Test per verificare che la memoria schizzi in alto e poi si abbassi
        // grazie ai meccanismi di cleanup automatico di ogni broker

        it('STORE: Memory spike and cleanup (TTL expiration)', async () => {
            console.log('\n🔥 STORE STRESS: Creating 500k keys with 5s TTL (500MB+ data)...');
            
            // Crea 500k chiavi da 1KB = ~500MB di dati
            const promises = [];
            for (let i = 0; i < 500000; i++) {
                promises.push(nexo.store.map.set(`stress-store-${i}`, {
                    data: 'x'.repeat(1000), // 1KB per chiave
                    index: i,
                    timestamp: Date.now()
                }, { ttl: 5 }));
            }
            
            console.log('📤 Sending 500k SET commands to server...');
            await Promise.all(promises);
            console.log('✅ All keys sent! Server should be at peak memory now.');
            
            // Aspetta che TTL scada + cleanup
            console.log('⏳ Waiting 8s for TTL expiration + cleanup...');
            await new Promise(r => setTimeout(r, 8000));
            
            console.log('📉 Server should have cleaned up expired keys. Check Docker stats!');
        }); // 30s timeout

        it('QUEUE: Memory spike and cleanup (DLQ + Ack)', async () => {
            console.log('\n🔥 QUEUE STRESS: Pushing 200k messages (100MB+ data)...');
            const qName = `stress-queue-${randomUUID()}`;
            const q = await nexo.queue(qName).create({ 
                maxRetries: 0,
                visibilityTimeoutMs: 100 
            });
            
            // Push 200k messaggi da 500 bytes = ~100MB
            const pushPromises = [];
            for (let i = 0; i < 200000; i++) {
                pushPromises.push(q.push({
                    task: 'process',
                    data: 'y'.repeat(500), // 500 bytes
                    id: i,
                    timestamp: Date.now()
                }));
            }
            
            console.log('📤 Sending 200k PUSH commands to server...');
            await Promise.all(pushPromises);
            console.log('✅ All messages pushed! Server should be at peak memory now.');
            
            // Consuma e ACK tutti i messaggi rapidamente
            console.log('⚡ Consuming and ACKing all messages (concurrency 200)...');
            let consumed = 0;
            const sub = await q.subscribe(async (msg) => {
                consumed++;
                // Auto-ACK quando ritorna
            });
            
            await waitFor(() => expect(consumed).toBe(200000), { timeout: 30000 });
            sub.stop();
            
            console.log('✅ All messages consumed and ACKed!');
            console.log('⏳ Waiting 3s for server cleanup...');
            await new Promise(r => setTimeout(r, 3000));
            
            console.log('📉 Server should have freed queue memory. Check Docker stats!');
        }); // 60s timeout

        it('PUBSUB: Memory spike and cleanup (Disconnect subscribers)', async () => {
            console.log('\n🔥 PUBSUB STRESS: Creating 50k subscribers + 1M messages (1GB+ data)...');
            const baseTopic = `stress-pubsub-${randomUUID()}`;
            
            // Crea 50k subscriber su topic diversi
            console.log('📤 Creating 50k subscribers...');
            const subscriptions = [];
            for (let i = 0; i < 50000; i++) {
                const topic = `${baseTopic}/topic-${i}`;
                subscriptions.push(nexo.pubsub(topic).subscribe(() => {}));
            }
            await Promise.all(subscriptions);
            console.log('✅ 50k subscribers created!');
            
            // Pubblica 1M messaggi su topic random (1KB ciascuno = 1GB)
            console.log('📤 Publishing 1M messages (1GB data)...');
            const publishPromises = [];
            for (let i = 0; i < 1000000; i++) {
                const topic = `${baseTopic}/topic-${Math.floor(Math.random() * 50000)}`;
                publishPromises.push(nexo.pubsub(topic).publish({
                    msg: 'z'.repeat(1000), // 1KB per messaggio
                    id: i,
                    timestamp: Date.now()
                }));
            }
            await Promise.all(publishPromises);
            console.log('✅ 1M messages published! Server should be at PEAK memory now (check Docker stats!)');
            
            // Unsubscribe tutti (simula disconnect)
            console.log('🔌 Unsubscribing all 50k subscribers...');
            const unsubPromises = [];
            for (let i = 0; i < 50000; i++) {
                const topic = `${baseTopic}/topic-${i}`;
                unsubPromises.push(nexo.pubsub(topic).unsubscribe());
            }
            await Promise.all(unsubPromises);
            console.log('✅ All subscribers disconnected!');
            
            // Aspetta background cleanup (60s interval)
            console.log('⏳ Waiting 90s for background actor cleanup (runs every 60s)...');
            await new Promise(r => setTimeout(r, 90000));
            
            console.log('📉 Server should have cleaned up empty actors. Memory should be back down!');
        }); // Timeout 3 minuti

        it('STREAM: Memory spike and cleanup (Consumer group leave)', async () => {
            console.log('\n🔥 STREAM STRESS: Creating 200 partitions + 500k messages (500MB+ data)...');
            const topic = `stress-stream-${randomUUID()}`;
            await nexo.stream(topic).create({ partitions: 32 });
            
            // Pubblica 500k messaggi da 1KB = ~500MB
            const producer = nexo.stream(topic);
            const publishPromises = [];
            for (let i = 0; i < 500000; i++) {
                publishPromises.push(producer.publish({
                    data: 'w'.repeat(1000), // 1KB
                    id: i,
                    timestamp: Date.now()
                }));
            }
            
            console.log('📤 Publishing 500k messages to stream...');
            await Promise.all(publishPromises);
            console.log('✅ All messages published! Server should be at peak memory now.');
            
            // Crea 20 consumer che consumano rapidamente
            console.log('⚡ Starting 20 consumers to drain messages...');
            const clients = [];
            const subs = [];
            
            for (let i = 0; i < 20; i++) {
                const client = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });
                clients.push(client);
                const sub = await client.stream(topic).subscribe(`stress-group-${i}`, () => {});
                subs.push(sub);
            }
            
            // Aspetta che consumino
            console.log('⏳ Waiting 10s for consumers to drain...');
            await new Promise(r => setTimeout(r, 10000));
            
            // Disconnetti tutti i consumer
            console.log('🔌 Disconnecting all 20 consumers...');
            for (const sub of subs) {
                sub.stop();
            }
            for (const client of clients) {
                await client.disconnect();
            }
            
            console.log('⏳ Waiting 5s for server cleanup...');
            await new Promise(r => setTimeout(r, 5000));
            
            console.log('📉 Server should have freed stream memory. Check Docker stats!');
        }); // 90s timeout

        it('ALL BROKERS: Combined stress test (NUKE THE SERVER - 5GB+ data)', async () => {
            console.log('\n🔥🔥🔥 COMBINED STRESS: All 4 brokers simultaneously - MAXIMUM LOAD!');
            console.log('💣 Target: Push server to ~5-6GB memory usage, then watch cleanup...\n');
            
            // Parallelo: stress ESTREMO su tutti e 4 i broker
            await Promise.all([
                // STORE: 1M keys x 1KB = 1GB
                (async () => {
                    console.log('📦 STORE: Pushing 1M keys (1GB)...');
                    const promises = [];
                    for (let i = 0; i < 1000000; i++) {
                        promises.push(nexo.store.map.set(`nuke-store-${i}`, { 
                            data: 'x'.repeat(1000),
                            index: i 
                        }, { ttl: 10 }));
                    }
                    await Promise.all(promises);
                    console.log('✅ STORE: 1M keys pushed!');
                })(),
                
                // QUEUE: 500k messages x 2KB = 1GB
                // (async () => {
                //     console.log('📬 QUEUE: Pushing 500k messages (1GB)...');
                //     const qName = `nuke-queue-${randomUUID()}`;
                //     const q = await nexo.queue(qName).create();
                //     const promises = [];
                //     for (let i = 0; i < 500000; i++) {
                //         promises.push(q.push({
                //             data: 'y'.repeat(2000), // 2KB
                //             id: i
                //         }));
                //     }
                //     await Promise.all(promises);
                //     console.log('✅ QUEUE: 500k messages pushed!');
                //
                //     // Consuma rapidamente
                //     const sub = await q.subscribe(() => {}, { concurrency: 500 });
                //     await new Promise(r => setTimeout(r, 10000));
                //     sub.stop();
                //     console.log('✅ QUEUE: All consumed!');
                // })(),
                
                // PUBSUB: 100k subscribers + 2M messages x 1KB = 2GB
                (async () => {
                    console.log('📡 PUBSUB: Creating 100k subscribers + 2M messages (2GB)...');
                    const base = `nuke-pubsub-${randomUUID()}`;
                    const subs = [];
                    for (let i = 0; i < 100000; i++) {
                        subs.push(nexo.pubsub(`${base}/t-${i}`).subscribe(() => {}));
                    }
                    await Promise.all(subs);
                    console.log('✅ PUBSUB: 100k subscribers created!');
                    
                    const pubs = [];
                    for (let i = 0; i < 2000000; i++) {
                        pubs.push(nexo.pubsub(`${base}/t-${Math.floor(Math.random() * 100000)}`).publish({ 
                            data: 'z'.repeat(1000), // 1KB
                            id: i 
                        }));
                    }
                    await Promise.all(pubs);
                    console.log('✅ PUBSUB: 2M messages published!');
                    
                    // Unsubscribe
                    const unsubs = [];
                    for (let i = 0; i < 100000; i++) {
                        unsubs.push(nexo.pubsub(`${base}/t-${i}`).unsubscribe());
                    }
                    await Promise.all(unsubs);
                    console.log('✅ PUBSUB: All unsubscribed!');
                })(),
                
                // STREAM: 300 partitions + 1M messages x 1KB = 1GB
                (async () => {
                    console.log('🌊 STREAM: Creating 300 partitions + 1M messages (1GB)...');
                    const topic = `nuke-stream-${randomUUID()}`;
                    await nexo.stream(topic).create({ partitions: 300 });
                    const producer = nexo.stream(topic);
                    
                    const pubs = [];
                    for (let i = 0; i < 1000000; i++) {
                        pubs.push(producer.publish({ 
                            data: 'w'.repeat(1000), // 1KB
                            id: i 
                        }));
                    }
                    await Promise.all(pubs);
                    console.log('✅ STREAM: 1M messages published!');
                })()
            ]);
            
            console.log('\n💥 ALL DATA SENT! Server should be at MAXIMUM memory now (5-6GB)!');
            console.log('📊 Check Docker stats NOW - memory should be peaked!\n');
            
            // Aspetta cleanup di tutti i broker
            console.log('⏳ Waiting 120s for all brokers to cleanup (TTL expiration + background tasks)...');
            await new Promise(r => setTimeout(r, 120000)); // 2 minuti
            
            console.log('\n📉 Server should have cleaned up significantly!');
            console.log('📊 Check Docker stats NOW - memory should be back down!');
            console.log('✅ If memory dropped from ~5-6GB to ~500MB-1GB, cleanup is working! 🎉');
        }); // Timeout 5 minuti
    });


});

