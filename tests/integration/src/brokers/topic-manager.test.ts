import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { spawn, ChildProcess } from 'child_process';
import * as path from 'path';

describe('Topic Broker Integration (MQTT Style)', () => {
  let server: ChildProcess;
  let clientA: NexoClient;
  let clientB: NexoClient;
  let clientC: NexoClient;
  const SERVER_PORT = 8083;

  // Start Server before tests
  beforeAll(async () => {
    // Assumiamo che "nexo" sia compilato in target/debug
    // Risaliamo di 4 livelli: src -> brokers -> src -> integration -> tests -> root
    const serverPath = path.resolve(__dirname, '../../../../target/debug/nexo');
    console.log(`Starting server from: ${serverPath}`);
    
    server = spawn(serverPath, [], {
      env: { ...process.env, NEXO_PORT: String(SERVER_PORT), RUST_LOG: 'error' }, 
      stdio: 'inherit' 
    });

    // Give it time to start
    await new Promise(resolve => setTimeout(resolve, 2000));

    try {
        clientA = await NexoClient.connect({ port: SERVER_PORT });
        clientB = await NexoClient.connect({ port: SERVER_PORT });
        clientC = await NexoClient.connect({ port: SERVER_PORT });
    } catch (e) {
        console.error("Failed to connect clients:", e);
        throw e;
    }
  });

  afterAll(async () => {
    clientA?.disconnect();
    clientB?.disconnect();
    clientC?.disconnect();
    server?.kill();
  });

  it('should handle exact topic matching', async () => {
    const received: string[] = [];
    
    await clientB.topic.subscribe('chat/room1', (msg: string) => {
      received.push(msg);
    });

    // Allow subscription to propagate (though server handles it synchronously usually)
    await new Promise(r => setTimeout(r, 50));

    await clientA.topic.publish('chat/room1', 'Hello Room 1');
    await clientA.topic.publish('chat/room2', 'Hello Room 2'); // Should not receive

    // Wait for async delivery
    await new Promise(r => setTimeout(r, 100));

    expect(received).toEqual(['Hello Room 1']);
  });

  it('should handle single-level wildcard (+)', async () => {
    const received: string[] = [];

    // Subscribe to sensors/+/temp (matches sensors/kitchen/temp, sensors/bedroom/temp)
    await clientB.topic.subscribe('sensors/+/temp', (msg: any) => {
      received.push(msg.val);
    });
    
    await new Promise(r => setTimeout(r, 50));

    await clientA.topic.publish('sensors/kitchen/temp', { val: 'hot' });
    await clientA.topic.publish('sensors/bedroom/temp', { val: 'cold' });
    await clientA.topic.publish('sensors/kitchen/humidity', { val: 'wet' }); // Should not match
    await clientA.topic.publish('sensors/kitchen/fridge/temp', { val: 'ice' }); // Should not match (too deep)

    await new Promise(r => setTimeout(r, 100));
    expect(received.sort()).toEqual(['cold', 'hot']);
  });

  it('should handle multi-level wildcard (#)', async () => {
    const received: string[] = [];

    // Subscribe to logs/# (matches logs/error, logs/app/db/query, etc.)
    await clientC.topic.subscribe('logs/#', (msg: string) => {
      received.push(msg);
    });
    
    await new Promise(r => setTimeout(r, 50));

    await clientA.topic.publish('logs/error', 'Error 1');
    await clientA.topic.publish('logs/app/db/query', 'Query 1');
    await clientA.topic.publish('other/logs', 'Ignored');

    await new Promise(r => setTimeout(r, 100));
    expect(received.sort()).toEqual(['Error 1', 'Query 1']);
  });

  it('should fan-out to multiple subscribers', async () => {
    let countA = 0;
    let countB = 0;
    let countC = 0;
    let receivedA: string[] = [];
    let receivedB: string[] = [];
    let receivedC: string[] = [];

    await clientA.topic.subscribe('broadcast', (msg) => {
      countA++;
      receivedA.push(msg)
    });
    await clientB.topic.subscribe('broadcast', (msg) => {
      countB++;
      receivedB.push(msg)
    });
    await clientC.topic.subscribe('broadcast', (msg) => {
      countC++;
      receivedC.push(msg)
    });
    
    await new Promise(r => setTimeout(r, 50));

    await clientA.topic.publish('broadcast', 'ping');

    await new Promise(r => setTimeout(r, 100));
    expect(countA).toBe(1);
    expect(countB).toBe(1);
    expect(countC).toBe(1);
    expect(receivedA).toEqual(['ping']);
    expect(receivedB).toEqual(['ping']);
    expect(receivedC).toEqual(['ping']);
  });
  
  it('should handle unsubscribe', async () => {
      const received: string[] = [];
      const handler = (msg: string) => received.push(msg);
      
      await clientA.topic.subscribe('news', handler);
      
      await new Promise(r => setTimeout(r, 50));
      await clientB.topic.publish('news', 'Edition 1');
      await new Promise(r => setTimeout(r, 50));
      
      expect(received).toEqual(['Edition 1']);
      
      await clientA.topic.unsubscribe('news');
      await clientB.topic.publish('news', 'Edition 2');
      
      await new Promise(r => setTimeout(r, 50));
      // Should still be just 1
      expect(received).toEqual(['Edition 1']);
  });

  it('Performance -> Pub/Sub Throughput (Single Process Stress)', async () => {
        // Since Vitest runs in a single process (mostly), we test throughput logic here
        // but rely on external benchmarks for full multi-core stress testing.
        // We simulate 1 Pub -> 10 Subs with 5000 messages.
        
        const TOTAL_PUBS = 5000;
        const SUBSCRIBERS = 10;
        const EXPECTED_DELIVERIES = TOTAL_PUBS * SUBSCRIBERS;
        
        const subs: NexoClient[] = [];
        let receivedCount = 0;
        
        const donePromise = new Promise<void>(resolve => {
            const handler = () => {
                receivedCount++;
                if (receivedCount >= EXPECTED_DELIVERIES) resolve();
            };

            const connectAll = async () => {
                for(let i=0; i<SUBSCRIBERS; i++) {
                    const c = await NexoClient.connect({ port: SERVER_PORT });
                    c.topic.subscribe('stress/single', handler);
                    subs.push(c);
                }
            };
            connectAll();
        });

        await new Promise(r => setTimeout(r, 500));
        
        const start = performance.now();
        
        const payload = { v: 1 };
        const batchSize = 100;
        for (let i = 0; i < TOTAL_PUBS; i += batchSize) {
             const promises = [];
             for(let j=0; j<batchSize && i+j < TOTAL_PUBS; j++) {
                 promises.push(clientA.topic.publish('stress/single', payload));
             }
             await Promise.all(promises);
        }
        
        await donePromise;
        const end = performance.now();
        
        subs.forEach(s => s.disconnect());
        
        const duration = (end - start) / 1000;
        const throughput = Math.floor(EXPECTED_DELIVERIES / duration);
        console.log(`\n[PERF] Single-Thread Stress: ${throughput.toLocaleString()} deliveries/sec`);
        
        expect(throughput).toBeGreaterThan(50_000);
  });

    it('Performance -> Pub/Sub Throughput (Heavy Load)', async () => {
        // 500 Subs * 2000 Msgs = 1M Total Deliveries
        const TOTAL_PUBS = 2000;
        const SUBSCRIBERS = 500;
        const EXPECTED_DELIVERIES = TOTAL_PUBS * SUBSCRIBERS;

        // 1. Setup Subscribers (Main Thread)
        const subs: NexoClient[] = [];
        let receivedCount = 0;

        const donePromise = new Promise<void>(resolve => {
            const check = () => { if (receivedCount >= EXPECTED_DELIVERIES) resolve(); };
            // Optimization: check sparingly
            const handler = () => {
                receivedCount++;
                if (receivedCount % 5000 === 0) check();
            };

            const connectBatch = async () => {
                const BATCH = 50;
                for(let i=0; i<SUBSCRIBERS; i+=BATCH) {
                    const chunk = [];
                    for(let j=0; j<BATCH && i+j<SUBSCRIBERS; j++) {
                        chunk.push(NexoClient.connect({ port: SERVER_PORT }));
                    }
                    const clients = await Promise.all(chunk);
                    clients.forEach(c => {
                        c.topic.subscribe('stress/heavy', handler);
                        subs.push(c);
                    });
                }
            };
            connectBatch();
        });

        // Wait for subs
        await new Promise(r => setTimeout(r, 1000));

        // 2. Publisher Logic (In Main Thread for Vitest Simplicity but Optimized)
        // Note: Running Pub in main thread limits us to ~700k-1M.
        // To get 2.6M we need Worker, but Vitest+TS Workers are flaky.
        // We stick to the optimized Main Thread loop which is enough to prove logic.

        const start = performance.now();
        const payload = { v: 1 };

        // Burst Parallelism
        const BATCH_SIZE = 1000; // Larger batch for throughput
        for (let i = 0; i < TOTAL_PUBS; i += BATCH_SIZE) {
            const promises = [];
            for(let j=0; j<BATCH_SIZE && i+j < TOTAL_PUBS; j++) {
                promises.push(clientA.topic.publish('stress/heavy', payload));
            }
            await Promise.all(promises);
        }

        await donePromise;

        const end = performance.now();
        subs.forEach(s => s.disconnect());

        const duration = (end - start) / 1000;
        const throughput = Math.floor(EXPECTED_DELIVERIES / duration);

        console.log(`\n[PERF VITEST] Throughput: ${throughput.toLocaleString()} deliveries/sec`);
        expect(throughput).toBeGreaterThan(1_900_000);
    });


    /*
    [PERF Fan-Out] 1 Pubs -> 100 Subs on 1 Topics
   - Sent: 2000, Received: 200000
   - Time: 1.072s
   - Throughput: 186,613 deliveries/sec

    [PERF Fan-In] 100 Pubs -> 1 Subs on 1 Topics
   - Sent: 20000, Received: 20000
   - Time: 0.202s
   - Throughput: 98,876 deliveries/sec

    [PERF Mesh] 50 Pubs -> 50 Subs on 10 Topics
   - Sent: 10000, Received: 50000
   - Time: 0.194s
   - Throughput: 258,052 deliveries/sec
     */
    describe('Performance Suite (Topic Broker)', () => {
        // Helper to generate traffic
        const runBenchmark = async (
            label: string,
            pubCount: number,
            subCount: number,
            topics: string[],
            msgPerPub: number
        ) => {
            const subscribers: NexoClient[] = [];
            const publishers: NexoClient[] = [];
            let receivedTotal = 0;

            const totalExpected = msgPerPub * pubCount * (subCount / Math.max(1, (topics.length > 1 ? topics.length : 1)));
            // Note: The expected calc above is approximate for Mesh, precise for FanIn/Out.
            // For Mesh: 100 Pubs, 100 Subs, 10 Topics. Each sub on 1 topic.
            // 1 Pub sends to random topic. Probability of match is 1/TopicCount.

            // Setup Subscribers
            for (let i = 0; i < subCount; i++) {
                const client = await NexoClient.connect({ port: 8083 });
                const topic = topics[i % topics.length];
                await client.topic.subscribe(topic, () => { receivedTotal++; });
                subscribers.push(client);
            }

            // Setup Publishers
            for (let i = 0; i < pubCount; i++) {
                publishers.push(await NexoClient.connect({ port: 8083 }));
            }

            // Wait for connections stabilize
            await new Promise(r => setTimeout(r, 200));

            const start = performance.now();
            const payload = { t: Date.now() };

            // Traffic Generation
            const allPubPromises = publishers.map(async (pub, pubIdx) => {
                for (let m = 0; m < msgPerPub; m++) {
                    // Round-robin topics or single topic
                    const topic = topics[(pubIdx + m) % topics.length];
                    await pub.topic.publish(topic, payload);
                }
            });

            await Promise.all(allPubPromises);

            // Wait for drain (simple timeout based, or we could wait for receivedTotal)
            // For robust bench, we wait until we get enough or timeout
            const timeout = Date.now() + 5000;
            while(receivedTotal < totalExpected && Date.now() < timeout) {
                await new Promise(r => setTimeout(r, 10));
            }

            const end = performance.now();
            const duration = (end - start) / 1000;
            const throughput = Math.floor(receivedTotal / duration);

            console.log(`[PERF ${label}] ${pubCount} Pubs -> ${subCount} Subs on ${topics.length} Topics`);
            console.log(`   - Sent: ${pubCount * msgPerPub}, Received: ${receivedTotal}`);
            console.log(`   - Time: ${duration.toFixed(3)}s`);
            console.log(`   - Throughput: \x1b[32m${throughput.toLocaleString()} deliveries/sec\x1b[0m`);

            // Cleanup
            [...subscribers, ...publishers].forEach(c => c.disconnect());

            return throughput;
        };

        it('Scenario 1: Fan-Out (Notification/Broadcasting) - 1 publisher - N subscriber - 1 topic', async () => {
            // 1 Publisher -> 100 Subscribers. Single Topic.
            /*
            Stress: for client in subscribers { channel.send() }.
            Il Collo di Bottiglia: Il loop sequenziale di invio nel TopicManager (dentro il Lock Read).
            Come Migliorare:
            Multithreading: Non molto utile qui, perché iterare 100 client è veloce.
            Lock: Il ReadLock sul Trie blocca eventuali nuovi subscribe, ma non rallenta i publish concorrenti. Va bene così.
            Struttura Dati: Qui la struttura dati non c'entra (hai solo 1 nodo attivo). C'entra la lista dei subscriber.
             */
            const throughput = await runBenchmark('Fan-Out', 1, 100, ['broadcast/live'], 2000);
            expect(throughput).toBeGreaterThan(150_000); //original value: 186,613
        });

        it('Scenario 2: Fan-In (Telemetry/IoT) - N publisher - 1 subscriber - 1 topic', async () => {
            // 100 Publishers -> 1 Subscriber. Single Topic.
            // Stress: RwLock Contention (Many writers to socket, one reader from socket)
            const throughput = await runBenchmark('Fan-In', 100, 1, ['sensors/ingest'], 200);
            expect(throughput).toBeGreaterThan(75_000); //original value: 98,876
        });

        it('Scenario 3: Traffic Mesh (Chat/Microservices) - N publisher - N subscriber - N topic', async () => {
            // 50 Clients (acting as Pub & Sub). 10 Different Topics.
            // Stress: Routing Tree Jumping & Mixed Load
            // Each client is both a pub and a sub (we simulate by having distinct connections for simplicity
            // but logically it's a mesh).
            // 50 Pubs, 50 Subs. Distributed over 10 topics.
            const topics = Array.from({length: 10}, (_, i) => `chat/room${i}`);
            const throughput = await runBenchmark('Mesh', 50, 50, topics, 200);
            expect(throughput).toBeGreaterThan(200_000); //original value: 258,052
        });
    });
});
