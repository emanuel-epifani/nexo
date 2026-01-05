import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { nexo } from '../nexo';
import { BenchmarkProbe } from "../utils/benchmark-misure";

const SERVER_PORT = parseInt(process.env.NEXO_PORT!);

describe('PubSub Broker (MQTT-Style)', () => {

    it('Should subscribe and receive exact matches', async () => {
        const topic = 'home/kitchen/temp';
        const payload = { value: 25.5 };
        let received = null;

        await nexo.pubsub(topic).subscribe((data) => {
            received = data;
        });

        await nexo.pubsub(topic).publish(payload);

        // Wait for push
        await new Promise(r => setTimeout(r, 100));
        expect(received).toEqual(payload);
    });

    it('Should support wildcards (+)', async () => {
        const pattern = 'home/+/temp';
        const received: any[] = [];

        await nexo.pubsub(pattern).subscribe((data) => {
            received.push(data);
        });

        await nexo.pubsub('home/kitchen/temp').publish({ loc: 'kitchen' });
        await nexo.pubsub('home/garage/temp').publish({ loc: 'garage' });
        await nexo.pubsub('home/kitchen/light').publish({ loc: 'light' }); // Should NOT match

        await new Promise(r => setTimeout(r, 100));
        expect(received).toHaveLength(2);
        expect(received).toContainEqual({ loc: 'kitchen' });
        expect(received).toContainEqual({ loc: 'garage' });
        expect(received).not.toContainEqual({ loc: 'light' });
    });

    it('Should support wildcards (#)', async () => {
        const pattern = 'sensors/#';
        const received: any[] = [];

        await nexo.pubsub(pattern).subscribe((data) => {
            received.push(data);
        });

        await nexo.pubsub('sensors/temp').publish(1);
        await nexo.pubsub('sensors/temp/ext').publish(2);
        await nexo.pubsub('sensors/a/b/c').publish(3);
        await nexo.pubsub('other/stuff').publish(4); // No match

        await new Promise(r => setTimeout(r, 100));
        expect(received).toHaveLength(3);
        expect(received).toContainEqual(1);
        expect(received).toContainEqual(2);
        expect(received).toContainEqual(3);
        expect(received).not.toContainEqual(4);
    });

    it('Should support UNSUBSCRIBE', async () => {
        const topic = 'chat/global';
        let count = 0;

        const handler = () => { count++; };

        // We need a fresh connection to test clean unsubscribe isolation
        const client2 = await NexoClient.connect({ port: SERVER_PORT });

        await client2.pubsub(topic).subscribe(handler);
        await client2.pubsub(topic).publish('msg1');
        await new Promise(r => setTimeout(r, 50));
        expect(count).toBe(1);

        await client2.pubsub(topic).unsubscribe();
        await client2.pubsub(topic).publish('msg2');
        await new Promise(r => setTimeout(r, 50));
        expect(count).toBe(1); // Should not increase

        client2.disconnect();
    });

    it('Should support RETAINED messages (Last Value Caching)', async () => {
        const topic = 'config/global/rate_limit';
        const value = { max: 100 };

        // 1. Publish with RETAIN = true
        await nexo.pubsub(topic).publish(value, { retain: true });

        // 2. Subscribe after publish
        let received = null;

        await nexo.pubsub(topic).subscribe((data) => {
            received = data;
        });

        // 3. Should receive the retained message immediately
        await new Promise(r => setTimeout(r, 200));
        expect(received).toEqual(value);

    });

    it('Should support RETAINED messages with Single-level Wildcard (+)', async () => {
        // Publish retained messages to different topics
        await nexo.pubsub('status/s1').publish('online', { retain: true });
        await nexo.pubsub('status/s2').publish('offline', { retain: true });
        await nexo.pubsub('status/s3/detail').publish('verbose', { retain: true }); // Deeper level

        const received: string[] = [];

        // Subscribe with wildcard
        await nexo.pubsub<string>('status/+').subscribe((data) => {
            received.push(data);
        });
        await new Promise(r => setTimeout(r, 500));

        // Should receive s1 and s2, but NOT s3 (because + matches one level)
        expect(received).toContain('online');
        expect(received).toContain('offline');
        expect(received).not.toContain('verbose');
        expect(received).toHaveLength(2);
    });

    it('Should support RETAINED messages with Multi-level Wildcard (#)', async () => {
        // Publish retained messages to a deep hierarchy
        await nexo.pubsub('config/app/db/host').publish('localhost', { retain: true });
        await nexo.pubsub('config/app/db/port').publish(5432, { retain: true });
        await nexo.pubsub('config/app/cache/ttl').publish(60, { retain: true });
        await nexo.pubsub('config/system/os').publish('linux', { retain: true }); // Different branch

        const received: any[] = [];

        // Subscribe to config/app/# -> should get db/host, db/port, cache/ttl. Should NOT get system/os.
        await nexo.pubsub('config/app/#').subscribe((data) => {
            received.push(data);
        });

        await new Promise(r => setTimeout(r, 200));

        expect(received).toHaveLength(3);
        expect(received).toContain('localhost');
        expect(received).toContain(5432);
        expect(received).toContain(60);
        expect(received).not.toContain('linux');
    });

    // --- PERFORMANCE ---
    it('Fan-Out (1 Pub -> 100 Subs)', async () => {
        const SUBSCRIBERS = 100;
        const MESSAGES = 100;
        const TOTAL_EVENTS = MESSAGES * SUBSCRIBERS;

        const clients: NexoClient[] = [];
        let received = 0;
        const probe = new BenchmarkProbe("PUBSUB - FANOUT", TOTAL_EVENTS);

        for (let i = 0; i < SUBSCRIBERS; i++) {
            const c = await NexoClient.connect({ port: SERVER_PORT });
            await c.pubsub('perf/fanout').subscribe((msg: any) => {
                received++;
                if (msg.ts) probe.recordLatency(msg.ts);
            });
            clients.push(c);
        }

        probe.startTimer();
        await Promise.all(Array.from({ length: MESSAGES }).map(() => nexo.pubsub('perf/fanout').publish({ ts: Date.now() })));

        while (received < TOTAL_EVENTS) {
            await new Promise(r => setTimeout(r, 10));
            if ((performance.now() - probe['start']) > 5000) break;
        }

        const stats = probe.printResult();
        clients.forEach(c => c.disconnect());
        expect(received).toBe(TOTAL_EVENTS);
        expect(stats.throughput).toBeGreaterThan(400_000);
        expect(stats.p99).toBeLessThan(15);
        expect(stats.max).toBeLessThan(15);
    });

    it('Fan-In (50 Pubs -> 1 Sub)', async () => {
        const PUBLISHERS = 50;
        const MSGS_PER_PUB = 50;
        const TOTAL_EXPECTED = PUBLISHERS * MSGS_PER_PUB;
        const clients: NexoClient[] = [];
        let received = 0;

        const probe = new BenchmarkProbe("PUBSUB - FANIN", TOTAL_EXPECTED);

        await nexo.pubsub('sensors/+').subscribe((msg: any) => {
            received++;
            if (msg.ts) probe.recordLatency(msg.ts);
        });

        for (let i = 0; i < PUBLISHERS; i++) {
            clients.push(await NexoClient.connect({ port: SERVER_PORT }));
        }

        probe.startTimer();
        await Promise.all(clients.map((c, i) => {
            const promises = [];
            for (let k = 0; k < MSGS_PER_PUB; k++) {
                promises.push(c.pubsub(`sensors/d${i}`).publish({ ts: Date.now() }));
            }
            return Promise.all(promises);
        }));

        while (received < TOTAL_EXPECTED) {
            await new Promise(r => setTimeout(r, 10));
            if ((performance.now() - probe['start']) > 5000) break;
        }

        const stats = probe.printResult();
        clients.forEach(c => c.disconnect());
        expect(received).toBe(TOTAL_EXPECTED);
        expect(stats.throughput).toBeGreaterThan(100_000);
        expect(stats.p99).toBeLessThan(15);
        expect(stats.max).toBeLessThan(20);

    });

    it('Wildcard Routing Stress', async () => {
        const OPS = 10_000;
        let received = 0;
        await nexo.pubsub('infra/+/+/cpu').subscribe(() => { received++; });

        const probe = new BenchmarkProbe("PUBSUB - WILDCARD", OPS);
        probe.startTimer();

        const worker = async () => {
            const opsPerWorker = OPS / 10;
            for (let i = 0; i < opsPerWorker; i++) {
                const t0 = performance.now();
                await nexo.pubsub('infra/us-east/server-1/cpu').publish({ u: 90 });
                probe.record(performance.now() - t0);
            }
        };
        await Promise.all(Array.from({ length: 10 }, worker));

        while (received < OPS) await new Promise(r => setTimeout(r, 10));

        const stats = probe.printResult();
        expect(stats.throughput).toBeGreaterThan(100_000);
        expect(stats.p99).toBeLessThan(0.5);
        expect(stats.max).toBeLessThan(2);
    });

});
