import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { nexo } from '../nexo'; // Singleton instance connected to global server

const SERVER_PORT = parseInt(process.env.NEXO_PORT!);

describe('Topic Broker (MQTT-Style)', () => {

    it('Should subscribe and receive exact matches', async () => {
        const topic = 'home/kitchen/temp';
        const payload = { value: 25.5 };
        let received = null;

        await nexo.topic.subscribe(topic, (data) => {
            received = data;
        });

        await nexo.topic.publish(topic, payload);

        // Wait for push
        await new Promise(r => setTimeout(r, 100));
        expect(received).toEqual(payload);
    });

    it('Should support wildcards (+)', async () => {
        const pattern = 'home/+/temp';
        const received: any[] = [];

        await nexo.topic.subscribe(pattern, (data) => {
            received.push(data);
        });

        await nexo.topic.publish('home/kitchen/temp', { loc: 'kitchen' });
        await nexo.topic.publish('home/garage/temp', { loc: 'garage' });
        await nexo.topic.publish('home/kitchen/light', { loc: 'light' }); // Should NOT match

        await new Promise(r => setTimeout(r, 100));
        expect(received).toHaveLength(2);
        expect(received).toContainEqual({ loc: 'kitchen' });
        expect(received).toContainEqual({ loc: 'garage' });
        expect(received).not.toContainEqual({ loc: 'light' });
    });

    it('Should support wildcards (#)', async () => {
        const pattern = 'sensors/#';
        const received: any[] = [];

        await nexo.topic.subscribe(pattern, (data) => {
            received.push(data);
        });

        await nexo.topic.publish('sensors/temp', 1);
        await nexo.topic.publish('sensors/temp/ext', 2);
        await nexo.topic.publish('sensors/a/b/c', 3);
        await nexo.topic.publish('other/stuff', 4); // No match

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
        
        await client2.topic.subscribe(topic, handler);
        await client2.topic.publish(topic, 'msg1');
        await new Promise(r => setTimeout(r, 50));
        expect(count).toBe(1);

        await client2.topic.unsubscribe(topic);
        await client2.topic.publish(topic, 'msg2');
        await new Promise(r => setTimeout(r, 50));
        expect(count).toBe(1); // Should not increase

        client2.disconnect();
    });

    it('Should support RETAINED messages (Last Value Caching)', async () => {
        const topic = 'config/global/rate_limit';
        const value = { max: 100 };

        // 1. Publish with RETAIN = true
        await nexo.topic.publish(topic, value, { retain: true });

        // 2. Subscribe after publish
        let received = null;

        await nexo.topic.subscribe(topic, (data) => {
            received = data;
        });

        // 3. Should receive the retained message immediately
        await new Promise(r => setTimeout(r, 200));
        expect(received).toEqual(value);

    });

    it('Should support RETAINED messages with Single-level Wildcard (+)', async () => {
        // Publish retained messages to different topics
        await nexo.topic.publish('status/s1', 'online', { retain: true });
        await nexo.topic.publish('status/s2', 'offline', { retain: true });
        await nexo.topic.publish('status/s3/detail', 'verbose', { retain: true }); // Deeper level

        const received: string[] = [];

        // Subscribe with wildcard
        await nexo.topic.subscribe('status/+', (data) => {
            received.push(data as string);
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
        await nexo.topic.publish('config/app/db/host', 'localhost', { retain: true });
        await nexo.topic.publish('config/app/db/port', 5432, { retain: true });
        await nexo.topic.publish('config/app/cache/ttl', 60, { retain: true });
        await nexo.topic.publish('config/system/os', 'linux', { retain: true }); // Different branch

        const received: any[] = [];

        // Subscribe to config/app/# -> should get db/host, db/port, cache/ttl. Should NOT get system/os.
        await nexo.topic.subscribe('config/app/#', (data) => {
            received.push(data);
        });

        await new Promise(r => setTimeout(r, 200));

        expect(received).toHaveLength(3);
        expect(received).toContain('localhost');
        expect(received).toContain(5432);
        expect(received).toContain(60);
        expect(received).not.toContain('linux');
    });
    
    // --- PERFORMANCE TESTS ---

    it('Perf: Fan-Out (Broadcasting)', async () => {
        // Scenario: 1 Publisher -> 100 Subscribers
        const subscribers = 100;
        const clients: NexoClient[] = [];
        let receivedCount = 0;

        for (let i = 0; i < subscribers; i++) {
            const c = await NexoClient.connect({ port: SERVER_PORT });
            await c.topic.subscribe('news/global', () => { receivedCount++; });
            clients.push(c);
        }

        await nexo.topic.publish('news/global', 'Breaking News');
        
        await new Promise(r => setTimeout(r, 500));
        expect(receivedCount).toBe(subscribers);

        clients.forEach(c => c.disconnect());
    });

    it('Perf: Fan-In (Telemetry)', async () => {
        // Scenario: 10 Publishers -> 1 Subscriber
        const publishers = 10;
        const clients: NexoClient[] = [];
        const payload = { temp: 20 };
        let receivedCount = 0;

        await nexo.topic.subscribe('sensors/+', () => { receivedCount++; });

        for (let i = 0; i < publishers; i++) {
            const c = await NexoClient.connect({ port: SERVER_PORT });
            clients.push(c);
        }

        // Parallel publish
        await Promise.all(clients.map((c, i) => c.topic.publish(`sensors/dev${i}`, payload)));

        await new Promise(r => setTimeout(r, 500));
        expect(receivedCount).toBe(publishers);

        clients.forEach(c => c.disconnect());
    });

    it('Perf: Traffic Mesh (Chat)', async () => {
        // Scenario: 10 Clients, everyone subscribes to "room1", everyone publishes 1 msg
        const count = 10;
        const clients: NexoClient[] = [];
        let totalDeliveries = 0;

        for (let i = 0; i < count; i++) {
            const c = await NexoClient.connect({ port: SERVER_PORT });
            await c.topic.subscribe('room/1', () => { totalDeliveries++; });
            clients.push(c);
        }

        await Promise.all(clients.map(c => c.topic.publish('room/1', 'Hello')));

        await new Promise(r => setTimeout(r, 500));
        expect(totalDeliveries).toBe(count * count);

        clients.forEach(c => c.disconnect());
    });

    it('Performance -> Throughput Benchmark (FULL PIPE STRESS)', async () => {
        const TOTAL_OPERATIONS = 100_000;
        const CONCURRENCY = 100; 
        
        const payload = { 
            id: "bench-123", 
            timestamp: Date.now(), 
            data: "stressing-the-pipe-with-some-bytes-to-parse-and-decode" 
        };
        
        const opsPerWorker = TOTAL_OPERATIONS / CONCURRENCY;
        const start = performance.now();
        
        const worker = async () => {
            for (let i = 0; i < opsPerWorker; i++) {
                await (nexo as any).debug.echo(payload);
            }
        };

        await Promise.all(Array.from({ length: CONCURRENCY }, worker));

        const end = performance.now();
        const durationSeconds = (end - start) / 1000;
        const throughput = Math.floor(TOTAL_OPERATIONS / durationSeconds);

        console.log(`\n\x1b[32m[PERF VITEST] Throughput: ${throughput.toLocaleString()} deliveries/sec\x1b[0m`);
        
        expect(throughput).toBeGreaterThan(100_000); 
    });
});
