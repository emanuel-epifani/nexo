import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { nexo } from '../nexo';

const SERVER_PORT = parseInt(process.env.NEXO_PORT!);

describe('PubSub Broker (MQTT-Style)', () => {

    it('Should subscribe and receive exact matches', async () => {
        const topic = 'home/kitchen/temp';
        const payload = { value: 25.5 };
        let received = null;

        await nexo.pubsub.subscribe(topic, (data) => {
            received = data;
        });

        await nexo.pubsub.publish(topic, payload);

        // Wait for push
        await new Promise(r => setTimeout(r, 100));
        expect(received).toEqual(payload);
    });

    it('Should support wildcards (+)', async () => {
        const pattern = 'home/+/temp';
        const received: any[] = [];

        await nexo.pubsub.subscribe(pattern, (data) => {
            received.push(data);
        });

        await nexo.pubsub.publish('home/kitchen/temp', { loc: 'kitchen' });
        await nexo.pubsub.publish('home/garage/temp', { loc: 'garage' });
        await nexo.pubsub.publish('home/kitchen/light', { loc: 'light' }); // Should NOT match

        await new Promise(r => setTimeout(r, 100));
        expect(received).toHaveLength(2);
        expect(received).toContainEqual({ loc: 'kitchen' });
        expect(received).toContainEqual({ loc: 'garage' });
        expect(received).not.toContainEqual({ loc: 'light' });
    });

    it('Should support wildcards (#)', async () => {
        const pattern = 'sensors/#';
        const received: any[] = [];

        await nexo.pubsub.subscribe(pattern, (data) => {
            received.push(data);
        });

        await nexo.pubsub.publish('sensors/temp', 1);
        await nexo.pubsub.publish('sensors/temp/ext', 2);
        await nexo.pubsub.publish('sensors/a/b/c', 3);
        await nexo.pubsub.publish('other/stuff', 4); // No match

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
        
        await client2.pubsub.subscribe(topic, handler);
        await client2.pubsub.publish(topic, 'msg1');
        await new Promise(r => setTimeout(r, 50));
        expect(count).toBe(1);

        await client2.pubsub.unsubscribe(topic);
        await client2.pubsub.publish(topic, 'msg2');
        await new Promise(r => setTimeout(r, 50));
        expect(count).toBe(1); // Should not increase

        client2.disconnect();
    });

    it('Should support RETAINED messages (Last Value Caching)', async () => {
        const topic = 'config/global/rate_limit';
        const value = { max: 100 };

        // 1. Publish with RETAIN = true
        await nexo.pubsub.publish(topic, value, { retain: true });

        // 2. Subscribe after publish
        let received = null;

        await nexo.pubsub.subscribe(topic, (data) => {
            received = data;
        });

        // 3. Should receive the retained message immediately
        await new Promise(r => setTimeout(r, 200));
        expect(received).toEqual(value);

    });

    it('Should support RETAINED messages with Single-level Wildcard (+)', async () => {
        // Publish retained messages to different topics
        await nexo.pubsub.publish('status/s1', 'online', { retain: true });
        await nexo.pubsub.publish('status/s2', 'offline', { retain: true });
        await nexo.pubsub.publish('status/s3/detail', 'verbose', { retain: true }); // Deeper level

        const received: string[] = [];

        // Subscribe with wildcard
        await nexo.pubsub.subscribe('status/+', (data) => {
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
        await nexo.pubsub.publish('config/app/db/host', 'localhost', { retain: true });
        await nexo.pubsub.publish('config/app/db/port', 5432, { retain: true });
        await nexo.pubsub.publish('config/app/cache/ttl', 60, { retain: true });
        await nexo.pubsub.publish('config/system/os', 'linux', { retain: true }); // Different branch

        const received: any[] = [];

        // Subscribe to config/app/# -> should get db/host, db/port, cache/ttl. Should NOT get system/os.
        await nexo.pubsub.subscribe('config/app/#', (data) => {
            received.push(data);
        });

        await new Promise(r => setTimeout(r, 200));

        expect(received).toHaveLength(3);
        expect(received).toContain('localhost');
        expect(received).toContain(5432);
        expect(received).toContain(60);
        expect(received).not.toContain('linux');
    });
    
});
