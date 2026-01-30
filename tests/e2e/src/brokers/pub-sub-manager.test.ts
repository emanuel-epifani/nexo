import { describe, it, expect } from 'vitest';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { nexo } from '../nexo';

const SERVER_PORT = parseInt(process.env.SERVER_PORT!);
const SERVER_HOST = process.env.SERVER_HOST!;


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

    it('Should support global wildcard (#) at root level', async () => {
        const received: any[] = [];

        // Subscribe to "#" - should receive ALL messages
        await nexo.pubsub('#').subscribe((data) => {
            received.push(data);
        });

        // Publish to various topics
        await nexo.pubsub('a').publish({ topic: 'a' });
        await nexo.pubsub('a/b').publish({ topic: 'a/b' });
        await nexo.pubsub('x/y/z').publish({ topic: 'x/y/z' });
        await nexo.pubsub('deep/nested/path/here').publish({ topic: 'deep' });

        await new Promise(r => setTimeout(r, 200));

        expect(received).toHaveLength(4);
        expect(received).toContainEqual({ topic: 'a' });
        expect(received).toContainEqual({ topic: 'a/b' });
        expect(received).toContainEqual({ topic: 'x/y/z' });
        expect(received).toContainEqual({ topic: 'deep' });

    });

    it('Should support UNSUBSCRIBE', async () => {
        const topic = 'chat/global';
        let count = 0;

        const handler = () => { count++; };

        // We need a fresh connection to test clean unsubscribe isolation
        const client2 = await NexoClient.connect({ host: SERVER_HOST, port: SERVER_PORT });

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

});
