import { describe, it } from 'vitest';
import { nexo } from '../nexo';

describe('NEXO Dashboard Prefill', () => {

    it('Should populate all brokers with demo data for dashboard visualization', async () => {
        console.log("🚀 Starting Data Population for Dashboard...");

        // 1. STORE (KV) - 100 Keys
        console.log("📦 Populating Store...");
        // Syntax: nexo.store.kv.set(key, value, ttl?)
        await nexo.store.kv.set('config:app_name', 'NexoSuperApp');
        await nexo.store.kv.set('config:version', '1.0.0');
        await nexo.store.kv.set('user:session:123', '{"user_id": 123, "role": "admin"}', 3600); // 1h TTL
        await nexo.store.kv.set('cache:home_page', '<html>...</html>', 300); // 5m TTL

        // Batch insert for volume
        const promises = [];
        for (let i = 0; i < 50; i++) {
            promises.push(nexo.store.kv.set(`sensor:temp:${i}`, `${20 + Math.random() * 10}`));
        }
        await Promise.all(promises);

        // Expiring keys
        await nexo.store.kv.set('otp:9999', '123456', 10); // 10s TTL (watch it expire!)

        // 2. QUEUES
        console.log("📥 Populating Queues...");
        // Queue 1: Email (Healthy)
        // Syntax: await nexo.queue(name).create(config?) -> Queue Instance
        const emailQ = await nexo.queue('email_notifications').create({ maxRetries: 3 });
        for (let i = 0; i < 5; i++) await emailQ.push({ to: `user${i}@example.com`, subject: "Welcome" });

        // Queue 2: Transcoding (Backlog)
        const videoQ = await nexo.queue('video_transcode').create({ visibilityTimeoutMs: 5000 });
        for (let i = 0; i < 20; i++) await videoQ.push({ file: `vid_${i}.mp4` });

        // Queue 3: DLQ Simulation
        const webhookQ = await nexo.queue('webhooks').create({ maxRetries: 1, visibilityTimeoutMs: 100 });
        await webhookQ.push({ url: "http://bad-url.com" });

        // Consume and fail to simulate DLQ movement (requires failing logic or wait for retry)
        // For dashboard purposes, just having it in pending is enough to show the queue exists.

        // 3. STREAM
        console.log("🌊 Populating Streams...");
        // Topic 1: Orders (Partitions & Consumers)
        // Syntax: await nexo.stream(topic).create({ partitions })
        const ordersTopicName = 'orders.v1';
        await nexo.stream(ordersTopicName).create({ partitions: 4 });
        const ordersPub = nexo.stream(ordersTopicName);

        // Publish events
        for (let i = 0; i < 100; i++) {
            await ordersPub.publish({ order_id: i, amount: Math.random() * 100 }, { key: `user_${i % 10}` });
        }

        // Consumer Group 1: Fulfillment (Up to date)
        const fulfillmentGroup = nexo.stream(ordersTopicName, 'fulfillment-service');
        await fulfillmentGroup.subscribe((msg) => { /* ack auto */ }, { batchSize: 50 });
        // Let it consume some
        await new Promise(r => setTimeout(r, 500));

        // Consumer Group 2: Audit (Lagging)
        // We subscribe but don't process everything to create lag
        const auditGroup = nexo.stream(ordersTopicName, 'audit-log');
        let count = 0;
        const subHandle = await auditGroup.subscribe((msg) => {
            count++;
        }, { batchSize: 1 });

        // Consume 10 messages then stop
        await new Promise(r => setTimeout(r, 200));
        if (count > 0) subHandle.stop();

        // 4. PUBSUB
        console.log("📢 Populating PubSub...");
        // Syntax: nexo.pubsub(topic).publish(payload, options?)
        await nexo.pubsub('settings/global/theme').publish('dark', { retain: true });
        await nexo.pubsub('settings/global/maintenance').publish('false', { retain: true });
        await nexo.pubsub('sensors/kitchen/temp').publish('24.5', { retain: true });
        await nexo.pubsub('sensors/livingroom/temp').publish('22.0', { retain: true });

        // Active Clients (Simulate by creating dummy connections)
        // The test runner itself is a client.
        await nexo.pubsub('sensors/#').subscribe((msg) => { });

        console.log("✅ Data Population Complete! Check Dashboard at http://localhost:8080");

        // Keep alive for a bit if running manually
        // await new Promise(r => setTimeout(r, 60000));
    });



});



