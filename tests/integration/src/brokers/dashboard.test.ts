import { describe, it } from 'vitest';
import {fetchSnapshot, nexo} from '../nexo';

describe('NEXO Dashboard Prefill', () => {

    it('Should populate all brokers with demo data for dashboard visualization', async () => {
        console.log("📥 Populating Store...");
        //1. STORE
        for (let i = 0; i < 250; i++) await nexo.store.kv.set(`user:${i}`, { name: `User ${i}`, role: "user" }, 3600)

        // 2. QUEUES
        console.log("📥 Populating Queues...");
        // Syntax: await nexo.queue(name).create(config?) -> Queue Instance
        const emailQ = await nexo.queue('email_notifications').create({ maxRetries: 3 });
        for (let i = 0; i < 5; i++) await emailQ.push({ to: `user${i}@example.com`, subject: "xxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsdsxxxsdsdsdssdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsdsddsdsdsds" });

        // Queue 2: Transcoding (Backlog)
        const videoQ = await nexo.queue('video_transcode').create({ visibilityTimeoutMs: 5000 });
        for (let i = 0; i < 5; i++) await videoQ.push({ file: `vid_${i}.mp4` });

        // Queue 3: DLQ Simulation
        const webhookQ = await nexo.queue('webhooks').create({ maxRetries: 1, visibilityTimeoutMs: 100 });
        await webhookQ.push({ url: "http://bad-url.com" });

        // Consume and fail to simulate DLQ movement (requires failing logic or wait for retry)
        // For dashboard purposes, just having it in pending is enough to show the queue exists.

        // 3. STREAM
        console.log("🌊 Populating Streams...");
        const ordersTopicName = 'orders';
        const ordersTopic = await nexo.stream(ordersTopicName,'finance').create();
        for (let i = 0; i < 1000; i++) await ordersTopic.publish({ order_id: i })
        await ordersTopic.subscribe((msg) => { /* ack auto */ }, { batchSize: 50 });
        await ordersTopic.subscribe((msg) => { /* ack auto */ }, { batchSize: 50 });


        // Consumer Group 1: Fulfillment (Up to date)
        // const fulfillmentGroup = nexo.stream(ordersTopicName, 'fulfillment-service');
        // await fulfillmentGroup.subscribe((msg) => { /* ack auto */ }, { batchSize: 50 });
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
        await nexo.pubsub('settings/global/theme').publish({ name: `User`, role: "user" }, { retain: true });
        await nexo.pubsub('settings/global/maintenance').publish({ name: `User`, role: "user" }, { retain: true });
        await nexo.pubsub('sensors/kitchen/temp').publish({ name: `User`, role: "user" }, { retain: true });
        await nexo.pubsub('sensors/livingroom/temp').publish({ name: `User`, role: "user" }, { retain: true });


        console.log("✅ Data Population Complete! Check Dashboard at http://localhost:8080");

        // Keep alive for a bit if running manually
        // await new Promise(r => setTimeout(r, 60000));
    });



});



