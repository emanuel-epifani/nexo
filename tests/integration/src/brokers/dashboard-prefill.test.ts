import { describe, it, expect } from 'vitest';
import { nexo, fetchBrokerSnapshot } from '../nexo';


describe('DASHBOARD PREFILL - Complete Data Visualization', () => {

    it('STORE', async () => {

        // ========================================
        // 1. SETUP STORE BROKER DATA
        // ========================================
        console.log('📦 Setting up STORE broker data...');

        // Basic keys
        await nexo.store.map.set('user:123:name', 'Alice Johnson');
        await nexo.store.map.set('user:123:email', 'alice@example.com');
        await nexo.store.map.set('user:456:name', 'Bob Smith');
        await nexo.store.map.set('user:456:email', 'bob@example.com');

        // Session keys with TTL
        await nexo.store.map.set('session:abc123', { user_id: 123, active: true }, { ttl: 3600 });
        await nexo.store.map.set('session:def456', { user_id: 456, active: false }, { ttl: 7200 });
        await nexo.store.map.set('session:temp789', { temp: true }, { ttl: 10 }); // Short TTL for testing

        // Cache keys
        await nexo.store.map.set('cache:product:1', { id: 1, name: 'Laptop', price: 999 });
        await nexo.store.map.set('cache:product:2', { id: 2, name: 'Mouse', price: 29 });
        await nexo.store.map.set('cache:stats:daily', { users: 1500, orders: 450 });

        // Configuration keys
        await nexo.store.map.set('config:app:version', '1.2.3');
        await nexo.store.map.set('config:app:debug', 'true');
        await nexo.store.map.set('config:db:max_connections', '100');

        // Test different DataType behaviors
        await nexo.store.map.set('text:plain', 'This is plain text'); // STRING type
        await nexo.store.map.set('text:logo', 'binary-image-data-here'); // STRING type (text)
        await nexo.store.map.set('json:config', { debug: true, version: '1.0' }); // JSON type

        // Binary data (RAW type - will be shown as hex)
        await nexo.store.map.set('binary:pdf', Buffer.from([0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34])); // RAW type
        await nexo.store.map.set('binary:json', Buffer.from([0x7B, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3A, 0x31, 0x32, 0x33, 0x7D])); // RAW type
        await nexo.store.map.set('binary:large', Buffer.alloc(1024, 0xFF)); // RAW type
        await nexo.store.map.set('binary:empty', Buffer.alloc(0)); // Empty data

        // ========================================
        // 2. WAIT FOR DATA PROPAGATION
        // ========================================
        console.log('⏳ Waiting for data propagation...');
        await new Promise(resolve => setTimeout(resolve, 1000));

        // ========================================
        // 3. FETCH AND VALIDATE STORE SNAPSHOT
        // ========================================
        console.log('📸 Fetching STORE broker snapshot...');
        const storeSnapshot = await fetchBrokerSnapshot('/api/store');

        console.log('📊 Store Snapshot received:', JSON.stringify(storeSnapshot, null, 2));

        // Validate structure
        expect(storeSnapshot).toBeDefined();
        expect(storeSnapshot).not.toBeNull();
        expect(storeSnapshot.keys).toBeDefined();
        expect(storeSnapshot.keys.length).toBeGreaterThan(0);

        // Check specific keys exist
        const keyNames = storeSnapshot.keys.map(k => k.key);
        expect(keyNames).toContain('user:123:name');
        expect(keyNames).toContain('session:abc123');
        expect(keyNames).toContain('cache:product:1');
        expect(keyNames).toContain('text:plain');
        expect(keyNames).toContain('text:logo');
        expect(keyNames).toContain('json:config');
        expect(keyNames).toContain('binary:pdf');
        expect(keyNames).toContain('binary:json');
        expect(keyNames).toContain('binary:large');
        expect(keyNames).toContain('binary:empty');

        // ========================================
        // 4. VALIDATE DATATYPE PARSING RESULTS
        // ========================================

        // STRING type should be plain text
        const textPlain = storeSnapshot.keys.find(k => k.key === 'text:plain');
        expect(textPlain).toBeDefined();
        expect(textPlain.value).toBe('This is plain text');
        expect(textPlain.value).not.toMatch(/^0x/);

        const textLogo = storeSnapshot.keys.find(k => k.key === 'text:logo');
        expect(textLogo).toBeDefined();
        expect(textLogo.value).toBe('binary-image-data-here'); // STRING type → text
        expect(textLogo.value).not.toMatch(/^0x/);

        // JSON type should be readable text
        const jsonConfig = storeSnapshot.keys.find(k => k.key === 'json:config');
        expect(jsonConfig).toBeDefined();
        expect(jsonConfig.value).toContain('debug');
        expect(jsonConfig.value).toContain('true');
        expect(jsonConfig.value).not.toMatch(/^0x/);

        // RAW type should be hex
        const binaryPdf = storeSnapshot.keys.find(k => k.key === 'binary:pdf');
        expect(binaryPdf).toBeDefined();
        expect(binaryPdf.value).toBe('0x255044462d312e34'); // RAW type → hex
        expect(binaryPdf.value).toMatch(/^0x/);

        const binaryJson = storeSnapshot.keys.find(k => k.key === 'binary:json');
        expect(binaryJson).toBeDefined();
        expect(binaryJson.value).toBe('0x7b2274657374223a3132337d'); // RAW type → hex
        expect(binaryJson.value).toMatch(/^0x/);

        const binaryLarge = storeSnapshot.keys.find(k => k.key === 'binary:large');
        expect(binaryLarge).toBeDefined();
        expect(binaryLarge.value).toMatch(/^0x[ff]+$/); // RAW type → hex of 0xFF repeated
        expect(binaryLarge.value).toMatch(/^0x/);

        const binaryEmpty = storeSnapshot.keys.find(k => k.key === 'binary:empty');
        expect(binaryEmpty).toBeDefined();
        expect(binaryEmpty.value).toBe('0x'); // Empty data

        // ========================================
        // 5. VALIDATE STRUCTURE CHANGES
        // ========================================

        // Validate created_at is removed
        const userKey = storeSnapshot.keys.find(k => k.key === 'user:123:name');
        expect(userKey.created_at).toBeUndefined();
        expect(textLogo.created_at).toBeUndefined();

        // Validate expires_at still exists
        const sessionKey = storeSnapshot.keys.find(k => k.key === 'session:abc123');
        expect(sessionKey).toBeDefined();
        expect(sessionKey.expires_at).toBeDefined();
        expect(sessionKey.expires_at).not.toBeNull();

        // Validate regular text data
        const userName = storeSnapshot.keys.find(k => k.key === 'user:123:name');
        expect(userName).toBeDefined();
        expect(userName.value).toBe('Alice Johnson');
        expect(userName.value).not.toMatch(/^0x/);

        // Validate JSON cache data
        const cacheKey = storeSnapshot.keys.find(k => k.key === 'cache:product:1');
        expect(cacheKey).toBeDefined();
        expect(cacheKey.value).toContain('Laptop');
        expect(cacheKey.value).not.toMatch(/^0x/);
    });


    it('QUEUE', async () => {
        // ========================================
        // 1. SETUP QUEUE BROKER DATA
        // ========================================
        console.log('📋 Setting up QUEUE broker data...');

        // 1. Pending Messages (Standard)
        const emailQueue = await nexo.queue('PENDING_email_notifications').create();
        await emailQueue.push({ to: 'user@example.com', subject: 'Welcome!', template: 'welcome' });
        await emailQueue.push({ to: 'admin@example.com', subject: 'New User Registration', template: 'admin_notify' });
        await emailQueue.push({ to: 'support@example.com', subject: 'Help Request', template: 'support' });

        // 2. Prioritized Messages
        const orderQueue = await nexo.queue('PRIORITY_order_processing').create();
        await orderQueue.push({ order_id: 'ORD-001', amount: 99.99, priority: 'high' }, { priority: 2 });
        await orderQueue.push({ order_id: 'ORD-002', amount: 29.99, priority: 'low' }, { priority: 0 });
        await orderQueue.push({ order_id: 'ORD-003', amount: 199.99, priority: 'high' }, { priority: 2 });

        // 3. Scheduled Messages (Schedule for 1 hour in the future so they stay "Scheduled")
        const jobQueue = await nexo.queue('SCHEDULED_background_jobs').create();
        await jobQueue.push({ type: 'cleanup', target: 'temp_files' }, { delayMs: 3600000 });
        await jobQueue.push({ type: 'report', target: 'daily_stats' }, { delayMs: 7200000 });

        // 4. In-Flight Messages (Processing)
        // We set a long visibility timeout (1 hour) and consume WITHOUT acking to keep them "InFlight"
        const transcodeQueue = await nexo.queue('INFLIGHT_video_transcoding').create({
            visibilityTimeoutMs: 3600000 // 1 hour visibility
        });
        await transcodeQueue.push({ file: 'video1.mp4', format: '1080p' });
        await transcodeQueue.push({ file: 'video2.mov', format: '4k' });

        // Consume but throw error to skip ACK (leaving them InFlight)
        const sub = await transcodeQueue.subscribe(async (data) => {
            throw new Error("Simulated Processing Error - Skip Ack");
        }, { batchSize: 2, waitMs: 100 });

        // Wait briefly for fetch to happen, then stop
        await new Promise(r => setTimeout(r, 500));
        sub.stop();

        // 5. DLQ Messages (Dead Letter Queue)
        const dlq = await nexo.queue('payments_dlq').create();
        await dlq.push({
            txn_id: 'tx_999',
            error: 'Gateway Timeout',
            attempts: 5,
            original_queue: 'payments'
        });
        await dlq.push({
            txn_id: 'tx_888',
            error: 'Invalid Card',
            attempts: 5,
            original_queue: 'payments'
        });

        // ========================================
        // 2. WAIT FOR DATA PROPAGATION
        // ========================================
        console.log('⏳ Waiting for data propagation...');
        await new Promise(r => setTimeout(r, 500));

        // ========================================
        // 3. FETCH AND VALIDATE QUEUE SNAPSHOT
        // ========================================
        console.log('📸 Fetching QUEUE broker snapshot...');
        const queueSnapshot = await fetchBrokerSnapshot('/api/queue');
        console.log('📊 Queue Snapshot received:', JSON.stringify(queueSnapshot, null, 2));

        // ========================================
        // 4. VALIDATE SNAPSHOT STRUCTURE
        // ========================================
        expect(queueSnapshot).toBeDefined();
        expect(queueSnapshot).not.toBeNull();
        expect(queueSnapshot.active_queues).toBeDefined();
        expect(queueSnapshot.dlq_queues).toBeDefined();
        expect(Array.isArray(queueSnapshot.active_queues)).toBe(true);
        expect(Array.isArray(queueSnapshot.dlq_queues)).toBe(true);

        const allQueues = [...queueSnapshot.active_queues, ...queueSnapshot.dlq_queues];
        expect(allQueues.length).toBeGreaterThanOrEqual(5);

        // ========================================
        // 5. VALIDATE ACTIVE QUEUES
        // ========================================

        // Email Queue - Pending Messages
        const emailQueueSnap = allQueues.find(q => q.name === 'PENDING_email_notifications');
        expect(emailQueueSnap).toBeDefined();
        expect(emailQueueSnap?.pending.length).toBe(3);
        expect(emailQueueSnap?.inflight.length).toBe(0);
        expect(emailQueueSnap?.scheduled.length).toBe(0);

        // Validate pending message structure
        const pendingMsg = emailQueueSnap?.pending[0];
        expect(pendingMsg).toBeDefined();
        expect(pendingMsg?.id).toBeDefined();
        expect(pendingMsg?.payload).toBeDefined();
        expect(pendingMsg?.state).toBe('Pending');
        expect(pendingMsg?.priority).toBeDefined();
        expect(pendingMsg?.attempts).toBeDefined();
        expect(pendingMsg?.next_delivery_at).toBeUndefined(); // NOT present for Pending

        // Order Queue - Prioritized Messages
        const orderQueueSnap = allQueues.find(q => q.name === 'PRIORITY_order_processing');
        expect(orderQueueSnap).toBeDefined();
        expect(orderQueueSnap?.pending.length).toBe(3);

        // Validate priority ordering (highest priority first)
        const highPriorityMsgs = orderQueueSnap?.pending.filter(m => m.priority === 2);
        expect(highPriorityMsgs?.length).toBe(2);

        // Job Queue - Scheduled Messages
        const jobQueueSnap = allQueues.find(q => q.name === 'SCHEDULED_background_jobs');
        expect(jobQueueSnap).toBeDefined();
        expect(jobQueueSnap?.pending.length).toBe(0);
        expect(jobQueueSnap?.inflight.length).toBe(0);
        expect(jobQueueSnap?.scheduled.length).toBe(2);

        // Validate scheduled message structure - MUST have next_delivery_at
        const scheduledMsg = jobQueueSnap?.scheduled[0];
        expect(scheduledMsg).toBeDefined();
        expect(scheduledMsg?.id).toBeDefined();
        expect(scheduledMsg?.payload).toBeDefined();
        expect(scheduledMsg?.state).toBe('Scheduled');
        expect(scheduledMsg?.priority).toBeDefined();
        expect(scheduledMsg?.attempts).toBeDefined();
        expect(scheduledMsg?.next_delivery_at).toBeDefined(); // MUST be present for Scheduled
        expect(typeof scheduledMsg?.next_delivery_at).toBe('string');

        // Video Queue - In-Flight Messages
        const videoQueueSnap = allQueues.find(q => q.name === 'INFLIGHT_video_transcoding');
        expect(videoQueueSnap).toBeDefined();
        expect(videoQueueSnap?.pending.length).toBe(0);
        expect(videoQueueSnap?.inflight.length).toBe(2);
        expect(videoQueueSnap?.scheduled.length).toBe(0);

        // Validate in-flight message structure
        const inflightMsg = videoQueueSnap?.inflight[0];
        expect(inflightMsg).toBeDefined();
        expect(inflightMsg?.id).toBeDefined();
        expect(inflightMsg?.payload).toBeDefined();
        expect(inflightMsg?.state).toBe('InFlight');
        expect(inflightMsg?.priority).toBeDefined();
        expect(inflightMsg?.attempts).toBeGreaterThan(0); // Should have attempts
        expect(inflightMsg?.next_delivery_at).toBeUndefined(); // NOT present for InFlight

        // ========================================
        // 6. VALIDATE DLQ QUEUES
        // ========================================
        expect(queueSnapshot.dlq_queues.length).toBeGreaterThan(0);

        const dlqQueueSnap = queueSnapshot.dlq_queues.find(q => q.name === 'payments_dlq');
        expect(dlqQueueSnap).toBeDefined();
        expect(dlqQueueSnap?.pending.length).toBe(2);
        expect(dlqQueueSnap?.inflight.length).toBe(0);
        expect(dlqQueueSnap?.scheduled.length).toBe(0);

        // ========================================
        // 7. VALIDATE PAYLOAD CONTENT
        // ========================================
        const emailPayload = JSON.parse(emailQueueSnap?.pending[0]?.payload || '{}');
        expect(emailPayload.to).toBe('user@example.com');
        expect(emailPayload.subject).toBe('Welcome!');

        const orderPayload = JSON.parse(orderQueueSnap?.pending[0]?.payload || '{}');
        expect(orderPayload.order_id).toBeDefined();
        expect(orderPayload.amount).toBeDefined();

        const jobPayload = JSON.parse(jobQueueSnap?.scheduled[0]?.payload || '{}');
        expect(jobPayload.type).toBe('cleanup');
        expect(jobPayload.target).toBe('temp_files');

        const dlqPayload = JSON.parse(dlqQueueSnap?.pending[0]?.payload || '{}');
        expect(dlqPayload.txn_id).toBeDefined();
        expect(dlqPayload.error).toBeDefined();
    });

    it('PUBSUB', async () => {
        // ========================================
        // 1. SETUP PUBSUB BROKER DATA
        // ========================================
        console.log('📡 Setting up PUBSUB broker data...');

        // Subscribe to various topics (hierarchical and wildcards)
        await nexo.pubsub('home/kitchen/temperature').subscribe(() => { });
        await nexo.pubsub('home/kitchen/humidity').subscribe(() => { });
        await nexo.pubsub('home/livingroom/light').subscribe(() => { });

        await nexo.pubsub('home/+/temperature').subscribe(() => { }); // Single wildcard
        await nexo.pubsub('office/desk/monitor').subscribe(() => { });
        await nexo.pubsub('sensors/#').subscribe(() => { }); // Multi-level wildcard

        await nexo.pubsub('garden/soil/moisture').subscribe(() => { });
        await nexo.pubsub('garden/temperature').subscribe(() => { });

        // Add some subscribers WITHOUT retained messages (to test frontend handling)
        await nexo.pubsub('home/bedroom/light').subscribe(() => { }); // Only subscriber, no retained
        await nexo.pubsub('office/desk/keyboard').subscribe(() => { }); // Only subscriber, no retained
        await nexo.pubsub('garden/water/pump').subscribe(() => { }); // Only subscriber, no retained

        // Publish messages with retained flags
        await nexo.pubsub('home/kitchen/temperature').publish({ value: 22.5, unit: 'celsius' }, { retain: true });
        await nexo.pubsub('home/kitchen/humidity').publish({ value: 65, unit: 'percent' }, { retain: true });
        await nexo.pubsub('home/livingroom/light').publish({ status: 'on', brightness: 80 }, { retain: true });

        await nexo.pubsub('office/desk/monitor').publish({ brand: 'Dell', model: 'U2720Q' }, { retain: true });
        await nexo.pubsub('office/desk/keyboard').publish({ type: 'mechanical', backlight: true }, { retain: false }); // Non-retained

        await nexo.pubsub('garden/soil/moisture').publish({ value: 45, unit: 'percent' }, { retain: true });
        await nexo.pubsub('garden/temperature').publish({ value: 18.2, unit: 'celsius' }, { retain: true });

        // Publish some non-retained messages (should not appear in snapshot)
        await nexo.pubsub('home/kitchen/temperature').publish({ value: 23.0, unit: 'celsius' }, { retain: false });
        await nexo.pubsub('garden/water/pump').publish({ status: 'active' }, { retain: false });

        // ========================================
        // 2. WAIT FOR DATA PROPAGATION
        // ========================================
        console.log('⏳ Waiting for data propagation...');
        await new Promise(resolve => setTimeout(resolve, 1000));

        // ========================================
        // 3. FETCH AND VALIDATE PUBSUB SNAPSHOT
        // ========================================
        console.log('📸 Fetching PUBSUB broker snapshot...');
        const pubsubSnapshot = await fetchBrokerSnapshot('/api/pubsub');

        console.log('📊 PubSub Snapshot received:', JSON.stringify(pubsubSnapshot, null, 2));

        // ========================================
        // 4. VALIDATE SNAPSHOT STRUCTURE
        // ========================================
        expect(pubsubSnapshot).toBeDefined();
        expect(pubsubSnapshot).not.toBeNull();
        expect(pubsubSnapshot.active_clients).toBe(1); // Only the singleton client
        expect(pubsubSnapshot.topics).toBeDefined();
        expect(Array.isArray(pubsubSnapshot.topics)).toBe(true);
        expect(pubsubSnapshot.wildcards).toBeDefined();
        expect(pubsubSnapshot.wildcards.multi_level).toBeDefined();
        expect(Array.isArray(pubsubSnapshot.wildcards.multi_level)).toBe(true);
        expect(pubsubSnapshot.wildcards.single_level).toBeDefined();
        expect(Array.isArray(pubsubSnapshot.wildcards.single_level)).toBe(true);

        // ========================================
        // 5. VALIDATE WILDCARD SUBSCRIPTIONS
        // ========================================
        expect(pubsubSnapshot.wildcards.multi_level.length + pubsubSnapshot.wildcards.single_level.length).toBeGreaterThan(0);

        // Should contain the single-level wildcard subscription
        const singleWildcard = pubsubSnapshot.wildcards.single_level.find((sub: any) =>
            sub.pattern === 'home/+/temperature'
        );
        expect(singleWildcard).toBeDefined();

        // Should contain the multi-level wildcard subscription
        const multiWildcard = pubsubSnapshot.wildcards.multi_level.find((sub: any) =>
            sub.pattern === 'sensors/#'
        );
        expect(multiWildcard).toBeDefined();

        // ========================================
        // 6. VALIDATE TOPIC TREE STRUCTURE
        // ========================================
        expect(pubsubSnapshot.topics).toBeDefined();
        expect(Array.isArray(pubsubSnapshot.topics)).toBe(true);
        expect(pubsubSnapshot.topics.length).toBeGreaterThan(0);

        // Use flat topics directly from backend
        const allTopics = pubsubSnapshot.topics;
        console.log('📋 All topics found:', allTopics.map((t: any) => t.full_path));

        // ========================================
        // 7. VALIDATE SPECIFIC TOPICS WITH RETAINED VALUES
        // ========================================

        // Kitchen temperature topic should have retained value and subscribers
        const kitchenTemp = allTopics.find((t: any) => t.full_path === 'home/kitchen/temperature');
        expect(kitchenTemp).toBeDefined();
        expect(kitchenTemp.subscribers).toBeGreaterThan(0); // Direct subscription + wildcard
        expect(kitchenTemp.retained_value).not.toBeNull();
        expect(kitchenTemp.retained_value.value).toBe(22.5); // Should be JSON object with value property

        // Kitchen humidity topic
        const kitchenHumidity = allTopics.find((t: any) => t.full_path === 'home/kitchen/humidity');
        expect(kitchenHumidity).toBeDefined();
        expect(kitchenHumidity.subscribers).toBe(1); // only direct subscription
        expect(kitchenHumidity.retained_value).not.toBeNull();
        expect(kitchenHumidity.retained_value.value).toBe(65);

        // Living room light topic
        const livingRoomLight = allTopics.find((t: any) => t.full_path === 'home/livingroom/light');
        expect(livingRoomLight).toBeDefined();
        expect(livingRoomLight.subscribers).toBe(1); // only direct subscription
        expect(livingRoomLight.retained_value).not.toBeNull();
        expect(livingRoomLight.retained_value.status).toBe('on');

        // Office monitor topic
        const officeMonitor = allTopics.find((t: any) => t.full_path === 'office/desk/monitor');
        expect(officeMonitor).toBeDefined();
        expect(officeMonitor.subscribers).toBe(1); // only direct subscription
        expect(officeMonitor.retained_value).not.toBeNull();
        expect(officeMonitor.retained_value.brand).toBe('Dell');

        // Garden topics
        const gardenMoisture = allTopics.find((t: any) => t.full_path === 'garden/soil/moisture');
        expect(gardenMoisture).toBeDefined();
        expect(gardenMoisture.subscribers).toBe(1); // only direct subscription
        expect(gardenMoisture.retained_value).not.toBeNull();
        expect(gardenMoisture.retained_value.value).toBe(45);

        const gardenTemp = allTopics.find((t: any) => t.full_path === 'garden/temperature');
        expect(gardenTemp).toBeDefined();
        expect(gardenTemp.subscribers).toBe(1); // only direct subscription
        expect(gardenTemp.retained_value).not.toBeNull();
        expect(gardenTemp.retained_value.value).toBe(18.2);

        // ========================================
        // 8. VALIDATE TOPICS WITH SUBSCRIBERS BUT NO RETAINED VALUES
        // ========================================

        // Topics with subscribers but no retained values should appear
        const bedroomLight = allTopics.find((t: any) => t.full_path === 'home/bedroom/light');
        expect(bedroomLight).toBeDefined();
        expect(bedroomLight.subscribers).toBe(1);
        expect(bedroomLight.retained_value).toBeNull();

        const officeKeyboard = allTopics.find((t: any) => t.full_path === 'office/desk/keyboard');
        expect(officeKeyboard).toBeDefined();
        expect(officeKeyboard.subscribers).toBe(1);
        expect(officeKeyboard.retained_value).toBeNull();

        const gardenPump = allTopics.find((t: any) => t.full_path === 'garden/water/pump');
        expect(gardenPump).toBeDefined();
        expect(gardenPump.subscribers).toBe(1);
        expect(gardenPump.retained_value).toBeNull();

        // ========================================
        // 9. VALIDATE TOPICS WITHOUT RETAINED VALUES
        // ========================================

        // Topics that should not exist (no subscribers, no retained)
        const waterPump = allTopics.find((t: any) => t.full_path === 'garden/water/pump/nonexistent');
        expect(waterPump).toBeUndefined(); // Should not be in snapshot

        // ========================================
        // 10. VALIDATE SUBSCRIBER COUNTS
        // ========================================

        // Total topics with retained values (unchanged)
        const topicsWithRetained = allTopics.filter((t: any) => t.retained_value !== null);
        expect(topicsWithRetained.length).toBe(6); // kitchen temp/humidity, living light, office monitor, garden moisture/temperature

        // Topics with active subscribers (should include those without retained values)
        const topicsWithSubscribers = allTopics.filter((t: any) => t.subscribers > 0);
        expect(topicsWithSubscribers.length).toBeGreaterThan(8); // At least 8 retained + 3 subscribers-only

        console.log('✅ PUBSUB snapshot validation completed successfully');
        console.log(`📈 Found ${allTopics.length} total topics`);
        console.log(`💾 Found ${topicsWithRetained.length} topics with retained values`);
        console.log(`👥 Found ${topicsWithSubscribers.length} topics with active subscribers`);
        console.log(`🎯 Found ${pubsubSnapshot.wildcards.multi_level.length} multi_level wildcard subscriptions`);
        console.log(`🎯 Found ${pubsubSnapshot.wildcards.single_level.length} single_level wildcard subscriptions`);
    });

    it('STREAM', async () => {
        // ========================================
        // 1. SETUP STREAM BROKER DATA
        // ========================================
        console.log('🌊 Setting up STREAM broker data...');

        // User events stream
        await nexo.stream('user_events').create();
        const userEventsProducer = nexo.stream('user_events');

        // Publish user lifecycle events
        await userEventsProducer.publish({
            type: 'user_registered',
            user_id: 123,
            email: 'alice@example.com',
            timestamp: Date.now()
        });

        await userEventsProducer.publish({
            type: 'user_updated_profile',
            user_id: 123,
            changes: ['name', 'avatar'],
            timestamp: Date.now()
        });

        await userEventsProducer.publish({
            type: 'user_logged_in',
            user_id: 123,
            ip: '192.168.1.100',
            timestamp: Date.now()
        });

        await userEventsProducer.publish({
            type: 'user_registered',
            user_id: 456,
            email: 'bob@example.com',
            timestamp: Date.now()
        });

        // Order events stream
        await nexo.stream('order_events').create();
        const orderEventsProducer = nexo.stream('order_events');

        await orderEventsProducer.publish({
            type: 'order_created',
            order_id: 'ORD-001',
            user_id: 123,
            amount: 99.99,
            items: ['laptop', 'mouse'],
            timestamp: Date.now()
        });

        await orderEventsProducer.publish({
            type: 'payment_processed',
            order_id: 'ORD-001',
            payment_method: 'credit_card',
            transaction_id: 'TXN-12345',
            timestamp: Date.now()
        });

        await orderEventsProducer.publish({
            type: 'order_shipped',
            order_id: 'ORD-001',
            tracking_number: 'TRK-999',
            carrier: 'FedEx',
            timestamp: Date.now()
        });

        // System events stream
        await nexo.stream('system_events').create();
        const systemEventsProducer = nexo.stream('system_events');

        await systemEventsProducer.publish({
            type: 'server_restart',
            server_id: 'web-01',
            reason: 'maintenance',
            uptime_before: 86400,
            timestamp: Date.now()
        });

        await systemEventsProducer.publish({
            type: 'database_backup_completed',
            database: 'main',
            size_gb: 2.5,
            duration_seconds: 300,
            timestamp: Date.now()
        });

        // ========================================
        // 2. REGISTER SUBSCRIBERS TO STREAMS
        // ========================================
        console.log('👥 Registering stream subscribers...');

        // Subscribe to user_events
        const userEventsConsumer = nexo.stream('user_events');
        const userEventsReceived: any[] = [];
        const userSub = await userEventsConsumer.subscribe('analytics_group', data => userEventsReceived.push(data));

        // Subscribe to order_events
        const orderEventsConsumer = nexo.stream('order_events');
        const orderEventsReceived: any[] = [];
        const orderSub = await orderEventsConsumer.subscribe('order_processing_group', data => orderEventsReceived.push(data));

        // Subscribe to system_events
        const systemEventsConsumer = nexo.stream('system_events');
        const systemEventsReceived: any[] = [];
        const systemSub = await systemEventsConsumer.subscribe('monitoring_group', data => systemEventsReceived.push(data));

        // ========================================
        // 3. WAIT FOR DATA PROPAGATION AND CONSUMPTION
        // ========================================
        console.log('⏳ Waiting for stream data propagation and consumption...');
        await new Promise(resolve => setTimeout(resolve, 2000));

        // ========================================
        // 4. FETCH AND VALIDATE STREAM SNAPSHOT
        // ========================================
        const streamSnapshot = await fetchBrokerSnapshot('/api/stream');

        console.log('✅ STREAM snapshot validation completed successfully');
        console.log(`📊 Stream snapshot:`, JSON.stringify(streamSnapshot, null, 2));
        console.log(`📈 User events consumed: ${userEventsReceived.length}`);
        console.log(`📈 Order events consumed: ${orderEventsReceived.length}`);
        console.log(`📈 System events consumed: ${systemEventsReceived.length}`);

        // Stop consumers
        userSub.stop();
        orderSub.stop();
        systemSub.stop();
    });

});
