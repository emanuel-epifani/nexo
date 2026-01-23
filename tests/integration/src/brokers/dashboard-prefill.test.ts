import { describe, it, expect, beforeEach } from 'vitest';
import { nexo, fetchBrokerSnapshot } from '../nexo';


describe('DASHBOARD PREFILL - Complete Data Visualization', () => {

    it('STORE', async () => {

        // ========================================
        // 1. SETUP STORE BROKER DATA
        // ========================================
        console.log('📦 Setting up STORE broker data...');

        // Basic keys
        await nexo.store.kv.set('user:123:name', 'Alice Johnson');
        await nexo.store.kv.set('user:123:email', 'alice@example.com');
        await nexo.store.kv.set('user:456:name', 'Bob Smith');
        await nexo.store.kv.set('user:456:email', 'bob@example.com');

        // Session keys with TTL
        await nexo.store.kv.set('session:abc123', JSON.stringify({ user_id: 123, active: true }), 3600);
        await nexo.store.kv.set('session:def456', JSON.stringify({ user_id: 456, active: false }), 7200);
        await nexo.store.kv.set('session:temp789', JSON.stringify({ temp: true }), 10); // Short TTL for testing

        // Cache keys
        await nexo.store.kv.set('cache:product:1', JSON.stringify({ id: 1, name: 'Laptop', price: 999 }));
        await nexo.store.kv.set('cache:product:2', JSON.stringify({ id: 2, name: 'Mouse', price: 29 }));
        await nexo.store.kv.set('cache:stats:daily', JSON.stringify({ users: 1500, orders: 450 }));

        // Configuration keys
        await nexo.store.kv.set('config:app:version', '1.2.3');
        await nexo.store.kv.set('config:app:debug', 'true');
        await nexo.store.kv.set('config:db:max_connections', '100');

        // Test different DataType behaviors
        await nexo.store.kv.set('text:plain', 'This is plain text'); // STRING type
        await nexo.store.kv.set('text:logo', 'binary-image-data-here'); // STRING type (text)
        await nexo.store.kv.set('json:config', JSON.stringify({ debug: true, version: '1.0' })); // JSON type

        // Binary data (RAW type - will be shown as hex)
        await nexo.store.kv.set('binary:pdf', Buffer.from([0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34])); // RAW type
        await nexo.store.kv.set('binary:json', Buffer.from([0x7B, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3A, 0x31, 0x32, 0x33, 0x7D])); // RAW type
        await nexo.store.kv.set('binary:large', Buffer.alloc(1024, 0xFF)); // RAW type
        await nexo.store.kv.set('binary:empty', Buffer.alloc(0)); // Empty data

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

    // TODO: it('PUBSUB ', () => { ... });
    // TODO: it('STREAM', () => { ... });

});
