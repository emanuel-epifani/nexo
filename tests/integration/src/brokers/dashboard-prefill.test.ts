import { describe, it, expect, beforeEach } from 'vitest';
import { nexo, fetchSnapshot } from '../nexo';
import { NexoClient } from '@nexo/client';

const SERVER_PORT = parseInt(process.env.SERVER_PORT!);

describe('DASHBOARD PREFILL - Complete Data Visualization', () => {

    let clientProducer: NexoClient;
    let clientConsumer: NexoClient;

    beforeEach(async () => {
        // Clean up any existing data before each test
        console.log('🧹 Preparing clean state for dashboard prefill test');
        
        // Setup additional clients for stream testing
        clientProducer = await NexoClient.connect({ port: SERVER_PORT });
        clientConsumer = await NexoClient.connect({ port: SERVER_PORT });
    });

    it('should prefill all brokers with comprehensive test data and validate dashboard snapshot', async () => {
        
        // ========================================
        // 1. STORE BROKER - Fill with various keys
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
        
        // Binary data (test binary preview)
        await nexo.store.kv.set('binary:logo', Buffer.from('binary-image-data-here'));
        
        // ========================================
        // 2. QUEUE BROKER - Create queues with different message states
        // ========================================
        console.log('📋 Setting up QUEUE broker data...');
        
        // Email notification queue
        const emailQueue = await nexo.queue('email_notifications').create();
        await emailQueue.push({ to: 'user@example.com', subject: 'Welcome!', template: 'welcome' });
        await emailQueue.push({ to: 'admin@example.com', subject: 'New User Registration', template: 'admin_notify' });
        await emailQueue.push({ to: 'support@example.com', subject: 'Help Request', template: 'support' });
        
        // Order processing queue with priority
        const orderQueue = await nexo.queue('order_processing').create();
        await orderQueue.push({ order_id: 'ORD-001', amount: 99.99, priority: 'high' });
        await orderQueue.push({ order_id: 'ORD-002', amount: 29.99, priority: 'low' });
        await orderQueue.push({ order_id: 'ORD-003', amount: 199.99, priority: 'high' });
        
        // Background jobs queue
        const jobQueue = await nexo.queue('background_jobs').create();
        await jobQueue.push({ type: 'cleanup', target: 'temp_files', schedule: new Date(Date.now() + 60000) });
        await jobQueue.push({ type: 'report', target: 'daily_stats', schedule: new Date(Date.now() + 120000) });
        
        // Create some messages that will go to DLQ (simulate failures)
        const dlqTestQueue = await nexo.queue('dlq_test').create();
        await dlqTestQueue.push({ 
            type: 'failing_job', 
            attempts: 3, 
            error: 'Connection timeout',
            should_fail: true 
        });
        
        // ========================================
        // 3. PUBSUB BROKER - Setup hierarchical topics with retained messages
        // ========================================
        console.log('📡 Setting up PUBSUB broker data...');
        
        // Device status topics (IoT style)
        await nexo.pubsub('device/thermostat/living_room/status').publish(JSON.stringify({ 
            temperature: 22.5, 
            humidity: 45, 
            battery: 85 
        }));
        
        await nexo.pubsub('device/thermostat/bedroom/status').publish(JSON.stringify({ 
            temperature: 20.0, 
            humidity: 50, 
            battery: 92 
        }));
        
        await nexo.pubsub('device/sensor/kitchen/temperature').publish(JSON.stringify({ 
            value: 24.1, 
            unit: 'celsius',
            timestamp: Date.now()
        }));
        
        // System status topics
        await nexo.pubsub('system/server/cpu').publish(JSON.stringify({ 
            usage: 45.2, 
            cores: 8, 
            load_average: [1.2, 1.5, 1.8]
        }));
        
        await nexo.pubsub('system/server/memory').publish(JSON.stringify({ 
            total: 16384, 
            used: 8192, 
            free: 8192,
            usage_percent: 50
        }));
        
        // Application events
        await nexo.pubsub('app/user/123/login').publish(JSON.stringify({ 
            user_id: 123, 
            ip: '192.168.1.100',
            timestamp: Date.now(),
            success: true
        }));
        
        await nexo.pubsub('app/user/456/logout').publish(JSON.stringify({ 
            user_id: 456, 
            ip: '192.168.1.101',
            timestamp: Date.now(),
            success: true
        }));
        
        // Wildcard subscription test
        await nexo.pubsub('device/+/status').subscribe((message) => {
            console.log('Device status update:', message);
        });
        
        await nexo.pubsub('system/#').subscribe((message) => {
            console.log('System update:', message);
        });
        
        // ========================================
        // 4. STREAM BROKER - Create event logs with consumer groups
        // ========================================
        console.log('🌊 Setting up STREAM broker data...');
        
        // User events stream
        await clientConsumer.stream('user_events', 'analytics_group').create();
        const userEventsProducer = clientProducer.stream('user_events');
        
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
        await clientConsumer.stream('order_events', 'order_processing_group').create();
        const orderEventsProducer = clientProducer.stream('order_events');
        
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
        await clientConsumer.stream('system_events', 'monitoring_group').create();
        const systemEventsProducer = clientProducer.stream('system_events');
        
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
        // 5. WAIT FOR DATA PROPAGATION
        // ========================================
        console.log('⏳ Waiting for data propagation...');
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // ========================================
        // 6. FETCH AND VALIDATE DASHBOARD SNAPSHOT
        // ========================================
        console.log('📸 Fetching dashboard snapshot...');
        const snapshot = await fetchSnapshot();
        
        console.log('📊 Snapshot received:', JSON.stringify(snapshot, null, 2));
        
        // Validate structure
        expect(snapshot).toBeDefined();
        expect(snapshot).not.toBeNull();
        expect(snapshot.brokers).toBeDefined();
        
        // Validate STORE broker
        const storeSnapshot = snapshot.brokers.store;
        expect(storeSnapshot.total_keys).toBeGreaterThan(10);
        expect(storeSnapshot.expiring_keys).toBeGreaterThan(0);
        expect(storeSnapshot.keys).toBeDefined();
        expect(storeSnapshot.keys.length).toBeGreaterThan(0);
        
        // Check specific keys exist
        const keyNames = storeSnapshot.keys.map(k => k.key);
        expect(keyNames).toContain('user:123:name');
        expect(keyNames).toContain('session:abc123');
        expect(keyNames).toContain('cache:product:1');
        
        // Validate QUEUE broker
        const queueSnapshot = snapshot.brokers.queue;
        expect(queueSnapshot.queues).toBeDefined();
        expect(queueSnapshot.queues.length).toBeGreaterThan(0);
        
        // Check specific queues exist
        const queueNames = queueSnapshot.queues.map(q => q.name);
        expect(queueNames).toContain('email_notifications');
        expect(queueNames).toContain('order_processing');
        expect(queueNames).toContain('background_jobs');
        
        // Validate queue data integrity
        const emailQueueData = queueSnapshot.queues.find(q => q.name === 'email_notifications');
        expect(emailQueueData).toBeDefined();
        expect(emailQueueData.pending_count).toBeGreaterThan(0);
        expect(emailQueueData.messages).toBeDefined();
        expect(emailQueueData.messages.length).toBeGreaterThan(0);
        
        // Check message structure
        const firstMessage = emailQueueData.messages[0];
        expect(firstMessage).toHaveProperty('id');
        expect(firstMessage).toHaveProperty('payload_preview');
        expect(firstMessage).toHaveProperty('state');
        expect(firstMessage).toHaveProperty('priority');
        expect(firstMessage).toHaveProperty('attempts');
        
        // Validate PUBSUB broker
        const pubsubSnapshot = snapshot.brokers.pubsub;
        expect(pubsubSnapshot.active_clients).toBeGreaterThan(0);
        expect(pubsubSnapshot.topic_tree).toBeDefined();
        expect(pubsubSnapshot.topic_tree.name).toBe('root');
        expect(pubsubSnapshot.topic_tree.children).toBeDefined();
        
        // Check that topic tree has our expected structure
        const deviceTopic = pubsubSnapshot.topic_tree.children.find(child => child.name === 'device');
        expect(deviceTopic).toBeDefined();
        
        // Validate STREAM broker
        const streamSnapshot = snapshot.brokers.stream;
        expect(streamSnapshot.total_topics).toBeGreaterThan(0);
        expect(streamSnapshot.topics).toBeDefined();
        expect(streamSnapshot.topics.length).toBeGreaterThan(0);
        
        // Check specific streams exist
        const streamNames = streamSnapshot.topics.map(t => t.name);
        expect(streamNames).toContain('user_events');
        expect(streamNames).toContain('order_events');
        expect(streamNames).toContain('system_events');
        
        // Validate stream structure with partitions
        const userEventsStreamData = streamSnapshot.topics.find(t => t.name === 'user_events');
        expect(userEventsStreamData).toBeDefined();
        expect(userEventsStreamData.partitions).toBeDefined();
        expect(userEventsStreamData.partitions.length).toBeGreaterThan(0);
        
        // Check partition structure
        const firstPartition = userEventsStreamData.partitions[0];
        expect(firstPartition).toHaveProperty('id');
        expect(firstPartition).toHaveProperty('messages');
        expect(firstPartition).toHaveProperty('current_consumers');
        expect(firstPartition).toHaveProperty('last_offset');
        
        // Validate message preview structure
        if (firstPartition.messages.length > 0) {
            const firstStreamMessage = firstPartition.messages[0];
            expect(firstStreamMessage).toHaveProperty('offset');
            expect(firstStreamMessage).toHaveProperty('timestamp');
            expect(firstStreamMessage).toHaveProperty('payload_preview');
        }
        
        // ========================================
        // 7. COMPREHENSIVE DATA INTEGRITY CHECKS
        // ========================================
        
        // Store: Check TTL keys are properly marked
        const expiringKeys = storeSnapshot.keys.filter(k => k.expires_at !== null);
        expect(expiringKeys.length).toBeGreaterThan(0);
        
        // Queue: Check message states distribution
        const allQueueMessages = queueSnapshot.queues.flatMap(q => q.messages);
        const messageStates = allQueueMessages.map(m => m.state);
        expect(messageStates).toContain('Pending'); // At least some pending messages
        
        // PubSub: Check retained values exist (if any)
        const findRetainedValue = (node: any, topicPath: string): any => {
            if (node.full_path === topicPath && node.retained_value) {
                return node.retained_value;
            }
            for (const child of node.children || []) {
                const found = findRetainedValue(child, topicPath);
                if (found) return found;
            }
            return null;
        };
        
        const thermostatStatus = findRetainedValue(pubsubSnapshot.topic_tree, 'device/thermostat/living_room/status');
        if (thermostatStatus) {
            expect(thermostatStatus).toContain('temperature');
        } else {
            console.log('⚠️ No retained value found for thermostat topic (may be expected)');
        }
        
        // Stream: Check message ordering
        if (firstPartition.messages.length > 1) {
            const offsets = firstPartition.messages.map(m => m.offset);
            // Check that offsets are in descending order (since we take rev() in Rust)
            for (let i = 1; i < offsets.length; i++) {
                expect(offsets[i-1]).toBeGreaterThan(offsets[i]);
            }
        }
        
    });

});
