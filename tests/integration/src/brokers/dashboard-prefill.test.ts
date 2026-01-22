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
        
        // Binary data (test DataType parsing)
        await nexo.store.kv.set('binary:logo', Buffer.from('binary-image-data-here')); // STRING type
        await nexo.store.kv.set('binary:pdf', Buffer.from([0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34])); // RAW type
        await nexo.store.kv.set('binary:json', Buffer.from([0x7B, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3A, 0x31, 0x32, 0x33, 0x7D])); // RAW type (invalid UTF-8)
        await nexo.store.kv.set('binary:large', Buffer.alloc(1024, 0xFF)); // RAW type
        await nexo.store.kv.set('binary:empty', Buffer.alloc(0)); // Empty data
        
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
        
        // Validate STORE broker with new map.keys structure
        const storeSnapshot = snapshot.brokers.store;
        expect(storeSnapshot.total_keys).toBeGreaterThan(15); // More keys now with binary data
        expect(storeSnapshot.expiring_keys).toBeGreaterThan(0);
        expect(storeSnapshot.map).toBeDefined();
        expect(storeSnapshot.map.keys).toBeDefined();
        expect(storeSnapshot.map.keys.length).toBeGreaterThan(0);
        
        // Check specific keys exist including binary ones
        const keyNames = storeSnapshot.map.keys.map(k => k.key);
        expect(keyNames).toContain('user:123:name');
        expect(keyNames).toContain('session:abc123');
        expect(keyNames).toContain('cache:product:1');
        expect(keyNames).toContain('binary:logo');
        expect(keyNames).toContain('binary:pdf');
        expect(keyNames).toContain('binary:json');
        expect(keyNames).toContain('binary:large');
        expect(keyNames).toContain('binary:empty');
        
        // Validate DataType parsing results
        const binaryLogo = storeSnapshot.map.keys.find(k => k.key === 'binary:logo');
        expect(binaryLogo).toBeDefined();
        expect(binaryLogo.value_preview).toBe('binary-image-data-here'); // STRING type → text
        
        const binaryPdf = storeSnapshot.map.keys.find(k => k.key === 'binary:pdf');
        expect(binaryPdf).toBeDefined();
        expect(binaryPdf.value_preview).toBe('0x255044462d312e34'); // RAW type → hex
        
        const binaryJson = storeSnapshot.map.keys.find(k => k.key === 'binary:json');
        expect(binaryJson).toBeDefined();
        expect(binaryJson.value_preview).toBe('0x7b2274657374223a3132337d'); // RAW type → hex
        
        const binaryLarge = storeSnapshot.map.keys.find(k => k.key === 'binary:large');
        expect(binaryLarge).toBeDefined();
        expect(binaryLarge.value_preview).toMatch(/^0x[ff]+$/); // RAW type → hex of 0xFF repeated
        
        const binaryEmpty = storeSnapshot.map.keys.find(k => k.key === 'binary:empty');
        expect(binaryEmpty).toBeDefined();
        expect(binaryEmpty.value_preview).toBe('[Empty]'); // Empty data
        
        // Validate text data is NOT in hex format
        const textKey = storeSnapshot.map.keys.find(k => k.key === 'user:123:name');
        expect(textKey).toBeDefined();
        expect(textKey.value_preview).not.toMatch(/^0x/);
        expect(textKey.value_preview).toBe('Alice Johnson');
        
        // Validate JSON text data
        const jsonKey = storeSnapshot.map.keys.find(k => k.key === 'cache:product:1');
        expect(jsonKey).toBeDefined();
        expect(jsonKey.value_preview).not.toMatch(/^0x/);
        expect(jsonKey.value_preview).toContain('Laptop');
        
        // Validate created_at is removed (should be undefined/null)
        expect(textKey.created_at).toBeUndefined();
        expect(binaryLogo.created_at).toBeUndefined();
        
        // Validate expires_at still exists
        const sessionKey = storeSnapshot.map.keys.find(k => k.key === 'session:abc123');
        expect(sessionKey).toBeDefined();
        expect(sessionKey.expires_at).toBeDefined();
        expect(sessionKey.expires_at).not.toBeNull();
    });

});
