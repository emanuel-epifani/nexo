#!/usr/bin/env node

const { NexoClient } = require('../sdk/ts/dist');

/**
 * 🚀 Script per popolare i broker NEXO con dati variegati
 * Utilizzato per visualizzare dati completi nel dashboard frontend
 */

async function main() {
    console.log('🔧 Connecting to NEXO server...');
    // console.log("process.env.SERVER_HOST",process.env.SERVER_HOST)
    // console.log("process.env.SERVER_PORT",process.env.SERVER_PORT)

    
    const nexo = await NexoClient.connect({
        host: "127.0.0.1",
        port: parseInt("7654", 10)
    });


    console.log('✅ Connected to NEXO server');

    try {
        await populateStore(nexo);
        await populateQueue(nexo);
        await populatePubSub(nexo);
        await populateStream(nexo);
        
        console.log('🎉 All brokers populated successfully!');
        console.log('🌐 Open the dashboard to see the populated data');
        
    } catch (error) {
        console.error('❌ Error populating brokers:', error);
        process.exit(1);
    } finally {
        await nexo.disconnect();
    }
}

async function populateStore(nexo) {
    console.log('📦 Populating STORE broker...');
    
    // Basic user data
    for (let i = 0; i < 10000; i++) {
        await nexo.store.map.set(`user:${i}:name`, `User ${i}`);
        await nexo.store.map.set(`user:${i}:email`, `user${i}@example.com`);
        await nexo.store.map.set(`user:${i}:profile`, JSON.stringify({
            id: i,
            name: `User ${i}`,
            email: `user${i}@example.com`,
            created: new Date().toISOString()
        }));
    }

    // Session keys with different TTLs
    await nexo.store.map.set('session:admin', { user_id: 1, role: 'admin', active: true }, { ttl: 3600 });
    await nexo.store.map.set('session:user:123', { user_id: 123, role: 'user', active: true }, { ttl: 1800 });
    await nexo.store.map.set('session:guest', { user_id: null, role: 'guest', active: false }, { ttl: 300 });
    await nexo.store.map.set('session:temp', { temp: true, expires_soon: true }, { ttl: 10 });

    // Cache data
    await nexo.store.map.set('cache:product:featured', JSON.stringify({
        products: [
            { id: 1, name: 'Laptop Pro', price: 1299.99, rating: 4.8 },
            { id: 2, name: 'Wireless Mouse', price: 49.99, rating: 4.5 },
            { id: 3, name: 'Mechanical Keyboard', price: 159.99, rating: 4.9 }
        ]
    }));

    await nexo.store.map.set('cache:stats:daily', JSON.stringify({
        users: 1523,
        orders: 447,
        revenue: 45678.90,
        timestamp: Date.now()
    }));

    await nexo.store.map.set('cache:config:app', JSON.stringify({
        version: '2.1.0',
        debug: false,
        maintenance: false,
        features: {
            dark_mode: true,
            notifications: true,
            analytics: false
        }
    }));

    // Configuration
    await nexo.store.map.set('config:database', JSON.stringify({
        host: 'localhost',
        port: 5432,
        name: 'nexo_prod',
        pool_size: 20
    }));

    await nexo.store.map.set('config:redis', JSON.stringify({
        host: 'localhost',
        port: 6379,
        db: 0,
        ttl: 3600
    }));

    // Text data
    await nexo.store.map.set('text:welcome', 'Welcome to NEXO Dashboard!');
    await nexo.store.map.set('text:footer', '© 2024 NEXO - High Performance Broker');
    await nexo.store.map.set('text:logo:svg', '<svg>...</svg>'); // SVG logo data

    // JSON configuration
    await nexo.store.map.set('json:theme', JSON.stringify({
        primary: '#3b82f6',
        secondary: '#64748b',
        accent: '#f59e0b',
        mode: 'dark'
    }));

    // Binary data
    await nexo.store.map.set('binary:pdf:sample', Buffer.from([0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34]));
    await nexo.store.map.set('binary:image:header', Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]));
    await nexo.store.map.set('binary:large:dataset', Buffer.alloc(2048, 0xAB));
    await nexo.store.map.set('binary:empty', Buffer.alloc(0));

    console.log('✅ STORE broker populated');
}

async function populateQueue(nexo) {
    console.log('📋 Populating QUEUE broker...');

    // Email notifications queue
    const emailQueue = await nexo.queue('email_notifications').create();
    await emailQueue.push({ 
        to: 'welcome@example.com', 
        subject: 'Welcome to NEXO!', 
        template: 'welcome',
        priority: 'high'
    }, { priority: 2 });
    
    await emailQueue.push({ 
        to: 'newsletter@example.com', 
        subject: 'Weekly Newsletter', 
        template: 'newsletter',
        priority: 'medium'
    }, { priority: 1 });
    
    await emailQueue.push({ 
        to: 'alert@example.com', 
        subject: 'System Alert', 
        template: 'alert',
        priority: 'low'
    }, { priority: 0 });

    // Order processing queue
    const orderQueue = await nexo.queue('order_processing').create();
    await orderQueue.push({ 
        order_id: 'ORD-2024-001', 
        user_id: 123, 
        amount: 299.99,
        items: ['laptop', 'mouse'],
        priority: 'urgent'
    }, { priority: 2 });
    
    await orderQueue.push({ 
        order_id: 'ORD-2024-002', 
        user_id: 456, 
        amount: 89.99,
        items: ['keyboard'],
        priority: 'normal'
    }, { priority: 1 });

    // Background jobs queue (scheduled)
    const jobQueue = await nexo.queue('background_jobs').create();
    await jobQueue.push({ 
        type: 'cleanup', 
        target: 'temp_files', 
        schedule: 'daily'
    }, { delayMs: 3600000 }); // 1 hour delay
    
    await jobQueue.push({ 
        type: 'backup', 
        target: 'database', 
        schedule: 'weekly'
    }, { delayMs: 7200000 }); // 2 hours delay

    // Video processing queue (in-flight simulation)
    const videoQueue = await nexo.queue('video_processing').create({
        visibilityTimeoutMs: 3600000 // 1 hour
    });
    
    await videoQueue.push({ 
        file: 'video1.mp4', 
        format: '1080p', 
        user_id: 123
    });
    
    await videoQueue.push({ 
        file: 'presentation.pptx', 
        format: '720p', 
        user_id: 456
    });

    // Simulate in-flight by consuming without ack
    const videoSub = await videoQueue.subscribe(async () => {
        throw new Error("Simulated processing - keep in-flight");
    }, { batchSize: 2, waitMs: 100 });
    
    await new Promise(r => setTimeout(r, 500));
    videoSub.stop();

    // Dead Letter Queue
    const dlq = await nexo.queue('payments_dlq').create();
    await dlq.push({
        transaction_id: 'txn_failed_001',
        error: 'Payment gateway timeout',
        attempts: 5,
        original_queue: 'payments',
        amount: 99.99,
        timestamp: Date.now()
    });
    
    await dlq.push({
        transaction_id: 'txn_invalid_002',
        error: 'Invalid credit card',
        attempts: 3,
        original_queue: 'payments',
        amount: 199.99,
        timestamp: Date.now()
    });

    console.log('✅ QUEUE broker populated');
}

async function populatePubSub(nexo) {
    console.log('📡 Populating PUBSUB broker...');

    // Home automation topics
    await nexo.pubsub('home/kitchen/temperature').subscribe(() => {});
    await nexo.pubsub('home/kitchen/humidity').subscribe(() => {});
    await nexo.pubsub('home/kitchen/lights').subscribe(() => {});
    await nexo.pubsub('home/livingroom/temperature').subscribe(() => {});
    await nexo.pubsub('home/livingroom/lights').subscribe(() => {});
    await nexo.pubsub('home/bedroom/lights').subscribe(() => {});

    // Wildcard subscriptions
    await nexo.pubsub('home/+/temperature').subscribe(() => {}); // Single wildcard
    await nexo.pubsub('sensors/#').subscribe(() => {}); // Multi-level wildcard

    // Office topics
    await nexo.pubsub('office/desk/monitor').subscribe(() => {});
    await nexo.pubsub('office/desk/keyboard').subscribe(() => {});
    await nexo.pubsub('office/meeting_room/projector').subscribe(() => {});

    // Garden topics
    await nexo.pubsub('garden/soil/moisture').subscribe(() => {});
    await nexo.pubsub('garden/soil/temperature').subscribe(() => {});
    await nexo.pubsub('garden/water/pump').subscribe(() => {});
    await nexo.pubsub('garden/lights').subscribe(() => {});

    // Publish retained messages
    await nexo.pubsub('home/kitchen/temperature').publish({ 
        value: 22.5, 
        unit: 'celsius', 
        sensor: 'DHT22',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('home/kitchen/humidity').publish({ 
        value: 65, 
        unit: 'percent',
        sensor: 'DHT22',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('home/kitchen/lights').publish({ 
        status: 'on', 
        brightness: 80,
        color: 'warm_white',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('home/livingroom/temperature').publish({ 
        value: 21.0, 
        unit: 'celsius',
        sensor: 'BME280',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('home/livingroom/lights').publish({ 
        status: 'on', 
        brightness: 60,
        color: 'cool_white',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('office/desk/monitor').publish({ 
        brand: 'Dell', 
        model: 'U2720Q',
        resolution: '4K',
        brightness: 75,
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('office/desk/keyboard').publish({ 
        brand: 'Keychron', 
        model: 'K2',
        type: 'mechanical',
        backlight: true,
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('garden/soil/moisture').publish({ 
        value: 45, 
        unit: 'percent',
        sensor: 'Capacitive',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('garden/soil/temperature').publish({ 
        value: 18.2, 
        unit: 'celsius',
        sensor: 'DS18B20',
        timestamp: Date.now()
    }, { retain: true });

    await nexo.pubsub('garden/lights').publish({ 
        status: 'on', 
        brightness: 40,
        color: 'RGB',
        mode: 'sunset',
        timestamp: Date.now()
    }, { retain: true });

    // Publish some non-retained messages (won't appear in dashboard)
    await nexo.pubsub('home/kitchen/temperature').publish({ 
        value: 23.0, 
        unit: 'celsius' 
    }, { retain: false });

    console.log('✅ PUBSUB broker populated');
}

async function populateStream(nexo) {
    console.log('🌊 Populating STREAM broker...');

    // User events stream
    await nexo.stream('user_events').create();
    const userEvents = nexo.stream('user_events');

    await userEvents.publish({
        type: 'user_registered',
        user_id: 123,
        email: 'alice@example.com',
        name: 'Alice Johnson',
        timestamp: Date.now()
    });

    await userEvents.publish({
        type: 'user_updated_profile',
        user_id: 123,
        changes: ['name', 'avatar', 'bio'],
        timestamp: Date.now()
    });

    await userEvents.publish({
        type: 'user_logged_in',
        user_id: 123,
        ip: '192.168.1.100',
        user_agent: 'Mozilla/5.0...',
        timestamp: Date.now()
    });

    await userEvents.publish({
        type: 'user_registered',
        user_id: 456,
        email: 'bob@example.com',
        name: 'Bob Smith',
        timestamp: Date.now()
    });

    // Order events stream
    await nexo.stream('order_events').create();
    const orderEvents = nexo.stream('order_events');

    await orderEvents.publish({
        type: 'order_created',
        order_id: 'ORD-2024-001',
        user_id: 123,
        amount: 299.99,
        currency: 'USD',
        items: [
            { id: 1, name: 'Laptop Pro', price: 249.99, quantity: 1 },
            { id: 2, name: 'Wireless Mouse', price: 49.99, quantity: 1 }
        ],
        timestamp: Date.now()
    });

    await orderEvents.publish({
        type: 'payment_processed',
        order_id: 'ORD-2024-001',
        payment_method: 'credit_card',
        transaction_id: 'TXN-123456',
        gateway: 'stripe',
        timestamp: Date.now()
    });

    await orderEvents.publish({
        type: 'order_shipped',
        order_id: 'ORD-2024-001',
        tracking_number: 'TRK-999-ABC',
        carrier: 'FedEx',
        estimated_delivery: '2024-02-15',
        timestamp: Date.now()
    });

    // System events stream
    await nexo.stream('system_events').create();
    const systemEvents = nexo.stream('system_events');

    await systemEvents.publish({
        type: 'server_restart',
        server_id: 'web-01',
        reason: 'maintenance',
        uptime_before: 86400,
        version: '2.1.0',
        timestamp: Date.now()
    });

    await systemEvents.publish({
        type: 'database_backup_completed',
        database: 'nexo_prod',
        size_gb: 2.5,
        duration_seconds: 300,
        backup_file: 'backup_20240212.sql.gz',
        timestamp: Date.now()
    });

    await systemEvents.publish({
        type: 'high_memory_usage',
        server_id: 'web-02',
        memory_usage_percent: 85,
        threshold_percent: 80,
        timestamp: Date.now()
    });

    // Create consumer groups
    const userConsumer = nexo.stream('user_events');
    const userSub = await userConsumer.subscribe('analytics_group', data => {
        console.log('User event consumed:', data.type);
    });

    const orderConsumer = nexo.stream('order_events');
    const orderSub = await orderConsumer.subscribe('order_processing_group', data => {
        console.log('Order event consumed:', data.type);
    });

    const systemConsumer = nexo.stream('system_events');
    const systemSub = await systemConsumer.subscribe('monitoring_group', data => {
        console.log('System event consumed:', data.type);
    });

    // Wait for consumption
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Stop consumers
    userSub.stop();
    orderSub.stop();
    systemSub.stop();

    console.log('✅ STREAM broker populated');
}

// Run the script
// if (require.main === module) {
//     main().catch(console.error);
// }

main()
