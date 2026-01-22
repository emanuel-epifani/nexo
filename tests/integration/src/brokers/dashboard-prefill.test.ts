import { describe, it, expect, beforeEach } from 'vitest';
import { nexo, fetchBrokerSnapshot } from '../nexo';


describe('DASHBOARD PREFILL - Complete Data Visualization', () => {

    describe('STORE', () => {

        it('should prefill store broker with comprehensive test data and validate snapshot', async () => {

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
            expect(storeSnapshot.total_keys).toBeGreaterThan(15);
            expect(storeSnapshot.expiring_keys).toBeGreaterThan(0);
            expect(storeSnapshot.map).toBeDefined();
            expect(storeSnapshot.map.keys).toBeDefined();
            expect(storeSnapshot.map.keys.length).toBeGreaterThan(0);

            // Check specific keys exist
            const keyNames = storeSnapshot.map.keys.map(k => k.key);
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
            const textPlain = storeSnapshot.map.keys.find(k => k.key === 'text:plain');
            expect(textPlain).toBeDefined();
            expect(textPlain.value_preview).toBe('This is plain text');
            expect(textPlain.value_preview).not.toMatch(/^0x/);

            const textLogo = storeSnapshot.map.keys.find(k => k.key === 'text:logo');
            expect(textLogo).toBeDefined();
            expect(textLogo.value_preview).toBe('binary-image-data-here'); // STRING type → text
            expect(textLogo.value_preview).not.toMatch(/^0x/);

            // JSON type should be readable text
            const jsonConfig = storeSnapshot.map.keys.find(k => k.key === 'json:config');
            expect(jsonConfig).toBeDefined();
            expect(jsonConfig.value_preview).toContain('debug');
            expect(jsonConfig.value_preview).toContain('true');
            expect(jsonConfig.value_preview).not.toMatch(/^0x/);

            // RAW type should be hex
            const binaryPdf = storeSnapshot.map.keys.find(k => k.key === 'binary:pdf');
            expect(binaryPdf).toBeDefined();
            expect(binaryPdf.value_preview).toBe('0x255044462d312e34'); // RAW type → hex
            expect(binaryPdf.value_preview).toMatch(/^0x/);

            const binaryJson = storeSnapshot.map.keys.find(k => k.key === 'binary:json');
            expect(binaryJson).toBeDefined();
            expect(binaryJson.value_preview).toBe('0x7b2274657374223a3132337d'); // RAW type → hex
            expect(binaryJson.value_preview).toMatch(/^0x/);

            const binaryLarge = storeSnapshot.map.keys.find(k => k.key === 'binary:large');
            expect(binaryLarge).toBeDefined();
            expect(binaryLarge.value_preview).toMatch(/^0x[ff]+$/); // RAW type → hex of 0xFF repeated
            expect(binaryLarge.value_preview).toMatch(/^0x/);

            const binaryEmpty = storeSnapshot.map.keys.find(k => k.key === 'binary:empty');
            expect(binaryEmpty).toBeDefined();
            expect(binaryEmpty.value_preview).toBe('0x'); // Empty data

            // ========================================
            // 5. VALIDATE STRUCTURE CHANGES
            // ========================================

            // Validate created_at is removed
            const userKey = storeSnapshot.map.keys.find(k => k.key === 'user:123:name');
            expect(userKey.created_at).toBeUndefined();
            expect(textLogo.created_at).toBeUndefined();

            // Validate expires_at still exists
            const sessionKey = storeSnapshot.map.keys.find(k => k.key === 'session:abc123');
            expect(sessionKey).toBeDefined();
            expect(sessionKey.expires_at).toBeDefined();
            expect(sessionKey.expires_at).not.toBeNull();

            // Validate regular text data
            const userName = storeSnapshot.map.keys.find(k => k.key === 'user:123:name');
            expect(userName).toBeDefined();
            expect(userName.value_preview).toBe('Alice Johnson');
            expect(userName.value_preview).not.toMatch(/^0x/);

            // Validate JSON cache data
            const cacheKey = storeSnapshot.map.keys.find(k => k.key === 'cache:product:1');
            expect(cacheKey).toBeDefined();
            expect(cacheKey.value_preview).toContain('Laptop');
            expect(cacheKey.value_preview).not.toMatch(/^0x/);
        });

        it('',async() => {
            // for(let i = 0; i < 1000000; i++) {
            for(let i = 0; i < 100; i++) {
                await nexo.store.kv.set(`user:${i}`, `Alice Johnson ${i}`);
            }
        })
    });

    // TODO: Add other brokers tests here
    // describe('QUEUE BROKER', () => { ... });
    // describe('PUBSUB BROKER', () => { ... });
    // describe('STREAM BROKER', () => { ... });

});
