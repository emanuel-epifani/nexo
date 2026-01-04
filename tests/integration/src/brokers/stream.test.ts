import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';

describe('Stream Broker', () => {

    it('Should create a topic explicitly', async () => {
        const topicName = 'orders-v1';
        
        // 1. Create Handle
        const stream = nexo.stream(topicName, 'test-group');
        
        // 2. Create Topic on Server
        await stream.create({ partitions: 4 });
        
        // No error thrown means success
        expect(true).toBe(true);
    });

    it('Should publish and consume messages (FIFO within partition)', async () => {
        const topicName = 'orders-v1';
        const streamPub = nexo.stream(topicName); // Producer only
        const streamSub = nexo.stream(topicName, 'billing-service'); // Consumer

        const messages = [
            { id: 1, val: 'A' },
            { id: 2, val: 'B' },
            { id: 3, val: 'C' }
        ];

        // 1. Subscribe
        const received: any[] = [];
        await streamSub.subscribe((data) => {
            received.push(data);
        });

        // 2. Publish (using same key to ensure same partition -> FIFO order)
        for (const msg of messages) {
            await streamPub.publish(msg, { key: 'user-1' });
        }

        // 3. Wait
        await new Promise(r => setTimeout(r, 500));

        // 4. Verify
        expect(received).toHaveLength(3);
        expect(received).toEqual(messages);
    });

    it('Should distribute messages across partitions (Round Robin or Key)', async () => {
        // We can't easily verify which partition received it from SDK, 
        // but we can verify that all messages arrive.
        const topicName = 'logs-v1';
        const stream = await nexo.stream(topicName, 'logger').create({partitions: 8});
        
        const TOTAL = 50;
        let receivedCount = 0;
        
        await stream.subscribe(() => {
            receivedCount++;
        });

        const promises = [];
        for(let i=0; i<TOTAL; i++) {
            // Random keys -> Random partitions
            promises.push(stream.publish({ i }, { key: `k-${i}` }));
        }
        await Promise.all(promises);

        await new Promise(r => setTimeout(r, 1000));
        expect(receivedCount).toBe(TOTAL);
    });

});

