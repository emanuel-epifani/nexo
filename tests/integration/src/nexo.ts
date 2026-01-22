import { NexoClient } from '@nexo/client';

/**
 * 🚀 TOP-LEVEL AWAIT SINGLETON
 * Guaranteed to be connected and validated by Vitest Config.
 */
export const nexo = await NexoClient.connect({ 
  host: process.env.SERVER_HOST!, 
  port: parseInt(process.env.SERVER_PORT!, 10) 
});

/**
 * 📸 Snapshot Helper for Integration Tests
 * Fetches the internal state of the broker via the HTTP Dashboard API.
 * Useful for black-box validation of side effects (e.g. queue size, consumer lag).
 */
export async function fetchSnapshot() {
  const host = process.env.SERVER_HOST;
  const port = process.env.SERVER_DASHBOARD_PORT;
  
  try {
    const [storeRes, queueRes, streamRes, pubsubRes] = await Promise.all([
      fetch(`http://${host}:${port}/api/store`),
      fetch(`http://${host}:${port}/api/queue`),
      fetch(`http://${host}:${port}/api/stream`),
      fetch(`http://${host}:${port}/api/pubsub`),
    ]);

    if (!storeRes.ok || !queueRes.ok || !streamRes.ok || !pubsubRes.ok) {
      throw new Error('Failed to fetch one or more broker snapshots');
    }

    const [store, queue, stream, pubsub] = await Promise.all([
      storeRes.json(),
      queueRes.json(),
      streamRes.json(),
      pubsubRes.json(),
    ]);

    return {
      brokers: {
        store,
        queue,
        stream,
        pubsub,
      },
    };
  } catch (err) {
    console.error("Failed to connect to Dashboard API for snapshot:", err);
    throw err;
  }
}

/**
 * 📸 Single Broker Snapshot Helper
 * Fetches a single broker's snapshot via the HTTP Dashboard API.
 * Useful for testing individual brokers in isolation.
 */
export async function fetchBrokerSnapshot(brokerPath: string) {
  const host = process.env.SERVER_HOST;
  const port = process.env.SERVER_DASHBOARD_PORT;
  
  try {
    const response = await fetch(`http://${host}:${port}${brokerPath}`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch broker snapshot: ${brokerPath}`);
    }
    
    return await response.json();
  } catch (err) {
    console.error(`Failed to connect to Dashboard API for broker ${brokerPath}:`, err);
    throw err;
  }
}
