import { NexoClient } from '../src/client';

/**
 * 🚀 TOP-LEVEL AWAIT SINGLETON
 * Guaranteed to be connected and validated by Vitest Config.
 */
export const nexo = await NexoClient.connect({ 
  host: process.env.SERVER_HOST!, 
  port: parseInt(process.env.SERVER_PORT!, 10) 
});

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
