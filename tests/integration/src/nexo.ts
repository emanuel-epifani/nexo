import { NexoClient } from '@nexo/client';

/**
 * 🚀 TOP-LEVEL AWAIT SINGLETON
 * Guaranteed to be connected and validated by Vitest Config.
 */
export const nexo = await NexoClient.connect({ 
  host: process.env.NEXO_HOST!, 
  port: parseInt(process.env.NEXO_PORT!, 10) 
});

/**
 * 📸 Snapshot Helper for Integration Tests
 * Fetches the internal state of the broker via the HTTP Dashboard API.
 * Useful for black-box validation of side effects (e.g. queue size, consumer lag).
 */
export async function fetchSnapshot() {
  const host = process.env.NEXO_HOST || 'localhost';
  // Dashboard is typically on 8080, but we could make it configurable if needed
  const port = 8080; 
  
  try {
    const response = await fetch(`http://${host}:${port}/api/state`);
    if (!response.ok) {
      throw new Error(`Failed to fetch snapshot: ${response.status} ${response.statusText}`);
    }
    return await response.json();
  } catch (err) {
    console.error("Failed to connect to Dashboard API for snapshot:", err);
    throw err;
  }
}
