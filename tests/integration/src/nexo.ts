import { NexoClient } from '@nexo/client';

/**
 * 🚀 TOP-LEVEL AWAIT SINGLETON
 * Guaranteed to be connected and validated by Vitest Config.
 */
export const nexo = await NexoClient.connect({ 
  host: process.env.NEXO_HOST!, 
  port: parseInt(process.env.NEXO_PORT!, 10) 
});
