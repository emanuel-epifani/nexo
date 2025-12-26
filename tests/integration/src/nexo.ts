import { NexoClient } from '@nexo/client';

const host = process.env.NEXO_HOST!;
const port = parseInt(process.env.NEXO_PORT!, 10);

if (!host || isNaN(port)) {
  throw new Error('NEXO_HOST or NEXO_PORT not found in environment variables. Ensure .env is loaded.');
}

/**
 * 🚀 TOP-LEVEL AWAIT SINGLETON
 * This client is guaranteed to be connected when imported.
 */
export const nexo = await NexoClient.connect({ host, port });

