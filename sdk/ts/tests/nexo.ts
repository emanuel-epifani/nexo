import { NexoClient } from '../src/client';
import { QueueCreateOptions } from '../src/brokers/queue';
import { StreamCreateOptions } from '../src/brokers/stream';

export const nexo: NexoClient = await NexoClient.connect();

export async function createQueue<T = any>(name: string, options: QueueCreateOptions = {}) {
   await nexo.queue.create(name, options);
   return nexo.queue.get<T>(name);
}

export async function createStream<T = any>(name: string, options: StreamCreateOptions = {}) {
   await nexo.stream.create(name, options);
   return nexo.stream.get<T>(name);
}
