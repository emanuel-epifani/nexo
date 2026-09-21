import { describe, expect, it } from 'vitest';
import { randomUUID } from 'crypto';
import { EventEmitter } from 'events';
import { NexoClient } from '../../src/client';
import { NexoPubSub } from '../../src/brokers/pubsub';
import { PubSubOpcode } from '../../src/protocol/generated';
import { Subscription } from '../../src/subscription';
import { waitFor } from '../utils/wait-for';

describe('PUBSUB REVIEW PROBES', () => {
  it('delivers one retained replay to each overlapping subscription', async () => {
    const client = await NexoClient.connect();
    const base = `review-overlap-${randomUUID()}`;
    const topic = client.pubsub.topic<string>(`${base}/value`);
    const first: string[] = [];
    const second: string[] = [];
    let firstSub: Subscription | undefined;
    let secondSub: Subscription | undefined;
    try {
      await topic.publish('retained', { retain: true });
      firstSub = await client.pubsub.pattern<string>(`${base}/#`).subscribe(value => first.push(value));
      await waitFor(() => expect(first.length).toBe(1));
      secondSub = await client.pubsub.pattern<string>(`${base}/+`).subscribe(value => second.push(value));
      await waitFor(() => expect(second.length).toBeGreaterThanOrEqual(1));
      await new Promise(resolve => setTimeout(resolve, 200));
      expect(first).toEqual(['retained']);
      expect(second).toEqual(['retained']);
    } finally {
      await secondSub?.stop().catch(() => undefined);
      await firstSub?.stop().catch(() => undefined);
      await topic.clearRetained().catch(() => undefined);
      client.disconnect();
    }
  });

  it('allows a callback to await its own subscription stop', async () => {
    const client = await NexoClient.connect();
    const topic = client.pubsub.topic<string>(`review-self-stop-${randomUUID()}`);
    let subscription!: Subscription;
    let stopPromise!: Promise<void>;
    let markEntered!: () => void;
    const entered = new Promise<void>(resolve => { markEntered = resolve; });
    try {
      subscription = await topic.subscribe(async () => {
        markEntered();
        stopPromise = subscription.stop();
        await stopPromise;
      });
      await topic.publish('message');
      await entered;
      const completed = await Promise.race([
        stopPromise.then(() => true),
        new Promise<boolean>(resolve => setTimeout(() => resolve(false), 200)),
      ]);
      expect(completed).toBe(true);
    } finally {
      client.disconnect();
    }
  });

  it('closes active subscriptions on explicit client disconnect', async () => {
    const client = await NexoClient.connect();
    const subscription = await client.pubsub
      .topic(`review-disconnect-${randomUUID()}`)
      .subscribe(() => undefined);
    client.disconnect();
    const closed = await Promise.race([
      subscription.closed.then(() => true),
      new Promise<boolean>(resolve => setTimeout(() => resolve(false), 200)),
    ]);
    const active = subscription.active;
    await subscription.stop().catch(() => undefined);
    expect(active).toBe(false);
    expect(closed).toBe(true);
  });

  it('stops the final listener cleanly while the socket is disconnected', async () => {
    const client = await NexoClient.connect();
    const subscription = await client.pubsub
      .topic(`review-stop-disconnected-${randomUUID()}`)
      .subscribe(() => undefined);
    const connection = (client as any).conn;
    connection.socket.destroy();
    await waitFor(() => expect(connection.isConnected).toBe(false));
    let stopError: unknown;
    try {
      await subscription.stop();
    } catch (error) {
      stopError = error;
    } finally {
      client.disconnect();
    }
    expect(stopError).toBeUndefined();
  });

  it('does not leave an orphan server subscription after an ambiguous SUB timeout', async () => {
    class FakeConnection extends EventEmitter {
      onPush?: (topic: string, data: unknown) => void;
      isConnected = true;
      serverSubscribed = false;

      async send(opcode: number): Promise<void> {
        if (opcode === PubSubOpcode.SUB) {
          this.serverSubscribed = true;
          throw new Error('timeout after server applied SUB');
        }
        if (opcode === PubSubOpcode.UNSUB) this.serverSubscribed = false;
      }
    }
    const connection = new FakeConnection();
    const broker = new NexoPubSub(connection as any, { info() {}, error() {} } as any);
    await expect(broker.topic('review-timeout').subscribe(() => undefined)).rejects.toThrow();
    expect(connection.serverSubscribed).toBe(false);
  });

  it('does not leave an active handle in silent limbo after resubscribe failure', async () => {
    class FakeConnection extends EventEmitter {
      onPush?: (topic: string, data: unknown) => void;
      isConnected = true;
      fail = false;

      async send(opcode: number): Promise<void> {
        if (this.fail && opcode === PubSubOpcode.SUB) throw new Error('transient restore failure');
      }
    }
    const connection = new FakeConnection();
    const broker = new NexoPubSub(connection as any, { info() {}, error() {} } as any);
    const subscription = await broker.topic('review-restore').subscribe(() => undefined);
    connection.fail = true;
    connection.emit('reconnect');
    await new Promise(resolve => setTimeout(resolve, 100));
    const entry = (broker as any).entries.get('review-restore');
    const silentlyInactive = subscription.active && !entry.wireSubscribed && !subscription.error;
    connection.fail = false;
    await subscription.stop();
    expect(silentlyInactive).toBe(false);
  });
});
