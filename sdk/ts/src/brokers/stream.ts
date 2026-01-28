import { NexoConnection } from '../connection';
import { FrameCodec, Cursor } from '../codec';
import { logger } from '../utils/logger';

export enum StreamOpcode {
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_COMMIT = 0x34,
  S_EXISTS = 0x35,
}

export interface StreamCreateOptions {
  partitions?: number;
}

export interface StreamPublishOptions {
  key?: string;
}

export const StreamCommands = {
  create: (conn: NexoConnection, name: string, options: StreamCreateOptions) =>
    conn.send(
      StreamOpcode.S_CREATE,
      FrameCodec.string(name),
      FrameCodec.string(JSON.stringify(options || {}))
    ),

  exists: async (conn: NexoConnection, name: string) => {
    try {
      const res = await conn.send(StreamOpcode.S_EXISTS, FrameCodec.string(name));
      return res.status === 0x00;
    } catch {
      return false;
    }
  },

  publish: (conn: NexoConnection, name: string, data: any, options: StreamPublishOptions) =>
    conn.send(
      StreamOpcode.S_PUB,
      FrameCodec.string(name),
      FrameCodec.string(JSON.stringify(options || {})),
      FrameCodec.any(data)
    ),

  join: async (conn: NexoConnection, stream: string, group: string) => {
    const res = await conn.send(StreamOpcode.S_JOIN, FrameCodec.string(group), FrameCodec.string(stream));
    const generationId = res.cursor.readU64();
    const count = res.cursor.readU32();
    const partitions: { id: number, offset: bigint }[] = [];
    for (let i = 0; i < count; i++) {
      partitions.push({
        id: res.cursor.readU32(),
        offset: res.cursor.readU64()
      });
    }
    return { generationId, partitions };
  },

  fetch: async <T>(
    conn: NexoConnection,
    stream: string,
    group: string,
    partition: number,
    offset: bigint,
    generationId: bigint,
    batchSize: number
  ): Promise<{ offset: bigint, data: T }[]> => {
    const res = await conn.send(
      StreamOpcode.S_FETCH,
      FrameCodec.u64(generationId),
      FrameCodec.string(stream),
      FrameCodec.string(group),
      FrameCodec.u32(partition),
      FrameCodec.u64(offset),
      FrameCodec.u32(batchSize)
    );

    const count = res.cursor.readU32();
    const messages: { offset: bigint; data: T }[] = [];
    for (let i = 0; i < count; i++) {
      const msgOffset = res.cursor.readU64();
      res.cursor.readU64(); // skip timestamp
      const payloadLen = res.cursor.readU32();
      const payloadBuf = res.cursor.readBuffer(payloadLen);
      const data = FrameCodec.decodeAny(new Cursor(payloadBuf));
      messages.push({ offset: msgOffset, data });
    }
    return messages;
  },

  commit: (conn: NexoConnection, stream: string, group: string, partition: number, offset: bigint, generationId: bigint) =>
    conn.send(
      StreamOpcode.S_COMMIT,
      FrameCodec.u64(generationId),
      FrameCodec.string(group),
      FrameCodec.string(stream),
      FrameCodec.u32(partition),
      FrameCodec.u64(offset)
    ),
};

export interface StreamSubscribeOptions {
  batchSize?: number; // Default 100
}

class StreamSubscription<T> {
  private active = false;
  private generationId = BigInt(0);
  private assignedPartitions: number[] = [];
  private offsets = new Map<number, bigint>();

  constructor(
    private conn: NexoConnection,
    private streamName: string,
    private group: string
  ) { }

  async start(
    callback: (data: T) => Promise<any> | any,
    options: StreamSubscribeOptions
  ): Promise<void> {
    this.active = true;
    const batchSize = options.batchSize ?? 100;

    // Run loop in background (fire & forget promise)
    this.runConsumerLoop(callback, batchSize).catch(err => {
      logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.active = false;
    });
  }

  stop() {
    this.active = false;
  }

  private async runConsumerLoop(callback: (data: T) => Promise<any> | any, batchSize: number) {
    while (this.conn.isConnected && this.active) {
      try {
        await this.joinGroup();
        await this.pollLoop(callback, batchSize);
      } catch (e: any) {
        if (!this.active || !this.conn.isConnected) break;

        if (this.isRebalanceError(e)) {
          logger.warn(`[${this.streamName}:${this.group}] Rebalance. Re-joining...`);
          await this.backoff(200);
          continue;
        }

        logger.error(`[${this.streamName}:${this.group}] Error. Retrying in 1s...`, e);
        await this.backoff(1000);
      }
    }
  }

  private async joinGroup(): Promise<void> {
    const { generationId, partitions } = await StreamCommands.join(this.conn, this.streamName, this.group);

    this.generationId = generationId;
    this.assignedPartitions = [];
    this.offsets.clear();

    for (const p of partitions) {
      this.assignedPartitions.push(p.id);
      this.offsets.set(p.id, p.offset);
    }

    logger.debug(`[${this.streamName}:${this.group}] Joined Gen ${this.generationId}. Partitions: ${this.assignedPartitions}`);
  }

  private async pollLoop(callback: (data: T) => Promise<any> | any, batchSize: number): Promise<void> {
    while (this.active && this.conn.isConnected) {
      if (this.assignedPartitions.length === 0) {
        await this.backoff(100);
        continue;
      }

      for (const partition of this.assignedPartitions) {
        if (!this.active) return;
        await this.fetchAndProcess(partition, batchSize, callback);
      }

      await this.backoff(0); // Yield to event loop
    }
  }

  private async fetchAndProcess(
    partition: number,
    batchSize: number,
    callback: (data: T) => Promise<any> | any
  ): Promise<void> {
    const currentOffset = this.offsets.get(partition) ?? BigInt(0);

    try {
      const messages = await StreamCommands.fetch<T>(
        this.conn,
        this.streamName,
        this.group,
        partition,
        currentOffset,
        this.generationId,
        batchSize
      );

      if (messages.length === 0) return;

      let lastProcessedOffset = currentOffset;
      let shouldCommit = true;

      for (const msg of messages) {
        try {
          await callback(msg.data);
          lastProcessedOffset = msg.offset;
        } catch (e: any) {
          if (this.isRebalanceError(e)) throw e;
          logger.error(`Error processing P${partition}:${msg.offset}. Stopping batch.`);
          shouldCommit = false;
          break;
        }
      }

      if (shouldCommit && this.conn.isConnected) {
        await this.commitOffset(partition, lastProcessedOffset + BigInt(1));
      }
    } catch (e: any) {
      if (this.isRebalanceError(e)) throw e;
      throw e;
    }
  }

  private async commitOffset(partition: number, nextOffset: bigint): Promise<void> {
    this.offsets.set(partition, nextOffset);
    try {
      await StreamCommands.commit(this.conn, this.streamName, this.group, partition, nextOffset, this.generationId);
    } catch (e: any) {
      if (this.isRebalanceError(e)) throw e;
      logger.error(`Commit failed P${partition}:${nextOffset}`, e);
    }
  }

  private isRebalanceError(e: any): boolean {
    return e?.message?.includes("REBALANCE");
  }

  private backoff(ms: number): Promise<void> {
    return new Promise(r => setTimeout(r, ms));
  }
}

export class NexoStream<T = any> {
  constructor(
    private conn: NexoConnection,
    public readonly name: string
  ) { }

  async create(options: StreamCreateOptions = {}): Promise<this> {
    await StreamCommands.create(this.conn, this.name, options);
    return this;
  }

  async exists(): Promise<boolean> {
    return StreamCommands.exists(this.conn, this.name);
  }

  async publish(data: T, options: StreamPublishOptions = {}): Promise<void> {
    await StreamCommands.publish(this.conn, this.name, data, options);
  }

  async subscribe(
    group: string,
    callback: (data: T) => Promise<any> | any,
    options: StreamSubscribeOptions = {}
  ): Promise<{ stop: () => void }> {
    if (!group) throw new Error("Consumer Group is required for subscription");

    // Fail Fast: Check existence first
    if (!(await this.exists())) {
      throw new Error(`Stream '${this.name}' not found`);
    }

    const subscription = new StreamSubscription<T>(this.conn, this.name, group);

    // Start the subscription loop (non-blocking)
    await subscription.start(callback, options);

    return {
      stop: () => subscription.stop()
    };
  }
}
