import { NexoConnection } from '../connection';
import { Opcode } from '../protocol';
import { FrameCodec, Cursor } from '../codec';
import { logger } from '../utils/logger';

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
  ) {}

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
    const res = await this.conn.send(
      Opcode.S_JOIN,
      FrameCodec.string(this.group),
      FrameCodec.string(this.streamName)
    );

    this.generationId = res.cursor.readU64();
    const count = res.cursor.readU32();

    this.assignedPartitions = [];
    this.offsets.clear();

    for (let i = 0; i < count; i++) {
      const partitionId = res.cursor.readU32();
      const offset = res.cursor.readU64();
      this.assignedPartitions.push(partitionId);
      this.offsets.set(partitionId, offset);
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

    const res = await this.conn.send(
      Opcode.S_FETCH,
      FrameCodec.u64(this.generationId),
      FrameCodec.string(this.streamName),
      FrameCodec.string(this.group),
      FrameCodec.u32(partition),
      FrameCodec.u64(currentOffset),
      FrameCodec.u32(batchSize)
    );

    const messages = this.parseMessages(res.cursor);
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
  }

  private parseMessages(cursor: Cursor): Array<{ offset: bigint; data: T }> {
    const count = cursor.readU32();
    const messages: Array<{ offset: bigint; data: T }> = [];

    for (let i = 0; i < count; i++) {
      const offset = cursor.readU64();
      cursor.readU64(); // timestamp
      const payloadLen = cursor.readU32();
      const payloadBuf = cursor.readBuffer(payloadLen);
      const data = FrameCodec.decodeAny(new Cursor(payloadBuf));
      messages.push({ offset, data });
    }
    return messages;
  }

  private async commitOffset(partition: number, nextOffset: bigint): Promise<void> {
    this.offsets.set(partition, nextOffset);
    try {
      await this.conn.send(
        Opcode.S_COMMIT,
        FrameCodec.u64(this.generationId),
        FrameCodec.string(this.group),
        FrameCodec.string(this.streamName),
        FrameCodec.u32(partition),
        FrameCodec.u64(nextOffset)
      );
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
  ) {}

  async create(): Promise<this> {
    await this.conn.send(Opcode.S_CREATE, FrameCodec.string(this.name));
    return this;
  }

  async exists(): Promise<boolean> {
    try {
      const res = await this.conn.send(
        Opcode.S_EXISTS,
        FrameCodec.string(this.name)
      );
      return res.status === 0x00; // OK
    } catch {
      return false;
    }
  }

  async publish(data: T): Promise<void> {
    await this.conn.send(Opcode.S_PUB, FrameCodec.string(this.name), FrameCodec.any(data));
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
