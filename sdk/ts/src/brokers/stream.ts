import { NexoConnection } from '../connection';
import { Opcode, StreamConfig, StreamPublishOptions, StreamSubscribeOptions } from '../protocol';
import { FrameCodec, Cursor } from '../codec';
import { logger } from '../utils/logger';

export class NexoStream<T = any> {
  private active = false;
  private isSubscribed = false;

  // Consumer Group State
  private generationId = BigInt(0);
  private assignedPartitions: number[] = [];
  private offsets = new Map<number, bigint>();

  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    public readonly consumerGroup?: string
  ) {}

  async create(_config: StreamConfig = {}): Promise<this> {
    await this.conn.send(Opcode.S_CREATE, FrameCodec.string(this.name));
    return this;
  }

  async publish(data: T, _options?: StreamPublishOptions): Promise<void> {
    await this.conn.send(Opcode.S_PUB, FrameCodec.string(this.name), FrameCodec.any(data));
  }

  async subscribe(
    callback: (data: T) => Promise<any> | any,
    options: StreamSubscribeOptions = {}
  ): Promise<void> {
    if (this.isSubscribed) throw new Error("Stream already subscribed.");
    if (!this.consumerGroup) throw new Error("Consumer Group required.");

    this.isSubscribed = true;
    this.active = true;

    this.runConsumerLoop(callback, options.batchSize ?? 100).catch(err => {
      logger.error("Stream consumer loop crashed", err);
      this.active = false;
      this.isSubscribed = false;
    });
  }

  stop() {
    this.active = false;
  }

  // ========================================
  // CONSUMER LOOP
  // ========================================

  private async runConsumerLoop(callback: (data: T) => Promise<any> | any, batchSize: number) {
    while (this.conn.isConnected && this.active) {
      try {
        await this.joinGroup();
        await this.pollLoop(callback, batchSize);
      } catch (e: any) {
        if (!this.active || !this.conn.isConnected) break;

        if (this.isRebalanceError(e)) {
          logger.warn("Rebalance triggered. Re-joining group...");
          await this.backoff(200); // Let server stabilize after rebalance
          continue;
        }

        logger.error("Stream error. Retrying in 1s...", e);
        await this.backoff(1000);
      }
    }
    this.isSubscribed = false;
  }

  private async joinGroup(): Promise<void> {
    logger.info(`Joining group ${this.consumerGroup}...`);
    
    const res = await this.conn.send(
      Opcode.S_JOIN,
      FrameCodec.string(this.consumerGroup!),
      FrameCodec.string(this.name)
    );

    // Parse: [GenID:8][PartitionCount:4][P1:4, P1_Off:8, ...]
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

    logger.info(`Joined Gen ${this.generationId}. Partitions: [${this.assignedPartitions}]`);
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
      FrameCodec.string(this.name),
      FrameCodec.string(this.consumerGroup!),
      FrameCodec.u32(partition),
      FrameCodec.u64(currentOffset),
      FrameCodec.u32(batchSize)
    );

    const messages = this.parseMessages(res.cursor);
    if (messages.length === 0) return;

    // Process batch
    let lastProcessedOffset = currentOffset;
    let shouldCommit = true;

    for (const msg of messages) {
      try {
        await callback(msg.data);
        lastProcessedOffset = msg.offset;
      } catch (e: any) {
        if (this.isRebalanceError(e)) throw e; // Propagate to outer loop
        logger.error(`Error processing P${partition}:${msg.offset}. Stopping batch.`);
        shouldCommit = false;
        break;
      }
    }

    // Commit only if entire batch processed successfully
    if (shouldCommit && this.conn.isConnected) {
      await this.commitOffset(partition, lastProcessedOffset + BigInt(1));
    }
  }

  private parseMessages(cursor: Cursor): Array<{ offset: bigint; data: T }> {
    const count = cursor.readU32();
    const messages: Array<{ offset: bigint; data: T }> = [];

    for (let i = 0; i < count; i++) {
      const offset = cursor.readU64();
      cursor.readU64(); // timestamp (unused)
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
        FrameCodec.string(this.consumerGroup!),
        FrameCodec.string(this.name),
        FrameCodec.u32(partition),
        FrameCodec.u64(nextOffset)
      );
    } catch (e: any) {
      if (this.isRebalanceError(e)) throw e; // Propagate
      logger.error(`Commit failed for P${partition}:${nextOffset}`, e);
    }
  }

  // ========================================
  // HELPERS
  // ========================================

  private isRebalanceError(e: any): boolean {
    return e?.message?.includes("REBALANCE");
  }

  private backoff(ms: number): Promise<void> {
    return new Promise(r => setTimeout(r, ms));
  }
}
