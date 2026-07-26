import { NexoConnection } from '../connection';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError, NotConnectedError } from '../errors';
import { runConcurrent } from '../utils/concurrent';
import { Subscription } from '../subscription';

const FETCH_TIMEOUT_MARGIN_MS = 5000;
const MAX_PUBLISH_BATCH = 65_536;
const MAX_FETCH_BATCH_SIZE = 65_536;
const textEncoder = new TextEncoder();

function encodeStreamKey(w: { u16(value: number): any; bytes(value: Uint8Array): any }, key?: string | Uint8Array): void {
  if (key === undefined) {
    w.u16(0);
    return;
  }
  const keyBytes = typeof key === 'string' ? textEncoder.encode(key) : key;
  if (keyBytes.length === 0) throw new Error('Stream key must not be empty');
  if (keyBytes.length > 0xFFFF) throw new Error('Stream key exceeds 65535 bytes');
  w.u16(keyBytes.length);
  w.bytes(keyBytes);
}

enum StreamOpcode {
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_ACK = 0x34,
  S_EXISTS = 0x35,
  S_DELETE = 0x36,
  S_ACK_BATCH = 0x37,
  S_SEEK = 0x38,
  S_LEAVE = 0x39,
  S_PEEK_DLT = 0x3A,
  S_MOVE_TO_STREAM = 0x3B,
  S_DELETE_DLT = 0x3C,
  S_PURGE_DLT = 0x3D,
}

export interface RetentionOptions {
  maxAgeMs?: number;
  maxBytes?: number;
}

export interface StreamCreateOptions {
  retention?: RetentionOptions;
}

export interface StreamSubscribeOptions {
  batchSize?: number;
  waitMs?: number;
  concurrency?: number;
  stopTimeoutMs?: number;
}

export interface StreamMessage<T> {
  seq: bigint;
  key?: Uint8Array;
  data: T;
}

export interface DltEntry {
  seq: bigint;
  reason: string;
  attempts: number;
  key?: Uint8Array;
}

function isRecoverableMembershipError(e: any): boolean {
  const msg = e instanceof Error ? e.message : String(e);
  return msg.includes('FENCED') || msg.includes('NOT_MEMBER');
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

class StreamSubscription<T> {
  active = false;
  private loopDone: Promise<void> = Promise.resolve();
  private consumerId: string | null = null;
  private generation: bigint = 0n;
  private phase: 'idle' | 'fetching' | 'processing' = 'idle';
  private loopError: unknown = null;
  private leftToCancelFetch = false;

  constructor(
    private readonly conn: NexoConnection,
    private readonly streamName: string,
    private readonly group: string,
    private readonly logger: Logger,
    private readonly callback: (data: T, meta: { seq: bigint; key?: Uint8Array }) => Promise<any> | any,
    private readonly batchSize: number,
    private readonly waitMs: number,
    private readonly concurrency: number,
    private readonly stopTimeoutMs: number,
  ) { }

  async start(): Promise<void> {
    this.active = true;
    // Synchronous first join: fail fast on permanent errors (e.g. topic not found).
    await this.join();
    this.loopDone = this.loop().catch(err => {
      this.logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.active = false;
      this.loopError = err;
    });
  }

  async stop(): Promise<void> {
    this.active = false;
    const leftWhileFetching = this.phase === 'fetching';
    if (leftWhileFetching) {
      this.leftToCancelFetch = true;
      await this.leave();
    }
    try {
      await this.waitForLoop();
      if (this.loopError !== null) throw this.loopError;
    } finally {
      if (!leftWhileFetching) await this.leave();
    }
  }

  private async waitForLoop(): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        this.loopDone,
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`Stream subscription stop timed out after ${this.stopTimeoutMs}ms`)),
            this.stopTimeoutMs,
          );
          timer.unref();
        }),
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  private async leave(): Promise<void> {
    if (this.consumerId !== null) {
      try {
        await this.conn.send(StreamOpcode.S_LEAVE, w => w
          .string(this.streamName)
          .string(this.group)
          .string(this.consumerId!)
          .u64(this.generation)
        );
      } catch { /* connection may already be closed or member already removed */ }
    }
  }

  private async join(): Promise<void> {
    if (!this.conn.isConnected) throw new NotConnectedError();
    const res = await this.conn.send(StreamOpcode.S_JOIN, w => w
      .string(this.streamName)
      .string(this.group)
    );
    res.cursor.readU64(); // ack_floor (unused client-side)
    this.generation = res.cursor.readU64();
    this.consumerId = res.cursor.readString();
  }

  private async loop(): Promise<void> {
    while (this.active) {
      try {
        if (this.consumerId === null) await this.join();
        await this.pollOnce();
      } catch (e: any) {
        if (!this.active) {
          if (this.leftToCancelFetch && isRecoverableMembershipError(e)) break;
          throw e;
        }
        this.consumerId = null;

        if (isRecoverableMembershipError(e)) continue;

        if (!this.conn.isConnected || e instanceof ConnectionClosedError || e.code === 'ECONNRESET') {
          await sleep(DEFAULT_CONFIG.connection.backoff.short);
          continue;
        }

        this.logger.error(`[${this.streamName}:${this.group}] Error. Retrying in ${DEFAULT_CONFIG.connection.backoff.long}ms...`, e);
        await sleep(DEFAULT_CONFIG.connection.backoff.long);
      }
    }
  }

  private async pollOnce(): Promise<void> {
    const consumerId = this.consumerId!;
    const generation = this.generation;

    this.phase = 'fetching';
    let res;
    try {
      res = await this.conn.send(StreamOpcode.S_FETCH, w => w
        .string(this.streamName)
        .string(this.group)
        .string(consumerId)
        .u64(generation)
        .u32(this.batchSize)
        .u32(this.waitMs)
        , { timeoutMs: this.waitMs + FETCH_TIMEOUT_MARGIN_MS });
    } finally {
      if (this.phase === 'fetching') this.phase = 'idle';
    }

    const count = res.cursor.readU32();
    if (count === 0) return;

    const batch: { seq: bigint; key?: Uint8Array; data: T }[] = [];
    for (let i = 0; i < count; i++) {
      const seq = res.cursor.readU64();
      res.cursor.readU64(); // skip timestamp
      const keyLen = res.cursor.readU16();
      const key = keyLen > 0 ? res.cursor.readBuffer(keyLen) : undefined;
      const payloadLen = res.cursor.readU32();
      batch.push({ seq, key, data: res.cursor.decodeAnyFromBuffer(payloadLen) as T });
    }

    this.phase = 'processing';
    const acknowledged: bigint[] = [];
    try {
      await runConcurrent(batch, this.concurrency, async ({ seq, key, data }) => {
        if (!this.active) return;
        try {
          await this.callback(data, { seq, key });
          acknowledged.push(seq);
        } catch (err) {
          this.logger.error(`[${this.streamName}:${this.group}] Processing error at seq=${seq}. Waiting for timeout-based retry.`, err);
        }
      });

      if (acknowledged.length > 0) {
        await this.conn.send(StreamOpcode.S_ACK_BATCH, w => {
          w.string(this.streamName)
            .string(this.group)
            .string(consumerId)
            .u64(generation)
            .u32(acknowledged.length);
          for (const seq of acknowledged) w.u64(seq);
        });
      }
    } finally {
      this.phase = 'idle';
    }
  }
}

export class NexoStream<T = any> {
  constructor(
    private readonly conn: NexoConnection,
    public readonly name: string,
    private readonly logger: Logger,
  ) { }

  async create(options: StreamCreateOptions = {}): Promise<this> {
    const retention = options.retention;
    const hasMaxAge = retention?.maxAgeMs !== undefined;
    const hasMaxBytes = retention?.maxBytes !== undefined;
    const flags = (hasMaxAge ? 0x01 : 0x00) | (hasMaxBytes ? 0x02 : 0x00);
    await this.conn.send(StreamOpcode.S_CREATE, w => {
      w.string(this.name).u8(flags);
      if (hasMaxAge) w.u64(retention!.maxAgeMs!);
      if (hasMaxBytes) w.u64(retention!.maxBytes!);
    });
    return this;
  }

  async exists(): Promise<boolean> {
    try {
      const res = await this.conn.send(StreamOpcode.S_EXISTS, w => w.string(this.name));
      return res.cursor.readU8() === 1;
    } catch {
      return false;
    }
  }

  async delete(): Promise<void> {
    await this.conn.send(StreamOpcode.S_DELETE, w => w.string(this.name));
  }

  async publish(data: T, options: { key?: string | Uint8Array } = {}): Promise<bigint> {
    const res = await this.conn.send(StreamOpcode.S_PUB, w => {
      w.string(this.name).u32(1);
      encodeStreamKey(w, options.key);
      w.anyWithLen(data);
    });
    const count = res.cursor.readU32();
    return count > 0 ? res.cursor.readU64() : 0n;
  }

  async publishBatch(items: { data: T, key?: string | Uint8Array }[]): Promise<bigint[]> {
    if (items.length === 0) return [];
    if (items.length > MAX_PUBLISH_BATCH) {
      throw new Error(`Publish batch too large: ${items.length} items (max: ${MAX_PUBLISH_BATCH})`);
    }
    const res = await this.conn.send(StreamOpcode.S_PUB, w => {
      w.string(this.name).u32(items.length);
      for (const item of items) {
        encodeStreamKey(w, item.key);
        w.anyWithLen(item.data);
      }
    });
    const count = res.cursor.readU32();
    const seqs: bigint[] = [];
    for (let i = 0; i < count; i++) {
      seqs.push(res.cursor.readU64());
    }
    return seqs;
  }

  async subscribe(
    group: string,
    callback: (data: T, meta: { seq: bigint; key?: Uint8Array }) => Promise<any> | any,
    options: StreamSubscribeOptions = {}
  ): Promise<Subscription> {
    if (!group) throw new Error('Consumer Group is required for subscription');

    const batchSize = options.batchSize ?? DEFAULT_CONFIG.stream.batchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.stream.waitMs;
    const concurrency = Math.max(1, options.concurrency ?? DEFAULT_CONFIG.stream.concurrency);
    const stopTimeoutMs = options.stopTimeoutMs ?? DEFAULT_CONFIG.stream.stopTimeoutMs;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAX_FETCH_BATCH_SIZE) {
      throw new Error(`batchSize must be an integer between 1 and ${MAX_FETCH_BATCH_SIZE}`);
    }
    if (!Number.isInteger(waitMs) || waitMs < 1) throw new Error('waitMs must be a positive integer');
    if (!Number.isInteger(stopTimeoutMs) || stopTimeoutMs < 1) {
      throw new Error('stopTimeoutMs must be a positive integer');
    }

    const sub = new StreamSubscription<T>(
      this.conn,
      this.name,
      group,
      this.logger,
      callback,
      batchSize,
      waitMs,
      concurrency,
      stopTimeoutMs,
    );
    await sub.start();

    return new Subscription(
      () => sub.stop(),
      () => sub.active,
    );
  }

  /** Seek to beginning or end of the stream for a consumer group. */
  async seek(group: string, target: 'beginning' | 'end'): Promise<void> {
    if (target !== 'beginning' && target !== 'end') {
      throw new Error(`Invalid seek target: ${String(target)}`);
    }
    await this.conn.send(StreamOpcode.S_SEEK, w => w
      .string(this.name)
      .string(group)
      .u8(target === 'beginning' ? 0 : 1)
    );
  }

  /** Peek at Dead Letter Topic entries for a consumer group. */
  async peekDlt(group: string, limit: number = 100, offset: number = 0): Promise<DltEntry[]> {
    const res = await this.conn.send(StreamOpcode.S_PEEK_DLT, w => w
      .string(this.name)
      .string(group)
      .u32(limit)
      .u32(offset)
    );
    const count = res.cursor.readU32();
    const entries: DltEntry[] = [];
    for (let i = 0; i < count; i++) {
      const seq = res.cursor.readU64();
      const reason = res.cursor.readString();
      const attempts = res.cursor.readU32();
      const keyLen = res.cursor.readU16();
      const key = keyLen > 0 ? res.cursor.readBuffer(keyLen) : undefined;
      entries.push({ seq, reason, attempts, key });
    }
    return entries;
  }

  /** Move a message from DLT back to the stream for redelivery. */
  async moveToStream(group: string, seq: bigint): Promise<void> {
    await this.conn.send(StreamOpcode.S_MOVE_TO_STREAM, w => w
      .string(this.name)
      .string(group)
      .u64(seq)
    );
  }

  /** Delete a message from the DLT permanently. */
  async deleteDlt(group: string, seq: bigint): Promise<void> {
    await this.conn.send(StreamOpcode.S_DELETE_DLT, w => w
      .string(this.name)
      .string(group)
      .u64(seq)
    );
  }

  /** Purge all messages from the DLT. Returns the count of removed entries. */
  async purgeDlt(group: string): Promise<number> {
    const res = await this.conn.send(StreamOpcode.S_PURGE_DLT, w => w
      .string(this.name)
      .string(group)
    );
    return res.cursor.readU32();
  }
}
