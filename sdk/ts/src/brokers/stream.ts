import { NexoConnection } from '../transport/tcp/connection';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError, NexoError, NotConnectedError, ProtocolError } from '../errors';
import { runConcurrent } from '../utils/concurrent';
import { Subscription } from '../subscription';
import { ProvisionOutcome, ProvisionResult } from '../provisioning';
import { Cursor } from '../protocol/codec';
import {
  ErrorCode,
  FLAG_STREAM_S_CREATE_HAS_MAX_AGE,
  FLAG_STREAM_S_CREATE_HAS_MAX_BYTES,
  ProvisionStatus as WireProvisionStatus,
  STREAM_MAX_FETCH_BATCH_SIZE,
  STREAM_MAX_KEY_BYTES,
  STREAM_MAX_PUBLISH_BATCH,
  StreamOpcode,
} from '../protocol/generated';

const FETCH_TIMEOUT_MARGIN_MS = 5000;
const textEncoder = new TextEncoder();

function encodeStreamKey(w: { u16(value: number): any; bytes(value: Uint8Array): any }, key?: string | Uint8Array): void {
  if (key === undefined) {
    w.u16(0);
    return;
  }
  const keyBytes = typeof key === 'string' ? textEncoder.encode(key) : key;
  if (keyBytes.length === 0) throw new Error('Stream key must not be empty');
  if (keyBytes.length > STREAM_MAX_KEY_BYTES) throw new Error(`Stream key exceeds ${STREAM_MAX_KEY_BYTES} bytes`);
  w.u16(keyBytes.length);
  w.bytes(keyBytes);
}

export interface RetentionOptions {
  maxAgeMs?: number;
  maxBytes?: number;
}

export interface StreamCreateOptions {
  retention?: RetentionOptions;
}

export interface StreamRetentionConfiguration {
  maxAgeMs: number | null;
  maxBytes: number | null;
}

export interface StreamConfiguration {
  retention: StreamRetentionConfiguration;
  maxSegmentSize: number;
  maxAckPending: number;
  ackWaitMs: number;
  maxDeliveries: number;
}

export interface StreamDefinition {
  name: string;
  config: StreamConfiguration;
}

export interface StreamSubscribeOptions {
  batchSize?: number;
  waitMs?: number;
  concurrency?: number;
  stopTimeoutMs?: number;
}

export interface StreamPublishOptions {
  key?: string | Uint8Array;
}

export interface StreamPublishItem<T> {
  data: T;
  key?: string | Uint8Array;
}

export interface StreamMessageMeta {
  seq: bigint;
  key?: Uint8Array;
}

export type StreamHandler<T> = (data: T, meta: StreamMessageMeta) => unknown | Promise<unknown>;

export interface StreamMessage<T> extends StreamMessageMeta {
  data: T;
}

export interface DlsEntry {
  seq: bigint;
  reason: string;
  attempts: number;
  key?: Uint8Array;
}

export interface DlsPeekOptions {
  limit?: number;
  offset?: number;
}

function readStreamDefinition(cursor: Cursor): StreamDefinition {
  const name = cursor.readString();
  const flags = cursor.readU8();
  const retention: StreamRetentionConfiguration = {
    maxAgeMs: flags & FLAG_STREAM_S_CREATE_HAS_MAX_AGE ? Number(cursor.readU64()) : null,
    maxBytes: flags & FLAG_STREAM_S_CREATE_HAS_MAX_BYTES ? Number(cursor.readU64()) : null,
  };
  return {
    name,
    config: {
      retention,
      maxSegmentSize: Number(cursor.readU64()),
      maxAckPending: Number(cursor.readU64()),
      ackWaitMs: Number(cursor.readU64()),
      maxDeliveries: cursor.readU32(),
    },
  };
}

function readProvisionStatus(cursor: Cursor): ProvisionOutcome {
  const status = cursor.readU8();
  if (status === WireProvisionStatus.CREATED) return 'created';
  if (status === WireProvisionStatus.UNCHANGED) return 'unchanged';
  throw new ProtocolError(`Unknown stream provision status: ${status}`);
}

const StreamCommands = {
  create: async (conn: NexoConnection, name: string, options: StreamCreateOptions): Promise<ProvisionResult<StreamDefinition>> => {
    const retention = options.retention;
    const hasMaxAge = retention?.maxAgeMs !== undefined;
    const hasMaxBytes = retention?.maxBytes !== undefined;
    const flags = (hasMaxAge ? FLAG_STREAM_S_CREATE_HAS_MAX_AGE : 0x00) | (hasMaxBytes ? FLAG_STREAM_S_CREATE_HAS_MAX_BYTES : 0x00);
    const res = await conn.send(StreamOpcode.S_CREATE, w => {
      w.string(name).u8(flags);
      if (hasMaxAge) w.u64(retention!.maxAgeMs!);
      if (hasMaxBytes) w.u64(retention!.maxBytes!);
    });
    return {
      status: readProvisionStatus(res.cursor),
      definition: readStreamDefinition(res.cursor),
    };
  },

  describe: async (conn: NexoConnection, name: string): Promise<StreamDefinition> => {
    const res = await conn.send(StreamOpcode.S_DESCRIBE, w => w.string(name));
    return readStreamDefinition(res.cursor);
  },

  exists: async (conn: NexoConnection, name: string): Promise<boolean> => {
    const res = await conn.send(StreamOpcode.S_EXISTS, w => w.string(name));
    return res.cursor.readU8() === 1;
  },

  delete: (conn: NexoConnection, name: string) =>
    conn.send(StreamOpcode.S_DELETE, w => w.string(name)),
};

function isRecoverableMembershipError(error: unknown): boolean {
  if (error instanceof StreamAckError) return error.errors.some(isRecoverableMembershipError);
  return error instanceof NexoError && (error.code === ErrorCode.FENCED || error.code === ErrorCode.NOT_MEMBER);
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

class StreamAckError extends Error {
  constructor(readonly errors: unknown[]) {
    super(`${errors.length} stream ACK request(s) failed`);
    this.name = 'StreamAckError';
  }
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
    private readonly callback: StreamHandler<T>,
    private readonly batchSize: number,
    private readonly waitMs: number,
    private readonly concurrency: number,
    private readonly stopTimeoutMs: number,
  ) { }

  get closed(): Promise<void> {
    return this.loopDone;
  }

  get error(): unknown {
    return this.loopError;
  }

  async start(): Promise<void> {
    this.active = true;
    // Synchronous first join: fail fast on permanent errors (e.g. stream not found).
    try {
      await this.join();
    } catch (error) {
      this.active = false;
      throw error;
    }
    this.loopDone = this.loop().catch(err => {
      this.logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.loopError = err;
    }).finally(() => {
      this.active = false;
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
      } catch (error) {
        if (!this.active) {
          if (this.leftToCancelFetch && isRecoverableMembershipError(error)) break;
          throw error;
        }
        this.consumerId = null;

        if (isRecoverableMembershipError(error)) continue;

        if (!this.conn.isConnected || error instanceof ConnectionClosedError || error instanceof NotConnectedError) {
          await sleep(DEFAULT_CONFIG.connection.backoff.short);
          continue;
        }
        if (error instanceof NexoError) throw error;

        this.logger.error(`[${this.streamName}:${this.group}] Error. Retrying in ${DEFAULT_CONFIG.connection.backoff.long}ms...`, error);
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
    const ackErrors: unknown[] = [];
    try {
      await runConcurrent(batch, this.concurrency, async ({ seq, key, data }) => {
        if (!this.active || ackErrors.length > 0) return;
        try {
          await this.callback(data, { seq, key });
        } catch (err) {
          this.logger.error(`[${this.streamName}:${this.group}] Processing error at seq=${seq}. Waiting for timeout-based retry.`, err);
          return;
        }

        try {
          await this.conn.send(StreamOpcode.S_ACK, w => w
            .string(this.streamName)
            .string(this.group)
            .string(consumerId)
            .u64(generation)
            .u64(seq)
          );
        } catch (err) {
          ackErrors.push(err);
          this.logger.error(`[${this.streamName}:${this.group}] ACK failed at seq=${seq}.`, err);
        }
      });

      if (ackErrors.length > 0) throw new StreamAckError(ackErrors);
    } finally {
      this.phase = 'idle';
    }
  }
}

export class NexoStreamDLS {
  constructor(
    private readonly conn: NexoConnection,
    private readonly streamName: string,
    private readonly groupName: string,
  ) { }

  /** Peek at DLS entries for a consumer group. */
  async peek(options: DlsPeekOptions = {}): Promise<DlsEntry[]> {
    const limit = options.limit ?? 100;
    const offset = options.offset ?? 0;
    const res = await this.conn.send(StreamOpcode.S_PEEK_DLS, w => w
      .string(this.streamName)
      .string(this.groupName)
      .u32(limit)
      .u32(offset)
    );
    const count = res.cursor.readU32();
    const entries: DlsEntry[] = [];
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

  /** Move a message from DLS back to the stream for redelivery. */
  async replay(seq: bigint): Promise<void> {
    await this.conn.send(StreamOpcode.S_MOVE_TO_STREAM, w => w
      .string(this.streamName)
      .string(this.groupName)
      .u64(seq)
    );
  }

  /** Delete a message from the DLS permanently. */
  async delete(seq: bigint): Promise<void> {
    await this.conn.send(StreamOpcode.S_DELETE_DLS, w => w
      .string(this.streamName)
      .string(this.groupName)
      .u64(seq)
    );
  }

  /** Purge all messages from the DLS. Returns the count of removed entries. */
  async purge(): Promise<number> {
    const res = await this.conn.send(StreamOpcode.S_PURGE_DLS, w => w
      .string(this.streamName)
      .string(this.groupName)
    );
    return res.cursor.readU32();
  }
}

export class NexoStreamGroup<T = any> {
  public readonly dls: NexoStreamDLS;

  constructor(
    private readonly conn: NexoConnection,
    private readonly streamName: string,
    public readonly name: string,
    private readonly logger: Logger,
  ) {
    this.dls = new NexoStreamDLS(conn, streamName, name);
  }

  async subscribe(
    callback: StreamHandler<T>,
    options: StreamSubscribeOptions = {}
  ): Promise<Subscription> {
    if (!this.name) throw new Error('Consumer Group is required for subscription');

    const batchSize = options.batchSize ?? DEFAULT_CONFIG.stream.batchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.stream.waitMs;
    const concurrency = Math.max(1, options.concurrency ?? DEFAULT_CONFIG.stream.concurrency);
    const stopTimeoutMs = options.stopTimeoutMs ?? DEFAULT_CONFIG.stream.stopTimeoutMs;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > STREAM_MAX_FETCH_BATCH_SIZE) {
      throw new Error(`batchSize must be an integer between 1 and ${STREAM_MAX_FETCH_BATCH_SIZE}`);
    }
    if (!Number.isInteger(waitMs) || waitMs < 1) throw new Error('waitMs must be a positive integer');
    if (!Number.isInteger(stopTimeoutMs) || stopTimeoutMs < 1) {
      throw new Error('stopTimeoutMs must be a positive integer');
    }

    const sub = new StreamSubscription<T>(
      this.conn,
      this.streamName,
      this.name,
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
      sub.closed,
      () => sub.error,
    );
  }

  /** Seek to beginning or end of the stream for a consumer group. */
  async seek(target: 'beginning' | 'end'): Promise<void> {
    if (target !== 'beginning' && target !== 'end') {
      throw new Error(`Invalid seek target: ${String(target)}`);
    }
    await this.conn.send(StreamOpcode.S_SEEK, w => w
      .string(this.streamName)
      .string(this.name)
      .u8(target === 'beginning' ? 0 : 1)
    );
  }
}

export class NexoStream<T = any> {
  constructor(
    private readonly conn: NexoConnection,
    public readonly name: string,
    private readonly logger: Logger,
  ) { }

  async publish(data: T, options: StreamPublishOptions = {}): Promise<bigint> {
    const res = await this.conn.send(StreamOpcode.S_PUB, w => {
      w.string(this.name).u32(1);
      encodeStreamKey(w, options.key);
      w.anyWithLen(data);
    });
    const count = res.cursor.readU32();
    return count > 0 ? res.cursor.readU64() : 0n;
  }

  async publishBatch(items: StreamPublishItem<T>[]): Promise<bigint[]> {
    if (items.length === 0) return [];
    if (items.length > STREAM_MAX_PUBLISH_BATCH) {
      throw new Error(`Publish batch too large: ${items.length} items (max: ${STREAM_MAX_PUBLISH_BATCH})`);
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

  group(name: string): NexoStreamGroup<T> {
    return new NexoStreamGroup<T>(this.conn, this.name, name, this.logger);
  }
}

export class NexoStreamFacade {
  constructor(
    private readonly conn: NexoConnection,
    private readonly logger: Logger,
  ) { }

  create(name: string, options: StreamCreateOptions = {}): Promise<ProvisionResult<StreamDefinition>> {
    return StreamCommands.create(this.conn, name, options);
  }

  describe(name: string): Promise<StreamDefinition> {
    return StreamCommands.describe(this.conn, name);
  }

  async get<T = any>(name: string): Promise<NexoStream<T>> {
    await StreamCommands.describe(this.conn, name);
    return new NexoStream<T>(this.conn, name, this.logger);
  }

  exists(name: string): Promise<boolean> {
    return StreamCommands.exists(this.conn, name);
  }

  async delete(name: string): Promise<void> {
    await StreamCommands.delete(this.conn, name);
  }
}
