import { NexoConnection } from '../connection';
import { Cursor } from '../codec';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError, NotConnectedError } from '../errors';

const FETCH_TIMEOUT_MARGIN_MS = 5000;

enum StreamOpcode {
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_ACK = 0x34,
  S_EXISTS = 0x35,
  S_DELETE = 0x36,
  S_SEEK = 0x38,
  S_LEAVE = 0x39,
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
}

export interface StreamMessage<T> {
  seq: bigint;
  data: T;
}

function isRecoverableMembershipError(e: any): boolean {
  const msg = e instanceof Error ? e.message : String(e);
  return msg.includes('FENCED') || msg.includes('NOT_MEMBER');
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

class StreamSubscription<T> {
  private active = false;
  private loopDone: Promise<void> = Promise.resolve();
  private consumerId: string | null = null;
  private generation: bigint = 0n;

  constructor(
    private readonly conn: NexoConnection,
    private readonly streamName: string,
    private readonly group: string,
    private readonly logger: Logger,
    private readonly callback: (data: T) => Promise<any> | any,
    private readonly batchSize: number,
    private readonly waitMs: number,
  ) { }

  async start(): Promise<void> {
    this.active = true;
    // Synchronous first join: fail fast on permanent errors (e.g. topic not found).
    await this.join();
    this.loopDone = this.loop().catch(err => {
      this.logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.active = false;
    });
  }

  async stop(): Promise<void> {
    this.active = false;
    if (this.consumerId !== null) {
      try {
        await this.conn.send(StreamOpcode.S_LEAVE, w => w
          .string(this.streamName)
          .string(this.group)
          .string(this.consumerId!)
          .u64(this.generation)
        );
      } catch { /* connection may already be closed */ }
    }
    await this.loopDone;
  }

  private async join(): Promise<void> {
    if (!this.conn.isConnected) throw new NotConnectedError();
    const res = await this.conn.send(StreamOpcode.S_JOIN, w => w
      .string(this.group)
      .string(this.streamName)
    );
    res.cursor.readU64(); // ack_floor (unused client-side)
    this.generation = res.cursor.readU64();
    this.consumerId = res.cursor.readString();
  }

  private async loop(): Promise<void> {
    while (this.active) {
      if (!this.conn.isConnected) {
        this.consumerId = null;
        await sleep(DEFAULT_CONFIG.connection.backoff.short);
        continue;
      }

      try {
        if (this.consumerId === null) await this.join();
        await this.pollOnce();
      } catch (e: any) {
        if (!this.active) break;

        if (!this.conn.isConnected || e instanceof ConnectionClosedError || e.code === 'ECONNRESET') {
          this.consumerId = null;
          await sleep(DEFAULT_CONFIG.connection.backoff.short);
          continue;
        }

        if (isRecoverableMembershipError(e)) {
          this.consumerId = null;
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

    const res = await this.conn.send(StreamOpcode.S_FETCH, w => w
      .string(this.streamName)
      .string(this.group)
      .string(consumerId)
      .u64(generation)
      .u32(this.batchSize)
      .u32(this.waitMs)
    , { timeoutMs: this.waitMs + FETCH_TIMEOUT_MARGIN_MS });

    const count = res.cursor.readU32();
    if (count === 0) return;

    for (let i = 0; i < count; i++) {
      const seq = res.cursor.readU64();
      res.cursor.readU64(); // skip timestamp
      const payloadLen = res.cursor.readU32();
      const payloadBuf = res.cursor.readBuffer(payloadLen);
      const data = new Cursor(payloadBuf).decodeAny() as T;

      if (!this.active) return;

      try {
        await this.callback(data);
        this.conn.sendFireAndForget(StreamOpcode.S_ACK, w => w
          .string(this.streamName)
          .string(this.group)
          .string(consumerId)
          .u64(generation)
          .u64(seq)
        );
      } catch (err) {
        this.logger.error(`[${this.streamName}:${this.group}] Processing error at seq=${seq}. Waiting for timeout-based retry.`, err);
      }
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
    await this.conn.send(StreamOpcode.S_CREATE, w => w
      .string(this.name)
      .string(JSON.stringify(options))
    );
    return this;
  }

  async exists(): Promise<boolean> {
    try {
      const res = await this.conn.send(StreamOpcode.S_EXISTS, w => w.string(this.name));
      return res.status === 0x00;
    } catch {
      return false;
    }
  }

  async delete(): Promise<void> {
    await this.conn.send(StreamOpcode.S_DELETE, w => w.string(this.name));
  }

  async publish(data: T): Promise<void> {
    await this.conn.send(StreamOpcode.S_PUB, w => w
      .string(this.name)
      .any(data)
    );
  }

  async subscribe(
    group: string,
    callback: (data: T) => Promise<any> | any,
    options: StreamSubscribeOptions = {}
  ): Promise<{ stop: () => Promise<void> }> {
    if (!group) throw new Error('Consumer Group is required for subscription');

    const batchSize = options.batchSize ?? DEFAULT_CONFIG.stream.batchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.stream.waitMs;

    const sub = new StreamSubscription<T>(this.conn, this.name, group, this.logger, callback, batchSize, waitMs);
    await sub.start();

    return { stop: () => sub.stop() };
  }

  /** Seek to beginning or end of the stream for a consumer group. */
  async seek(group: string, target: 'beginning' | 'end'): Promise<void> {
    await this.conn.send(StreamOpcode.S_SEEK, w => w
      .string(this.name)
      .string(group)
      .u8(target === 'beginning' ? 0 : 1)
    );
  }
}
