import { NexoConnection } from '../transport/tcp/connection';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import {
  ConnectionClosedError,
  NexoError,
  NotConnectedError,
  ProtocolError,
  RequestCancelledError,
  RequestTimeoutError,
} from '../errors';
import { Subscription } from '../subscription';
import { ProvisionOutcome, ProvisionResult } from '../provisioning';
import { Cursor } from '../protocol/codec';
import {
  ErrorCode,
  FLAG_STREAM_S_CREATE_HAS_MAX_AGE,
  FLAG_STREAM_S_CREATE_HAS_MAX_BYTES,
  FLAG_STREAM_S_UPDATE_BATCH_RELEASE,
  FLAG_STREAM_S_UPDATE_BATCH_RETRY,
  ProvisionStatus as WireProvisionStatus,
  STREAM_MAX_FETCH_BATCH_SIZE,
  STREAM_MAX_KEY_BATCH_SIZE,
  STREAM_MAX_KEY_BYTES,
  STREAM_MAX_PUBLISH_BATCH,
  StreamOpcode,
} from '../protocol/generated';

const FETCH_MAX_BYTES = 10 * 1024 * 1024;
const FETCH_PAGE_MIN_BYTES = 54;
const ACTIVE_FETCH_WAIT_MS = 5;
const UPDATE_FRAME_MAX = STREAM_MAX_FETCH_BATCH_SIZE;

const FETCH_TIMEOUT_MARGIN_MS = 5000;
const CANCELLATION_SETTLE_MS = 100;
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
  maxAckPending: number;
  maxAckPendingBytes: number;
  ackWaitMs: number;
  maxDeliveries: number;
}

export interface StreamDefinition {
  name: string;
  config: StreamConfiguration;
}

export interface StreamSubscribeOptions {
  batchSize?: number;
  keyBatchSize?: number;
  waitMs?: number;
  concurrency?: number;
  stopTimeoutMs?: number;
  callbackTimeoutMs?: number;
  maxBufferedMessages?: number;
  maxBufferedBytes?: number;
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

export interface StreamHandlerMeta extends StreamMessageMeta {
  signal: AbortSignal;
}

export type StreamHandler<T> = (data: T, meta: StreamHandlerMeta) => unknown | Promise<unknown>;

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
      maxAckPending: Number(cursor.readU64()),
      maxAckPendingBytes: Number(cursor.readU64()),
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
  return error instanceof NexoError && (error.code === ErrorCode.FENCED || error.code === ErrorCode.NOT_MEMBER);
}

export interface FetchPageLimits {
  maxMessages: number;
  maxKeyMessages: number;
  maxKeyBatches: number;
  maxBytes: number;
}

interface DecodedBatch<T> {
  receipt: string;
  key?: Uint8Array;
  messages: { seq: bigint; data: T }[];
  wireBytes: number;
}

export interface DecodedFetchPage<T> {
  leaseMs: number;
  batches: DecodedBatch<T>[];
}

const MIN_BATCH_WIRE_BYTES = 42;
const MIN_EVENT_WIRE_BYTES = 20;

function malformedFetchPage(reason: string): ProtocolError {
  return new ProtocolError(`Malformed fetch page: ${reason}`);
}

export function decodeFetchPage<T>(cursor: Cursor, limits: FetchPageLimits): DecodedFetchPage<T> {
  if (cursor.buf.length > limits.maxBytes) {
    throw malformedFetchPage('response exceeds the requested byte budget');
  }
  const leaseMs = Number(cursor.readU64());
  if (!Number.isFinite(leaseMs) || leaseMs <= 0 || leaseMs > 0x7fffffff) {
    throw malformedFetchPage('lease_ms is not a valid timer duration');
  }
  const remaining = () => cursor.buf.length - cursor.offset;
  const batchCount = cursor.readU32();
  if (
    batchCount > limits.maxMessages ||
    batchCount > Math.floor(remaining() / MIN_BATCH_WIRE_BYTES)
  ) {
    throw malformedFetchPage('batch count exceeds the payload');
  }
  const seenReceipts = new Set<string>();
  const seenKeys = new Set<string>();
  let totalMessages = 0;
  let keyedCount = 0;
  const batches: DecodedBatch<T>[] = [];
  for (let i = 0; i < batchCount; i++) {
    const batchStart = cursor.offset;
    const receipt = cursor.readUUID();
    if (seenReceipts.has(receipt)) throw malformedFetchPage('duplicate receipt');
    seenReceipts.add(receipt);
    const keyLen = cursor.readU16();
    const key = keyLen > 0 ? Buffer.from(cursor.readBuffer(keyLen)) : undefined;
    const count = cursor.readU32();
    if (count === 0) throw malformedFetchPage('empty batch');
    if (key === undefined) {
      if (count !== 1) throw malformedFetchPage('keyless batch must be a singleton');
    } else {
      keyedCount++;
      if (keyedCount > limits.maxKeyBatches) {
        throw malformedFetchPage('more keyed batches than requested');
      }
      if (count > limits.maxKeyMessages) {
        throw malformedFetchPage('keyed batch exceeds max_key_messages');
      }
      const hexKey = Buffer.from(key).toString('hex');
      if (seenKeys.has(hexKey)) {
        throw malformedFetchPage('same key leased twice in one page');
      }
      seenKeys.add(hexKey);
    }
    totalMessages += count;
    if (totalMessages > limits.maxMessages) {
      throw malformedFetchPage('more messages than requested');
    }
    if (count > Math.floor(remaining() / MIN_EVENT_WIRE_BYTES)) {
      throw malformedFetchPage('message count exceeds the payload');
    }
    const messages: { seq: bigint; data: T }[] = [];
    let prevSeq = -1n;
    for (let j = 0; j < count; j++) {
      const seq = cursor.readU64();
      cursor.readU64(); // skip timestamp
      const payloadLen = cursor.readU32();
      let data = cursor.decodeAnyFromBuffer(payloadLen) as T;
      if (data instanceof Uint8Array) data = Buffer.from(data) as T;
      if (key !== undefined && seq <= prevSeq) {
        throw malformedFetchPage('keyed batch out of order');
      }
      prevSeq = seq;
      messages.push({ seq, data });
    }
    batches.push({ receipt, key, messages, wireBytes: cursor.offset - batchStart });
  }
  if (remaining() !== 0) throw malformedFetchPage('trailing bytes');
  return { leaseMs, batches };
}

interface ConsumerSession {
  readonly epoch: string;
  readonly consumerId: string;
  valid: boolean;
}

interface LeasedBatch<T> {
  readonly session: ConsumerSession;
  readonly receipt: string;
  readonly key?: Uint8Array;
  readonly keyId?: string;
  readonly messages: { seq: bigint; data: T }[];
  readonly wireBytes: number;
  readonly leaseMs: number;
  completed: number;
  terminal: boolean;
  dead: boolean;
  running: boolean;
  settled: boolean;
  pendingCalls: number;
  renewAtMs: number;
  currentAbort?: AbortController;
  currentInvalidate?: () => void;
}

interface QueuedUpdate<T> {
  completed: number;
  flags: number;
  terminal: boolean;
  batch: LeasedBatch<T>;
}

interface FetchRequest {
  maxMessages: number;
  maxKeyMessages: number;
  maxKeyBatches: number;
  maxBytes: number;
  waitMs: number;
}

class StreamSubscription<T> {
  active = false;
  private loopDone: Promise<void> = Promise.resolve();
  private loopError: unknown = null;

  private session: ConsumerSession | null = null;
  private joining = false;
  private stopping = false;
  private stopped = false;
  private fatalError: unknown = null;
  private teardownDone: Promise<void> | null = null;

  private readonly batches = new Map<string, LeasedBatch<T>>();
  private readonly ownedKeys = new Map<string, LeasedBatch<T>>();
  private readonly executingKeys = new Map<string, LeasedBatch<T>>();
  private readonly blockedKeys = new Map<string, LeasedBatch<T>>();
  private ready: (LeasedBatch<T> | undefined)[] = [];
  private readyHead = 0;

  private bufferedMessages = 0;
  private bufferedBytes = 0;
  private reservedMessages = 0;
  private reservedBytes = 0;
  private reservedKeySlots = 0;

  private runningBatches = 0;
  private retiredInvocations = 0;
  private callsAbandoned = false;
  private capacityTimer: NodeJS.Timeout | null = null;

  private cleanupSession: ConsumerSession | null = null;
  private cleanupPromise: Promise<void> | null = null;
  private joinPromise: Promise<void> | null = null;

  private readonly updateQueue = new Map<string, QueuedUpdate<T>>();
  private updateInflight = false;
  private inflightFetch: Promise<void> | null = null;

  private readonly listeners = new Set<() => void>();
  private driveScheduled = false;
  private renewTimer: NodeJS.Timeout | null = null;

  constructor(
    private readonly conn: NexoConnection,
    private readonly streamName: string,
    private readonly group: string,
    private readonly logger: Logger,
    private readonly callback: StreamHandler<T>,
    private readonly batchSize: number,
    private readonly keyBatchSize: number,
    private readonly waitMs: number,
    private readonly concurrency: number,
    private readonly stopTimeoutMs: number,
    private readonly callbackTimeoutMs: number,
    private readonly maxBufferedMessages: number,
    private readonly maxBufferedBytes: number,
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
    this.loopDone = this.supervise().catch(err => {
      this.logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.loopError = err;
    }).finally(() => {
      this.active = false;
    });
    this.poke();
  }

  async stop(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    this.active = false;
    this.poke();
    for (const b of this.batches.values()) {
      if (!b.running && !b.terminal && !b.dead && !b.settled && b.session.valid) {
        this.enqueueUpdate(b, FLAG_STREAM_S_UPDATE_BATCH_RELEASE, true);
      }
    }
    const drainedInGrace = await this.waitDrainedUntil(performance.now() + this.stopTimeoutMs);
    if (!drainedInGrace) {
      for (const b of this.batches.values()) {
        if (b.dead || b.settled || b.terminal) continue;
        if (!b.session.valid) { b.dead = true; this.maybeRelease(b); continue; }
        if (b.running && b.pendingCalls > 0) {
          this.callsAbandoned = true;
          this.abortCall(b);
          this.enqueueUpdate(b, FLAG_STREAM_S_UPDATE_BATCH_RETRY, true);
        } else {
          this.enqueueUpdate(b, FLAG_STREAM_S_UPDATE_BATCH_RELEASE, true);
        }
      }
      await this.waitDrainedUntil(performance.now() + FETCH_TIMEOUT_MARGIN_MS);
    }
    this.stopped = true;
    this.poke();
    await this.teardown();
    if (!drainedInGrace) {
      throw new Error(`Stream subscription stop timed out after ${this.stopTimeoutMs}ms`);
    }
    if (this.loopError !== null) throw this.loopError;
  }

  private async supervise(): Promise<void> {
    while (this.fatalError === null && !this.stopped) {
      await this.waitForChange();
    }
    try {
      await this.teardown();
    } catch (error) {
      this.logger.error(`[${this.streamName}:${this.group}] Stream teardown failed`, error);
      if (this.fatalError === null) {
        this.fatalError = error;
        this.loopError = error;
      }
    }
    if (this.fatalError !== null) throw this.fatalError;
  }

  private fail(error: unknown): void {
    if (this.fatalError !== null) return;
    this.fatalError = error;
    this.loopError = error;
    this.active = false;
    this.callsAbandoned = true;
    if (this.capacityTimer !== null) {
      clearTimeout(this.capacityTimer);
      this.capacityTimer = null;
    }
    const session = this.session;
    if (session !== null) this.invalidateSession(session);
    for (const b of this.batches.values()) {
      if (!b.dead) {
        b.dead = true;
        this.abortCall(b);
        this.maybeRelease(b);
      }
    }
    this.updateQueue.clear();
    this.poke();
  }

  private wake(): void {
    for (const listener of [...this.listeners]) listener();
  }

  private waitForChange(timeoutMs?: number): Promise<boolean> {
    return new Promise<boolean>(resolve => {
      let timer: NodeJS.Timeout | undefined;
      const done = (changed: boolean) => {
        this.listeners.delete(listener);
        if (timer !== undefined) clearTimeout(timer);
        resolve(changed);
      };
      const listener = () => done(true);
      this.listeners.add(listener);
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => done(false), timeoutMs);
        timer.unref();
      }
    });
  }

  private poke(): void {
    this.wake();
    if (this.driveScheduled) return;
    this.driveScheduled = true;
    queueMicrotask(() => {
      this.driveScheduled = false;
      this.drive();
    });
  }

  private assessCapacitySoon(): void {
    if (
      this.capacityTimer !== null
      || this.retiredInvocations < this.concurrency
      || this.fatalError !== null
      || this.stopped
    ) return;
    this.capacityTimer = setTimeout(() => {
      this.capacityTimer = null;
      if (this.retiredInvocations >= this.concurrency && this.fatalError === null && !this.stopped) {
        this.fail(new Error(
          'Stream callbacks did not settle after cancellation; callback capacity exhausted',
        ));
      }
    }, CANCELLATION_SETTLE_MS);
    this.capacityTimer.unref();
  }

  private takeReadyBatch(): LeasedBatch<T> | undefined {
    while (this.readyHead < this.ready.length) {
      const index = this.readyHead++;
      const batch = this.ready[index];
      this.ready[index] = undefined;
      if (batch === undefined) continue;
      if (batch.dead || batch.terminal || batch.settled || !batch.session.valid || batch.running) {
        this.maybeRelease(batch);
        continue;
      }
      if (batch.keyId !== undefined) {
        if (this.executingKeys.has(batch.keyId)) {
          this.blockedKeys.set(batch.keyId, batch);
          continue;
        }
        this.executingKeys.set(batch.keyId, batch);
      }
      return batch;
    }
    return undefined;
  }

  private drive(): void {
    if (this.active && !this.stopping && this.fatalError === null) {
      this.ensureSession();
      while (this.runningBatches + this.retiredInvocations < this.concurrency) {
        const batch = this.takeReadyBatch();
        if (batch === undefined) break;
        batch.running = true;
        this.runningBatches++;
        this.runBatch(batch).catch(error => this.fail(error));
      }
      if (this.readyHead > 64 && this.readyHead * 2 >= this.ready.length) {
        this.ready = this.ready.slice(this.readyHead);
        this.readyHead = 0;
      }
      this.maybeFetch();
    }
    this.kickUpdates();
  }

  private async join(): Promise<void> {
    if (!this.conn.isConnected) throw new NotConnectedError();
    const res = await this.conn.send(StreamOpcode.S_JOIN, w => w
      .string(this.streamName)
      .string(this.group)
    );
    res.cursor.readU64(); // ack_floor (unused client-side)
    const epoch = res.cursor.readUUID();
    const consumerId = res.cursor.readString();
    const session: ConsumerSession = { epoch, consumerId, valid: true };
    if (!this.active || this.stopping || this.fatalError !== null || this.session !== null) {
      session.valid = false;
      this.captureCleanup(session);
      return;
    }
    this.session = session;
  }

  private ensureSession(): void {
    if (
      this.session !== null || this.joining
      || !this.active || this.stopping || this.fatalError !== null
    ) return;
    this.joining = true;
    let joiner!: Promise<void>;
    joiner = (async () => {
      try {
        while (this.active && !this.stopping && this.fatalError === null && this.session === null) {
          if (!this.conn.isConnected) {
            await this.waitForChange(DEFAULT_CONFIG.connection.backoff.short);
            continue;
          }
          const cleanup = this.cleanupPromise;
          if (cleanup !== null) {
            await cleanup.catch(() => undefined);
            if (this.cleanupPromise === cleanup) this.cleanupPromise = null;
          }
          try {
            await this.join();
            break;
          } catch (error) {
            if (isRecoverableMembershipError(error)) continue;
            if (
              !this.conn.isConnected
              || error instanceof ConnectionClosedError
              || error instanceof NotConnectedError
            ) {
              await this.waitForChange(DEFAULT_CONFIG.connection.backoff.short);
              continue;
            }
            if (error instanceof NexoError) {
              this.fail(error);
              return;
            }
            this.logger.error(
              `[${this.streamName}:${this.group}] Join failed. Retrying in ${DEFAULT_CONFIG.connection.backoff.long}ms...`,
              error,
            );
            await this.waitForChange(DEFAULT_CONFIG.connection.backoff.long);
          }
        }
      } finally {
        this.joining = false;
        if (this.joinPromise === joiner) this.joinPromise = null;
        this.poke();
      }
    })();
    this.joinPromise = joiner;
    joiner.catch(error => this.fail(error));
  }

  private captureCleanup(session: ConsumerSession): void {
    if (this.cleanupSession === session) return;
    this.cleanupSession = session;
    if (!this.conn.isConnected) return;
    const leave = this.leaveMember(session);
    leave.catch(() => undefined);
    const aggregate = this.cleanupPromise === null
      ? leave
      : Promise.all([this.cleanupPromise, leave]).then(() => undefined);
    this.cleanupPromise = aggregate;
    aggregate.catch(() => undefined);
  }

  private invalidateSession(session: ConsumerSession): void {
    if (!session.valid) return;
    session.valid = false;
    if (this.session === session) this.session = null;
    this.captureCleanup(session);
    for (const b of this.batches.values()) {
      if (b.session === session && !b.dead) {
        b.dead = true;
        this.abortCall(b);
        this.maybeRelease(b);
      }
    }
    for (const [receipt, u] of this.updateQueue) {
      if (u.batch.session === session) this.updateQueue.delete(receipt);
    }
    this.scheduleRenew();
    this.poke();
  }

  private onRpcError(session: ConsumerSession, error: unknown): void {
    if (!session.valid || this.session !== session) return;
    if (this.stopping || !this.active) {
      this.fail(error);
      return;
    }
    if (isRecoverableMembershipError(error)) {
      this.invalidateSession(session);
      return;
    }
    if (
      !this.conn.isConnected
      || error instanceof ConnectionClosedError
      || error instanceof NotConnectedError
    ) {
      this.invalidateSession(session);
      return;
    }
    if (error instanceof RequestTimeoutError || error instanceof RequestCancelledError) {
      this.logger.error(
        `[${this.streamName}:${this.group}] Stream request failed. Rejoining.`,
        error,
      );
      this.invalidateSession(session);
      return;
    }
    if (error instanceof NexoError) {
      this.fail(error);
      return;
    }
    this.logger.error(`[${this.streamName}:${this.group}] Stream request failed. Rejoining.`, error);
    this.invalidateSession(session);
  }

  private async leaveMember(session: ConsumerSession): Promise<void> {
    if (!this.conn.isConnected) return;
    const sent = this.conn.send(StreamOpcode.S_LEAVE, w => w
      .string(this.streamName)
      .string(this.group)
      .string(session.consumerId)
      .uuid(session.epoch)
      , { timeoutMs: FETCH_TIMEOUT_MARGIN_MS }).then(() => true, () => false);
    const settled = await this.withDeadline(sent, FETCH_TIMEOUT_MARGIN_MS)
      .catch(() => 'timeout' as const);
    if (settled === 'timeout') throw new Error('Stream member leave timed out');
  }

  private abortCall(batch: LeasedBatch<T>): void {
    batch.currentAbort?.abort();
    batch.currentInvalidate?.();
  }

  private maybeFetch(): void {
    const session = this.session;
    if (session === null || !session.valid || this.inflightFetch !== null) return;
    const freeMessages = this.maxBufferedMessages - this.bufferedMessages - this.reservedMessages;
    const freeBytes = this.maxBufferedBytes - this.bufferedBytes - this.reservedBytes;
    const freeKeySlots = this.concurrency - this.ownedKeys.size - this.reservedKeySlots;
    if (freeMessages <= 0 || freeBytes < FETCH_PAGE_MIN_BYTES || freeKeySlots <= 0) return;
    const readyEmpty = this.readyHead >= this.ready.length;
    if (
      freeMessages < this.batchSize
      && !(readyEmpty && this.runningBatches + this.retiredInvocations < this.concurrency)
    ) return;
    const maxMessages = Math.min(this.batchSize, freeMessages);
    const maxBytes = Math.min(FETCH_MAX_BYTES, freeBytes);
    const maxKeyBatches = Math.min(freeKeySlots, maxMessages);
    const maxKeyMessages = Math.min(
      this.keyBatchSize,
      Math.max(1, Math.floor(this.maxBufferedMessages / 2)),
    );
    const workActive = this.batches.size > 0 || this.runningBatches > 0;
    const waitMs = workActive ? Math.min(this.waitMs, ACTIVE_FETCH_WAIT_MS) : this.waitMs;
    const req: FetchRequest = { maxMessages, maxKeyMessages, maxKeyBatches, maxBytes, waitMs };
    this.reservedMessages += maxMessages;
    this.reservedBytes += maxBytes;
    this.reservedKeySlots += maxKeyBatches;
    const pending = this.fetch(session, req).finally(() => {
      if (this.inflightFetch === pending) this.inflightFetch = null;
      this.poke();
    });
    this.inflightFetch = pending;
  }

  private async fetch(session: ConsumerSession, req: FetchRequest): Promise<void> {
    let res;
    try {
      res = await this.conn.send(StreamOpcode.S_FETCH, w => w
        .string(this.streamName)
        .string(this.group)
        .string(session.consumerId)
        .uuid(session.epoch)
        .u32(req.maxMessages)
        .u32(req.maxKeyMessages)
        .u32(req.maxKeyBatches)
        .u32(req.maxBytes)
        .u32(req.waitMs)
        , { timeoutMs: req.waitMs + FETCH_TIMEOUT_MARGIN_MS });
    } catch (error) {
      this.unreserve(req);
      this.onRpcError(session, error);
      return;
    }
    this.unreserve(req);
    if (session !== this.session || !session.valid) return;
    let page: DecodedFetchPage<T>;
    try {
      page = decodeFetchPage<T>(res.cursor, req);
    } catch (error) {
      this.fail(error);
      return;
    }
    this.admit(session, page);
  }

  private unreserve(req: FetchRequest): void {
    this.reservedMessages -= req.maxMessages;
    this.reservedBytes -= req.maxBytes;
    this.reservedKeySlots -= req.maxKeyBatches;
  }

  private admit(session: ConsumerSession, page: DecodedFetchPage<T>): void {
    for (const d of page.batches) {
      const keyId = d.key !== undefined ? Buffer.from(d.key).toString('hex') : undefined;
      const previous = keyId === undefined ? undefined : this.ownedKeys.get(keyId);
      if (this.batches.has(d.receipt)) {
        this.fail(new ProtocolError('Duplicate batch receipt across pages'));
        return;
      }
      if (previous !== undefined) {
        previous.dead = true;
        previous.settled = true;
        this.updateQueue.delete(previous.receipt);
        this.abortCall(previous);
        this.maybeRelease(previous);
      }
      const batch: LeasedBatch<T> = {
        session,
        receipt: d.receipt,
        key: d.key,
        keyId,
        messages: d.messages,
        wireBytes: d.wireBytes,
        leaseMs: page.leaseMs,
        completed: 0,
        terminal: false,
        dead: false,
        running: false,
        settled: false,
        pendingCalls: 0,
        renewAtMs: performance.now() + Math.max(1, Math.floor(page.leaseMs / 3)),
      };
      this.batches.set(batch.receipt, batch);
      if (keyId !== undefined) this.ownedKeys.set(keyId, batch);
      this.bufferedMessages += d.messages.length;
      this.bufferedBytes += d.wireBytes;
      if (this.stopping || this.fatalError !== null) {
        this.enqueueUpdate(batch, FLAG_STREAM_S_UPDATE_BATCH_RELEASE, true);
      } else {
        this.ready.push(batch);
      }
    }
    this.scheduleRenew();
    this.poke();
  }

  private enqueueUpdate(batch: LeasedBatch<T>, flags: number, terminal: boolean): void {
    if (batch.dead || batch.settled) return;
    if (terminal) batch.terminal = true;
    if (!batch.session.valid) {
      batch.dead = true;
      this.maybeRelease(batch);
      return;
    }
    const existing = this.updateQueue.get(batch.receipt);
    if (existing !== undefined && existing.terminal && !terminal) return;
    this.updateQueue.set(batch.receipt, {
      completed: batch.completed,
      flags,
      terminal,
      batch,
    });
    this.kickUpdates();
  }

  private kickUpdates(): void {
    if (this.updateInflight || this.updateQueue.size === 0) return;
    const session = this.session;
    if (session === null || !session.valid) return;
    this.updateInflight = true;
    void this.flushUpdates(session).catch(error => this.fail(error)).finally(() => {
      this.updateInflight = false;
      this.poke();
    });
  }

  private async flushUpdates(session: ConsumerSession): Promise<void> {
    const items = [...this.updateQueue.entries()]
      .filter(([, u]) => u.batch.session === session)
      .slice(0, UPDATE_FRAME_MAX);
    if (items.length === 0) return;
    const sent = new Set(items.map(([receipt]) => receipt));
    for (const [receipt, u] of items) {
      if (this.updateQueue.get(receipt) === u) this.updateQueue.delete(receipt);
    }
    let res;
    try {
      res = await this.conn.send(StreamOpcode.S_UPDATE_BATCHES, w => {
        w.string(this.streamName)
          .string(this.group)
          .string(session.consumerId)
          .uuid(session.epoch)
          .u32(items.length);
        for (const [receipt, u] of items) w.uuid(receipt).u32(u.completed).u8(u.flags);
      });
    } catch (error) {
      this.onRpcError(session, error);
      return;
    }
    if (!session.valid || this.session !== session) return;
    let failures: Map<string, number>;
    try {
      const count = res.cursor.readU32();
      if (count > sent.size) throw new ProtocolError('more failures than sent updates');
      failures = new Map();
      for (let i = 0; i < count; i++) {
        const receipt = res.cursor.readUUID();
        const code = res.cursor.readU8();
        if (failures.has(receipt)) {
          throw new ProtocolError('duplicate outcome for a batch receipt');
        }
        failures.set(receipt, code);
      }
      if (res.cursor.offset !== res.cursor.buf.length) {
        throw new ProtocolError('trailing bytes in batch outcome response');
      }
      for (const receipt of failures.keys()) {
        if (!sent.has(receipt)) throw new ProtocolError('outcome for an unknown receipt');
      }
    } catch (error) {
      this.fail(error instanceof ProtocolError
        ? error
        : new ProtocolError('Malformed batch outcome response'));
      return;
    }
    for (const [receipt, u] of items) {
      const code = failures.get(receipt);
      if (code === undefined) {
        if (u.terminal) {
          u.batch.settled = true;
        } else {
          u.batch.renewAtMs = performance.now() + Math.max(1, Math.floor(u.batch.leaseMs / 3));
        }
      } else if (code === ErrorCode.FENCED) {
        u.batch.dead = true;
        u.batch.settled = true;
        this.updateQueue.delete(receipt);
        this.abortCall(u.batch);
        this.logger.error(
          `[${this.streamName}:${this.group}] Batch update fenced for receipt=${receipt}.`,
        );
      } else {
        this.fail(new ProtocolError(`Batch update rejected (code ${code}) for receipt=${receipt}`));
        return;
      }
      this.maybeRelease(u.batch);
    }
    this.scheduleRenew();
    this.poke();
  }

  private scheduleRenew(): void {
    if (this.renewTimer !== null) {
      clearTimeout(this.renewTimer);
      this.renewTimer = null;
    }
    if (this.stopped || this.fatalError !== null) return;
    let next = Infinity;
    for (const b of this.batches.values()) {
      if (!b.terminal && !b.dead && !b.settled && b.session.valid) {
        next = Math.min(next, b.renewAtMs);
      }
    }
    if (!Number.isFinite(next)) return;
    this.renewTimer = setTimeout(() => this.onRenewTick(), Math.max(1, next - performance.now()));
    this.renewTimer.unref();
  }

  private onRenewTick(): void {
    this.renewTimer = null;
    const now = performance.now();
    for (const b of this.batches.values()) {
      if (!b.terminal && !b.dead && !b.settled && b.session.valid && b.renewAtMs <= now) {
        b.renewAtMs = now + Math.max(1, Math.floor(b.leaseMs / 3));
        this.enqueueUpdate(b, 0, false);
      }
    }
    this.scheduleRenew();
  }

  private async runBatch(batch: LeasedBatch<T>): Promise<void> {
    try {
      while (batch.completed < batch.messages.length) {
        if (
          this.stopping || this.fatalError !== null
          || batch.dead || batch.terminal || !batch.session.valid
        ) break;
        const msg = batch.messages[batch.completed];
        const outcome = await this.invokeCallback(batch, msg);
        if (outcome !== 'ok') {
          if (
            outcome !== 'aborted'
            && !batch.dead && batch.session.valid && !batch.terminal
          ) {
            this.enqueueUpdate(batch, FLAG_STREAM_S_UPDATE_BATCH_RETRY, true);
          }
          break;
        }
        if (
          batch.dead || batch.terminal || !batch.session.valid
          || this.fatalError !== null || this.callsAbandoned
        ) break;
        batch.completed++;
      }
    } finally {
      batch.running = false;
      this.runningBatches--;
      this.finishBatch(batch);
      this.poke();
    }
  }

  private async invokeCallback(
    batch: LeasedBatch<T>,
    msg: { seq: bigint; data: T },
  ): Promise<'ok' | 'failed' | 'timeout' | 'aborted'> {
    const abort = new AbortController();
    batch.currentAbort = abort;
    batch.pendingCalls++;
    let retired = false;
    let invalidate!: () => void;
    const invalidated = new Promise<'aborted'>(resolve => {
      invalidate = () => resolve('aborted');
    });
    batch.currentInvalidate = invalidate;
    let settled = false;
    let watched: Promise<'ok' | 'failed'>;
    try {
      watched = Promise.resolve(this.callback(msg.data, {
        seq: msg.seq,
        key: batch.key,
        signal: abort.signal,
      })).then(
        () => 'ok' as const,
        () => 'failed' as const,
      );
    } catch {
      watched = Promise.resolve<'failed'>('failed');
    }
    void watched.then(() => {
      settled = true;
      batch.pendingCalls--;
      if (retired) this.retiredInvocations--;
      if (this.retiredInvocations < this.concurrency && this.capacityTimer !== null) {
        clearTimeout(this.capacityTimer);
        this.capacityTimer = null;
      }
      if (batch.currentAbort === abort) batch.currentAbort = undefined;
      if (batch.currentInvalidate === invalidate) batch.currentInvalidate = undefined;
      this.maybeRelease(batch);
      this.poke();
    });
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<'timeout'>(resolve => {
      timer = setTimeout(() => resolve('timeout'), this.callbackTimeoutMs);
      timer.unref();
    });
    const result = await Promise.race([watched, timeout, invalidated]);
    if (timer !== undefined) clearTimeout(timer);
    if (result === 'timeout') {
      if (!settled) {
        retired = true;
        this.retiredInvocations++;
        this.assessCapacitySoon();
      }
      abort.abort();
      this.logger.error(
        `[${this.streamName}:${this.group}] Callback timed out after ${this.callbackTimeoutMs}ms at seq=${msg.seq}.`,
      );
    } else if (result === 'aborted') {
      if (!settled) {
        retired = true;
        this.retiredInvocations++;
        this.assessCapacitySoon();
      }
    } else if (result === 'failed') {
      this.logger.error(
        `[${this.streamName}:${this.group}] Processing error at seq=${msg.seq}. Batch retries after the lease wait.`,
      );
    }
    return result;
  }

  private finishBatch(batch: LeasedBatch<T>): void {
    if (batch.terminal || batch.dead) {
      this.maybeRelease(batch);
      return;
    }
    if (!batch.session.valid || this.fatalError !== null) {
      batch.dead = true;
      this.maybeRelease(batch);
      return;
    }
    const flags = batch.completed === batch.messages.length
      ? 0
      : FLAG_STREAM_S_UPDATE_BATCH_RELEASE;
    this.enqueueUpdate(batch, flags, true);
    this.maybeRelease(batch);
  }

  private releaseExecution(batch: LeasedBatch<T>): void {
    if (batch.keyId === undefined || batch.running || batch.pendingCalls > 0
      || this.executingKeys.get(batch.keyId) !== batch) return;
    this.executingKeys.delete(batch.keyId);
    const waiting = this.blockedKeys.get(batch.keyId);
    if (waiting !== undefined) {
      this.blockedKeys.delete(batch.keyId);
      if (!waiting.dead && !waiting.terminal && !waiting.settled && waiting.session.valid) {
        this.ready.push(waiting);
      }
    }
  }

  private maybeRelease(batch: LeasedBatch<T>): void {
    this.releaseExecution(batch);
    if (!batch.settled && !batch.dead) return;
    if (batch.keyId !== undefined && this.blockedKeys.get(batch.keyId) === batch) {
      this.blockedKeys.delete(batch.keyId);
    }
    if (batch.keyId !== undefined && this.ownedKeys.get(batch.keyId) === batch) {
      this.ownedKeys.delete(batch.keyId);
    }
    if (batch.running || batch.pendingCalls > 0) return;
    if (!this.batches.delete(batch.receipt)) return;
    this.bufferedMessages -= batch.messages.length;
    this.bufferedBytes -= batch.wireBytes;
    batch.messages.length = 0;
  }

  private drained(): boolean {
    if (!this.callsAbandoned && this.runningBatches > 0) return false;
    if (this.updateInflight || this.updateQueue.size > 0) return false;
    for (const b of this.batches.values()) {
      if (!b.settled && !b.dead && b.session.valid) return false;
    }
    return true;
  }

  private async waitDrainedUntil(deadlineMs: number): Promise<boolean> {
    while (!this.drained()) {
      const remaining = deadlineMs - performance.now();
      if (remaining <= 0) return this.drained();
      if (!(await this.waitForChange(remaining))) return this.drained();
    }
    return true;
  }

  private withDeadline<R>(promise: Promise<R>, ms: number): Promise<R> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('deadline exceeded')), ms);
      timer.unref();
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }

  private teardown(): Promise<void> {
    if (this.teardownDone === null) this.teardownDone = this.doTeardown();
    return this.teardownDone;
  }

  private async doTeardown(): Promise<void> {
    if (this.renewTimer !== null) {
      clearTimeout(this.renewTimer);
      this.renewTimer = null;
    }
    if (this.capacityTimer !== null) {
      clearTimeout(this.capacityTimer);
      this.capacityTimer = null;
    }
    const session = this.session;
    if (session !== null) this.invalidateSession(session);
    const joiner = this.joinPromise;
    if (joiner !== null) {
      const settled = await this.withDeadline(
        joiner.then(() => true, () => true),
        FETCH_TIMEOUT_MARGIN_MS,
      ).catch(() => 'timeout' as const);
      if (settled === 'timeout') {
        throw new Error('Stream join did not settle during teardown');
      }
    }
    if (this.cleanupPromise !== null) await this.cleanupPromise;
    const pending = this.inflightFetch;
    if (pending !== null) {
      const settled = await this.withDeadline(
        pending.then(() => true, () => true),
        FETCH_TIMEOUT_MARGIN_MS,
      ).catch(() => 'timeout' as const);
      if (settled === 'timeout') {
        throw new Error('Stream fetch did not settle during teardown');
      }
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
    const keyBatchSize = options.keyBatchSize ?? DEFAULT_CONFIG.stream.keyBatchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.stream.waitMs;
    const concurrency = options.concurrency ?? DEFAULT_CONFIG.stream.concurrency;
    const stopTimeoutMs = options.stopTimeoutMs ?? DEFAULT_CONFIG.stream.stopTimeoutMs;
    const callbackTimeoutMs = options.callbackTimeoutMs ?? DEFAULT_CONFIG.stream.callbackTimeoutMs;
    const maxBufferedMessages = options.maxBufferedMessages ?? 2 * batchSize;
    const maxBufferedBytes = options.maxBufferedBytes ?? DEFAULT_CONFIG.stream.maxBufferedBytes;
    const positiveTimer = (v: number) => Number.isInteger(v) && v >= 1 && v <= 0x7fffffff;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > STREAM_MAX_FETCH_BATCH_SIZE) {
      throw new Error(`batchSize must be an integer between 1 and ${STREAM_MAX_FETCH_BATCH_SIZE}`);
    }
    if (!Number.isInteger(keyBatchSize) || keyBatchSize < 1 || keyBatchSize > STREAM_MAX_KEY_BATCH_SIZE) {
      throw new Error(`keyBatchSize must be an integer between 1 and ${STREAM_MAX_KEY_BATCH_SIZE}`);
    }
    if (!positiveTimer(waitMs)) throw new Error('waitMs must be a positive integer');
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > STREAM_MAX_FETCH_BATCH_SIZE) {
      throw new Error(`concurrency must be an integer between 1 and ${STREAM_MAX_FETCH_BATCH_SIZE}`);
    }
    if (!positiveTimer(stopTimeoutMs)) {
      throw new Error('stopTimeoutMs must be a positive integer');
    }
    if (!positiveTimer(callbackTimeoutMs)) {
      throw new Error('callbackTimeoutMs must be a positive integer');
    }
    if (
      !Number.isInteger(maxBufferedMessages)
      || maxBufferedMessages < 1
      || maxBufferedMessages > 2 * STREAM_MAX_FETCH_BATCH_SIZE
    ) {
      throw new Error(`maxBufferedMessages must be an integer between 1 and ${2 * STREAM_MAX_FETCH_BATCH_SIZE}`);
    }
    if (!Number.isInteger(maxBufferedBytes) || maxBufferedBytes < 54 || maxBufferedBytes > 0xffffffff) {
      throw new Error('maxBufferedBytes must be an integer between 54 and 4294967295');
    }

    const sub = new StreamSubscription<T>(
      this.conn,
      this.streamName,
      this.name,
      this.logger,
      callback,
      batchSize,
      keyBatchSize,
      waitMs,
      concurrency,
      stopTimeoutMs,
      callbackTimeoutMs,
      maxBufferedMessages,
      maxBufferedBytes,
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
