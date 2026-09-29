import * as net from 'net';
import { EventEmitter } from 'events';
import { Logger } from '../../utils/logger';
import { NexoConnectionConfig } from '../../config';
import { ErrorCode, FrameType, ResponseStatus, PROTOCOL_VERSION, HEADER_SIZE, HEADER_OFFSET } from '../../protocol/generated';
import { Cursor, FrameWriter } from '../../protocol/codec';
import { ConnectionClosedError, decodeErrorPayload, NotConnectedError, RequestTimeoutError, RequestCancelledError } from '../../errors';

/** @internal */
export class NexoConnection extends EventEmitter {
  public socket: net.Socket;
  public isConnected = false;
  private nextId = 1;
  private pending = new Map<number, {
    resolve: (res: { status: number, data: Buffer }) => void,
    reject: (err: Error) => void,
    timer: NodeJS.Timeout,
  }>();
  private readonly config: NexoConnectionConfig;
  private readonly logger: Logger;

  public onPush?: (topic: string, data: any) => void;

  // Unconsumed inbound bytes: chunks[0][chunkOffset..] plus the rest of chunks.
  // chunkOffset avoids subarray-ing the head chunk on every partial consume.
  private chunks: Buffer[] = [];
  private chunkOffset = 0;
  private chunksLen = 0;
  private readonly headerScratch = Buffer.allocUnsafe(HEADER_SIZE);

  private readonly host: string;
  private readonly port: number;

  private shouldReconnect = true;
  private isReconnecting = false;

  // Reused across calls; the underlying byte buffer is allocated fresh on each
  // begin() (see FrameWriter docs).
  private readonly writer = new FrameWriter();

  constructor(config: NexoConnectionConfig, logger: Logger) {
    super();
    this.config = config;
    this.host = config.host;
    this.port = config.port;
    this.logger = logger;
    this.socket = new net.Socket();
  }

  async connect(): Promise<void> {
    this.shouldReconnect = true;
    return this.createSocketAndConnect();
  }

  private createSocketAndConnect(): Promise<void> {
    if (this.socket.destroyed || this.socket.connecting) {
      this.socket.removeAllListeners();
      this.socket = new net.Socket();
    }

    // TCP tuning:
    // - noDelay disables Nagle's algorithm. On real networks this prevents
    //   Nagle/delayed-ACK interactions that can add up to 40ms to sporadic
    //   small writes (industry standard for RPC clients). On loopback the
    //   effect is neutral-to-slightly-negative on median but improves MAX.
    // - keepAlive makes the kernel probe idle connections so dead sockets
    //   (NAT/LB idle timeouts) are detected in seconds instead of minutes.
    this.socket.setNoDelay(true);
    this.socket.setKeepAlive(true, 30_000);

    this.setupListeners();

    return new Promise((res, rej) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true;
        res();
      });
      this.socket.once('error', rej);
    });
  }

  private setupListeners() {
    this.socket.on('data', (chunk) => {
      this.chunks.push(chunk);
      this.chunksLen += chunk.length;
      this.processBuffer();
    });

    const cleanup = (err: any) => {
      const wasConnected = this.isConnected;
      this.isConnected = false;

      if (wasConnected || this.isReconnecting) {
        this.logger.error(`[Connection] SOCKET CLOSED. Error: ${err ? err.message : 'Clean close'}. Reconnecting: ${this.shouldReconnect}`);
      }

      this.pending.forEach(p => {
        clearTimeout(p.timer);
        p.reject(new ConnectionClosedError());
      });
      this.pending.clear();
      this.chunks = [];
      this.chunkOffset = 0;
      this.chunksLen = 0;

      if (this.shouldReconnect && !this.isReconnecting) {
        this.startReconnectLoop();
      }
    };

    this.socket.on('error', (err) => this.logger.error("Socket error", err));
    this.socket.on('close', cleanup);
  }

  private async startReconnectLoop() {
    this.isReconnecting = true;
    this.logger.warn("⚠️ Connection lost. Attempting to reconnect...");

    while (this.shouldReconnect && !this.isConnected) {
      await new Promise(r => setTimeout(r, this.config.reconnectDelayMs));

      try {
        await this.createSocketAndConnect();
        this.logger.info("✅ Reconnected to Nexo Server");
        this.isReconnecting = false;
        this.emit('reconnect');
      } catch (e) {
        // Retry silently
      }
    }
  }

  private processBuffer() {
    while (this.chunksLen >= HEADER_SIZE) {
      const totalFrameLen = HEADER_SIZE + this.readPayloadLen();
      if (this.chunksLen < totalFrameLen) break;
      this.handleFrame(this.takeFrame(totalFrameLen));
    }
  }

  // Header (11 bytes): [Version:1][Type:1][Meta:1][ID:4][Len:4]. Only copy it
  // into scratch when it straddles a chunk boundary.
  private readPayloadLen(): number {
    const first = this.chunks[0];
    if (first.length - this.chunkOffset >= HEADER_SIZE) {
      return first.readUInt32BE(this.chunkOffset + HEADER_OFFSET.PAYLOAD_LEN);
    }
    this.copyBuffered(this.headerScratch, HEADER_SIZE);
    return this.headerScratch.readUInt32BE(HEADER_OFFSET.PAYLOAD_LEN);
  }

  private takeFrame(len: number): Buffer {
    const first = this.chunks[0];
    let frame: Buffer;
    if (first.length - this.chunkOffset >= len) {
      frame = first.subarray(this.chunkOffset, this.chunkOffset + len);
    } else {
      frame = Buffer.allocUnsafe(len);
      this.copyBuffered(frame, len);
    }
    this.consumeBytes(len);
    return frame;
  }

  private copyBuffered(dest: Buffer, len: number) {
    let written = 0;
    let off = this.chunkOffset;
    for (let i = 0; written < len; i++) {
      const chunk = this.chunks[i];
      const take = Math.min(chunk.length - off, len - written);
      chunk.copy(dest, written, off, off + take);
      written += take;
      off = 0;
    }
  }

  private consumeBytes(n: number) {
    this.chunksLen -= n;
    let consumed = 0;
    let off = this.chunkOffset;
    while (n > 0) {
      const avail = this.chunks[consumed].length - off;
      if (n < avail) {
        off += n;
        n = 0;
      } else {
        n -= avail;
        off = 0;
        consumed++;
      }
    }
    if (consumed > 0) this.chunks.splice(0, consumed);
    this.chunkOffset = off;
  }

  private handleFrame(frame: Buffer) {
    const version = frame.readUInt8(HEADER_OFFSET.VERSION);
    if (version !== PROTOCOL_VERSION) {
      this.logger.error(`Unsupported protocol version: 0x${version.toString(16).padStart(2, '0')} (expected 0x${PROTOCOL_VERSION.toString(16).padStart(2, '0')})`);
      return;
    }
    const type = frame.readUInt8(HEADER_OFFSET.TYPE);
    const meta = frame.readUInt8(HEADER_OFFSET.META);
    const id = frame.readUInt32BE(HEADER_OFFSET.ID);
    const payload = frame.subarray(HEADER_SIZE);

    switch (type) {
      case FrameType.RESPONSE: {
        const req = this.pending.get(id);
        if (req) {
          this.pending.delete(id);
          // Status is in header (byte 1), payload is clean data
          req.resolve({ status: meta, data: payload });
        }
        break;
      }
      case FrameType.PUSH_PUBSUB: {
        if (this.onPush) {
          const pushCursor = new Cursor(payload);
          const topic = pushCursor.readString();
          const data = pushCursor.decodeAny();
          this.onPush(topic, data);
        }
        break;
      }
      default:
        this.logger.warn(`Unknown frame type: 0x${type.toString(16).padStart(2, '0')}`);
    }
  }

  send(
    opcode: number,
    build?: (w: FrameWriter) => void,
    options?: { timeoutMs?: number; signal?: AbortSignal }
  ): Promise<{ status: ResponseStatus, cursor: Cursor }> {
    const id = this.nextId;
    this.nextId = (this.nextId + 1) & 0xFFFFFFFF || 1;

    this.writer.begin();
    if (build) build(this.writer);
    const packet = this.writer.finish(id, opcode);

    const timeoutMs = options?.timeoutMs ?? this.config.requestTimeoutMs;
    const signal = options?.signal;

    return new Promise((resolve, reject) => {
      if (!this.isConnected) {
        return reject(new NotConnectedError());
      }
      if (signal?.aborted) {
        return reject(new RequestCancelledError());
      }

      const timer = setTimeout(() => {
        if (this.pending.delete(id)) {
          cleanupAbort();
          reject(new RequestTimeoutError(timeoutMs));
        }
      }, timeoutMs);
      timer.unref();

      const onAbort = () => {
        if (this.pending.delete(id)) {
          clearTimeout(timer);
          reject(new RequestCancelledError());
        }
      };
      const cleanupAbort = () => signal?.removeEventListener('abort', onAbort);
      signal?.addEventListener('abort', onAbort, { once: true });

      this.pending.set(id, {
        resolve: (res) => {
          clearTimeout(timer);
          cleanupAbort();
          if (res.status === ResponseStatus.ERR) {
            const error = decodeErrorPayload(res.data);
            // Silence common expected errors by their stable protocol code.
            if (error.code !== ErrorCode.FENCED && error.code !== ErrorCode.NOT_MEMBER && error.code !== ErrorCode.RESOURCE_NOT_FOUND) {
              this.logger.error(`<- ERROR 0x${opcode.toString(16).padStart(2, '0')} (${error.message})`);
            }
            reject(error);
            return;
          }
          resolve({ status: res.status, cursor: new Cursor(res.data) });
        },
        reject,
        timer,
      });

      this.socket.write(packet);
    });
  }

  /**
   * Send a command without waiting for the server's response.
   * Uses FrameType.REQUEST_NO_RESPONSE so the server processes the command
   * but does not send a response frame at all (no wasted encode/send/decode).
   * Used for ack/nack where fire-and-forget is acceptable.
   */
  sendFireAndForget(opcode: number, build?: (w: FrameWriter) => void): void {
    if (!this.isConnected) return;

    const id = this.nextId;
    this.nextId = (this.nextId + 1) & 0xFFFFFFFF || 1;

    this.writer.begin();
    if (build) build(this.writer);
    const packet = this.writer.finish(id, opcode, FrameType.REQUEST_NO_RESPONSE);
    this.socket.write(packet);
  }

  disconnect() {
    this.shouldReconnect = false;
    this.isReconnecting = false;
    this.pending.forEach(p => {
      clearTimeout(p.timer);
      p.reject(new ConnectionClosedError());
    });
    this.pending.clear();
    this.socket.destroy();
    this.isConnected = false;
  }
}
