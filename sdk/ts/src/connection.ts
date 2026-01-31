import * as net from 'net';
import { EventEmitter } from 'events';
import { logger } from './utils/logger';
import { FrameType, ResponseStatus } from './protocol';
import type { NexoOptions } from './client';
import { Cursor, FrameCodec } from './codec';
import { ConnectionClosedError, NotConnectedError, RequestTimeoutError } from './errors';

/** @internal */
export class NexoConnection extends EventEmitter {
  public socket: net.Socket;
  public isConnected = false;
  private nextId = 1;
  private pending = new Map<number, {
    resolve: (res: { status: number, data: Buffer }) => void,
    reject: (err: Error) => void,
    timer: NodeJS.Timeout
  }>();
  private readonly requestTimeoutMs: number;

  public onPush?: (topic: string, data: any) => void;

  private buffer: Buffer = Buffer.alloc(0);
  private chunks: Buffer[] = [];

  private readonly host: string;
  private readonly port: number;

  private shouldReconnect = true;
  private isReconnecting = false;

  constructor(options: NexoOptions) {
    super();
    this.host = options.host;
    this.port = options.port;
    this.socket = new net.Socket();
    this.requestTimeoutMs = options.requestTimeoutMs ?? 10000;
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
      this.processBuffer();
    });

    const cleanup = (err: any) => {
      const wasConnected = this.isConnected;
      this.isConnected = false;

      if (wasConnected || this.isReconnecting) {
        logger.error(`[Connection] SOCKET CLOSED. Error: ${err ? err.message : 'Clean close'}. Reconnecting: ${this.shouldReconnect}`);
      }

      this.pending.forEach(p => {
        clearTimeout(p.timer);
        p.reject(new ConnectionClosedError());
      });
      this.pending.clear();
      this.chunks = [];
      this.buffer = Buffer.alloc(0);

      if (this.shouldReconnect && !this.isReconnecting) {
        this.startReconnectLoop();
      }
    };

    this.socket.on('error', (err) => logger.error("Socket error", err));
    this.socket.on('close', cleanup);
  }

  private async startReconnectLoop() {
    this.isReconnecting = true;
    logger.warn("⚠️ Connection lost. Attempting to reconnect...");

    while (this.shouldReconnect && !this.isConnected) {
      await new Promise(r => setTimeout(r, 1500));

      try {
        await this.createSocketAndConnect();
        logger.info("✅ Reconnected to Nexo Server");
        this.isReconnecting = false;
        this.emit('reconnect');
      } catch (e) {
        // Retry silently
      }
    }
  }

  private processBuffer() {
    if (this.chunks.length > 0) {
      if (this.buffer.length > 0) {
        this.buffer = Buffer.concat([this.buffer, ...this.chunks]);
      } else {
        this.buffer = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks);
      }
      this.chunks = [];
    }

    while (true) {
      // Need at least header (9 bytes)
      if (this.buffer.length < 9) break;

      const payloadLen = this.buffer.readUInt32BE(5);
      const totalFrameLen = 9 + payloadLen;

      if (this.buffer.length < totalFrameLen) break;

      // 2. Slice Frame
      const frame = this.buffer.subarray(0, totalFrameLen);
      this.buffer = this.buffer.subarray(totalFrameLen); // Advance buffer

      this.handleFrame(frame);
    }
  }

  private handleFrame(frame: Buffer) {
    const cursor = new Cursor(frame);
    const type = cursor.readU8();
    const id = cursor.readU32();
    cursor.readU32(); // Skip payloadLen

    const payload = cursor.buf.subarray(cursor.offset);

    switch (type) {
      case FrameType.RESPONSE: {
        const req = this.pending.get(id);
        if (req) {
          this.pending.delete(id);
          // Payload: [Status:1][Data...]
          req.resolve({ status: payload[0], data: payload.subarray(1) });
        }
        break;
      }
      case FrameType.PUSH: {
        if (this.onPush) {
          const pushCursor = new Cursor(payload);
          const topic = pushCursor.readString();
          const data = FrameCodec.decodeAny(pushCursor);
          this.onPush(topic, data);
        }
        break;
      }
      case FrameType.ERROR:
        logger.error('Received Protocol Error');
        break;
      case FrameType.PING:
        // Optional: Send PONG
        break;
    }
  }

  send(opcode: number, ...args: Buffer[]): Promise<{ status: ResponseStatus, cursor: Cursor }> {
    const id = this.nextId++;
    if (this.nextId === 0) this.nextId = 1;

    const packet = FrameCodec.packRequest(id, opcode, ...args);

    return new Promise((resolve, reject) => {
      if (!this.isConnected) {
        return reject(new NotConnectedError());
      }

      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new RequestTimeoutError(this.requestTimeoutMs));
      }, this.requestTimeoutMs);

      this.pending.set(id, {
        resolve: (res) => {
          clearTimeout(timer);
          if (res.status === ResponseStatus.ERR) {
            const errCursor = new Cursor(res.data);
            const errMsg = errCursor.readString();
            // Silence common expected errors
            if (!errMsg.includes('FENCED') && !errMsg.includes('REBALANCE') && !errMsg.includes('not found')) {
              logger.error(`<- ERROR 0x${opcode.toString(16).padStart(2, '0')} (${errMsg})`);
            }
            reject(new Error(errMsg));
          } else {
            resolve({ status: res.status, cursor: new Cursor(res.data) });
          }
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
        timer
      });

      this.socket.write(packet);
    });
  }

  disconnect() {
    this.shouldReconnect = false;
    this.isReconnecting = false;
    for (const req of this.pending.values()) {
      clearTimeout(req.timer);
    }
    this.pending.clear();
    this.socket.destroy();
    this.isConnected = false;
  }
}
