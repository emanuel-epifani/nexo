import * as net from 'net';
import { logger } from './utils/logger';
import { FrameType, ResponseStatus, Opcode, NexoOptions } from './protocol';
import { Cursor, FrameCodec } from './codec';

/** @internal */
export class NexoConnection {
  private socket: net.Socket;
  public isConnected = false;
  private nextId = 1;
  private pending = new Map<number, { 
    resolve: (res: { status: number, data: Buffer }) => void, 
    reject: (err: Error) => void,
    ts: number 
  }>();
  private readonly requestTimeoutMs: number;
  private readonly timeoutInterval?: NodeJS.Timeout;
  
  public onPush?: (topic: string, data: any) => void;

  private buffer: Buffer = Buffer.alloc(0);
  private chunks: Buffer[] = [];

  constructor(private host: string, private port: number, options: NexoOptions = {}) {
    this.socket = new net.Socket();
    this.socket.setNoDelay(true);
    this.requestTimeoutMs = options.requestTimeoutMs ?? 10000;
    this.setupListeners();
    this.timeoutInterval = setInterval(() => this.checkTimeouts(), this.requestTimeoutMs);
    if (this.timeoutInterval.unref) this.timeoutInterval.unref();
  }

  private checkTimeouts() {
    const now = Date.now();
    for (const [id, req] of this.pending) {
      if (now - req.ts > this.requestTimeoutMs) {
        this.pending.delete(id);
        req.reject(new Error(`Request timeout after ${this.requestTimeoutMs}ms`));
      }
    }
  }

  async connect(): Promise<void> {
    return new Promise((res, rej) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true; res();
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
      this.isConnected = false;
      this.pending.clear();
      this.chunks = [];
    };

    this.socket.on('error', (err) => logger.error("Socket error", err));
    this.socket.on('close', cleanup);
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

  send(opcode: Opcode, ...args: Buffer[]): Promise<{ status: ResponseStatus, cursor: Cursor }> {
    const id = this.nextId++;
    if (this.nextId === 0) this.nextId = 1;

    const packet = FrameCodec.packRequest(id, opcode, ...args);

    return new Promise((resolve, reject) => {
      if (!this.isConnected) {
        return reject(new Error("Client not connected"));
      }

      this.pending.set(id, {
        resolve: (res) => {
          if (res.status === ResponseStatus.ERR) {
            const errCursor = new Cursor(res.data);
            const errMsg = errCursor.readString();
            // Silence common expected errors
            if (!errMsg.includes('FENCED') && !errMsg.includes('REBALANCE') && !errMsg.includes('not found')) {
              logger.error(`<- ERROR ${Opcode[opcode]} (${errMsg})`);
            }
            reject(new Error(errMsg));
          } else {
            resolve({ status: res.status, cursor: new Cursor(res.data) });
          }
        },
        reject,
        ts: Date.now()
      });

      this.socket.write(packet);
    });
  }

  disconnect() {
    if (this.timeoutInterval) clearInterval(this.timeoutInterval);
    this.socket.destroy();
    this.isConnected = false;
  }
}
