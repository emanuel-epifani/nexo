import { NexoConnection } from '../connection';
import { Opcode, ResponseStatus } from '../protocol';
import { FrameCodec } from '../codec';

export class NexoKV {
  constructor(private conn: NexoConnection) { }

  async set(key: string, value: any, ttlSeconds = 0): Promise<void> {
    await this.conn.send(
      Opcode.KV_SET,
      FrameCodec.u64(ttlSeconds),
      FrameCodec.string(key),
      FrameCodec.any(value)
    );
  }

  async get<T = any>(key: string): Promise<T | null> {
    const res = await this.conn.send(Opcode.KV_GET, FrameCodec.string(key));
    if (res.status === ResponseStatus.NULL) return null;
    return FrameCodec.decodeAny(res.cursor);
  }

  async del(key: string): Promise<void> {
    await this.conn.send(Opcode.KV_DEL, FrameCodec.string(key));
  }
}

export class NexoStore {
  public readonly kv: NexoKV;
  constructor(conn: NexoConnection) {
    this.kv = new NexoKV(conn);
  }
}
