import { NexoConnection } from '../connection';
import { ResponseStatus } from '../protocol';
import { FrameCodec } from '../codec';

export enum StoreOpcode {
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,
}

export const StoreCommands = {
  kvSet: (conn: NexoConnection, key: string, value: any, options: StoreSetOptions) =>
    conn.send(
      StoreOpcode.KV_SET,
      FrameCodec.string(key),
      FrameCodec.string(JSON.stringify(options || {})),
      FrameCodec.any(value)
    ),

  kvGet: async (conn: NexoConnection, key: string) => {
    const res = await conn.send(StoreOpcode.KV_GET, FrameCodec.string(key));
    if (res.status === ResponseStatus.NULL) return null;
    return FrameCodec.decodeAny(res.cursor);
  },

  kvDel: (conn: NexoConnection, key: string) =>
    conn.send(StoreOpcode.KV_DEL, FrameCodec.string(key)),
};

export interface StoreSetOptions {
  ttl?: number;
}

export class NexoKV {
  constructor(private conn: NexoConnection) { }

  async set(key: string, value: any, ttlSeconds?: number): Promise<void> {
    const options: StoreSetOptions = ttlSeconds ? { ttl: ttlSeconds } : {};
    await StoreCommands.kvSet(this.conn, key, value, options);
  }

  async get<T = any>(key: string): Promise<T | null> {
    return StoreCommands.kvGet(this.conn, key);
  }

  async del(key: string): Promise<void> {
    await StoreCommands.kvDel(this.conn, key);
  }
}

export class NexoStore {
  public readonly kv: NexoKV;
  constructor(conn: NexoConnection) {
    this.kv = new NexoKV(conn);
  }
}
