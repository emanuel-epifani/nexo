import { NexoConnection } from '../connection';
import { ResponseStatus } from '../protocol';
import { FrameCodec } from '../codec';

export enum StoreOpcode {
  MAP_SET = 0x02,
  MAP_GET = 0x03,
  MAP_DEL = 0x04,
}

export const StoreCommands = {
  mapSet: (conn: NexoConnection, key: string, value: any, options: MapSetOptions) =>
    conn.send(
      StoreOpcode.MAP_SET,
      FrameCodec.string(key),
      FrameCodec.string(JSON.stringify(options || {})),
      FrameCodec.any(value)
    ),

  mapGet: async (conn: NexoConnection, key: string) => {
    const res = await conn.send(StoreOpcode.MAP_GET, FrameCodec.string(key));
    if (res.status === ResponseStatus.NULL) return null;
    return FrameCodec.decodeAny(res.cursor);
  },

  mapDel: (conn: NexoConnection, key: string) =>
    conn.send(StoreOpcode.MAP_DEL, FrameCodec.string(key)),
};

export interface MapSetOptions {
  ttl?: number;
}

export class NexoMap {
  constructor(private conn: NexoConnection) { }

  async set(key: string, value: any, ttlSeconds?: number): Promise<void> {
    const options: MapSetOptions = ttlSeconds ? { ttl: ttlSeconds } : {};
    await StoreCommands.mapSet(this.conn, key, value, options);
  }

  async get<T = any>(key: string): Promise<T | null> {
    return StoreCommands.mapGet(this.conn, key);
  }

  async del(key: string): Promise<void> {
    await StoreCommands.mapDel(this.conn, key);
  }
}

export class NexoStore {
  public readonly map: NexoMap;
  constructor(conn: NexoConnection) {
    this.map = new NexoMap(conn);
  }
}
