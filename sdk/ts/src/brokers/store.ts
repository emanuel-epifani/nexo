import { NexoConnection } from '../transport/tcp/connection';
import { FLAG_STORE_MAP_SET_HAS_TTL, ResponseStatus, StoreOpcode } from '../protocol/generated';

const StoreCommands = {
  mapSet: (conn: NexoConnection, key: string, value: any, options: MapSetOptions) => {
    const hasTtl = options?.ttl !== undefined;
    const flags = hasTtl ? FLAG_STORE_MAP_SET_HAS_TTL : 0x00;
    return conn.send(StoreOpcode.MAP_SET, w => {
      w.string(key).u8(flags);
      if (hasTtl) w.u64(options!.ttl!);
      w.any(value);
    });
  },

  mapGet: async (conn: NexoConnection, key: string) => {
    const res = await conn.send(StoreOpcode.MAP_GET, w => w.string(key));
    if (res.status === ResponseStatus.NULL) return null;
    return res.cursor.decodeAny();
  },

  mapDel: (conn: NexoConnection, key: string) =>
    conn.send(StoreOpcode.MAP_DEL, w => w.string(key)),

  mapIncr: async (conn: NexoConnection, key: string, delta: number) => {
    const res = await conn.send(StoreOpcode.MAP_INCR, w => w.string(key).i64(delta));
    if (res.status === ResponseStatus.DATA) {
      return res.cursor.decodeAny() as number;
    }
    throw new Error(res.cursor.readString());
  },

  mapClearAll: async (conn: NexoConnection) => {
    const res = await conn.send(StoreOpcode.MAP_CLEAR_ALL, () => { });
    if (res.status === ResponseStatus.DATA) {
      return res.cursor.decodeAny() as number;
    }
    throw new Error(res.cursor.readString());
  },

  mapClearPrefix: async (conn: NexoConnection, prefix: string) => {
    const res = await conn.send(StoreOpcode.MAP_CLEAR_PREFIX, w => w.string(prefix));
    if (res.status === ResponseStatus.DATA) {
      return res.cursor.decodeAny() as number;
    }
    throw new Error(res.cursor.readString());
  },
};

export interface MapSetOptions {
  ttl?: number;
}

export class NexoMap {
  constructor(private conn: NexoConnection) { }

  async set(key: string, value: any, options: MapSetOptions = {}): Promise<void> {
    await StoreCommands.mapSet(this.conn, key, value, options);
  }

  async get<T = any>(key: string): Promise<T | null> {
    return StoreCommands.mapGet(this.conn, key);
  }

  async delete(key: string): Promise<void> {
    await StoreCommands.mapDel(this.conn, key);
  }

  async incr(key: string, delta: number = 1): Promise<number> {
    return StoreCommands.mapIncr(this.conn, key, delta);
  }

  async clearAll(): Promise<number> {
    return StoreCommands.mapClearAll(this.conn);
  }

  async clearWithPrefix(prefix: string): Promise<number> {
    return StoreCommands.mapClearPrefix(this.conn, prefix);
  }
}

export class NexoStore {
  public readonly map: NexoMap;
  constructor(conn: NexoConnection) {
    this.map = new NexoMap(conn);
  }
}
