import { DEFAULT_CONFIG } from '../config';
import { SlowConsumerError } from '../errors';
import { NexoConnection } from '../transport/tcp/connection';
import { Logger } from '../utils/logger';
import { Subscription } from '../subscription';
import { FLAG_PUBSUB_PUB_CLEAR, FLAG_PUBSUB_PUB_HAS_TTL, FLAG_PUBSUB_PUB_RETAIN, PubSubOpcode } from '../protocol/generated';

const PubSubCommands = {
  publish: (conn: NexoConnection, topic: string, data: any, options: PublishOptions) => {
    const retain = options.retain === true;
    if (options.ttl !== undefined && (options.ttl < 0 || !Number.isInteger(options.ttl))) {
      throw new Error(`[PubSub] Invalid ttl: ${options.ttl}`);
    }
    const hasTtl = options.ttl !== undefined;
    const flags = (retain ? FLAG_PUBSUB_PUB_RETAIN : 0x00) | (hasTtl ? FLAG_PUBSUB_PUB_HAS_TTL : 0x00);
    return conn.send(PubSubOpcode.PUB, w => {
      w.string(topic).u8(flags);
      if (hasTtl) w.u32(options.ttl!);
      w.any(data);
    });
  },

  clear: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.PUB, w => w.string(topic).u8(FLAG_PUBSUB_PUB_CLEAR).any(Buffer.alloc(0))),

  subscribe: (conn: NexoConnection, pattern: string) =>
    conn.send(PubSubOpcode.SUB, w => w.string(pattern)),

  unsubscribe: (conn: NexoConnection, pattern: string) =>
    conn.send(PubSubOpcode.UNSUB, w => w.string(pattern)),
};

export interface PublishOptions {
  retain?: boolean;
  ttl?: number;
}

export interface PubSubSubscribeOptions {
  queueCapacity?: number;
}

export interface PubSubMessageMeta {
  topic: string;
}

export type PubSubHandler<T> = (data: T, meta: PubSubMessageMeta) => unknown | Promise<unknown>;

interface PatternEntry {
  readonly pattern: string;
  readonly wildcard: boolean;
  readonly listeners: Map<number, LocalListener<any>>;
  wireSubscribed: boolean;
  transition: Promise<void>;
}

class TrieNode {
  readonly literals = new Map<string, TrieNode>();
  plus: TrieNode | null = null;
  terminal: PatternEntry | null = null;
  hash: PatternEntry | null = null;
}

class PatternTrie {
  private readonly root = new TrieNode();

  insert(pattern: string, entry: PatternEntry): void {
    const parts = pattern.split('/');
    let node = this.root;
    for (const part of parts) {
      if (part === '#') {
        node.hash = entry;
        return;
      }
      if (part === '+') {
        if (node.plus === null) node.plus = new TrieNode();
        node = node.plus;
      } else {
        let child = node.literals.get(part);
        if (!child) {
          child = new TrieNode();
          node.literals.set(part, child);
        }
        node = child;
      }
    }
    node.terminal = entry;
  }

  remove(pattern: string, entry: PatternEntry): void {
    const parts = pattern.split('/');
    const path: { parent: TrieNode; part: string; node: TrieNode }[] = [];
    let node = this.root;
    for (const part of parts) {
      if (part === '#') {
        if (node.hash === entry) node.hash = null;
        this.prune(path);
        return;
      }
      const child = part === '+' ? node.plus : node.literals.get(part);
      if (!child) return;
      path.push({ parent: node, part, node: child });
      node = child;
    }
    if (node.terminal === entry) node.terminal = null;
    this.prune(path);
  }

  match(topic: string, visit: (entry: PatternEntry) => void): void {
    const parts = topic.split('/');
    const stack: { node: TrieNode; depth: number }[] = [{ node: this.root, depth: 0 }];
    while (stack.length > 0) {
      const current = stack.pop()!;
      if (current.node.hash) visit(current.node.hash);
      if (current.depth === parts.length) {
        if (current.node.terminal) visit(current.node.terminal);
        continue;
      }
      const literal = current.node.literals.get(parts[current.depth]);
      if (literal) stack.push({ node: literal, depth: current.depth + 1 });
      if (current.node.plus) stack.push({ node: current.node.plus, depth: current.depth + 1 });
    }
  }

  private prune(path: { parent: TrieNode; part: string; node: TrieNode }[]): void {
    for (let index = path.length - 1; index >= 0; index--) {
      const { parent, part, node } = path[index];
      if (node.terminal || node.hash || node.plus || node.literals.size > 0) return;
      if (part === '+') parent.plus = null;
      else parent.literals.delete(part);
    }
  }
}

class LocalListener<T> {
  active = true;
  failure: SlowConsumerError | null = null;
  private readonly queue: Array<{ data: T; meta: PubSubMessageMeta } | undefined> = [];
  private head = 0;
  private tail = 0;
  private size = 0;
  private running = false;
  private resolveClosed!: () => void;
  readonly closed: Promise<void>;

  constructor(
    private readonly handler: PubSubHandler<T>,
    private readonly capacity: number,
    private readonly logger: Logger,
    private readonly onOverflow: (error: SlowConsumerError) => void,
  ) {
    this.closed = new Promise(resolve => {
      this.resolveClosed = resolve;
    });
  }

  enqueue(data: T, meta: PubSubMessageMeta): void {
    if (!this.active) return;
    if (this.size === this.capacity) {
      const error = new SlowConsumerError(`PubSub listener queue exceeded capacity ${this.capacity}`);
      this.failure = error;
      this.deactivate();
      this.onOverflow(error);
      return;
    }

    this.queue[this.tail] = { data, meta };
    this.tail = (this.tail + 1) % this.capacity;
    this.size++;
    if (!this.running) {
      this.running = true;
      void this.consume();
    }
  }

  deactivate(): void {
    if (!this.active) return;
    this.active = false;
    this.queue.length = 0;
    this.head = 0;
    this.tail = 0;
    this.size = 0;
    if (!this.running) this.resolveClosed();
  }

  private async consume(): Promise<void> {
    while (this.active && this.size > 0) {
      const message = this.queue[this.head]!;
      this.queue[this.head] = undefined;
      this.head = (this.head + 1) % this.capacity;
      this.size--;
      try {
        await this.handler(message.data, message.meta);
      } catch (error) {
        this.logger.error('[PubSub] handler error', error);
      }
    }
    this.running = false;
    if (!this.active) this.resolveClosed();
  }
}

export class NexoTopic<T = any> {
  constructor(private readonly broker: NexoPubSub, public readonly name: string) { }

  publish(data: T, options: PublishOptions = {}): Promise<void> {
    return this.broker.publish(this.name, data, options);
  }

  clearRetained(): Promise<void> {
    return this.broker.clearRetained(this.name);
  }

  subscribe(callback: PubSubHandler<T>, options: PubSubSubscribeOptions = {}): Promise<Subscription> {
    return this.broker.subscribe(this.name, callback, options);
  }
}

export class NexoPattern<T = any> {
  constructor(private readonly broker: NexoPubSub, public readonly pattern: string) { }

  subscribe(callback: PubSubHandler<T>, options: PubSubSubscribeOptions = {}): Promise<Subscription> {
    return this.broker.subscribe(this.pattern, callback, options);
  }
}

export class NexoPubSub {
  private readonly entries = new Map<string, PatternEntry>();
  private readonly exact = new Map<string, PatternEntry>();
  private readonly wildcard = new PatternTrie();
  private nextListenerId = 1;

  constructor(private readonly conn: NexoConnection, private readonly logger: Logger) {
    conn.onPush = (topic, data) => this.dispatch(topic, data);

    conn.on('reconnect', () => {
      void this.restoreSubscriptions();
    });
  }

  topic<T = any>(concrete: string): NexoTopic<T> {
    NexoPubSub.validateTopic(concrete);
    return new NexoTopic<T>(this, concrete);
  }

  pattern<T = any>(wildcard: string): NexoPattern<T> {
    NexoPubSub.validatePattern(wildcard);
    return new NexoPattern<T>(this, wildcard);
  }

  async publish(topic: string, data: any, options: PublishOptions): Promise<void> {
    await PubSubCommands.publish(this.conn, topic, data, options);
  }

  async clearRetained(topic: string): Promise<void> {
    await PubSubCommands.clear(this.conn, topic);
  }

  async subscribe<T>(pattern: string, callback: PubSubHandler<T>, options: PubSubSubscribeOptions): Promise<Subscription> {
    const capacity = options.queueCapacity ?? DEFAULT_CONFIG.pubsub.listenerQueueCapacity;
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new Error(`queueCapacity must be a positive integer, got ${capacity}`);
    }

    let entry = this.entries.get(pattern);
    if (!entry) {
      entry = {
        pattern,
        wildcard: NexoPubSub.isWildcard(pattern),
        listeners: new Map(),
        wireSubscribed: false,
        transition: Promise.resolve(),
      };
      this.entries.set(pattern, entry);
      if (entry.wildcard) this.wildcard.insert(pattern, entry);
      else this.exact.set(pattern, entry);
    }

    const listenerId = this.nextListenerId++;
    const listener = new LocalListener<T>(callback, capacity, this.logger, error => {
      void this.handleOverflow(entry!, listenerId, listener, error);
    });
    entry.listeners.set(listenerId, listener);

    try {
      await this.ensureWireSubscribed(entry);
      if (listener.failure) throw listener.failure;
    } catch (error) {
      await this.detach(entry, listenerId, listener).catch(() => undefined);
      throw error;
    }

    return new Subscription(
      () => this.detach(entry!, listenerId, listener),
      () => listener.active,
      listener.closed,
      () => listener.failure,
    );
  }

  private dispatch(topic: string, data: any): void {
    const exactEntry = this.exact.get(topic);
    if (exactEntry) this.dispatchEntry(exactEntry, topic, data);
    this.wildcard.match(topic, entry => this.dispatchEntry(entry, topic, data));
  }

  private dispatchEntry(entry: PatternEntry, topic: string, data: any): void {
    const meta = { topic };
    for (const listener of entry.listeners.values()) {
      listener.enqueue(data, meta);
    }
  }

  private async detach(entry: PatternEntry, listenerId: number, listener: LocalListener<any>): Promise<void> {
    if (entry.listeners.get(listenerId) !== listener) {
      await listener.closed;
      return;
    }

    entry.listeners.delete(listenerId);
    listener.deactivate();
    let transition: Promise<void> = Promise.resolve();
    if (entry.listeners.size === 0) {
      transition = this.enqueueTransition(entry, async () => {
        if (entry.listeners.size > 0) return;
        try {
          if (entry.wireSubscribed) await PubSubCommands.unsubscribe(this.conn, entry.pattern);
        } finally {
          entry.wireSubscribed = false;
          if (entry.listeners.size === 0) this.removeEntry(entry);
        }
      });
    }
    await Promise.all([transition, listener.closed]);
  }

  private ensureWireSubscribed(entry: PatternEntry): Promise<void> {
    return this.enqueueTransition(entry, async () => {
      if (entry.listeners.size === 0 || entry.wireSubscribed) return;
      await PubSubCommands.subscribe(this.conn, entry.pattern);
      entry.wireSubscribed = true;
    });
  }

  private enqueueTransition(entry: PatternEntry, operation: () => Promise<void>): Promise<void> {
    const transition = entry.transition.catch(() => undefined).then(operation);
    entry.transition = transition;
    void transition.catch(() => undefined);
    return transition;
  }

  private removeEntry(entry: PatternEntry): void {
    if (this.entries.get(entry.pattern) !== entry || entry.listeners.size > 0) return;
    this.entries.delete(entry.pattern);
    if (entry.wildcard) this.wildcard.remove(entry.pattern, entry);
    else this.exact.delete(entry.pattern);
  }

  private async handleOverflow(entry: PatternEntry, listenerId: number, listener: LocalListener<any>, error: SlowConsumerError): Promise<void> {
    this.logger.error(`[PubSub] ${error.message}`);
    try {
      await this.detach(entry, listenerId, listener);
    } catch (detachError) {
      this.logger.error(`[PubSub] Failed to stop slow listener for "${entry.pattern}"`, detachError);
    }
  }

  private async restoreSubscriptions(): Promise<void> {
    const entries = Array.from(this.entries.values()).filter(entry => entry.listeners.size > 0);
    if (entries.length === 0) return;
    this.logger.info(`[PubSub] Restoring ${entries.length} subscription(s)...`);
    const results = await Promise.allSettled(entries.map(entry => this.enqueueTransition(entry, async () => {
      entry.wireSubscribed = false;
      if (entry.listeners.size === 0) return;
      await PubSubCommands.subscribe(this.conn, entry.pattern);
      entry.wireSubscribed = true;
    })));
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        this.logger.error(`[PubSub] Failed to resubscribe to ${entries[index].pattern}`, result.reason);
      }
    });
  }

  private static isWildcard(pattern: string): boolean {
    return pattern.split('/').some(part => part === '+' || part === '#');
  }

  private static validateTopic(topic: string): void {
    const parts = topic.split('/');
    if (!topic || parts.some(part => part === '')) throw new Error('[PubSub] Topic cannot contain empty segments');
    if (parts.some(part => part.includes('+') || part.includes('#'))) {
      throw new Error(`[PubSub] Concrete topic cannot contain wildcards: "${topic}"`);
    }
  }

  private static validatePattern(pattern: string): void {
    const parts = pattern.split('/');
    if (!pattern || parts.some(part => part === '')) throw new Error('[PubSub] Pattern cannot contain empty segments');
    let wildcard = false;
    for (let index = 0; index < parts.length; index++) {
      const part = parts[index];
      if (part.includes('+') && part !== '+') throw new Error('[PubSub] + wildcard must occupy an entire segment');
      if (part.includes('#') && part !== '#') throw new Error('[PubSub] # wildcard must occupy an entire segment');
      if (part === '#' && index !== parts.length - 1) throw new Error('[PubSub] # wildcard must be the last segment');
      wildcard ||= part === '+' || part === '#';
    }
    if (!wildcard) throw new Error(`[PubSub] Pattern must contain a wildcard segment: "${pattern}"`);
  }
}
