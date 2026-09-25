import Redis from 'ioredis';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { Row, Spec, Workload, env, makePayload, measure } from '../report';

type KvOps = {
  set: (k: string, v: string) => Promise<unknown>;
  get: (k: string) => Promise<unknown>;
};

// Same workload shapes for both systems: fairness comes from identical call patterns.
function kvImpl(system: string, ops: KvOps) {
  return {
    async set(w: Workload, runId: string, value: string): Promise<Row> {
      const m = await measure(w, w.ops!, undefined, i => ops.set(`b:${runId}:s:${i}`, value));
      return m.toRow(w.id, system, w.ops!, w.hot);
    },
    async get(w: Workload, runId: string, value: string): Promise<Row> {
      const pre = w.prefill ?? w.ops!;
      for (let i = 0; i < pre; i++) await ops.set(`b:${runId}:g:${i}`, value);
      const m = await measure(w, w.ops!, undefined, i => ops.get(`b:${runId}:g:${i % pre}`));
      return m.toRow(w.id, system, w.ops!, w.hot);
    },
    async mix(w: Workload, runId: string, value: string): Promise<Row> {
      const pre = w.prefill ?? 1000;
      for (let i = 0; i < pre; i++) await ops.set(`b:${runId}:m:${i}`, value);
      const writeEvery = Math.round(1 / (1 - (w.read_ratio ?? 0.8)));
      const m = await measure(w, w.ops!, undefined, i =>
        i % writeEvery === 0
          ? ops.set(`b:${runId}:m:${i % pre}`, value)
          : ops.get(`b:${runId}:m:${i % pre}`),
      );
      return m.toRow(w.id, system, w.ops!, w.hot, `read_ratio=${w.read_ratio}`);
    },
  };
}

export async function run(spec: Spec): Promise<Row[]> {
  const nexo = await NexoClient.connect({
    host: env('NEXO_HOST', '127.0.0.1'),
    port: Number(env('NEXO_PORT', '7654')),
  });
  const redis = new Redis(env('REDIS_URL', 'redis://127.0.0.1:6379'));
  await redis.ping();

  const value = makePayload(spec.payload_bytes);
  const runId = Date.now().toString(36);
  const impls: Record<string, ReturnType<typeof kvImpl>> = {
    nexo: kvImpl('nexo', {
      set: (k, v) => nexo.store.map.set(k, v),
      get: k => nexo.store.map.get(k),
    }),
    redis: kvImpl('redis', {
      set: (k, v) => redis.set(k, v),
      get: k => redis.get(k),
    }),
  };

  const rows: Row[] = [];
  for (const w of spec.workloads) {
    for (const system of ['nexo', spec.reference]) {
      const impl = impls[system][w.op as keyof ReturnType<typeof kvImpl>];
      if (!impl) throw new Error(`unknown op '${w.op}' for ${system}`);
      process.stderr.write(`  · ${w.id} on ${system}...\n`);
      rows.push(await impl(w, runId, value));
    }
  }

  nexo.disconnect();
  redis.disconnect();
  return rows;
}
