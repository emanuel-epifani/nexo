import * as amqplib from 'amqplib';
import type { ChannelModel, ConfirmChannel } from 'amqplib';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { Meter, Row, Spec, Workload, embeddedTime, env, makePayload, measure, timedPayload } from '../report';

const Q = `bench.q.${Date.now().toString(36)}`;

export async function run(spec: Spec): Promise<Row[]> {
  const nexo = await NexoClient.connect({
    host: env('NEXO_HOST', '127.0.0.1'),
    port: Number(env('NEXO_PORT', '7654')),
  });
  const conn: ChannelModel = await amqplib.connect(env('RABBITMQ_URL', 'amqp://127.0.0.1:5672'));
  // Two durability tiers: nexo push confirms after the shared-WAL
  // transaction commits (synchronous=NORMAL), so it sits between rabbit-d
  // (disk + confirm) and rabbit-v (fire-and-forget). Report both.
  const chD: ConfirmChannel = await conn.createConfirmChannel();
  const chV = await conn.createChannel();

  const value = makePayload(spec.payload_bytes);
  const rows: Row[] = [];

  // ---- nexo impl ----
  const nexoImpl = {
    async push(w: Workload): Promise<Row> {
      const name = `${Q}.np.${w.id}`;
      await nexo.queue.create(name);
      const q = await nexo.queue.get(name);
      const m = await measure(w, w.ops!, undefined, () => q.push(value));
      m.stop();
      await nexo.queue.delete(name);
      return m.toRow(w.id, 'nexo', w.ops!, w.hot);
    },
    async consume(w: Workload): Promise<Row> {
      const name = `${Q}.nc.${w.id}`;
      await nexo.queue.create(name);
      const q = await nexo.queue.get(name);
      const msgs = w.msgs!;
      const chunk = Array.from({ length: 500 }, () => ({ data: value }));
      for (let i = 0; i < (w.prefill ?? msgs); i += 500) await q.pushBatch(chunk);

      const meter = new Meter();
      let got = 0;
      const subs = await Promise.all(
        Array.from({ length: w.consumers ?? 1 }, () =>
          q.subscribe(async () => { got++; }, {
            batchSize: w.batch_size ?? 50,
            waitMs: w.wait_ms ?? 200,
            concurrency: 1,
          }),
        ),
      );
      const t0 = performance.now();
      while (got < msgs) {
        if (performance.now() - t0 > 120_000) throw new Error('nexo consume drain timeout');
        await new Promise(r => setTimeout(r, 5));
      }
      meter.stop();
      await Promise.all(subs.map(s => s.stop()));
      await nexo.queue.delete(name);
      return meter.toRow(w.id, 'nexo', msgs, w.hot, 'drain pre-filled queue (push+ack)');
    },
    async pipeline(w: Workload): Promise<Row> {
      const name = `${Q}.nx.${w.id}`;
      await nexo.queue.create(name);
      const q = await nexo.queue.get(name);
      const msgs = w.msgs!;

      const meter = new Meter();
      let got = 0;
      let resolveDone!: () => void;
      const done = new Promise<void>(res => (resolveDone = res));
      const subs = await Promise.all(
        Array.from({ length: w.consumers ?? 1 }, () =>
          q.subscribe(async (data: string) => {
            meter.recordRaw(performance.now() - embeddedTime(data));
            if (++got === msgs) resolveDone();
          }, { batchSize: w.batch_size ?? 50, waitMs: w.wait_ms ?? 200, concurrency: 1 }),
        ),
      );

      const per = Math.ceil(msgs / (w.producers ?? 1));
      await Promise.all(
        Array.from({ length: w.producers ?? 1 }, async (_, k) => {
          for (let i = 0; i < per && k * per + i < msgs; i++) {
            await q.push(timedPayload(performance.now(), spec.payload_bytes));
          }
        }),
      );
      await done;
      meter.stop();
      await Promise.all(subs.map(s => s.stop()));
      await nexo.queue.delete(name);
      return meter.toRow(w.id, 'nexo', msgs, w.hot, 'e2e push->delivered, latency=enqueue->delivery');
    },
  };

  // ---- rabbitmq impl (parametrized by durability tier) ----
  const rabbitPush = async (w: Workload, d: boolean): Promise<Row> => {
    const ch = d ? chD : chV;
    const name = `${Q}.rp.${w.id}.${d ? 'd' : 'v'}`;
    await ch.assertQueue(name, { durable: d, exclusive: !d });
    const buf = Buffer.from(value);
    const m = await measure(w, w.ops!, undefined, async () => {
      ch.sendToQueue(name, buf, { persistent: d });
      if (d) await (ch as ConfirmChannel).waitForConfirms();
    });
    m.stop();
    await ch.deleteQueue(name);
    return m.toRow(w.id, d ? 'rabbit-d' : 'rabbit-v', w.ops!, w.hot,
      d ? 'persistent msg + confirm' : 'non-persistent, no confirm');
  };

  const rabbitConsume = async (w: Workload, d: boolean): Promise<Row> => {
    const ch = d ? chD : chV;
    const name = `${Q}.rc.${w.id}.${d ? 'd' : 'v'}`;
    await ch.assertQueue(name, { durable: d, exclusive: !d });
    const msgs = w.msgs!;
    const buf = Buffer.from(value);
    for (let i = 0; i < (w.prefill ?? msgs); i++) ch.sendToQueue(name, buf, { persistent: d });
    if (d) await (ch as ConfirmChannel).waitForConfirms();

    if (d) await ch.prefetch(w.prefetch ?? 50);
    const meter = new Meter();
    let got = 0;
    let resolveDone!: () => void;
    const done = new Promise<void>(res => (resolveDone = res));
    const tags: string[] = [];
    for (let c = 0; c < (w.consumers ?? 1); c++) {
      const { consumerTag } = await ch.consume(name, msg => {
        if (!msg) return;
        if (d) ch.ack(msg);
        if (++got === msgs) resolveDone();
      }, { noAck: !d });
      tags.push(consumerTag);
    }
    await done;
    meter.stop();
    await Promise.all(tags.map(t => ch.cancel(t)));
    await ch.deleteQueue(name);
    return meter.toRow(w.id, d ? 'rabbit-d' : 'rabbit-v', msgs, w.hot,
      d ? 'drain pre-filled (deliver+ack)' : 'drain pre-filled (deliver, auto-ack)');
  };

  const rabbitPipeline = async (w: Workload, d: boolean): Promise<Row> => {
    const ch = d ? chD : chV;
    const name = `${Q}.rx.${w.id}.${d ? 'd' : 'v'}`;
    await ch.assertQueue(name, { durable: d, exclusive: !d });
    const msgs = w.msgs!;

    if (d) await ch.prefetch(w.prefetch ?? 50);
    const meter = new Meter();
    let got = 0;
    let resolveDone!: () => void;
    const done = new Promise<void>(res => (resolveDone = res));
    const tags: string[] = [];
    for (let c = 0; c < (w.consumers ?? 1); c++) {
      const { consumerTag } = await ch.consume(name, msg => {
        if (!msg) return;
        if (d) ch.ack(msg);
        meter.recordRaw(performance.now() - embeddedTime(msg.content.toString()));
        if (++got === msgs) resolveDone();
      }, { noAck: !d });
      tags.push(consumerTag);
    }

    const per = Math.ceil(msgs / (w.producers ?? 1));
    await Promise.all(
      Array.from({ length: w.producers ?? 1 }, async (_, k) => {
        for (let i = 0; i < per && k * per + i < msgs; i++) {
          ch.sendToQueue(name, Buffer.from(timedPayload(performance.now(), spec.payload_bytes)), { persistent: d });
          if (d) await (ch as ConfirmChannel).waitForConfirms();
        }
      }),
    );
    await done;
    meter.stop();
    await Promise.all(tags.map(t => ch.cancel(t)));
    await ch.deleteQueue(name);
    return meter.toRow(w.id, d ? 'rabbit-d' : 'rabbit-v', msgs, w.hot,
      d ? 'e2e push->delivered, persistent+confirm' : 'e2e push->delivered, non-persistent, no confirm');
  };

  const impls: Record<string, Record<string, (w: Workload) => Promise<Row>>> = {
    nexo: nexoImpl,
    'rabbit-d': {
      push: w => rabbitPush(w, true),
      consume: w => rabbitConsume(w, true),
      pipeline: w => rabbitPipeline(w, true),
    },
    'rabbit-v': {
      push: w => rabbitPush(w, false),
      consume: w => rabbitConsume(w, false),
      pipeline: w => rabbitPipeline(w, false),
    },
  };

  for (const w of spec.workloads) {
    for (const system of ['nexo', 'rabbit-d', 'rabbit-v']) {
      const impl = impls[system]?.[w.op];
      if (!impl) throw new Error(`unknown op '${w.op}' for ${system}`);
      process.stderr.write(`  · ${w.id} on ${system}...\n`);
      rows.push(await impl(w));
    }
  }

  nexo.disconnect();
  await conn.close();
  return rows;
}
