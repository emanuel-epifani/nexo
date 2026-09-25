import { connect } from '@nats-io/transport-node';
import {
  AckPolicy,
  DeliverPolicy,
  JsMsg,
  RetentionPolicy,
  StorageType,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { Meter, Row, Spec, Workload, embeddedTime, env, makePayload, measure, timedPayload } from '../report';

const RUN = Date.now().toString(36);
const STREAM = `BENCH_${RUN}`;
const SUBJECTS = `bench.stream.${RUN}.*`;
const subj = (w: Workload) => `bench.stream.${RUN}.${w.id}`;

export async function run(spec: Spec): Promise<Row[]> {
  const nexo = await NexoClient.connect({
    host: env('NEXO_HOST', '127.0.0.1'),
    port: Number(env('NEXO_PORT', '7654')),
  });

  const nc = await connect({ servers: env('NATS_URL', 'nats://127.0.0.1:4222') });
  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({
    name: STREAM,
    subjects: [SUBJECTS],
    storage: StorageType.File,
    retention: RetentionPolicy.Limits,
    num_replicas: 1,
  });
  const js = jetstream(nc);

  const value = makePayload(spec.payload_bytes);
  const rows: Row[] = [];

  const nexoImpl = {
    async publish(w: Workload): Promise<Row> {
      const name = `bench-${RUN}-${w.id}`;
      await nexo.stream.create(name);
      const s = await nexo.stream.get(name);
      const m = await measure(w, w.ops!, undefined, () => s.publish(value));
      await nexo.stream.delete(name);
      return m.toRow(w.id, 'nexo', w.ops!, w.hot);
    },
    async publish_batch(w: Workload): Promise<Row> {
      const name = `bench-${RUN}-${w.id}`;
      await nexo.stream.create(name);
      const s = await nexo.stream.get(name);
      const batch = w.batch ?? 100;
      const items = Array.from({ length: batch }, () => ({ data: value }));
      const calls = Math.ceil(w.ops! / batch);
      const m = await measure(w, calls, undefined, () => s.publishBatch(items));
      await nexo.stream.delete(name);
      return m.toRow(w.id, 'nexo', w.ops!, w.hot, `latency per ${batch}-msg call`);
    },
    async consume(w: Workload): Promise<Row> {
      const name = `bench-${RUN}-${w.id}`;
      await nexo.stream.create(name);
      const s = await nexo.stream.get(name);
      const msgs = w.msgs!;
      const items = Array.from({ length: 100 }, () => ({ data: value }));
      for (let i = 0; i < msgs / 100; i++) await s.publishBatch(items);

      const meter = new Meter();
      let got = 0;
      const group = await s.group(`drain-${w.id}`).subscribe(async () => { got++; }, {
        batchSize: w.batch_size ?? 500,
        waitMs: w.wait_ms ?? 100,
        concurrency: w.concurrency ?? 1,
      });
      const t0 = performance.now();
      while (got < msgs) {
        if (performance.now() - t0 > 180_000) throw new Error('nexo stream drain timeout');
        await new Promise(r => setTimeout(r, 5));
      }
      meter.stop();
      await group.stop();
      await nexo.stream.delete(name);
      return meter.toRow(w.id, 'nexo', msgs, w.hot, 'group read of pre-filled stream (fetch+ack)');
    },
    async pipeline(w: Workload): Promise<Row> {
      const name = `bench-${RUN}-${w.id}`;
      await nexo.stream.create(name);
      const s = await nexo.stream.get(name);
      const msgs = w.msgs!;

      const meter = new Meter();
      let got = 0;
      let resolveDone!: () => void;
      const done = new Promise<void>(res => (resolveDone = res));
      const subs = await Promise.all(
        Array.from({ length: w.consumers ?? 1 }, (_, c) =>
          s.group(`pipe-${w.id}-${c}`).subscribe(async (data: string) => {
            meter.recordRaw(performance.now() - embeddedTime(data));
            if (++got === msgs) resolveDone();
          }, { batchSize: w.batch_size ?? 500, waitMs: w.wait_ms ?? 100, concurrency: w.concurrency ?? 1 }),
        ),
      );

      const per = Math.ceil(msgs / (w.producers ?? 1));
      await Promise.all(
        Array.from({ length: w.producers ?? 1 }, async (_, k) => {
          for (let i = 0; i < per && k * per + i < msgs; i++) {
            await s.publish(timedPayload(performance.now(), spec.payload_bytes));
          }
        }),
      );
      await done;
      meter.stop();
      await Promise.all(subs.map(sub => sub.stop()));
      await nexo.stream.delete(name);
      return meter.toRow(w.id, 'nexo', msgs, w.hot, 'e2e publish->delivered via consumer group');
    },
  };

  const jsImpl = {
    async publish(w: Workload): Promise<Row> {
      const subject = subj(w);
      const m = await measure(w, w.ops!, undefined, () => js.publish(subject, value));
      return m.toRow(w.id, 'jetstream', w.ops!, w.hot);
    },
    async publish_batch(w: Workload): Promise<Row> {
      const subject = subj(w);
      const batch = w.batch ?? 100;
      const calls = Math.ceil(w.ops! / batch);
      // JetStream has no wire-level batch publish: N parallel confirmed publishes.
      const m = await measure(w, calls, undefined, () =>
        Promise.all(Array.from({ length: batch }, () => js.publish(subject, value))),
      );
      return m.toRow(w.id, 'jetstream', w.ops!, w.hot, `latency per ${batch}-msg call (parallel pub)`);
    },
    async consume(w: Workload): Promise<Row> {
      const subject = subj(w);
      const msgs = w.msgs!;
      for (let i = 0; i < msgs / 100; i++) {
        await Promise.all(Array.from({ length: 100 }, () => js.publish(subject, value)));
      }

      const durable = `drain-${w.id}`;
      await jsm.consumers.add(STREAM, {
        durable_name: durable,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        filter_subject: subject,
      });
      const consumer = await js.consumers.get(STREAM, durable);
      const meter = new Meter();
      let got = 0;
      let resolveDone!: () => void;
      const done = new Promise<void>(res => (resolveDone = res));
      const sub = await consumer.consume({
        callback: (m: JsMsg) => {
          m.ack();
          if (++got === msgs) resolveDone();
        },
      });
      await done;
      meter.stop();
      sub.stop();
      return meter.toRow(w.id, 'jetstream', msgs, w.hot, 'consumer read of pre-filled stream (deliver+ack)');
    },
    async pipeline(w: Workload): Promise<Row> {
      const subject = subj(w);
      const msgs = w.msgs!;
      const durable = `pipe-${w.id}`;
      await jsm.consumers.add(STREAM, {
        durable_name: durable,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        filter_subject: subject,
      });
      const consumer = await js.consumers.get(STREAM, durable);
      const meter = new Meter();
      let got = 0;
      let resolveDone!: () => void;
      const done = new Promise<void>(res => (resolveDone = res));
      const sub = await consumer.consume({
        callback: (m: JsMsg) => {
          m.ack();
          meter.recordRaw(performance.now() - embeddedTime(m.string()));
          if (++got === msgs) resolveDone();
        },
      });

      const per = Math.ceil(msgs / (w.producers ?? 1));
      await Promise.all(
        Array.from({ length: w.producers ?? 1 }, async (_, k) => {
          for (let i = 0; i < per && k * per + i < msgs; i++) {
            await js.publish(subject, timedPayload(performance.now(), spec.payload_bytes));
          }
        }),
      );
      await done;
      meter.stop();
      sub.stop();
      return meter.toRow(w.id, 'jetstream', msgs, w.hot, 'e2e publish->delivered via durable consumer');
    },
  };

  const impls: Record<string, Record<string, (w: Workload) => Promise<Row>>> = {
    nexo: nexoImpl,
    jetstream: jsImpl,
  };

  for (const w of spec.workloads) {
    for (const system of ['nexo', spec.reference]) {
      const impl = impls[system]?.[w.op];
      if (!impl) throw new Error(`unknown op '${w.op}' for ${system}`);
      process.stderr.write(`  · ${w.id} on ${system}...\n`);
      rows.push(await impl(w));
    }
  }

  nexo.disconnect();
  await jsm.streams.delete(STREAM).catch(() => undefined);
  await nc.drain();
  return rows;
}
