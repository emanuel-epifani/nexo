import mqtt, { MqttClient } from 'mqtt';
import { NexoClient } from '../../../../sdk/ts/src/client';
import { Meter, Row, Spec, Workload, embeddedTime, env, makePayload, measure, timedPayload } from '../report';

const RUN = Date.now().toString(36);
const TOPIC = `bench/${RUN}/metric`;

async function nexoSubs(w: Workload, onMsg: (data: string) => void): Promise<NexoClient[]> {
  const n = w.subscribers ?? 1;
  const clients: NexoClient[] = [];
  for (let i = 0; i < n; i++) {
    const c = await NexoClient.connect({
      host: env('NEXO_HOST', '127.0.0.1'),
      port: Number(env('NEXO_PORT', '7654')),
    });
    if (w.pattern) await c.pubsub.pattern(`${w.pattern.split('/')[0]}/${RUN}/${w.pattern.split('/').slice(1).join('/')}`).subscribe(async (d: string) => onMsg(d));
    else await c.pubsub.topic(TOPIC).subscribe(async (d: string) => onMsg(d));
    clients.push(c);
  }
  return clients;
}

export async function run(spec: Spec): Promise<Row[]> {
  const pub = await NexoClient.connect({
    host: env('NEXO_HOST', '127.0.0.1'),
    port: Number(env('NEXO_PORT', '7654')),
  });
  const pubTopic = pub.pubsub.topic(TOPIC);

  const rows: Row[] = [];
  const value = makePayload(spec.payload_bytes);

  const nexoImpl = {
    async publish(w: Workload): Promise<Row> {
      let delivered = 0;
      const subs = await nexoSubs(w, () => delivered++);
      const patternTopic = w.topic ? `bench/${RUN}/${w.topic.split('/').slice(1).join('/')}` : TOPIC;
      const m = await measure(w, w.ops!, undefined, () => pub.pubsub.publish(patternTopic, value, {}));
      const t0 = performance.now();
      while (delivered < w.ops! * (w.subscribers ?? 1) && performance.now() - t0 < 30_000) {
        await new Promise(r => setTimeout(r, 5));
      }
      subs.forEach(c => c.disconnect());
      return m.toRow(w.id, 'nexo', w.ops!, w.hot, `delivered=${delivered}`);
    },
    async fanout(w: Workload): Promise<Row> {
      let delivered = 0;
      const subs = await nexoSubs(w, () => delivered++);
      const expected = w.ops! * (w.subscribers ?? 1);
      const m = await measure(w, w.ops!, undefined, () => pubTopic.publish(value));
      const t0 = performance.now();
      while (delivered < expected && performance.now() - t0 < 60_000) {
        await new Promise(r => setTimeout(r, 5));
      }
      subs.forEach(c => c.disconnect());
      return m.toRow(w.id, 'nexo', w.ops!, w.hot, `1->${w.subscribers} delivered=${delivered}`);
    },
    async latency(w: Workload): Promise<Row> {
      const meter = new Meter();
      let resolveNext: (() => void) | null = null;
      const subs = await nexoSubs(w, data => {
        meter.recordRaw(performance.now() - embeddedTime(data));
        resolveNext?.();
      });
      for (let i = 0; i < w.ops!; i++) {
        const got = new Promise<void>(res => (resolveNext = res));
        await pubTopic.publish(timedPayload(performance.now(), spec.payload_bytes));
        await got;
      }
      meter.stop();
      subs.forEach(c => c.disconnect());
      return meter.toRow(w.id, 'nexo', w.ops!, w.hot, 'pub->deliver latency');
    },
  };

  const mqttUrl = env('MQTT_URL', 'mqtt://127.0.0.1:1883');

  async function mqttSubs(w: Workload, onMsg: (payload: string) => void): Promise<MqttClient[]> {
    const n = w.subscribers ?? 1;
    const pattern = w.pattern
      ? `${w.pattern.split('/')[0]}/${RUN}/${w.pattern.split('/').slice(1).join('/')}`
      : TOPIC;
    const clients: MqttClient[] = [];
    for (let i = 0; i < n; i++) {
      const c = mqtt.connect(mqttUrl);
      await new Promise<void>((res, rej) => { c.once('connect', () => res()); c.once('error', rej); });
      await c.subscribeAsync(pattern, { qos: 0 });
      c.on('message', (_t, payload) => onMsg(payload.toString()));
      clients.push(c);
    }
    return clients;
  }

  async function mqttPublish(c: MqttClient, topic: string, payload: string): Promise<void> {
    await c.publishAsync(topic, payload, { qos: 1 });
  }

  const mqttImpl = {
    async publish(w: Workload): Promise<Row> {
      let delivered = 0;
      const subs = await mqttSubs(w, () => delivered++);
      const c = mqtt.connect(mqttUrl);
      await new Promise<void>((res, rej) => { c.once('connect', () => res()); c.once('error', rej); });
      const patternTopic = w.topic ? `bench/${RUN}/${w.topic.split('/').slice(1).join('/')}` : TOPIC;
      const m = await measure(w, w.ops!, undefined, () => mqttPublish(c, patternTopic, value));
      const t0 = performance.now();
      while (delivered < w.ops! * (w.subscribers ?? 1) && performance.now() - t0 < 30_000) {
        await new Promise(r => setTimeout(r, 5));
      }
      await c.endAsync();
      await Promise.all(subs.map(s => s.endAsync()));
      return m.toRow(w.id, 'mqtt', w.ops!, w.hot, `delivered=${delivered}, qos1 pub`);
    },
    async fanout(w: Workload): Promise<Row> {
      let delivered = 0;
      const subs = await mqttSubs(w, () => delivered++);
      const c = mqtt.connect(mqttUrl);
      await new Promise<void>((res, rej) => { c.once('connect', () => res()); c.once('error', rej); });
      const expected = w.ops! * (w.subscribers ?? 1);
      const m = await measure(w, w.ops!, undefined, () => mqttPublish(c, TOPIC, value));
      const t0 = performance.now();
      while (delivered < expected && performance.now() - t0 < 60_000) {
        await new Promise(r => setTimeout(r, 5));
      }
      await c.endAsync();
      await Promise.all(subs.map(s => s.endAsync()));
      return m.toRow(w.id, 'mqtt', w.ops!, w.hot, `1->${w.subscribers} delivered=${delivered}, qos1 pub`);
    },
    async latency(w: Workload): Promise<Row> {
      const meter = new Meter();
      let resolveNext: (() => void) | null = null;
      const subs = await mqttSubs(w, data => {
        meter.recordRaw(performance.now() - embeddedTime(data));
        resolveNext?.();
      });
      const c = mqtt.connect(mqttUrl);
      await new Promise<void>((res, rej) => { c.once('connect', () => res()); c.once('error', rej); });
      for (let i = 0; i < w.ops!; i++) {
        const got = new Promise<void>(res => (resolveNext = res));
        await mqttPublish(c, TOPIC, timedPayload(performance.now(), spec.payload_bytes));
        await got;
      }
      meter.stop();
      await c.endAsync();
      await Promise.all(subs.map(s => s.endAsync()));
      return meter.toRow(w.id, 'mqtt', w.ops!, w.hot, 'pub->deliver latency, qos1 pub');
    },
  };

  const impls: Record<string, Record<string, (w: Workload) => Promise<Row>>> = {
    nexo: nexoImpl,
    mqtt: mqttImpl,
  };

  for (const w of spec.workloads) {
    for (const system of ['nexo', spec.reference]) {
      const impl = impls[system]?.[w.op];
      if (!impl) throw new Error(`unknown op '${w.op}' for ${system}`);
      process.stderr.write(`  · ${w.id} on ${system}...\n`);
      rows.push(await impl(w));
    }
  }

  pub.disconnect();
  return rows;
}
