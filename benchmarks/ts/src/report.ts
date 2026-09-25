import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
export const BENCH_ROOT = join(HERE, '..', '..');
export const SCENARIOS_DIR = join(BENCH_ROOT, 'scenarios');
export const RESULTS_DIR = join(BENCH_ROOT, 'results');

export interface Workload {
  id: string;
  op: string;
  ops?: number;
  msgs?: number;
  mode?: 'concurrent' | 'sequential';
  workers?: number;
  producers?: number;
  consumers?: number;
  concurrency?: number;
  batch?: number;
  batch_size?: number;
  wait_ms?: number;
  prefetch?: number;
  subscribers?: number;
  pattern?: string;
  topic?: string;
  prefill?: number;
  read_ratio?: number;
  hot?: boolean;
}

export interface Spec {
  id: string;
  title: string;
  reference: string;
  hot_path: string;
  durability: { nexo: string; reference: string };
  payload_bytes: number;
  workloads: Workload[];
}

export interface Row {
  workload: string;
  system: string;
  ops: number;
  secs: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  hot?: boolean;
  note?: string;
}

export function env(key: string, fallback: string): string {
  return process.env[key] ?? fallback;
}

export function loadSpec(name: string): Spec {
  return JSON.parse(readFileSync(join(SCENARIOS_DIR, `${name}.json`), 'utf8')) as Spec;
}

export function makePayload(bytes: number): string {
  return 'x'.repeat(Math.max(1, bytes));
}

// Payload embedding the send timestamp for pub->delivered latency measurement.
export function timedPayload(now: number, bytes: number): string {
  const head = `${now}|`;
  return head + 'x'.repeat(Math.max(0, bytes - head.length));
}

export function embeddedTime(data: unknown): number {
  const s = typeof data === 'string' ? data : String(data);
  const sep = s.indexOf('|');
  return sep > 0 ? parseFloat(s.slice(0, sep)) : NaN;
}

export class Meter {
  readonly latencies: number[] = [];
  private readonly t0 = performance.now();
  secs = 0;

  /** Record latency of an op that started at `startMs` (performance.now()). */
  record(startMs: number): void {
    this.latencies.push(performance.now() - startMs);
  }

  /** Record a latency measured from an embedded timestamp (e2e delivery). */
  recordRaw(ms: number): void {
    if (Number.isFinite(ms)) this.latencies.push(ms);
  }

  stop(): void {
    this.secs = (performance.now() - this.t0) / 1000;
  }

  toRow(workload: string, system: string, ops: number, hot?: boolean, note?: string): Row {
    const l = [...this.latencies].sort((a, b) => a - b);
    const at = (p: number) => (l.length ? l[Math.min(Math.floor((l.length * p) / 100), l.length - 1)] : 0);
    const sum = l.reduce((a, b) => a + b, 0);
    return {
      workload,
      system,
      ops,
      secs: this.secs,
      avg: l.length ? sum / l.length : 0,
      p50: at(50),
      p95: at(95),
      p99: at(99),
      max: l.length ? l[l.length - 1] : 0,
      hot,
      note,
    };
  }
}

/**
 * Run `ops` iterations split across `workers` sequential loops.
 * Per-op latency = fn round-trip; ops/sec measured over the wall-clock span.
 */
export async function measure(
  w: Workload,
  ops: number,
  workersOverride: number | undefined,
  fn: (i: number) => Promise<unknown>,
): Promise<Meter> {
  const meter = new Meter();
  const workers = workersOverride ?? (w.mode === 'sequential' ? 1 : (w.workers ?? 10));
  const per = Math.ceil(ops / workers);
  await Promise.all(
    Array.from({ length: workers }, async (_, k) => {
      for (let i = 0; i < per; i++) {
        const idx = k * per + i;
        if (idx >= ops) break;
        const s = performance.now();
        await fn(idx);
        meter.record(s);
      }
    }),
  );
  meter.stop();
  return meter;
}

function fmtMs(ms: number): string {
  return ms >= 100 ? ms.toFixed(0) : ms.toFixed(3);
}

export function printResults(spec: Spec, rows: Row[]): void {
  console.log(`\n${'='.repeat(96)}`);
  console.log(`  ${spec.title}`);
  console.log(`  hot path: ${spec.hot_path}`);
  console.log(`${'='.repeat(96)}`);
  const head = ['workload', 'system', 'ops', 'ops/sec', 'avg ms', 'p50', 'p95', 'p99', 'max', 'note'];
  console.log(
    `  ${head[0].padEnd(20)} ${head[1].padEnd(9)} ${head[2].padStart(8)} ${head[3].padStart(10)} ` +
      `${head[4].padStart(8)} ${head[5].padStart(8)} ${head[6].padStart(8)} ${head[7].padStart(8)} ${head[8].padStart(8)}  ${head[9]}`,
  );
  for (const r of rows) {
    const id = `${r.hot ? '*' : ' '}${r.workload}`.padEnd(20);
    console.log(
      `  ${id} ${r.system.padEnd(9)} ${String(r.ops).padStart(8)} ${(r.ops / r.secs).toFixed(0).padStart(10)} ` +
        `${fmtMs(r.avg).padStart(8)} ${fmtMs(r.p50).padStart(8)} ${fmtMs(r.p95).padStart(8)} ` +
        `${fmtMs(r.p99).padStart(8)} ${fmtMs(r.max).padStart(8)}  ${r.note ?? ''}`,
    );
  }
  console.log(`${'='.repeat(96)}\n  * = hot-path workload\n`);
}

export function saveResults(spec: Spec, rows: Row[], lang: string): string {
  mkdirSync(RESULTS_DIR, { recursive: true });
  const lines: string[] = [
    `# ${spec.title}`,
    '',
    `_run: ${new Date().toISOString()} · harness: ${lang}_`,
    '',
    `**hot path**: ${spec.hot_path}`,
    '',
    `**durability** — nexo: ${spec.durability.nexo} · ${spec.reference}: ${spec.durability.reference}`,
    '',
    '| workload | system | ops | ops/sec | avg ms | p50 | p95 | p99 | max | note |',
    '|---|---|---|---|---|---|---|---|---|---|',
    ...rows.map(
      r =>
        `| ${r.hot ? '**' + r.workload + '**' : r.workload} | ${r.system} | ${r.ops} | ` +
        `${(r.ops / r.secs).toFixed(0)} | ${fmtMs(r.avg)} | ${fmtMs(r.p50)} | ${fmtMs(r.p95)} | ` +
        `${fmtMs(r.p99)} | ${fmtMs(r.max)} | ${r.note ?? ''} |`,
    ),
    '',
  ];
  const out = join(RESULTS_DIR, `${spec.id}.${lang}.md`);
  writeFileSync(out, lines.join('\n'));
  return out;
}
