import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import {BenchmarkProbe} from "../utils/benchmark-misure";


describe('STORE.KV broker', () => {
  it('should be able to set and get a value', async () => {
    const key = 'test_key';
    const value = 'test_value';

    // Set value
    await nexo.store.kv.set(key, value);

    // Get value
    const result = await nexo.store.kv.get(key);

    expect(result).not.toBeNull();
    expect(result?.toString()).toBe(value);
  });

  it('should return null for non-existent key', async () => {
    const res = await nexo.store.kv.get('non_existent_key');
    expect(res).toBeNull();
  });

  it('should be able to delete a key', async () => {
    const key = 'to_delete';
    const value = 'content';

    await nexo.store.kv.set(key, value);
    let result = await nexo.store.kv.get(key);
    expect(result?.toString()).toBe(value);

    await nexo.store.kv.del(key);
    result = await nexo.store.kv.get(key);
    expect(result).toBeNull();
  });

  it('should expire key after TTL', async () => {
    const key = "temp_key";
    const value = "valore";
    // Set with TTL of 1 second
    await nexo.store.kv.set(key, value, 1);

    // Should exist immediately
    const val1 = await nexo.store.kv.get(key);
    expect(val1?.toString()).toBe(value);

    // Wait for expiration
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Should be expired now
    const val2 = await nexo.store.kv.get(key);
    expect(val2).toBeNull();
  });

  // -- PERFORMANCE --
  it('Write Throughput', async () => {
    const TOTAL = 100_000;
    const CONCURRENCY = 200;

    const probe = new BenchmarkProbe("STORE - WRITE", TOTAL);
    probe.startTimer();

    const worker = async (id: number) => {
      const opsPerWorker = TOTAL / CONCURRENCY;
      for (let i = 0; i < opsPerWorker; i++) {
        const t0 = performance.now();
        await nexo.store.kv.set(`stress:${id}:${i}`, "val");
        probe.record(performance.now() - t0);
      }
    };
    await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

    const stats = probe.printResult();
    expect(stats.throughput).toBeGreaterThan(480_000);
    expect(stats.p99).toBeLessThan(2);
    expect(stats.max).toBeLessThan(4);
  });

  it('Read Throughput', async () => {
    const PREFILL = 100_000;
    const READ_OPS = 50_000;
    const CONCURRENCY = 100;

    const batchSize = 1000;
    for (let i = 0; i < PREFILL; i += batchSize) {
      const batch = [];
      for (let j = 0; j < batchSize; j++) batch.push(nexo.store.kv.set(`k:${i + j}`, "static"));
      await Promise.all(batch);
    }

    const probe = new BenchmarkProbe("STORE - READ", READ_OPS);
    probe.startTimer();

    const worker = async () => {
      const opsPerWorker = READ_OPS / CONCURRENCY;
      for (let i = 0; i < opsPerWorker; i++) {
        const key = `k:${Math.floor(Math.random() * PREFILL)}`;
        const t0 = performance.now();
        await nexo.store.kv.get(key);
        probe.record(performance.now() - t0);
      }
    };
    await Promise.all(Array.from({ length: CONCURRENCY }, worker));

    const stats = probe.printResult();
    expect(stats.throughput).toBeGreaterThan(550_000);
    expect(stats.p99).toBeLessThan(1);
    expect(stats.max).toBeLessThan(2);
  });

});
