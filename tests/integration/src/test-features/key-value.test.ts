import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import {performance} from "perf_hooks";

describe('KV broker', () => {
  it('should be able to set and get a value', async () => {
    const key = 'test_key';
    const value = 'test_value';

    // Set value
    await nexo.kv.set(key, value);

    // Get value
    const result = await nexo.kv.get(key);

    expect(result).not.toBeNull();
    expect(result?.toString()).toBe(value);
  });

  it('should return null for non-existent key', async () => {
    const res = await nexo.kv.get('non_existent_key');
    expect(res).toBeNull();
  });

  it('should be able to delete a key', async () => {
    const key = 'to_delete';
    const value = 'content';

    await nexo.kv.set(key, value);
    let result = await nexo.kv.get(key);
    expect(result?.toString()).toBe(value);

    await nexo.kv.del(key);
    result = await nexo.kv.get(key);
    expect(result).toBeNull();
  });

  it('should expire key after TTL', async () => {
    const key = "temp_key";
    const value = "valore";
    // Set with TTL of 1 second
    await nexo.kv.set(key, value, 1);
    
    // Should exist immediately
    const val1 = await nexo.kv.get(key);
    expect(val1?.toString()).toBe(value);
    
    // Wait for expiration
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    // Should be expired now
    const val2 = await nexo.kv.get(key);
    expect(val2).toBeNull();
  });

  /*
  AVG result -> Throughput: 978,462 ops/sec
   */
  it('AVG Throughput (ops/sec) with 10 Million existing keys', async () => {
    const PREFILL_COUNT = 10_000_000;
    const CONCURRENCY = 1000;
    const DURATION_MS = 5000;

    console.log(`📦 Fase 1: Pre-fill di ${PREFILL_COUNT.toLocaleString()} chiavi...`);

    // Riempiamo la memoria (usiamo i batch per fare in fretta)
    for (let i = 0; i < PREFILL_COUNT; i += 1000) {
      const batch = [];
      for (let j = 0; j < 1000; j++) {
        batch.push(nexo.kv.set(`dummy:${i + j}`, "payload", 3)); // Scadono tra 3 secondi
      }
      await Promise.all(batch);
    }

    console.log(`🚀 Fase 2: Avvio Benchmark su ${CONCURRENCY} workers...`);

    let operations = 0;
    let isRunning = true;
    const start = performance.now();

    const worker = async () => {
      while (isRunning) {
        await nexo.kv.set(`bench:${operations++}`, "x".repeat(256));
      }
    };

    const workers = Array(CONCURRENCY).fill(null).map(() => worker());

    await new Promise(resolve => setTimeout(resolve, DURATION_MS));
    isRunning = false;
    await Promise.all(workers);

    const totalTime = (performance.now() - start) / 1000;
    const opsPerSec = Math.floor(operations / totalTime);

    console.log(`\n--- STRESS RESULT (with ${PREFILL_COUNT.toLocaleString()} keys) ---`);
    console.log(`Throughput: ${opsPerSec.toLocaleString()} ops/sec`);
    console.log(`------------------------------------\n`);
  });});


/*
--- RESULT ---
Total Operations: 5,424,720
Throughput:       1,084,068 ops/sec
Latency (avg):    0.922 ms
--------------
 */