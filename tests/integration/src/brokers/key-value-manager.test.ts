import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';
import { performance } from "perf_hooks";
import {logger} from "../../../../sdk/ts/src/utils/logger";

describe('KEY-VALUE broker', () => {
  it('should be able to set and get a value', async () => {
    const key = 'test_key';
    const value = 'test_value';

    // Set value
    await nexo.kv().set(key, value);

    // Get value
    const result = await nexo.kv().get(key);

    expect(result).not.toBeNull();
    expect(result?.toString()).toBe(value);
  });

  it('should return null for non-existent key', async () => {
    const res = await nexo.kv().get('non_existent_key');
    expect(res).toBeNull();
  });

  it('should be able to delete a key', async () => {
    const key = 'to_delete';
    const value = 'content';

    await nexo.kv().set(key, value);
    let result = await nexo.kv().get(key);
    expect(result?.toString()).toBe(value);

    await nexo.kv().del(key);
    result = await nexo.kv().get(key);
    expect(result).toBeNull();
  });

  it('should expire key after TTL', async () => {
    const key = "temp_key";
    const value = "valore";
    // Set with TTL of 1 second
    await nexo.kv().set(key, value, 1);

    // Should exist immediately
    const val1 = await nexo.kv().get(key);
    expect(val1?.toString()).toBe(value);

    // Wait for expiration
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Should be expired now
    const val2 = await nexo.kv().get(key);
    expect(val2).toBeNull();
  });

});