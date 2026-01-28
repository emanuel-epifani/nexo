import { describe, it, expect } from 'vitest';
import { nexo } from '../nexo';


describe('STORE.MAP broker', () => {
  it('should be able to set and get a value', async () => {
    const key = 'test_key';
    const value = 'test_value';

    // Set value
    await nexo.store.map.set(key, value, { ttl: 60 });

    // Get value
    const result = await nexo.store.map.get(key);

    expect(result).not.toBeNull();
    expect(result?.toString()).toBe(value);
  });

  it('should return null for non-existent key', async () => {
    const res = await nexo.store.map.get('non_existent_key');
    expect(res).toBeNull();
  });

  it('should be able to delete a key', async () => {
    const key = 'to_delete';
    const value = 'content';

    await nexo.store.map.set(key, value);
    let result = await nexo.store.map.get(key);
    expect(result?.toString()).toBe(value);

    await nexo.store.map.del(key);
    result = await nexo.store.map.get(key);
    expect(result).toBeNull();
  });

  it('should expire key after TTL', async () => {
    const key = "temp_key";
    const value = "valore";
    // Set with TTL of 1 second
    await nexo.store.map.set(key, value, { ttl: 1 });

    // Should exist immediately
    const val1 = await nexo.store.map.get(key);
    expect(val1?.toString()).toBe(value);

    // Wait for expiration
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Should be expired now
    const val2 = await nexo.store.map.get(key);
    expect(val2).toBeNull();
  });

});
