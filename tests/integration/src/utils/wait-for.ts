/**
 * Waits for a condition to become true, with timeout.
 * Use this instead of setTimeout() for robust async tests.
 */
export async function waitFor(
  condition: () => boolean,
  timeoutMs = 5000,
  intervalMs = 10
): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timeout after ${timeoutMs}ms`);
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }
}

