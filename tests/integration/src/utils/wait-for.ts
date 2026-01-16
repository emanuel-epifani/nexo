/**
 * Waits for a condition to become true, or for a set of assertions to pass.
 *
 * Supports two modes:
 * 1. Boolean Predicate: () => boolean. Waits until it returns true.
 * 2. Assertion Block: () => void. Waits until it executes without throwing (useful with expect()).
 *
 * @param assertionOrCondition Function to evaluate
 * @param options.timeout Max wait time in ms (default 5000)
 * @param options.interval Check interval in ms (default 50)
 */
export async function waitFor(
  assertionOrCondition: () => boolean | void | Promise<boolean | void>,
  options: { timeout?: number; interval?: number } = {}
): Promise<void> {
  const timeout = options.timeout ?? 5000;
  const interval = options.interval ?? 50;
  const start = Date.now();
  let lastError: any;

  while (true) {
    try {
      const result = await assertionOrCondition();
      // If it returns explicit false, treat as failure (for boolean predicates)
      if (result === false) {
        throw new Error("Condition returned false");
      }
      return; // Success (true or void/undefined)
    } catch (e) {
      lastError = e;
      if (Date.now() - start > timeout) {
        // Timeout reached: throw the last error to help debugging
        throw lastError;
      }
      await new Promise(r => setTimeout(r, interval));
    }
  }
}
