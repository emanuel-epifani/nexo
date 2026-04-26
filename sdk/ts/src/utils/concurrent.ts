/**
 * Run an async function over a list of items with bounded concurrency.
 *
 * - With `concurrency = 1` items are processed strictly in order, sequentially.
 * - With `concurrency > 1` items are processed in parallel by up to N workers.
 *   Order of completion is not guaranteed; ordering of `fn` invocations across
 *   workers is non-deterministic.
 *
 * Resolves once every item has been processed.
 */
export async function runConcurrent<T>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<void>,
): Promise<void> {
  if (items.length === 0) return;
  if (concurrency <= 1) {
    for (const item of items) await fn(item);
    return;
  }
  const queue = [...items];
  const workers = Array(Math.min(concurrency, items.length))
    .fill(null)
    .map(async () => {
      while (queue.length) await fn(queue.shift()!);
    });
  await Promise.all(workers);
}
