/**
 * Stats and metrics operations
 */
import type { IFlashQClient, QueueStats, Metrics } from './types';

/**
 * Get queue statistics.
 *
 * @param client - FlashQ client instance
 * @returns Queue stats (queued, processing, delayed, dlq counts)
 *
 * @example
 * ```typescript
 * const stats = await client.stats();
 * console.log(`Queued: ${stats.queued}`);
 * console.log(`Processing: ${stats.processing}`);
 * console.log(`Delayed: ${stats.delayed}`);
 * console.log(`DLQ: ${stats.dlq}`);
 * ```
 */
export async function stats(
  client: IFlashQClient
): Promise<QueueStats> {
  const response = await client.send<{
    ok: boolean;
    queued: number;
    processing: number;
    delayed: number;
    dlq: number;
  }>({
    cmd: 'STATS',
  });
  return {
    queued: response.queued,
    processing: response.processing,
    delayed: response.delayed,
    dlq: response.dlq,
  };
}

/**
 * Get detailed metrics.
 *
 * @param client - FlashQ client instance
 * @returns Detailed metrics object
 *
 * @example
 * ```typescript
 * const metrics = await client.metrics();
 * console.log('Throughput:', metrics.throughput);
 * ```
 */
export async function metrics(
  client: IFlashQClient
): Promise<Metrics> {
  const response = await client.send<{ ok: boolean; metrics: Metrics }>({
    cmd: 'METRICS',
  });
  return response.metrics;
}
