/**
 * Queue control operations
 */
import type { IFlashQClient, QueueInfo } from './types';

/**
 * Pause a queue. Workers will stop pulling jobs.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 *
 * @example
 * ```typescript
 * await client.pause('emails');
 * ```
 */
export async function pause(
  client: IFlashQClient,
  queue: string
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'PAUSE',
    queue,
  });
}

/**
 * Resume a paused queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 *
 * @example
 * ```typescript
 * await client.resume('emails');
 * ```
 */
export async function resume(
  client: IFlashQClient,
  queue: string
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'RESUME',
    queue,
  });
}

/**
 * Check if a queue is paused.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @returns true if paused
 *
 * @example
 * ```typescript
 * if (await client.isPaused('emails')) {
 *   console.log('Queue is paused');
 * }
 * ```
 */
export async function isPaused(
  client: IFlashQClient,
  queue: string
): Promise<boolean> {
  const response = await client.send<{ ok: boolean; paused: boolean }>({
    cmd: 'ISPAUSED',
    queue,
  });
  return response.paused;
}

/**
 * Set rate limit for a queue (jobs per second).
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param limit - Jobs per second
 *
 * @example
 * ```typescript
 * // Allow 10 jobs per second
 * await client.setRateLimit('api-calls', 10);
 * ```
 */
export async function setRateLimit(
  client: IFlashQClient,
  queue: string,
  limit: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'RATELIMIT',
    queue,
    limit,
  });
}

/**
 * Clear rate limit for a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 *
 * @example
 * ```typescript
 * await client.clearRateLimit('api-calls');
 * ```
 */
export async function clearRateLimit(
  client: IFlashQClient,
  queue: string
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'RATELIMITCLEAR',
    queue,
  });
}

/**
 * Set concurrency limit for a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param limit - Max concurrent jobs
 *
 * @example
 * ```typescript
 * // Allow max 5 concurrent jobs
 * await client.setConcurrency('heavy-tasks', 5);
 * ```
 */
export async function setConcurrency(
  client: IFlashQClient,
  queue: string,
  limit: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'SETCONCURRENCY',
    queue,
    limit,
  });
}

/**
 * Clear concurrency limit for a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 *
 * @example
 * ```typescript
 * await client.clearConcurrency('heavy-tasks');
 * ```
 */
export async function clearConcurrency(
  client: IFlashQClient,
  queue: string
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'CLEARCONCURRENCY',
    queue,
  });
}

/**
 * List all queues.
 *
 * @param client - FlashQ client instance
 * @returns Array of queue info
 *
 * @example
 * ```typescript
 * const queues = await client.listQueues();
 * for (const q of queues) {
 *   console.log(`${q.name}: ${q.size} jobs`);
 * }
 * ```
 */
export async function listQueues(
  client: IFlashQClient
): Promise<QueueInfo[]> {
  const response = await client.send<{ ok: boolean; queues: QueueInfo[] }>({
    cmd: 'LISTQUEUES',
  });
  return response.queues;
}
