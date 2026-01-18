/**
 * Dead Letter Queue operations
 */
import type { IFlashQClient, Job } from './types';

/**
 * Get jobs from the dead letter queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param count - Max jobs to return (default: 100)
 * @returns Array of failed jobs
 *
 * @example
 * ```typescript
 * const failed = await client.getDlq('emails', 50);
 * console.log(`${failed.length} jobs in DLQ`);
 * ```
 */
export async function getDlq(
  client: IFlashQClient,
  queue: string,
  count = 100
): Promise<Job[]> {
  const response = await client.send<{ ok: boolean; jobs: Job[] }>({
    cmd: 'DLQ',
    queue,
    count,
  });
  return response.jobs;
}

/**
 * Retry jobs from the dead letter queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param jobId - Optional specific job ID to retry
 * @returns Number of jobs retried
 *
 * @example
 * ```typescript
 * // Retry all jobs in DLQ
 * const retried = await client.retryDlq('emails');
 *
 * // Retry specific job
 * await client.retryDlq('emails', 123);
 * ```
 */
export async function retryDlq(
  client: IFlashQClient,
  queue: string,
  jobId?: number
): Promise<number> {
  const response = await client.send<{ ok: boolean; ids: number[] }>({
    cmd: 'RETRYDLQ',
    queue,
    id: jobId,
  });
  return response.ids?.[0] ?? 0;
}

/**
 * Purge all jobs from the dead letter queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @returns Number of jobs purged
 *
 * @example
 * ```typescript
 * const purged = await client.purgeDlq('emails');
 * console.log(`Purged ${purged} failed jobs`);
 * ```
 */
export async function purgeDlq(
  client: IFlashQClient,
  queue: string
): Promise<number> {
  const response = await client.send<{ ok: boolean; count: number }>({
    cmd: 'PURGEDLQ',
    queue,
  });
  return response.count;
}
