/**
 * Cron job operations
 */
import type { IFlashQClient, CronJob, CronOptions } from './types';

/**
 * Add a cron job for scheduled recurring tasks.
 *
 * @param client - FlashQ client instance
 * @param name - Unique cron job name
 * @param options - Cron options
 *
 * @example
 * ```typescript
 * // Using cron schedule (6-field: sec min hour day month weekday)
 * await client.addCron('daily-cleanup', {
 *   queue: 'maintenance',
 *   data: { task: 'cleanup' },
 *   schedule: '0 0 0 * * *', // Every day at midnight
 * });
 *
 * // Using repeat_every (milliseconds)
 * await client.addCron('heartbeat', {
 *   queue: 'health',
 *   data: { check: 'ping' },
 *   repeat_every: 5000, // Every 5 seconds
 *   limit: 100, // Stop after 100 executions
 * });
 * ```
 */
export async function addCron(
  client: IFlashQClient,
  name: string,
  options: CronOptions
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'CRON',
    name,
    queue: options.queue,
    data: options.data,
    schedule: options.schedule,
    repeat_every: options.repeat_every,
    priority: options.priority ?? 0,
    limit: options.limit,
  });
}

/**
 * Delete a cron job.
 *
 * @param client - FlashQ client instance
 * @param name - Cron job name
 * @returns true if deleted
 *
 * @example
 * ```typescript
 * await client.deleteCron('daily-cleanup');
 * ```
 */
export async function deleteCron(
  client: IFlashQClient,
  name: string
): Promise<boolean> {
  const response = await client.send<{ ok: boolean }>({
    cmd: 'CRONDELETE',
    name,
  });
  return response.ok;
}

/**
 * List all cron jobs.
 *
 * @param client - FlashQ client instance
 * @returns Array of cron jobs
 *
 * @example
 * ```typescript
 * const crons = await client.listCrons();
 * for (const cron of crons) {
 *   console.log(`${cron.name}: ${cron.schedule || cron.repeat_every + 'ms'}`);
 * }
 * ```
 */
export async function listCrons(
  client: IFlashQClient
): Promise<CronJob[]> {
  const response = await client.send<{ ok: boolean; crons: CronJob[] }>({
    cmd: 'CRONLIST',
  });
  return response.crons;
}
