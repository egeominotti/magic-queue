/**
 * BullMQ-like advanced operations
 */
import type { IFlashQClient, Job, JobState, JobWithState } from './types';

/**
 * Get jobs filtered by queue and/or state with pagination.
 *
 * @param client - FlashQ client instance
 * @param options - Filter options
 * @returns Jobs and total count
 *
 * @example
 * ```typescript
 * // Get all waiting jobs
 * const { jobs, total } = await client.getJobs({ state: 'waiting' });
 *
 * // Paginate results
 * const { jobs } = await client.getJobs({ limit: 10, offset: 20 });
 * ```
 */
export async function getJobs(
  client: IFlashQClient,
  options: {
    queue?: string;
    state?: JobState;
    limit?: number;
    offset?: number;
  } = {}
): Promise<{ jobs: JobWithState[]; total: number }> {
  const response = await client.send<{
    ok: boolean;
    jobs: Array<{ job: Job; state: JobState }>;
    total: number;
  }>({
    cmd: 'GETJOBS',
    queue: options.queue,
    state: options.state,
    limit: options.limit ?? 100,
    offset: options.offset ?? 0,
  });
  return {
    jobs: response.jobs.map((j) => ({ job: j.job, state: j.state })),
    total: response.total,
  };
}

/**
 * Get job counts by state for a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @returns Counts by state
 *
 * @example
 * ```typescript
 * const counts = await client.getJobCounts('emails');
 * console.log(`Waiting: ${counts.waiting}`);
 * console.log(`Active: ${counts.active}`);
 * ```
 */
export async function getJobCounts(
  client: IFlashQClient,
  queue: string
): Promise<{
  waiting: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
}> {
  const response = await client.send<{
    ok: boolean;
    waiting: number;
    active: number;
    delayed: number;
    completed: number;
    failed: number;
  }>({
    cmd: 'GETJOBCOUNTS',
    queue,
  });
  return {
    waiting: response.waiting,
    active: response.active,
    delayed: response.delayed,
    completed: response.completed,
    failed: response.failed,
  };
}

/**
 * Get total count of jobs in a queue (waiting + delayed).
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @returns Total job count
 *
 * @example
 * ```typescript
 * const total = await client.count('emails');
 * console.log(`${total} jobs in queue`);
 * ```
 */
export async function count(
  client: IFlashQClient,
  queue: string
): Promise<number> {
  const response = await client.send<{ ok: boolean; count: number }>({
    cmd: 'COUNT',
    queue,
  });
  return response.count;
}

/**
 * Clean jobs older than grace period by state.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param grace - Grace period in ms
 * @param state - Job state to clean
 * @param limit - Optional max jobs to clean
 * @returns Number of jobs cleaned
 *
 * @example
 * ```typescript
 * // Clean completed jobs older than 1 hour
 * const cleaned = await client.clean('emails', 3600000, 'completed');
 * ```
 */
export async function clean(
  client: IFlashQClient,
  queue: string,
  grace: number,
  state: 'waiting' | 'delayed' | 'completed' | 'failed',
  limit?: number
): Promise<number> {
  const response = await client.send<{ ok: boolean; count: number }>({
    cmd: 'CLEAN',
    queue,
    grace,
    state,
    limit,
  });
  return response.count;
}

/**
 * Drain all waiting jobs from a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @returns Number of jobs drained
 *
 * @example
 * ```typescript
 * const drained = await client.drain('emails');
 * console.log(`Drained ${drained} jobs`);
 * ```
 */
export async function drain(
  client: IFlashQClient,
  queue: string
): Promise<number> {
  const response = await client.send<{ ok: boolean; count: number }>({
    cmd: 'DRAIN',
    queue,
  });
  return response.count;
}

/**
 * Remove ALL data for a queue (jobs, DLQ, cron, state).
 * Use with caution - this is destructive!
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @returns Total items removed
 *
 * @example
 * ```typescript
 * const removed = await client.obliterate('test-queue');
 * ```
 */
export async function obliterate(
  client: IFlashQClient,
  queue: string
): Promise<number> {
  const response = await client.send<{ ok: boolean; count: number }>({
    cmd: 'OBLITERATE',
    queue,
  });
  return response.count;
}

/**
 * Change job priority.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param priority - New priority (higher = processed first)
 *
 * @example
 * ```typescript
 * await client.changePriority(jobId, 100);
 * ```
 */
export async function changePriority(
  client: IFlashQClient,
  jobId: number,
  priority: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'CHANGEPRIORITY',
    id: jobId,
    priority,
  });
}

/**
 * Move job from processing back to delayed.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param delay - Delay in ms
 *
 * @example
 * ```typescript
 * // Delay for 5 minutes
 * await client.moveToDelayed(jobId, 300000);
 * ```
 */
export async function moveToDelayed(
  client: IFlashQClient,
  jobId: number,
  delay: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'MOVETODELAYED',
    id: jobId,
    delay,
  });
}

/**
 * Promote delayed job to waiting immediately.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 *
 * @example
 * ```typescript
 * await client.promote(jobId);
 * ```
 */
export async function promote(
  client: IFlashQClient,
  jobId: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'PROMOTE',
    id: jobId,
  });
}

/**
 * Update job data.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param data - New data payload
 *
 * @example
 * ```typescript
 * await client.update(jobId, { email: 'new@example.com' });
 * ```
 */
export async function update<T = unknown>(
  client: IFlashQClient,
  jobId: number,
  data: T
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'UPDATEJOB',
    id: jobId,
    data,
  });
}

/**
 * Discard job - move directly to DLQ.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 *
 * @example
 * ```typescript
 * await client.discard(jobId);
 * ```
 */
export async function discard(
  client: IFlashQClient,
  jobId: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'DISCARD',
    id: jobId,
  });
}
