/**
 * Core operations: push, pull, ack, fail
 */
import type { IFlashQClient, Job, PushOptions } from './types';

/** Maximum batch size allowed by the server */
export const MAX_BATCH_SIZE = 1000;

/**
 * Push a job to a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param data - Job data payload
 * @param options - Push options (priority, delay, ttl, etc.)
 * @returns Created job
 *
 * @example
 * ```typescript
 * const job = await client.push('emails', { to: 'user@example.com' });
 * ```
 */
export async function push<T = unknown>(
  client: IFlashQClient,
  queue: string,
  data: T,
  options: PushOptions = {}
): Promise<Job> {
  const response = await client.send<{ ok: boolean; id: number }>({
    cmd: 'PUSH',
    queue,
    data,
    priority: options.priority ?? 0,
    delay: options.delay,
    ttl: options.ttl,
    timeout: options.timeout,
    max_attempts: options.max_attempts,
    backoff: options.backoff,
    unique_key: options.unique_key,
    depends_on: options.depends_on,
    tags: options.tags,
    lifo: options.lifo ?? false,
    remove_on_complete: options.remove_on_complete ?? false,
    remove_on_fail: options.remove_on_fail ?? false,
    stall_timeout: options.stall_timeout,
    debounce_id: options.debounce_id,
    debounce_ttl: options.debounce_ttl,
    job_id: options.jobId,
    keep_completed_age: options.keepCompletedAge,
    keep_completed_count: options.keepCompletedCount,
  });

  return {
    id: response.id,
    queue,
    data,
    priority: options.priority ?? 0,
    created_at: Date.now(),
    run_at: options.delay ? Date.now() + options.delay : Date.now(),
    started_at: 0,
    attempts: 0,
    max_attempts: options.max_attempts ?? 0,
    backoff: options.backoff ?? 0,
    ttl: options.ttl ?? 0,
    timeout: options.timeout ?? 0,
    unique_key: options.unique_key,
    depends_on: options.depends_on ?? [],
    progress: 0,
    tags: options.tags ?? [],
    lifo: options.lifo ?? false,
    remove_on_complete: options.remove_on_complete ?? false,
    remove_on_fail: options.remove_on_fail ?? false,
    last_heartbeat: 0,
    stall_timeout: options.stall_timeout ?? 30000,
    stall_count: 0,
    parent_id: undefined,
    children_ids: [],
    children_completed: 0,
    custom_id: options.jobId,
    keep_completed_age: options.keepCompletedAge ?? 0,
    keep_completed_count: options.keepCompletedCount ?? 0,
    completed_at: 0,
  };
}

/**
 * Push multiple jobs to a queue in a single batch.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param jobs - Array of jobs with data and options
 * @returns Array of created job IDs
 *
 * @example
 * ```typescript
 * const ids = await client.pushBatch('emails', [
 *   { data: { to: 'user1@example.com' } },
 *   { data: { to: 'user2@example.com' }, priority: 10 },
 * ]);
 * ```
 */
export async function pushBatch<T = unknown>(
  client: IFlashQClient,
  queue: string,
  jobs: Array<{ data: T } & PushOptions>
): Promise<number[]> {
  if (jobs.length > MAX_BATCH_SIZE) {
    throw new Error(`Batch size ${jobs.length} exceeds maximum allowed (${MAX_BATCH_SIZE})`);
  }

  const response = await client.send<{ ok: boolean; ids: number[] }>({
    cmd: 'PUSHB',
    queue,
    jobs: jobs.map((j) => ({
      data: j.data,
      priority: j.priority ?? 0,
      delay: j.delay,
      ttl: j.ttl,
      timeout: j.timeout,
      max_attempts: j.max_attempts,
      backoff: j.backoff,
      unique_key: j.unique_key,
      depends_on: j.depends_on,
      tags: j.tags,
      lifo: j.lifo ?? false,
      remove_on_complete: j.remove_on_complete ?? false,
      remove_on_fail: j.remove_on_fail ?? false,
      stall_timeout: j.stall_timeout,
      debounce_id: j.debounce_id,
      debounce_ttl: j.debounce_ttl,
      job_id: j.jobId,
      keep_completed_age: j.keepCompletedAge,
      keep_completed_count: j.keepCompletedCount,
    })),
  });
  return response.ids;
}

/**
 * Pull a job from a queue (blocking with server-side timeout).
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param timeout - Server-side timeout in ms (default: 60s)
 * @returns Job or null if timeout
 *
 * @example
 * ```typescript
 * const job = await client.pull('emails');
 * if (job) {
 *   console.log('Processing:', job.data);
 * }
 * ```
 */
export async function pull<T = unknown>(
  client: IFlashQClient,
  queue: string,
  timeout?: number
): Promise<(Job & { data: T }) | null> {
  const serverTimeout = timeout ?? 60000;
  const clientTimeout = serverTimeout + 5000;
  const response = await client.send<{ ok: boolean; job: Job | null }>(
    {
      cmd: 'PULL',
      queue,
      timeout: serverTimeout,
    },
    clientTimeout
  );
  return response.job as (Job & { data: T }) | null;
}

/**
 * Pull multiple jobs from a queue.
 *
 * @param client - FlashQ client instance
 * @param queue - Queue name
 * @param count - Number of jobs to pull
 * @param timeout - Server-side timeout in ms (default: 60s)
 * @returns Array of jobs
 *
 * @example
 * ```typescript
 * const jobs = await client.pullBatch('emails', 10);
 * for (const job of jobs) {
 *   await processJob(job);
 * }
 * ```
 */
export async function pullBatch<T = unknown>(
  client: IFlashQClient,
  queue: string,
  count: number,
  timeout?: number
): Promise<Array<Job & { data: T }>> {
  if (count > MAX_BATCH_SIZE) {
    throw new Error(`Batch size ${count} exceeds maximum allowed (${MAX_BATCH_SIZE})`);
  }

  const serverTimeout = timeout ?? 60000;
  const clientTimeout = serverTimeout + 5000;
  const response = await client.send<{ ok: boolean; jobs: Job[] }>(
    {
      cmd: 'PULLB',
      queue,
      count,
      timeout: serverTimeout,
    },
    clientTimeout
  );
  return (response.jobs ?? []) as Array<Job & { data: T }>;
}

/**
 * Acknowledge a job as completed.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param result - Optional result data
 *
 * @example
 * ```typescript
 * await client.ack(job.id, { sent: true });
 * ```
 */
export async function ack(
  client: IFlashQClient,
  jobId: number,
  result?: unknown
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'ACK',
    id: jobId,
    result,
  });
}

/**
 * Acknowledge multiple jobs at once.
 *
 * @param client - FlashQ client instance
 * @param jobIds - Array of job IDs
 * @returns Number of jobs acknowledged
 *
 * @example
 * ```typescript
 * await client.ackBatch([1, 2, 3]);
 * ```
 */
export async function ackBatch(
  client: IFlashQClient,
  jobIds: number[]
): Promise<number> {
  if (jobIds.length > MAX_BATCH_SIZE) {
    throw new Error(`Batch size ${jobIds.length} exceeds maximum allowed (${MAX_BATCH_SIZE})`);
  }

  const response = await client.send<{ ok: boolean; ids: number[] }>({
    cmd: 'ACKB',
    ids: jobIds,
  });
  return response.ids?.[0] ?? 0;
}

/**
 * Fail a job (will retry or move to DLQ).
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param error - Optional error message
 *
 * @example
 * ```typescript
 * await client.fail(job.id, 'Connection timeout');
 * ```
 */
export async function fail(
  client: IFlashQClient,
  jobId: number,
  error?: string
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'FAIL',
    id: jobId,
    error,
  });
}
