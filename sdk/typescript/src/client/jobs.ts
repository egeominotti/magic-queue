/**
 * Job management operations
 */
import type { IFlashQClient, Job, JobState, JobWithState, JobLogEntry } from './types';

/**
 * Get a job with its current state.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @returns Job with state, or null if not found
 *
 * @example
 * ```typescript
 * const result = await client.getJob(123);
 * if (result) {
 *   console.log(`Job ${result.job.id} is ${result.state}`);
 * }
 * ```
 */
export async function getJob(
  client: IFlashQClient,
  jobId: number
): Promise<JobWithState | null> {
  const response = await client.send<{
    ok: boolean;
    job: Job | null;
    state: JobState | null;
  }>({
    cmd: 'GETJOB',
    id: jobId,
  });
  if (!response.job) return null;
  return { job: response.job, state: response.state! };
}

/**
 * Get job state only.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @returns Job state or null if not found
 *
 * @example
 * ```typescript
 * const state = await client.getState(123);
 * console.log(`Job state: ${state}`);
 * ```
 */
export async function getState(
  client: IFlashQClient,
  jobId: number
): Promise<JobState | null> {
  const response = await client.send<{
    ok: boolean;
    id: number;
    state: JobState | null;
  }>({
    cmd: 'GETSTATE',
    id: jobId,
  });
  return response.state;
}

/**
 * Get job result.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @returns Job result or null
 *
 * @example
 * ```typescript
 * const result = await client.getResult<{ sent: boolean }>(123);
 * ```
 */
export async function getResult<T = unknown>(
  client: IFlashQClient,
  jobId: number
): Promise<T | null> {
  const response = await client.send<{
    ok: boolean;
    id: number;
    result: T | null;
  }>({
    cmd: 'GETRESULT',
    id: jobId,
  });
  return response.result;
}

/**
 * Wait for a job to complete and return its result.
 * Useful for synchronous workflows.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param timeout - Timeout in ms (default: 30000)
 * @returns Job result or null
 * @throws Error if job fails or times out
 *
 * @example
 * ```typescript
 * const job = await client.push('process', { data: 'value' });
 * const result = await client.finished(job.id);
 * console.log('Result:', result);
 * ```
 */
export async function finished<T = unknown>(
  client: IFlashQClient,
  jobId: number,
  timeout?: number
): Promise<T | null> {
  const waitTimeout = timeout ?? 30000;
  const requestTimeout = waitTimeout + 5000;

  const response = await client.send<{
    ok: boolean;
    result: T | null;
    error?: string;
  }>(
    {
      cmd: 'WAITJOB',
      id: jobId,
      timeout: waitTimeout,
    },
    requestTimeout
  );

  if (response.error) {
    throw new Error(response.error);
  }
  return response.result;
}

/**
 * Get a job by its custom ID.
 * Custom IDs are set using the `jobId` option when pushing.
 *
 * @param client - FlashQ client instance
 * @param customId - Custom job ID
 * @returns Job with state or null
 *
 * @example
 * ```typescript
 * // Push with custom ID
 * await client.push('process', { orderId: 123 }, { jobId: 'order-123' });
 *
 * // Retrieve by custom ID
 * const result = await client.getJobByCustomId('order-123');
 * ```
 */
export async function getJobByCustomId(
  client: IFlashQClient,
  customId: string
): Promise<JobWithState | null> {
  const response = await client.send<{
    ok: boolean;
    job: Job | null;
    state: JobState | null;
  }>({
    cmd: 'GETJOBBYCUSTOMID',
    job_id: customId,
  });
  if (!response.job) return null;
  return { job: response.job, state: response.state! };
}

/**
 * Get multiple jobs by their IDs in a single call.
 *
 * @param client - FlashQ client instance
 * @param jobIds - Array of job IDs
 * @returns Array of jobs with states
 *
 * @example
 * ```typescript
 * const jobs = await client.getJobsBatch([1, 2, 3, 4, 5]);
 * ```
 */
export async function getJobsBatch(
  client: IFlashQClient,
  jobIds: number[]
): Promise<JobWithState[]> {
  // Server uses #[serde(flatten)] so job fields are at the top level with state
  const response = await client.send<{
    ok: boolean;
    jobs: Array<Job & { state: JobState }>;
  }>({
    cmd: 'GETJOBSBATCH',
    ids: jobIds,
  });
  return response.jobs.map((j) => {
    const { state, ...job } = j;
    return { job: job as Job, state };
  });
}

/**
 * Cancel a pending job.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 *
 * @example
 * ```typescript
 * await client.cancel(job.id);
 * ```
 */
export async function cancel(
  client: IFlashQClient,
  jobId: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'CANCEL',
    id: jobId,
  });
}

/**
 * Update job progress.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param progress - Progress value (0-100)
 * @param message - Optional progress message
 *
 * @example
 * ```typescript
 * await client.progress(job.id, 50, 'Halfway done');
 * ```
 */
export async function progress(
  client: IFlashQClient,
  jobId: number,
  progressValue: number,
  message?: string
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'PROGRESS',
    id: jobId,
    progress: Math.min(100, Math.max(0, progressValue)),
    message,
  });
}

/**
 * Get job progress.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @returns Progress value and message
 *
 * @example
 * ```typescript
 * const { progress, message } = await client.getProgress(job.id);
 * ```
 */
export async function getProgress(
  client: IFlashQClient,
  jobId: number
): Promise<{ progress: number; message?: string }> {
  const response = await client.send<{
    ok: boolean;
    progress: {
      id: number;
      progress: number;
      message?: string;
    };
  }>({
    cmd: 'GETPROGRESS',
    id: jobId,
  });
  return { progress: response.progress.progress, message: response.progress.message };
}

/**
 * Add a log entry to a job.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @param message - Log message
 * @param level - Log level (info, warn, error)
 *
 * @example
 * ```typescript
 * await client.log(job.id, 'Processing step 1', 'info');
 * await client.log(job.id, 'Warning: low memory', 'warn');
 * ```
 */
export async function log(
  client: IFlashQClient,
  jobId: number,
  message: string,
  level: 'info' | 'warn' | 'error' = 'info'
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'LOG',
    job_id: jobId,
    message,
    level,
  });
}

/**
 * Get log entries for a job.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 * @returns Array of log entries
 *
 * @example
 * ```typescript
 * const logs = await client.getLogs(job.id);
 * ```
 */
export async function getLogs(
  client: IFlashQClient,
  jobId: number
): Promise<JobLogEntry[]> {
  const response = await client.send<{ ok: boolean; logs: JobLogEntry[] }>({
    cmd: 'GETLOGS',
    job_id: jobId,
  });
  return response.logs;
}

/**
 * Send a heartbeat to prevent job from being marked as stalled.
 *
 * @param client - FlashQ client instance
 * @param jobId - Job ID
 *
 * @example
 * ```typescript
 * // In a long-running job processor
 * for (const item of largeDataset) {
 *   await processItem(item);
 *   await client.heartbeat(job.id);
 * }
 * ```
 */
export async function heartbeat(
  client: IFlashQClient,
  jobId: number
): Promise<void> {
  await client.send<{ ok: boolean }>({
    cmd: 'HEARTBEAT',
    job_id: jobId,
  });
}
