/**
 * FlashQ Client - High-performance job queue client
 *
 * @example
 * ```typescript
 * import { FlashQ } from 'flashq';
 *
 * const client = new FlashQ();
 *
 * // Add a job (auto-connects!)
 * const job = await client.add('emails', { to: 'user@example.com' });
 *
 * // That's it! No connect() needed.
 * ```
 *
 * @example
 * ```typescript
 * // With options
 * const client = new FlashQ({
 *   host: 'localhost',
 *   port: 6789,
 *   token: 'secret'
 * });
 *
 * // Add with job options
 * await client.add('emails', { to: 'user@example.com' }, {
 *   priority: 10,
 *   delay: 5000,
 *   max_attempts: 3
 * });
 * ```
 */
import { FlashQConnection } from './connection';
import * as core from './core';
import * as jobs from './jobs';
import * as queue from './queue';
import * as dlq from './dlq';
import * as cron from './cron';
import * as metrics from './metrics';
import * as flows from './flows';
import * as advanced from './advanced';

import type {
  Job,
  JobState,
  JobWithState,
  PushOptions,
  QueueInfo,
  QueueStats,
  Metrics,
  CronJob,
  CronOptions,
  JobLogEntry,
  FlowChild,
  FlowResult,
  FlowOptions,
} from './types';

/**
 * FlashQ Client - High-performance job queue client with auto-connect.
 */
export class FlashQ extends FlashQConnection {
  // ============== Core Operations ==============

  /**
   * Push a job to a queue.
   *
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
  push<T = unknown>(queueName: string, data: T, options: PushOptions = {}): Promise<Job> {
    return core.push(this, queueName, data, options);
  }

  /**
   * Add a job to a queue (alias for push).
   *
   * @param queue - Queue name
   * @param data - Job data payload
   * @param options - Push options
   * @returns Created job
   */
  add<T = unknown>(queueName: string, data: T, options: PushOptions = {}): Promise<Job> {
    return this.push(queueName, data, options);
  }

  /**
   * Push multiple jobs to a queue in a single batch.
   *
   * @param queue - Queue name
   * @param jobs - Array of jobs with data and options
   * @returns Array of created job IDs
   */
  pushBatch<T = unknown>(queueName: string, jobList: Array<{ data: T } & PushOptions>): Promise<number[]> {
    return core.pushBatch(this, queueName, jobList);
  }

  /**
   * Add multiple jobs to a queue (alias for pushBatch).
   */
  addBulk<T = unknown>(queueName: string, jobList: Array<{ data: T } & PushOptions>): Promise<number[]> {
    return this.pushBatch(queueName, jobList);
  }

  /**
   * Pull a job from a queue (blocking with server-side timeout).
   *
   * @param queue - Queue name
   * @param timeout - Server-side timeout in ms (default: 60s)
   * @returns Job or null if timeout
   */
  pull<T = unknown>(queueName: string, timeout?: number): Promise<(Job & { data: T }) | null> {
    return core.pull(this, queueName, timeout);
  }

  /**
   * Pull multiple jobs from a queue.
   *
   * @param queue - Queue name
   * @param count - Number of jobs to pull
   * @param timeout - Server-side timeout in ms (default: 60s)
   * @returns Array of jobs
   */
  pullBatch<T = unknown>(queueName: string, count: number, timeout?: number): Promise<Array<Job & { data: T }>> {
    return core.pullBatch(this, queueName, count, timeout);
  }

  /**
   * Acknowledge a job as completed.
   *
   * @param jobId - Job ID
   * @param result - Optional result data
   */
  ack(jobId: number, result?: unknown): Promise<void> {
    return core.ack(this, jobId, result);
  }

  /**
   * Acknowledge multiple jobs at once.
   *
   * @param jobIds - Array of job IDs
   * @returns Number of jobs acknowledged
   */
  ackBatch(jobIds: number[]): Promise<number> {
    return core.ackBatch(this, jobIds);
  }

  /**
   * Fail a job (will retry or move to DLQ).
   *
   * @param jobId - Job ID
   * @param error - Optional error message
   */
  fail(jobId: number, error?: string): Promise<void> {
    return core.fail(this, jobId, error);
  }

  // ============== Job Management ==============

  /**
   * Get a job with its current state.
   *
   * @param jobId - Job ID
   * @returns Job with state, or null if not found
   */
  getJob(jobId: number): Promise<JobWithState | null> {
    return jobs.getJob(this, jobId);
  }

  /**
   * Get job state only.
   *
   * @param jobId - Job ID
   * @returns Job state or null if not found
   */
  getState(jobId: number): Promise<JobState | null> {
    return jobs.getState(this, jobId);
  }

  /**
   * Get job result.
   *
   * @param jobId - Job ID
   * @returns Job result or null
   */
  getResult<T = unknown>(jobId: number): Promise<T | null> {
    return jobs.getResult(this, jobId);
  }

  /**
   * Wait for a job to complete and return its result.
   *
   * @param jobId - Job ID
   * @param timeout - Timeout in ms (default: 30000)
   * @returns Job result or null
   * @throws Error if job fails or times out
   */
  finished<T = unknown>(jobId: number, timeout?: number): Promise<T | null> {
    return jobs.finished(this, jobId, timeout);
  }

  /**
   * Get a job by its custom ID.
   *
   * @param customId - Custom job ID
   * @returns Job with state or null
   */
  getJobByCustomId(customId: string): Promise<JobWithState | null> {
    return jobs.getJobByCustomId(this, customId);
  }

  /**
   * Get multiple jobs by their IDs in a single call.
   *
   * @param jobIds - Array of job IDs
   * @returns Array of jobs with states
   */
  getJobsBatch(jobIds: number[]): Promise<JobWithState[]> {
    return jobs.getJobsBatch(this, jobIds);
  }

  /**
   * Cancel a pending job.
   *
   * @param jobId - Job ID
   */
  cancel(jobId: number): Promise<void> {
    return jobs.cancel(this, jobId);
  }

  /**
   * Update job progress.
   *
   * @param jobId - Job ID
   * @param progress - Progress value (0-100)
   * @param message - Optional progress message
   */
  progress(jobId: number, progressValue: number, message?: string): Promise<void> {
    return jobs.progress(this, jobId, progressValue, message);
  }

  /**
   * Get job progress.
   *
   * @param jobId - Job ID
   * @returns Progress value and message
   */
  getProgress(jobId: number): Promise<{ progress: number; message?: string }> {
    return jobs.getProgress(this, jobId);
  }

  /**
   * Add a log entry to a job.
   *
   * @param jobId - Job ID
   * @param message - Log message
   * @param level - Log level (info, warn, error)
   */
  log(jobId: number, message: string, level: 'info' | 'warn' | 'error' = 'info'): Promise<void> {
    return jobs.log(this, jobId, message, level);
  }

  /**
   * Get log entries for a job.
   *
   * @param jobId - Job ID
   * @returns Array of log entries
   */
  getLogs(jobId: number): Promise<JobLogEntry[]> {
    return jobs.getLogs(this, jobId);
  }

  /**
   * Send a heartbeat for a long-running job.
   *
   * @param jobId - Job ID
   */
  heartbeat(jobId: number): Promise<void> {
    return jobs.heartbeat(this, jobId);
  }

  // ============== Queue Control ==============

  /**
   * Pause a queue.
   *
   * @param queue - Queue name
   */
  pause(queueName: string): Promise<void> {
    return queue.pause(this, queueName);
  }

  /**
   * Resume a paused queue.
   *
   * @param queue - Queue name
   */
  resume(queueName: string): Promise<void> {
    return queue.resume(this, queueName);
  }

  /**
   * Check if a queue is paused.
   *
   * @param queue - Queue name
   * @returns true if paused
   */
  isPaused(queueName: string): Promise<boolean> {
    return queue.isPaused(this, queueName);
  }

  /**
   * Set rate limit for a queue (jobs per second).
   *
   * @param queue - Queue name
   * @param limit - Jobs per second
   */
  setRateLimit(queueName: string, limit: number): Promise<void> {
    return queue.setRateLimit(this, queueName, limit);
  }

  /**
   * Clear rate limit for a queue.
   *
   * @param queue - Queue name
   */
  clearRateLimit(queueName: string): Promise<void> {
    return queue.clearRateLimit(this, queueName);
  }

  /**
   * Set concurrency limit for a queue.
   *
   * @param queue - Queue name
   * @param limit - Max concurrent jobs
   */
  setConcurrency(queueName: string, limit: number): Promise<void> {
    return queue.setConcurrency(this, queueName, limit);
  }

  /**
   * Clear concurrency limit for a queue.
   *
   * @param queue - Queue name
   */
  clearConcurrency(queueName: string): Promise<void> {
    return queue.clearConcurrency(this, queueName);
  }

  /**
   * List all queues.
   *
   * @returns Array of queue info
   */
  listQueues(): Promise<QueueInfo[]> {
    return queue.listQueues(this);
  }

  // ============== Dead Letter Queue ==============

  /**
   * Get jobs from the dead letter queue.
   *
   * @param queue - Queue name
   * @param count - Max jobs to return (default: 100)
   * @returns Array of failed jobs
   */
  getDlq(queueName: string, count = 100): Promise<Job[]> {
    return dlq.getDlq(this, queueName, count);
  }

  /**
   * Retry jobs from the dead letter queue.
   *
   * @param queue - Queue name
   * @param jobId - Optional specific job ID to retry
   * @returns Number of jobs retried
   */
  retryDlq(queueName: string, jobId?: number): Promise<number> {
    return dlq.retryDlq(this, queueName, jobId);
  }

  /**
   * Purge all jobs from the dead letter queue.
   *
   * @param queue - Queue name
   * @returns Number of jobs purged
   */
  purgeDlq(queueName: string): Promise<number> {
    return dlq.purgeDlq(this, queueName);
  }

  // ============== Cron Jobs ==============

  /**
   * Add a cron job for scheduled recurring tasks.
   *
   * @param name - Unique cron job name
   * @param options - Cron options
   */
  addCron(name: string, options: CronOptions): Promise<void> {
    return cron.addCron(this, name, options);
  }

  /**
   * Delete a cron job.
   *
   * @param name - Cron job name
   * @returns true if deleted
   */
  deleteCron(name: string): Promise<boolean> {
    return cron.deleteCron(this, name);
  }

  /**
   * List all cron jobs.
   *
   * @returns Array of cron jobs
   */
  listCrons(): Promise<CronJob[]> {
    return cron.listCrons(this);
  }

  // ============== Stats & Metrics ==============

  /**
   * Get queue statistics.
   *
   * @returns Queue stats
   */
  stats(): Promise<QueueStats> {
    return metrics.stats(this);
  }

  /**
   * Get detailed metrics.
   *
   * @returns Detailed metrics object
   */
  metrics(): Promise<Metrics> {
    return metrics.metrics(this);
  }

  // ============== Flows ==============

  /**
   * Push a flow (parent job with children).
   *
   * @param queue - Parent queue name
   * @param parentData - Parent job data
   * @param children - Array of child jobs
   * @param options - Flow options
   * @returns Parent and children IDs
   */
  pushFlow<T = unknown>(
    queueName: string,
    parentData: T,
    children: FlowChild[],
    options: FlowOptions = {}
  ): Promise<FlowResult> {
    return flows.pushFlow(this, queueName, parentData, children, options);
  }

  /**
   * Get children job IDs for a parent job in a flow.
   *
   * @param jobId - Parent job ID
   * @returns Array of children job IDs
   */
  getChildren(jobId: number): Promise<number[]> {
    return flows.getChildren(this, jobId);
  }

  // ============== BullMQ Advanced Features ==============

  /**
   * Get jobs filtered by queue and/or state with pagination.
   *
   * @param options - Filter options
   * @returns Jobs and total count
   */
  getJobs(options: {
    queue?: string;
    state?: JobState;
    limit?: number;
    offset?: number;
  } = {}): Promise<{ jobs: JobWithState[]; total: number }> {
    return advanced.getJobs(this, options);
  }

  /**
   * Get job counts by state for a queue.
   *
   * @param queue - Queue name
   * @returns Counts by state
   */
  getJobCounts(queueName: string): Promise<{
    waiting: number;
    active: number;
    delayed: number;
    completed: number;
    failed: number;
  }> {
    return advanced.getJobCounts(this, queueName);
  }

  /**
   * Get total count of jobs in a queue (waiting + delayed).
   *
   * @param queue - Queue name
   * @returns Total job count
   */
  count(queueName: string): Promise<number> {
    return advanced.count(this, queueName);
  }

  /**
   * Clean jobs older than grace period by state.
   *
   * @param queue - Queue name
   * @param grace - Grace period in ms
   * @param state - Job state to clean
   * @param limit - Optional max jobs to clean
   * @returns Number of jobs cleaned
   */
  clean(
    queueName: string,
    grace: number,
    state: 'waiting' | 'delayed' | 'completed' | 'failed',
    limit?: number
  ): Promise<number> {
    return advanced.clean(this, queueName, grace, state, limit);
  }

  /**
   * Drain all waiting jobs from a queue.
   *
   * @param queue - Queue name
   * @returns Number of jobs drained
   */
  drain(queueName: string): Promise<number> {
    return advanced.drain(this, queueName);
  }

  /**
   * Remove ALL data for a queue.
   *
   * @param queue - Queue name
   * @returns Total items removed
   */
  obliterate(queueName: string): Promise<number> {
    return advanced.obliterate(this, queueName);
  }

  /**
   * Change job priority.
   *
   * @param jobId - Job ID
   * @param priority - New priority
   */
  changePriority(jobId: number, priority: number): Promise<void> {
    return advanced.changePriority(this, jobId, priority);
  }

  /**
   * Move job from processing back to delayed.
   *
   * @param jobId - Job ID
   * @param delay - Delay in ms
   */
  moveToDelayed(jobId: number, delay: number): Promise<void> {
    return advanced.moveToDelayed(this, jobId, delay);
  }

  /**
   * Promote delayed job to waiting immediately.
   *
   * @param jobId - Job ID
   */
  promote(jobId: number): Promise<void> {
    return advanced.promote(this, jobId);
  }

  /**
   * Update job data.
   *
   * @param jobId - Job ID
   * @param data - New data payload
   */
  update<T = unknown>(jobId: number, data: T): Promise<void> {
    return advanced.update(this, jobId, data);
  }

  /**
   * Discard job - move directly to DLQ.
   *
   * @param jobId - Job ID
   */
  discard(jobId: number): Promise<void> {
    return advanced.discard(this, jobId);
  }

  // ============== Event Subscriptions ==============

  /**
   * Subscribe to real-time events via SSE.
   *
   * @param queue - Optional queue to filter events
   * @returns EventSubscriber instance
   *
   * @example
   * ```typescript
   * const events = client.subscribe();
   * events.on('completed', (e) => console.log(`Job ${e.jobId} completed`));
   * await events.connect();
   * ```
   */
  subscribe(queueName?: string): import('../events').EventSubscriber {
    const { EventSubscriber } = require('../events');
    return new EventSubscriber({
      host: this.options.host,
      httpPort: this.options.httpPort,
      token: this.options.token,
      queue: queueName,
      type: 'sse',
    });
  }

  /**
   * Subscribe to real-time events via WebSocket.
   *
   * @param queue - Optional queue to filter events
   * @returns EventSubscriber instance
   */
  subscribeWs(queueName?: string): import('../events').EventSubscriber {
    const { EventSubscriber } = require('../events');
    return new EventSubscriber({
      host: this.options.host,
      httpPort: this.options.httpPort,
      token: this.options.token,
      queue: queueName,
      type: 'websocket',
    });
  }
}

export default FlashQ;
