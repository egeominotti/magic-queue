import { FlashQ } from './client';
import { Worker } from './worker';
import type { Job, PushOptions, ClientOptions, WorkerOptions, JobProcessor } from './types';

/**
 * Queue - High-level API for flashQ
 *
 * A simpler, more intuitive way to work with job queues.
 *
 * @example
 * ```typescript
 * import { Queue } from 'flashq';
 *
 * const emails = new Queue('emails');
 *
 * // Add a job (auto-connects!)
 * await emails.add({ to: 'user@example.com', subject: 'Hello!' });
 *
 * // Add with options
 * await emails.add({ to: 'vip@example.com' }, { priority: 10 });
 *
 * // Process jobs
 * emails.process(async (job) => {
 *   await sendEmail(job.data);
 *   return { sent: true };
 * });
 * ```
 *
 * @example
 * ```typescript
 * // With connection options
 * const queue = new Queue('tasks', {
 *   connection: { host: 'queue.example.com', port: 6789, token: 'secret' }
 * });
 * ```
 */
export class Queue<TData = unknown, TResult = unknown> {
  private client: FlashQ;
  private worker: Worker<TData, TResult> | null = null;
  private queueName: string;
  private processorFn: JobProcessor<TData, TResult> | null = null;

  constructor(
    name: string,
    options: {
      connection?: ClientOptions;
    } = {}
  ) {
    this.queueName = name;
    this.client = new FlashQ(options.connection ?? {});
  }

  /**
   * Queue name
   */
  get name(): string {
    return this.queueName;
  }

  /**
   * Add a job to the queue
   *
   * @example
   * ```typescript
   * await queue.add({ email: 'user@test.com' });
   * await queue.add({ email: 'vip@test.com' }, { priority: 10 });
   * ```
   */
  async add(data: TData, options: PushOptions = {}): Promise<Job> {
    return this.client.add(this.queueName, data, options);
  }

  /**
   * Add multiple jobs at once
   *
   * @example
   * ```typescript
   * await queue.addBulk([
   *   { data: { email: 'a@test.com' } },
   *   { data: { email: 'b@test.com' }, priority: 5 },
   * ]);
   * ```
   */
  async addBulk(jobs: Array<{ data: TData } & PushOptions>): Promise<number[]> {
    return this.client.addBulk(this.queueName, jobs);
  }

  /**
   * Process jobs from this queue
   *
   * @example
   * ```typescript
   * queue.process(async (job) => {
   *   console.log('Processing:', job.data);
   *   await doWork(job.data);
   *   return { success: true };
   * });
   *
   * // With concurrency
   * queue.process(async (job) => {
   *   await doWork(job.data);
   * }, { concurrency: 10 });
   * ```
   */
  process(
    processor: JobProcessor<TData, TResult>,
    options: Partial<WorkerOptions> = {}
  ): Worker<TData, TResult> {
    this.processorFn = processor;

    this.worker = new Worker<TData, TResult>(
      this.queueName,
      processor,
      {
        ...options,
        host: this.client['options'].host,
        port: this.client['options'].port,
        token: this.client['options'].token,
      }
    );

    // Auto-start
    this.worker.start().catch((err) => {
      this.worker?.emit('error', err);
    });

    return this.worker;
  }

  /**
   * Pause the queue
   */
  async pause(): Promise<void> {
    return this.client.pause(this.queueName);
  }

  /**
   * Resume the queue
   */
  async resume(): Promise<void> {
    return this.client.resume(this.queueName);
  }

  /**
   * Get jobs from dead letter queue
   */
  async getFailed(count = 100): Promise<Job[]> {
    return this.client.getDlq(this.queueName, count);
  }

  /**
   * Retry failed jobs
   */
  async retryFailed(jobId?: number): Promise<number> {
    return this.client.retryDlq(this.queueName, jobId);
  }

  /**
   * Set rate limit (jobs per second)
   */
  async setRateLimit(limit: number): Promise<void> {
    return this.client.setRateLimit(this.queueName, limit);
  }

  /**
   * Set concurrency limit
   */
  async setConcurrency(limit: number): Promise<void> {
    return this.client.setConcurrency(this.queueName, limit);
  }

  /**
   * Close the queue and stop processing
   */
  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.stop();
      this.worker = null;
    }
    await this.client.close();
  }

  /**
   * Get the underlying client (for advanced operations)
   */
  getClient(): FlashQ {
    return this.client;
  }
}

export default Queue;
