/**
 * BullMQ-compatible Queue class
 */
import { FlashQ } from './client';
import type { Job, PushOptions, ClientOptions } from './types';

export interface QueueOptions extends ClientOptions {
  /** Default job options for all jobs in this queue */
  defaultJobOptions?: JobOptions;
}

export interface JobOptions {
  /** Job priority (higher = first) */
  priority?: number;
  /** Delay in ms */
  delay?: number;
  /** Number of retry attempts (BullMQ-compatible alias for max_attempts) */
  attempts?: number;
  /** Backoff configuration */
  backoff?: number | { type: 'exponential' | 'fixed'; delay: number };
  /** Job timeout in ms */
  timeout?: number;
  /** Time-to-live in ms */
  ttl?: number;
  /** Unique key for deduplication */
  jobId?: string;
  /** Remove job on completion */
  removeOnComplete?: boolean | number;
  /** Remove job on failure */
  removeOnFail?: boolean | number;
  /** Job IDs that must complete before this job runs */
  depends_on?: number[];
  /** Tags for filtering */
  tags?: string[];
}

/**
 * BullMQ-compatible Queue class
 *
 * @example
 * ```typescript
 * import { Queue } from 'flashq';
 *
 * const emailQueue = new Queue('emails');
 *
 * await emailQueue.add('send', { to: 'user@example.com' }, {
 *   attempts: 3,
 *   backoff: { type: 'exponential', delay: 1000 }
 * });
 * ```
 */
export class Queue<T = unknown> {
  private client: FlashQ;
  private defaultJobOptions: JobOptions;

  readonly name: string;

  constructor(name: string, options: QueueOptions = {}) {
    this.name = name;
    this.client = new FlashQ(options);
    this.defaultJobOptions = options.defaultJobOptions || {};
  }

  /**
   * Add a job to the queue (BullMQ-compatible)
   */
  async add(name: string, data: T, options: JobOptions = {}): Promise<Job<T>> {
    const opts = { ...this.defaultJobOptions, ...options };

    // Convert BullMQ options to flashQ options
    const pushOpts: PushOptions = {
      priority: opts.priority,
      delay: opts.delay,
      max_attempts: opts.attempts,
      timeout: opts.timeout,
      ttl: opts.ttl,
      jobId: opts.jobId,
      depends_on: opts.depends_on,
      tags: opts.tags,
    };

    // Handle backoff (BullMQ uses object, flashQ uses number)
    if (opts.backoff) {
      if (typeof opts.backoff === 'number') {
        pushOpts.backoff = opts.backoff;
      } else {
        pushOpts.backoff = opts.backoff.delay;
      }
    }

    return this.client.add(this.name, { name, ...data as object }, pushOpts) as Promise<Job<T>>;
  }

  /**
   * Add multiple jobs (BullMQ-compatible)
   */
  async addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]> {
    const flashqJobs = jobs.map(job => {
      const opts = { ...this.defaultJobOptions, ...job.opts };
      return {
        data: { name: job.name, ...job.data as object },
        priority: opts.priority,
        delay: opts.delay,
        max_attempts: opts.attempts,
        timeout: opts.timeout,
        ttl: opts.ttl,
        jobId: opts.jobId,
        backoff: typeof opts.backoff === 'number' ? opts.backoff : opts.backoff?.delay,
      };
    });

    const ids = await this.client.addBulk(this.name, flashqJobs);
    return ids.map((id, i) => ({
      id,
      queue: this.name,
      data: jobs[i].data,
    })) as Job<T>[];
  }

  /**
   * Get a job by ID
   */
  async getJob(jobId: number): Promise<Job<T> | null> {
    const result = await this.client.getJob(jobId);
    return result?.job as Job<T> | null;
  }

  /**
   * Wait for a job to complete and return its result
   */
  async finished<R = unknown>(jobId: number, timeout?: number): Promise<R | null> {
    return this.client.finished<R>(jobId, timeout);
  }

  /**
   * Pause the queue
   */
  async pause(): Promise<void> {
    await this.client.pause(this.name);
  }

  /**
   * Resume the queue
   */
  async resume(): Promise<void> {
    await this.client.resume(this.name);
  }

  /**
   * Check if queue is paused
   */
  async isPaused(): Promise<boolean> {
    return this.client.isPaused(this.name);
  }

  /**
   * Get job counts by state
   */
  async getJobCounts(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
  }> {
    return this.client.getJobCounts(this.name);
  }

  /**
   * Drain the queue (remove all waiting jobs)
   */
  async drain(): Promise<void> {
    await this.client.drain(this.name);
  }

  /**
   * Obliterate the queue (remove all data)
   */
  async obliterate(): Promise<void> {
    await this.client.obliterate(this.name);
  }

  /**
   * Clean jobs by state and age
   */
  async clean(grace: number, limit: number, type: 'completed' | 'failed' | 'delayed' | 'waiting'): Promise<number[]> {
    const count = await this.client.clean(this.name, grace, type, limit);
    return Array(count).fill(0);
  }

  /**
   * Close the queue connection
   */
  async close(): Promise<void> {
    await this.client.close();
  }
}
