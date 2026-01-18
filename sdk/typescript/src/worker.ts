import { EventEmitter } from 'events';
import { FlashQ } from './client';
import type {
  Job,
  JobProcessor,
  WorkerOptions,
  ClientOptions,
} from './types';

/**
 * FlashQ Worker
 *
 * High-performance job processor using batch operations by default.
 * Processes jobs from one or more queues with configurable concurrency.
 *
 * @example
 * ```typescript
 * // Simple usage - uses batch mode by default (15x faster)
 * const worker = new Worker('emails', async (job) => {
 *   await sendEmail(job.data.to, job.data.subject, job.data.body);
 *   return { sent: true };
 * });
 *
 * await worker.start();
 *
 * // High-performance configuration
 * const worker = new Worker('tasks', processor, {
 *   concurrency: 20,    // 20 parallel workers
 *   batchSize: 100,     // 100 jobs per batch (default)
 * });
 *
 * // Graceful shutdown
 * process.on('SIGTERM', () => worker.stop());
 * ```
 */
export class Worker<T = unknown, R = unknown> extends EventEmitter {
  private clients: FlashQ[] = [];
  private clientOptions: ClientOptions;
  private queues: string[];
  private processor: JobProcessor<T, R>;
  private options: Required<WorkerOptions>;
  private running = false;
  private processing = 0;
  private jobsProcessed = 0;
  private workers: Promise<void>[] = [];
  private heartbeatTimer?: NodeJS.Timeout;

  constructor(
    queues: string | string[],
    processor: JobProcessor<T, R>,
    options: WorkerOptions & ClientOptions = {}
  ) {
    super();
    this.queues = Array.isArray(queues) ? queues : [queues];
    this.processor = processor;
    this.options = {
      id: options.id ?? `worker-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      concurrency: options.concurrency ?? 10,      // Default 10 workers
      batchSize: options.batchSize ?? 100,         // Default 100 jobs per batch
      heartbeatInterval: options.heartbeatInterval ?? 1000,
      autoAck: options.autoAck ?? true,
    };

    this.clientOptions = {
      host: options.host,
      port: options.port,
      httpPort: options.httpPort,
      token: options.token,
      timeout: options.timeout,
      useHttp: options.useHttp,
    };
  }

  /**
   * Start processing jobs
   */
  async start(): Promise<void> {
    if (this.running) return;

    // Create a separate client for each worker (TCP pull is blocking)
    for (let i = 0; i < this.options.concurrency; i++) {
      const client = new FlashQ(this.clientOptions);
      await client.connect();
      this.clients.push(client);
    }

    this.running = true;
    this.emit('ready');

    // Start heartbeat
    this.startHeartbeat();

    // Start worker loops (each with its own client)
    for (let i = 0; i < this.options.concurrency; i++) {
      this.workers.push(this.batchWorkerLoop(i, this.clients[i]));
    }
  }

  /**
   * Stop processing jobs (graceful shutdown)
   */
  async stop(): Promise<void> {
    if (!this.running) return;

    this.running = false;
    this.emit('stopping');

    // Stop heartbeat
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }

    // Wait for current jobs to finish
    await Promise.all(this.workers);
    this.workers = [];

    // Close all clients
    await Promise.all(this.clients.map((c) => c.close()));
    this.clients = [];
    this.emit('stopped');
  }

  /**
   * Check if worker is running
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get number of jobs currently being processed
   */
  getProcessingCount(): number {
    return this.processing;
  }

  /**
   * Get total number of jobs processed by this worker
   */
  getJobsProcessed(): number {
    return this.jobsProcessed;
  }

  /**
   * Batch worker loop - pulls and processes jobs in batches for maximum throughput
   */
  private async batchWorkerLoop(workerId: number, client: FlashQ): Promise<void> {
    const batchSize = this.options.batchSize;

    while (this.running) {
      for (const queue of this.queues) {
        if (!this.running) break;

        try {
          // Batch pull with SHORT timeout (500ms) for responsive shutdown
          const jobs = await client.pullBatch<T>(queue, batchSize, 500);

          // No jobs available - continue polling
          if (!jobs || jobs.length === 0) {
            continue;
          }

          this.processing += jobs.length;

          // Track successful and failed jobs
          const successJobs: Array<{ job: Job & { data: T }; result: R }> = [];
          const failedJobs: Array<{ job: Job & { data: T }; error: string }> = [];

          // Process all jobs in parallel
          await Promise.all(
            jobs.map(async (job) => {
              this.emit('active', job, workerId);

              try {
                const result = await this.processJob(job);
                successJobs.push({ job, result });
              } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                failedJobs.push({ job, error: errorMessage });
              }
            })
          );

          // Batch ack successful jobs - THEN emit completed
          if (this.options.autoAck && successJobs.length > 0) {
            await client.ackBatch(successJobs.map(j => j.job.id));
            // Emit completed AFTER ack succeeds
            for (const { job, result } of successJobs) {
              this.jobsProcessed++;
              this.emit('completed', job, result, workerId);
            }
          } else if (!this.options.autoAck && successJobs.length > 0) {
            // If autoAck is disabled, emit completed after processing
            for (const { job, result } of successJobs) {
              this.jobsProcessed++;
              this.emit('completed', job, result, workerId);
            }
          }

          // Fail individual jobs that errored - THEN emit failed
          if (this.options.autoAck && failedJobs.length > 0) {
            await Promise.all(
              failedJobs.map(async ({ job, error }) => {
                await client.fail(job.id, error);
                this.emit('failed', job, new Error(error), workerId);
              })
            );
          } else if (!this.options.autoAck && failedJobs.length > 0) {
            for (const { job, error } of failedJobs) {
              this.emit('failed', job, new Error(error), workerId);
            }
          }

          this.processing -= jobs.length;
        } catch (error) {
          // Timeout is expected when no jobs available - not an error
          const errorMsg = error instanceof Error ? error.message : String(error);
          if (errorMsg.includes('timeout') || errorMsg.includes('Timeout')) {
            // Normal - no jobs available, retry
            continue;
          }
          // Connection error - wait before retry
          if (this.running) {
            this.emit('error', error);
            await this.sleep(1000);
          }
        }
      }
    }
  }

  private async processJob(job: Job & { data: T }): Promise<R> {
    return this.processor(job);
  }

  private startHeartbeat(): void {
    const sendHeartbeat = async () => {
      if (!this.running) return;
      try {
        const url = `http://${this.clientOptions.host ?? 'localhost'}:${this.clientOptions.httpPort ?? 6790}/workers/${this.options.id}/heartbeat`;
        await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            queues: this.queues,
            concurrency: this.options.concurrency,
            jobs_processed: this.jobsProcessed,
          }),
        });
      } catch {
        // Ignore heartbeat errors (HTTP may not be available)
      }
    };

    this.heartbeatTimer = setInterval(sendHeartbeat, this.options.heartbeatInterval);
    sendHeartbeat();
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Update progress for the current job
   * (Use this within your processor function)
   */
  async updateProgress(jobId: number, progress: number, message?: string): Promise<void> {
    if (this.clients.length > 0) {
      await this.clients[0].progress(jobId, progress, message);
    }
  }
}

export default Worker;
