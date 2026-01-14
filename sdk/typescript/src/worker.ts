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
 * Processes jobs from one or more queues.
 *
 * @example
 * ```typescript
 * const worker = new Worker('emails', async (job) => {
 *   await sendEmail(job.data.to, job.data.subject, job.data.body);
 *   return { sent: true };
 * });
 *
 * await worker.start();
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
      concurrency: options.concurrency ?? 1,
      heartbeatInterval: options.heartbeatInterval ?? 1000, // 1 second default
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
      this.workers.push(this.workerLoop(i, this.clients[i]));
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

  private async workerLoop(workerId: number, client: FlashQ): Promise<void> {
    while (this.running) {
      for (const queue of this.queues) {
        if (!this.running) break;

        try {
          // Pull with short server-side timeout for graceful shutdown
          const job = await client.pull<T>(queue, 2000);

          // No job available (server timeout) - continue polling
          if (!job) continue;

          this.processing++;
          this.emit('active', job, workerId);

          try {
            const result = await this.processJob(job);

            if (this.options.autoAck) {
              await client.ack(job.id, result);
            }

            this.jobsProcessed++;
            this.emit('completed', job, result, workerId);
          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.emit('failed', job, error, workerId);

            if (this.options.autoAck) {
              await client.fail(job.id, errorMessage);
            }
          } finally {
            this.processing--;
          }
        } catch (error) {
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
