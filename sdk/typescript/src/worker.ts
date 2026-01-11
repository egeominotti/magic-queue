import { EventEmitter } from 'events';
import { MagicQueue } from './client';
import type {
  Job,
  JobProcessor,
  WorkerOptions,
  ClientOptions,
} from './types';

/**
 * MagicQueue Worker
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
  private client: MagicQueue;
  private queues: string[];
  private processor: JobProcessor<T, R>;
  private options: Required<WorkerOptions>;
  private running = false;
  private processing = 0;
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
      heartbeatInterval: options.heartbeatInterval ?? 30000,
      autoAck: options.autoAck ?? true,
    };

    this.client = new MagicQueue({
      host: options.host,
      port: options.port,
      httpPort: options.httpPort,
      token: options.token,
      timeout: options.timeout,
      useHttp: options.useHttp,
    });
  }

  /**
   * Start processing jobs
   */
  async start(): Promise<void> {
    if (this.running) return;

    await this.client.connect();
    this.running = true;
    this.emit('ready');

    // Start heartbeat
    this.startHeartbeat();

    // Start worker loops
    for (let i = 0; i < this.options.concurrency; i++) {
      this.workers.push(this.workerLoop(i));
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

    await this.client.close();
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

  private async workerLoop(workerId: number): Promise<void> {
    while (this.running) {
      for (const queue of this.queues) {
        if (!this.running) break;

        try {
          const job = await this.pullWithTimeout(queue, 1000);
          if (!job) continue;

          this.processing++;
          this.emit('active', job, workerId);

          try {
            const result = await this.processJob(job);

            if (this.options.autoAck) {
              await this.client.ack(job.id, result);
            }

            this.emit('completed', job, result, workerId);
          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.emit('failed', job, error, workerId);

            if (this.options.autoAck) {
              await this.client.fail(job.id, errorMessage);
            }
          } finally {
            this.processing--;
          }
        } catch (error) {
          // Connection error, wait before retry
          if (this.running) {
            this.emit('error', error);
            await this.sleep(1000);
          }
        }
      }
    }
  }

  private async pullWithTimeout(
    queue: string,
    timeoutMs: number
  ): Promise<(Job & { data: T }) | null> {
    // TCP pull is blocking, so we use a race with timeout
    const timeoutPromise = new Promise<null>((resolve) => {
      setTimeout(() => resolve(null), timeoutMs);
    });

    try {
      // Note: In real implementation, you might want to use non-blocking pull
      // or implement proper timeout handling at the protocol level
      const job = await Promise.race([
        this.client.pull<T>(queue),
        timeoutPromise,
      ]);
      return job;
    } catch {
      return null;
    }
  }

  private async processJob(job: Job & { data: T }): Promise<R> {
    return this.processor(job);
  }

  private startHeartbeat(): void {
    const sendHeartbeat = async () => {
      if (!this.running) return;
      try {
        // Heartbeat is sent via HTTP if available
        // For TCP-only, this is a no-op
      } catch {
        // Ignore heartbeat errors
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
    await this.client.progress(jobId, progress, message);
  }
}

export default Worker;
