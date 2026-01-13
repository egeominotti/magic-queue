/**
 * Sandboxed Processors for flashQ
 *
 * Run job processors in isolated child processes for safety and resource management.
 *
 * @example
 * ```typescript
 * // processor.ts - the sandboxed processor file
 * import { createProcessor } from 'flashq/sandbox';
 *
 * createProcessor(async (job) => {
 *   // Your processing logic here
 *   return { processed: true };
 * });
 * ```
 *
 * @example
 * ```typescript
 * // main.ts - start sandboxed workers
 * import { SandboxedWorker } from 'flashq';
 *
 * const worker = new SandboxedWorker('emails', {
 *   processorFile: './processor.ts',
 *   concurrency: 4,
 *   timeout: 30000,
 * });
 *
 * await worker.run();
 * ```
 */

import { fork, type ChildProcess } from 'child_process';
import type { Job, JobProcessor } from './types';
import { FlashQ } from './client';

export interface SandboxOptions {
  /** Path to worker script that uses createProcessor() */
  processorFile: string;
  /** Number of concurrent sandboxed workers (default: 1) */
  concurrency?: number;
  /** Timeout for each job in ms (default: 30000) */
  timeout?: number;
  /** Connection options for flashQ client */
  connection?: {
    host?: string;
    port?: number;
    token?: string;
  };
}

interface WorkerMessage {
  type: 'result' | 'error' | 'progress' | 'log';
  jobId: number;
  data?: unknown;
  error?: string;
  progress?: number;
  message?: string;
  level?: 'info' | 'warn' | 'error';
}

interface JobMessage {
  type: 'job';
  job: Job;
}

/**
 * Sandboxed Worker - runs job processors in child processes
 */
export class SandboxedWorker {
  private queue: string;
  private options: Required<SandboxOptions>;
  private client: FlashQ;
  private running = false;
  private activeProcesses: Map<number, ChildProcess> = new Map();

  constructor(queue: string, options: SandboxOptions) {
    this.queue = queue;
    this.options = {
      processorFile: options.processorFile,
      concurrency: options.concurrency ?? 1,
      timeout: options.timeout ?? 30000,
      connection: options.connection ?? {},
    };

    this.client = new FlashQ({
      host: this.options.connection.host,
      port: this.options.connection.port,
      token: this.options.connection.token,
    });
  }

  /**
   * Start processing jobs from the queue
   */
  async run(): Promise<void> {
    this.running = true;
    await this.client.connect();

    // Start worker loops
    const workers: Promise<void>[] = [];
    for (let i = 0; i < this.options.concurrency; i++) {
      workers.push(this.workerLoop());
    }

    await Promise.all(workers);
  }

  /**
   * Stop processing and close all workers
   */
  async close(): Promise<void> {
    this.running = false;

    // Kill all active processes
    for (const [jobId, child] of this.activeProcesses) {
      child.kill('SIGTERM');
      this.activeProcesses.delete(jobId);
    }

    await this.client.close();
  }

  private async workerLoop(): Promise<void> {
    while (this.running) {
      try {
        const job = await this.client.pull(this.queue);
        if (!job) continue;

        try {
          const result = await this.processInSandbox(job);
          await this.client.ack(job.id, result);
        } catch (err) {
          const error = err instanceof Error ? err.message : String(err);
          await this.client.fail(job.id, error);
        }
      } catch (err) {
        // Connection error or other issue - wait and retry
        if (this.running) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }
  }

  private async processInSandbox(job: Job): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const child = fork(this.options.processorFile, [], {
        stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
        env: {
          ...process.env,
          FLASHQ_JOB_ID: String(job.id),
        },
      });

      this.activeProcesses.set(job.id, child);

      const timeout = setTimeout(() => {
        child.kill('SIGTERM');
        reject(new Error(`Job ${job.id} timed out after ${this.options.timeout}ms`));
      }, this.options.timeout);

      child.on('message', async (msg: WorkerMessage) => {
        switch (msg.type) {
          case 'result':
            clearTimeout(timeout);
            resolve(msg.data);
            break;

          case 'error':
            clearTimeout(timeout);
            reject(new Error(msg.error ?? 'Unknown error'));
            break;

          case 'progress':
            // Forward progress to server
            if (msg.progress !== undefined) {
              await this.client.progress(job.id, msg.progress, msg.message);
            }
            break;

          case 'log':
            // Forward logs to server
            if (msg.message) {
              await this.client.log(job.id, msg.message, msg.level ?? 'info');
            }
            break;
        }
      });

      child.on('error', (err) => {
        clearTimeout(timeout);
        this.activeProcesses.delete(job.id);
        reject(err);
      });

      child.on('exit', (code) => {
        clearTimeout(timeout);
        this.activeProcesses.delete(job.id);
        if (code !== 0 && code !== null) {
          reject(new Error(`Worker exited with code ${code}`));
        }
      });

      // Send job to worker
      const message: JobMessage = { type: 'job', job };
      child.send(message);
    });
  }
}

/**
 * Create a sandboxed processor.
 * Call this in your processor file to handle jobs.
 *
 * @example
 * ```typescript
 * // processor.ts
 * import { createProcessor } from 'flashq/sandbox';
 *
 * createProcessor(async (job, { progress, log }) => {
 *   log('Starting processing', 'info');
 *
 *   for (let i = 0; i < 100; i++) {
 *     await doWork(i);
 *     progress(i + 1, `Processed ${i + 1} items`);
 *   }
 *
 *   log('Processing complete', 'info');
 *   return { success: true, items: 100 };
 * });
 * ```
 */
export function createProcessor<T = unknown, R = unknown>(
  processor: (
    job: Job & { data: T },
    helpers: {
      progress: (value: number, message?: string) => void;
      log: (message: string, level?: 'info' | 'warn' | 'error') => void;
    }
  ) => R | Promise<R>
): void {
  const sendMessage = (msg: WorkerMessage) => {
    if (process.send) {
      process.send(msg);
    }
  };

  const helpers = {
    progress: (value: number, message?: string) => {
      const jobId = parseInt(process.env.FLASHQ_JOB_ID ?? '0', 10);
      sendMessage({
        type: 'progress',
        jobId,
        progress: value,
        message,
      });
    },
    log: (message: string, level: 'info' | 'warn' | 'error' = 'info') => {
      const jobId = parseInt(process.env.FLASHQ_JOB_ID ?? '0', 10);
      sendMessage({
        type: 'log',
        jobId,
        message,
        level,
      });
    },
  };

  process.on('message', async (msg: JobMessage) => {
    if (msg.type !== 'job') return;

    const jobId = msg.job.id;

    try {
      const result = await processor(msg.job as Job & { data: T }, helpers);
      sendMessage({ type: 'result', jobId, data: result });
      process.exit(0);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      sendMessage({ type: 'error', jobId, error });
      process.exit(1);
    }
  });
}

export default SandboxedWorker;
