import { MagicQueueClient, ClientOptions } from "./client";

export interface Job<T = any> {
  id: number;
  queue: string;
  data: T;
  priority: number;
  created_at: number;
  run_at: number;
  attempts: number;
  max_attempts: number;
  progress: number;
}

export interface PullResponse<T = any> {
  ok: boolean;
  job: Job<T>;
}

export interface PullBatchResponse<T = any> {
  ok: boolean;
  jobs: Job<T>[];
}

export interface JobContext<T = any> {
  job: Job<T>;
  updateProgress: (progress: number, message?: string) => Promise<void>;
}

export type JobHandler<T = any> = (job: Job<T>, ctx: JobContext<T>) => Promise<void> | void;

export interface WorkerOptions extends ClientOptions {
  concurrency?: number;
  batchSize?: number;
}

export class Worker<T = any> {
  private client: MagicQueueClient;
  private queueName: string;
  private handler: JobHandler<T>;
  private running: boolean = false;
  private concurrency: number;
  private batchSize: number;

  constructor(
    queueName: string,
    handler: JobHandler<T>,
    options: WorkerOptions = {}
  ) {
    this.queueName = queueName;
    this.handler = handler;
    this.client = new MagicQueueClient(options);
    this.concurrency = options.concurrency || 1;
    this.batchSize = options.batchSize || 10;
  }

  async start(): Promise<void> {
    await this.client.connect();
    this.running = true;

    console.log(
      `Worker started: queue="${this.queueName}" concurrency=${this.concurrency} batchSize=${this.batchSize}`
    );

    if (this.batchSize > 1) {
      await this.batchProcessLoop();
    } else {
      const workers = Array.from({ length: this.concurrency }, () =>
        this.processLoop()
      );
      await Promise.all(workers);
    }
  }

  private createContext(job: Job<T>): JobContext<T> {
    return {
      job,
      updateProgress: async (progress: number, message?: string) => {
        await this.client.send({
          cmd: "PROGRESS",
          id: job.id,
          progress: Math.min(100, Math.max(0, progress)),
          message,
        });
      },
    };
  }

  private async batchProcessLoop(): Promise<void> {
    while (this.running) {
      try {
        const response = await this.client.send<PullBatchResponse<T>>({
          cmd: "PULLB",
          queue: this.queueName,
          count: this.batchSize,
        });

        const jobs = response.jobs;
        if (!jobs || jobs.length === 0) continue;

        const results = await Promise.allSettled(
          jobs.map(async (job) => {
            const ctx = this.createContext(job);
            await this.handler(job, ctx);
            return job.id;
          })
        );

        const successIds: number[] = [];
        const failedJobs: { id: number; error: string }[] = [];

        results.forEach((result, idx) => {
          if (result.status === "fulfilled") {
            successIds.push(result.value);
          } else {
            failedJobs.push({
              id: jobs[idx].id,
              error: result.reason?.message || "Unknown error",
            });
          }
        });

        if (successIds.length > 0) {
          await this.client.send({ cmd: "ACKB", ids: successIds });
        }

        for (const failed of failedJobs) {
          await this.client.send({
            cmd: "FAIL",
            id: failed.id,
            error: failed.error,
          });
        }
      } catch (error) {
        if (this.running) {
          console.error("Worker error:", error);
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }
  }

  private async processLoop(): Promise<void> {
    while (this.running) {
      try {
        const response = await this.client.send<PullResponse<T>>({
          cmd: "PULL",
          queue: this.queueName,
        });

        const job = response.job;
        const ctx = this.createContext(job);

        try {
          await this.handler(job, ctx);
          await this.client.send({ cmd: "ACK", id: job.id });
        } catch (error) {
          const errorMsg =
            error instanceof Error ? error.message : String(error);
          await this.client.send({
            cmd: "FAIL",
            id: job.id,
            error: errorMsg,
          });
        }
      } catch (error) {
        if (this.running) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.client.close();
  }
}
