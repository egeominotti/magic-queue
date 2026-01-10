import { MagicQueueClient, ClientOptions } from "./client";

export interface PushOptions {
  priority?: number;
  delay?: number;
  ttl?: number;           // Job expires after N ms
  maxAttempts?: number;   // Max retries before DLQ
  backoff?: number;       // Base backoff in ms (exponential)
  uniqueKey?: string;     // Deduplication key
  dependsOn?: number[];   // Job dependencies
}

export interface JobInput<T = any> {
  data: T;
  priority?: number;
  delay?: number;
  ttl?: number;
  maxAttempts?: number;
  backoff?: number;
  uniqueKey?: string;
  dependsOn?: number[];
}

export interface PushResponse {
  ok: boolean;
  id: number;
  error?: string;
}

export interface BatchResponse {
  ok: boolean;
  ids: number[];
}

export interface StatsResponse {
  ok: boolean;
  queued: number;
  processing: number;
  delayed: number;
  dlq: number;
}

export interface MetricsResponse {
  ok: boolean;
  metrics: {
    total_pushed: number;
    total_completed: number;
    total_failed: number;
    jobs_per_second: number;
    avg_latency_ms: number;
    queues: Array<{
      name: string;
      pending: number;
      processing: number;
      dlq: number;
      rate_limit: number | null;
    }>;
  };
}

export interface ProgressResponse {
  ok: boolean;
  progress: {
    id: number;
    progress: number;
    message: string | null;
  };
}

export interface CronJob {
  name: string;
  queue: string;
  data: any;
  schedule: string;
  priority: number;
  next_run: number;
}

export interface CronListResponse {
  ok: boolean;
  crons: CronJob[];
}

export class Queue<T = any> {
  private client: MagicQueueClient;
  private queueName: string;
  private autoConnect: boolean;

  constructor(queueName: string, options: ClientOptions = {}) {
    this.queueName = queueName;
    this.client = new MagicQueueClient(options);
    this.autoConnect = true;
  }

  async connect(): Promise<void> {
    await this.client.connect();
  }

  private async ensureConnected(): Promise<void> {
    if (!this.client.isConnected() && this.autoConnect) {
      await this.connect();
    }
  }

  async push(data: T, options: PushOptions = {}): Promise<number> {
    await this.ensureConnected();

    const response = await this.client.send<PushResponse>({
      cmd: "PUSH",
      queue: this.queueName,
      data,
      priority: options.priority ?? 0,
      delay: options.delay,
      ttl: options.ttl,
      max_attempts: options.maxAttempts,
      backoff: options.backoff,
      unique_key: options.uniqueKey,
      depends_on: options.dependsOn,
    });

    if (!response.ok) {
      throw new Error(response.error || "Push failed");
    }

    return response.id;
  }

  async pushBatch(jobs: JobInput<T>[]): Promise<number[]> {
    await this.ensureConnected();

    const response = await this.client.send<BatchResponse>({
      cmd: "PUSHB",
      queue: this.queueName,
      jobs: jobs.map((j) => ({
        data: j.data,
        priority: j.priority ?? 0,
        delay: j.delay,
        ttl: j.ttl,
        max_attempts: j.maxAttempts,
        backoff: j.backoff,
        unique_key: j.uniqueKey,
        depends_on: j.dependsOn,
      })),
    });

    return response.ids;
  }

  async pushMany(items: T[], options: PushOptions = {}): Promise<number[]> {
    const jobs = items.map((data) => ({
      data,
      priority: options.priority,
      delay: options.delay,
      ttl: options.ttl,
      maxAttempts: options.maxAttempts,
      backoff: options.backoff,
    }));
    return this.pushBatch(jobs);
  }

  async cancel(jobId: number): Promise<void> {
    await this.ensureConnected();
    const response = await this.client.send<{ ok: boolean; error?: string }>({
      cmd: "CANCEL",
      id: jobId,
    });
    if (!response.ok) {
      throw new Error(response.error || "Cancel failed");
    }
  }

  async getProgress(jobId: number): Promise<{ progress: number; message: string | null }> {
    await this.ensureConnected();
    const response = await this.client.send<ProgressResponse>({
      cmd: "GETPROGRESS",
      id: jobId,
    });
    return {
      progress: response.progress.progress,
      message: response.progress.message,
    };
  }

  async getDlq(count?: number): Promise<any[]> {
    await this.ensureConnected();
    const response = await this.client.send<{ ok: boolean; jobs: any[] }>({
      cmd: "DLQ",
      queue: this.queueName,
      count,
    });
    return response.jobs;
  }

  async retryDlq(jobId?: number): Promise<number> {
    await this.ensureConnected();
    const response = await this.client.send<BatchResponse>({
      cmd: "RETRYDLQ",
      queue: this.queueName,
      id: jobId,
    });
    return response.ids[0];
  }

  async stats(): Promise<StatsResponse> {
    await this.ensureConnected();
    return this.client.send<StatsResponse>({ cmd: "STATS" });
  }

  async metrics(): Promise<MetricsResponse> {
    await this.ensureConnected();
    return this.client.send<MetricsResponse>({ cmd: "METRICS" });
  }

  async setRateLimit(limit: number): Promise<void> {
    await this.ensureConnected();
    await this.client.send({
      cmd: "RATELIMIT",
      queue: this.queueName,
      limit,
    });
  }

  async clearRateLimit(): Promise<void> {
    await this.ensureConnected();
    await this.client.send({
      cmd: "RATELIMITCLEAR",
      queue: this.queueName,
    });
  }

  async addCron(name: string, schedule: string, data: T, priority: number = 0): Promise<void> {
    await this.ensureConnected();
    await this.client.send({
      cmd: "CRON",
      name,
      queue: this.queueName,
      data,
      schedule,
      priority,
    });
  }

  async deleteCron(name: string): Promise<void> {
    await this.ensureConnected();
    await this.client.send({
      cmd: "CRONDELETE",
      name,
    });
  }

  async listCrons(): Promise<CronJob[]> {
    await this.ensureConnected();
    const response = await this.client.send<CronListResponse>({
      cmd: "CRONLIST",
    });
    return response.crons;
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
