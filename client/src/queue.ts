import { MagicQueueClient, ClientOptions } from "./client";

export interface PushOptions {
  priority?: number;
  delay?: number; // millisecondi
}

export interface JobInput<T = any> {
  data: T;
  priority?: number;
  delay?: number;
}

export interface PushResponse {
  ok: boolean;
  id: string;
}

export interface BatchResponse {
  ok: boolean;
  ids: string[];
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

  /**
   * Push singolo job
   */
  async push(data: T, options: PushOptions = {}): Promise<string> {
    await this.ensureConnected();

    const response = await this.client.send<PushResponse>({
      cmd: "PUSH",
      queue: this.queueName,
      data,
      priority: options.priority ?? 0,
      delay: options.delay,
    });

    return response.id;
  }

  /**
   * Batch push - più job in una richiesta (molto più veloce)
   */
  async pushBatch(jobs: JobInput<T>[]): Promise<string[]> {
    await this.ensureConnected();

    const response = await this.client.send<BatchResponse>({
      cmd: "PUSHB",
      queue: this.queueName,
      jobs: jobs.map((j) => ({
        data: j.data,
        priority: j.priority ?? 0,
        delay: j.delay,
      })),
    });

    return response.ids;
  }

  /**
   * Push multipli come array semplice (usa batch internamente)
   */
  async pushMany(items: T[], options: PushOptions = {}): Promise<string[]> {
    const jobs = items.map((data) => ({
      data,
      priority: options.priority,
      delay: options.delay,
    }));
    return this.pushBatch(jobs);
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
