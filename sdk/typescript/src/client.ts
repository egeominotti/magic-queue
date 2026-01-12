import * as net from 'net';
import { EventEmitter } from 'events';
import type {
  ClientOptions,
  Job,
  JobState,
  JobWithState,
  PushOptions,
  QueueInfo,
  QueueStats,
  Metrics,
  CronJob,
  CronOptions,
  WebhookConfig,
  WebhookOptions,
  WorkerInfo,
  ApiResponse,
} from './types';

/**
 * FlashQ Client
 *
 * Connects to FlashQ server via TCP protocol.
 *
 * @example
 * ```typescript
 * const client = new FlashQ({ host: 'localhost', port: 6789 });
 * await client.connect();
 *
 * // Push a job
 * const job = await client.push('emails', { to: 'user@example.com' });
 *
 * // Pull and process
 * const pulled = await client.pull('emails');
 * await client.ack(pulled.id);
 *
 * await client.close();
 * ```
 */
export class FlashQ extends EventEmitter {
  private options: Required<ClientOptions>;
  private socket: net.Socket | null = null;
  private connected = false;
  private authenticated = false;
  private responseQueue: Array<{
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
  }> = [];
  private buffer = '';
  private reconnecting = false;

  constructor(options: ClientOptions = {}) {
    super();
    this.options = {
      host: options.host ?? 'localhost',
      port: options.port ?? 6789,
      httpPort: options.httpPort ?? 6790,
      socketPath: options.socketPath ?? '',
      token: options.token ?? '',
      timeout: options.timeout ?? 5000,
      useHttp: options.useHttp ?? false,
    };
  }

  // ============== Connection Management ==============

  /**
   * Connect to FlashQ server
   */
  async connect(): Promise<void> {
    if (this.options.useHttp) {
      this.connected = true;
      return;
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this.options.timeout);

      // Use Unix socket if socketPath is set, otherwise TCP
      const connectionOptions = this.options.socketPath
        ? { path: this.options.socketPath }
        : { host: this.options.host, port: this.options.port };

      this.socket = net.createConnection(
        connectionOptions,
        async () => {
          clearTimeout(timeout);
          this.connected = true;
          this.setupSocketHandlers();

          // Authenticate if token provided
          if (this.options.token) {
            try {
              await this.auth(this.options.token);
              this.authenticated = true;
            } catch (err) {
              this.socket?.destroy();
              reject(err);
              return;
            }
          }

          this.emit('connect');
          resolve();
        }
      );

      this.socket.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  private setupSocketHandlers(): void {
    if (!this.socket) return;

    this.socket.on('data', (data) => {
      this.buffer += data.toString();
      this.processBuffer();
    });

    this.socket.on('close', () => {
      this.connected = false;
      this.emit('disconnect');
    });

    this.socket.on('error', (err) => {
      this.emit('error', err);
    });
  }

  private processBuffer(): void {
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop() ?? '';

    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const response = JSON.parse(line);
        const pending = this.responseQueue.shift();
        if (pending) {
          if (response.ok === false && response.error) {
            pending.reject(new Error(response.error));
          } else {
            pending.resolve(response);
          }
        }
      } catch {
        // Ignore parse errors
      }
    }
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    this.connected = false;
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  // ============== Internal Methods ==============

  private async send<T>(command: Record<string, unknown>): Promise<T> {
    if (this.options.useHttp) {
      return this.sendHttp<T>(command);
    }
    return this.sendTcp<T>(command);
  }

  private async sendTcp<T>(command: Record<string, unknown>): Promise<T> {
    if (!this.socket || !this.connected) {
      throw new Error('Not connected');
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Request timeout'));
      }, this.options.timeout);

      this.responseQueue.push({
        resolve: (value) => {
          clearTimeout(timeout);
          resolve(value as T);
        },
        reject: (err) => {
          clearTimeout(timeout);
          reject(err);
        },
      });

      this.socket!.write(JSON.stringify(command) + '\n');
    });
  }

  private async sendHttp<T>(command: Record<string, unknown>): Promise<T> {
    const { cmd, ...params } = command;
    const baseUrl = `http://${this.options.host}:${this.options.httpPort}`;

    // Map commands to HTTP endpoints
    const response = await this.httpRequest(baseUrl, cmd as string, params);
    return response as T;
  }

  private async httpRequest(
    baseUrl: string,
    cmd: string,
    params: Record<string, unknown>
  ): Promise<unknown> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.options.token) {
      headers['Authorization'] = `Bearer ${this.options.token}`;
    }

    let url: string;
    let method: string;
    let body: string | undefined;

    switch (cmd) {
      case 'PUSH':
        url = `${baseUrl}/queues/${params.queue}/jobs`;
        method = 'POST';
        body = JSON.stringify({
          data: params.data,
          priority: params.priority,
          delay: params.delay,
          ttl: params.ttl,
          timeout: params.timeout,
          max_attempts: params.max_attempts,
          backoff: params.backoff,
          unique_key: params.unique_key,
          depends_on: params.depends_on,
          tags: params.tags,
        });
        break;
      case 'PULL':
        url = `${baseUrl}/queues/${params.queue}/jobs?count=1`;
        method = 'GET';
        break;
      case 'PULLB':
        url = `${baseUrl}/queues/${params.queue}/jobs?count=${params.count}`;
        method = 'GET';
        break;
      case 'ACK':
        url = `${baseUrl}/jobs/${params.id}/ack`;
        method = 'POST';
        body = JSON.stringify({ result: params.result });
        break;
      case 'FAIL':
        url = `${baseUrl}/jobs/${params.id}/fail`;
        method = 'POST';
        body = JSON.stringify({ error: params.error });
        break;
      case 'STATS':
        url = `${baseUrl}/stats`;
        method = 'GET';
        break;
      case 'METRICS':
        url = `${baseUrl}/metrics`;
        method = 'GET';
        break;
      case 'LISTQUEUES':
        url = `${baseUrl}/queues`;
        method = 'GET';
        break;
      default:
        throw new Error(`HTTP not supported for command: ${cmd}`);
    }

    const res = await fetch(url, { method, headers, body });
    const json = (await res.json()) as ApiResponse;

    if (!json.ok) {
      throw new Error(json.error ?? 'Unknown error');
    }

    return json;
  }

  // ============== Authentication ==============

  /**
   * Authenticate with the server
   */
  async auth(token: string): Promise<void> {
    const response = await this.send<{ ok: boolean }>({
      cmd: 'AUTH',
      token,
    });
    if (!response.ok) {
      throw new Error('Authentication failed');
    }
    this.options.token = token;
    this.authenticated = true;
  }

  // ============== Core Operations ==============

  /**
   * Push a job to a queue
   */
  async push<T = unknown>(
    queue: string,
    data: T,
    options: PushOptions = {}
  ): Promise<Job> {
    const response = await this.send<{ ok: boolean; id: number }>({
      cmd: 'PUSH',
      queue,
      data,
      priority: options.priority ?? 0,
      delay: options.delay,
      ttl: options.ttl,
      timeout: options.timeout,
      max_attempts: options.max_attempts,
      backoff: options.backoff,
      unique_key: options.unique_key,
      depends_on: options.depends_on,
      tags: options.tags,
    });

    return {
      id: response.id,
      queue,
      data,
      priority: options.priority ?? 0,
      created_at: Date.now(),
      run_at: options.delay ? Date.now() + options.delay : Date.now(),
      started_at: 0,
      attempts: 0,
      max_attempts: options.max_attempts ?? 0,
      backoff: options.backoff ?? 0,
      ttl: options.ttl ?? 0,
      timeout: options.timeout ?? 0,
      unique_key: options.unique_key,
      depends_on: options.depends_on ?? [],
      progress: 0,
      tags: options.tags ?? [],
    };
  }

  /**
   * Push multiple jobs to a queue
   */
  async pushBatch<T = unknown>(
    queue: string,
    jobs: Array<{ data: T } & PushOptions>
  ): Promise<number[]> {
    const response = await this.send<{ ok: boolean; ids: number[] }>({
      cmd: 'PUSHB',
      queue,
      jobs: jobs.map((j) => ({
        data: j.data,
        priority: j.priority ?? 0,
        delay: j.delay,
        ttl: j.ttl,
        timeout: j.timeout,
        max_attempts: j.max_attempts,
        backoff: j.backoff,
        unique_key: j.unique_key,
        depends_on: j.depends_on,
        tags: j.tags,
      })),
    });
    return response.ids;
  }

  /**
   * Pull a job from a queue (blocking)
   */
  async pull<T = unknown>(queue: string): Promise<Job & { data: T }> {
    const response = await this.send<{ ok: boolean; job: Job }>({
      cmd: 'PULL',
      queue,
    });
    return response.job as Job & { data: T };
  }

  /**
   * Pull multiple jobs from a queue
   */
  async pullBatch<T = unknown>(
    queue: string,
    count: number
  ): Promise<Array<Job & { data: T }>> {
    const response = await this.send<{ ok: boolean; jobs: Job[] }>({
      cmd: 'PULLB',
      queue,
      count,
    });
    return response.jobs as Array<Job & { data: T }>;
  }

  /**
   * Acknowledge a job as completed
   */
  async ack(jobId: number, result?: unknown): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'ACK',
      id: jobId,
      result,
    });
  }

  /**
   * Acknowledge multiple jobs
   */
  async ackBatch(jobIds: number[]): Promise<number> {
    const response = await this.send<{ ok: boolean; ids: number[] }>({
      cmd: 'ACKB',
      ids: jobIds,
    });
    return response.ids?.[0] ?? 0;
  }

  /**
   * Fail a job (will retry or move to DLQ)
   */
  async fail(jobId: number, error?: string): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'FAIL',
      id: jobId,
      error,
    });
  }

  // ============== Job Management ==============

  /**
   * Get a job with its current state
   */
  async getJob(jobId: number): Promise<JobWithState | null> {
    const response = await this.send<{
      ok: boolean;
      job: Job | null;
      state: JobState | null;
    }>({
      cmd: 'GETJOB',
      id: jobId,
    });
    if (!response.job) return null;
    return { job: response.job, state: response.state! };
  }

  /**
   * Get job state only
   */
  async getState(jobId: number): Promise<JobState | null> {
    const response = await this.send<{
      ok: boolean;
      id: number;
      state: JobState | null;
    }>({
      cmd: 'GETSTATE',
      id: jobId,
    });
    return response.state;
  }

  /**
   * Get job result
   */
  async getResult<T = unknown>(jobId: number): Promise<T | null> {
    const response = await this.send<{
      ok: boolean;
      id: number;
      result: T | null;
    }>({
      cmd: 'GETRESULT',
      id: jobId,
    });
    return response.result;
  }

  /**
   * Cancel a pending job
   */
  async cancel(jobId: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'CANCEL',
      id: jobId,
    });
  }

  /**
   * Update job progress
   */
  async progress(
    jobId: number,
    progress: number,
    message?: string
  ): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'PROGRESS',
      id: jobId,
      progress: Math.min(100, Math.max(0, progress)),
      message,
    });
  }

  /**
   * Get job progress
   */
  async getProgress(jobId: number): Promise<{ progress: number; message?: string }> {
    const response = await this.send<{
      ok: boolean;
      id: number;
      progress: number;
      message?: string;
    }>({
      cmd: 'GETPROGRESS',
      id: jobId,
    });
    return { progress: response.progress, message: response.message };
  }

  // ============== Dead Letter Queue ==============

  /**
   * Get jobs from the dead letter queue
   */
  async getDlq(queue: string, count = 100): Promise<Job[]> {
    const response = await this.send<{ ok: boolean; jobs: Job[] }>({
      cmd: 'DLQ',
      queue,
      count,
    });
    return response.jobs;
  }

  /**
   * Retry jobs from DLQ
   */
  async retryDlq(queue: string, jobId?: number): Promise<number> {
    const response = await this.send<{ ok: boolean; ids: number[] }>({
      cmd: 'RETRYDLQ',
      queue,
      id: jobId,
    });
    return response.ids?.[0] ?? 0;
  }

  // ============== Queue Control ==============

  /**
   * Pause a queue
   */
  async pause(queue: string): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'PAUSE',
      queue,
    });
  }

  /**
   * Resume a paused queue
   */
  async resume(queue: string): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'RESUME',
      queue,
    });
  }

  /**
   * Set rate limit for a queue (jobs per second)
   */
  async setRateLimit(queue: string, limit: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'RATELIMIT',
      queue,
      limit,
    });
  }

  /**
   * Clear rate limit for a queue
   */
  async clearRateLimit(queue: string): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'RATELIMITCLEAR',
      queue,
    });
  }

  /**
   * Set concurrency limit for a queue
   */
  async setConcurrency(queue: string, limit: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'SETCONCURRENCY',
      queue,
      limit,
    });
  }

  /**
   * Clear concurrency limit for a queue
   */
  async clearConcurrency(queue: string): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'CLEARCONCURRENCY',
      queue,
    });
  }

  /**
   * List all queues
   */
  async listQueues(): Promise<QueueInfo[]> {
    const response = await this.send<{ ok: boolean; queues: QueueInfo[] }>({
      cmd: 'LISTQUEUES',
    });
    return response.queues;
  }

  // ============== Cron Jobs ==============

  /**
   * Add a cron job
   *
   * @example
   * ```typescript
   * // Run every minute
   * await client.addCron('cleanup', {
   *   queue: 'maintenance',
   *   data: { task: 'cleanup' },
   *   schedule: '0 * * * * *', // Every minute at second 0
   * });
   *
   * // Using interval shorthand: use string like "STAR/60" where STAR is asterisk
   * // This runs every 60 seconds
   * ```
   */
  async addCron(name: string, options: CronOptions): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'CRON',
      name,
      queue: options.queue,
      data: options.data,
      schedule: options.schedule,
      priority: options.priority ?? 0,
    });
  }

  /**
   * Delete a cron job
   */
  async deleteCron(name: string): Promise<boolean> {
    const response = await this.send<{ ok: boolean }>({
      cmd: 'CRONDELETE',
      name,
    });
    return response.ok;
  }

  /**
   * List all cron jobs
   */
  async listCrons(): Promise<CronJob[]> {
    const response = await this.send<{ ok: boolean; crons: CronJob[] }>({
      cmd: 'CRONLIST',
    });
    return response.crons;
  }

  // ============== Stats & Metrics ==============

  /**
   * Get queue statistics
   */
  async stats(): Promise<QueueStats> {
    const response = await this.send<{
      ok: boolean;
      queued: number;
      processing: number;
      delayed: number;
      dlq: number;
    }>({
      cmd: 'STATS',
    });
    return {
      queued: response.queued,
      processing: response.processing,
      delayed: response.delayed,
      dlq: response.dlq,
    };
  }

  /**
   * Get detailed metrics
   */
  async metrics(): Promise<Metrics> {
    const response = await this.send<{ ok: boolean; metrics: Metrics }>({
      cmd: 'METRICS',
    });
    return response.metrics;
  }
}

export default FlashQ;
