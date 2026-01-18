import * as net from 'net';
import { EventEmitter } from 'events';
import { encode, decode } from '@msgpack/msgpack';
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
  JobLogEntry,
  FlowChild,
  FlowOptions,
  FlowResult,
} from './types';

/**
 * flashQ Client
 *
 * High-performance job queue client with auto-connect.
 *
 * @example
 * ```typescript
 * const client = new FlashQ();
 *
 * // Add a job (auto-connects!)
 * const job = await client.add('emails', { to: 'user@example.com' });
 *
 * // That's it! No connect() needed.
 * ```
 *
 * @example
 * ```typescript
 * // With options
 * const client = new FlashQ({
 *   host: 'localhost',
 *   port: 6789,
 *   token: 'secret'
 * });
 *
 * // Add with job options
 * await client.add('emails', { to: 'user@example.com' }, {
 *   priority: 10,
 *   delay: 5000,
 *   max_attempts: 3
 * });
 * ```
 */
// Ultra-fast request ID generator (no crypto overhead)
let requestIdCounter = 0;
const generateReqId = (): string => `r${++requestIdCounter}`;

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

export class FlashQ extends EventEmitter {
  private options: Required<ClientOptions>;
  private socket: net.Socket | null = null;
  private connected = false;
  private authenticated = false;
  // Map for O(1) lookup by request ID (multiplexing support)
  private pendingRequests: Map<string, PendingRequest> = new Map();
  // Fallback queue for servers without reqId support
  private responseQueue: Array<{
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
  }> = [];
  private buffer = '';
  // Binary protocol buffers
  private binaryBuffer: Buffer = Buffer.alloc(0);
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
      useBinary: options.useBinary ?? false,
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
      if (this.options.useBinary) {
        this.binaryBuffer = Buffer.concat([this.binaryBuffer, data]);
        this.processBinaryBuffer();
      } else {
        this.buffer += data.toString();
        this.processBuffer();
      }
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
        this.handleResponse(response);
      } catch {
        // Ignore parse errors
      }
    }
  }

  /** Process binary protocol buffer (length-prefixed MessagePack frames) */
  private processBinaryBuffer(): void {
    while (this.binaryBuffer.length >= 4) {
      // Read 4-byte length prefix (big-endian)
      const len = this.binaryBuffer.readUInt32BE(0);

      // Check if we have the complete frame
      if (this.binaryBuffer.length < 4 + len) {
        break; // Wait for more data
      }

      // Extract and decode the frame
      const frameData = this.binaryBuffer.subarray(4, 4 + len);
      this.binaryBuffer = this.binaryBuffer.subarray(4 + len);

      try {
        const response = decode(frameData) as Record<string, unknown>;
        this.handleResponse(response);
      } catch {
        // Ignore decode errors
      }
    }
  }

  /** Handle a parsed response (shared by text and binary protocols) */
  private handleResponse(response: Record<string, unknown>): void {
    // Check for request ID (multiplexing)
    if (response.reqId) {
      const pending = this.pendingRequests.get(response.reqId as string);
      if (pending) {
        this.pendingRequests.delete(response.reqId as string);
        clearTimeout(pending.timer);
        if (response.ok === false && response.error) {
          pending.reject(new Error(response.error as string));
        } else {
          pending.resolve(response);
        }
      }
    } else {
      // Fallback: FIFO queue for servers without reqId support
      const pending = this.responseQueue.shift();
      if (pending) {
        if (response.ok === false && response.error) {
          pending.reject(new Error(response.error as string));
        } else {
          pending.resolve(response);
        }
      }
    }
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    this.connected = false;

    // Clear all pending requests
    for (const [reqId, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Connection closed'));
    }
    this.pendingRequests.clear();

    // Clear fallback queue
    for (const pending of this.responseQueue) {
      pending.reject(new Error('Connection closed'));
    }
    this.responseQueue.length = 0;

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

  private async send<T>(command: Record<string, unknown>, customTimeout?: number): Promise<T> {
    // Auto-connect if not connected
    if (!this.connected) {
      await this.connect();
    }

    if (this.options.useHttp) {
      return this.sendHttp<T>(command, customTimeout);
    }
    return this.sendTcp<T>(command, customTimeout);
  }

  private async sendTcp<T>(command: Record<string, unknown>, customTimeout?: number): Promise<T> {
    if (!this.socket || !this.connected) {
      throw new Error('Not connected');
    }

    return new Promise((resolve, reject) => {
      // Generate unique request ID for multiplexing
      const reqId = generateReqId();
      const timeoutMs = customTimeout ?? this.options.timeout;

      const timer = setTimeout(() => {
        // Cleanup on timeout
        this.pendingRequests.delete(reqId);
        reject(new Error('Request timeout'));
      }, timeoutMs);

      // Store in Map for O(1) lookup when response arrives
      this.pendingRequests.set(reqId, {
        resolve: (value) => resolve(value as T),
        reject,
        timer,
      });

      if (this.options.useBinary) {
        // Binary protocol: length-prefixed MessagePack
        const payload = { ...command, reqId };
        const encoded = encode(payload);
        const frame = Buffer.alloc(4 + encoded.length);
        frame.writeUInt32BE(encoded.length, 0);
        frame.set(encoded, 4);
        this.socket!.write(frame);
      } else {
        // Text protocol: JSON + newline
        this.socket!.write(JSON.stringify({ ...command, reqId }) + '\n');
      }
    });
  }

  private async sendHttp<T>(command: Record<string, unknown>, _customTimeout?: number): Promise<T> {
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
          lifo: params.lifo,
          remove_on_complete: params.remove_on_complete,
          remove_on_fail: params.remove_on_fail,
          stall_timeout: params.stall_timeout,
          debounce_id: params.debounce_id,
          debounce_ttl: params.debounce_ttl,
          job_id: params.job_id,
          keep_completed_age: params.keep_completed_age,
          keep_completed_count: params.keep_completed_count,
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
      lifo: options.lifo ?? false,
      remove_on_complete: options.remove_on_complete ?? false,
      remove_on_fail: options.remove_on_fail ?? false,
      stall_timeout: options.stall_timeout,
      debounce_id: options.debounce_id,
      debounce_ttl: options.debounce_ttl,
      job_id: options.jobId,
      keep_completed_age: options.keepCompletedAge,
      keep_completed_count: options.keepCompletedCount,
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
      lifo: options.lifo ?? false,
      remove_on_complete: options.remove_on_complete ?? false,
      remove_on_fail: options.remove_on_fail ?? false,
      last_heartbeat: 0,
      stall_timeout: options.stall_timeout ?? 30000,
      stall_count: 0,
      parent_id: undefined,
      children_ids: [],
      children_completed: 0,
      custom_id: options.jobId,
      keep_completed_age: options.keepCompletedAge ?? 0,
      keep_completed_count: options.keepCompletedCount ?? 0,
      completed_at: 0,
    };
  }

  /**
   * Add a job to a queue (alias for push)
   *
   * @example
   * ```typescript
   * await client.add('emails', { to: 'user@example.com' });
   * ```
   */
  async add<T = unknown>(
    queue: string,
    data: T,
    options: PushOptions = {}
  ): Promise<Job> {
    return this.push(queue, data, options);
  }

  /**
   * Add multiple jobs to a queue (alias for pushBatch)
   */
  async addBulk<T = unknown>(
    queue: string,
    jobs: Array<{ data: T } & PushOptions>
  ): Promise<number[]> {
    return this.pushBatch(queue, jobs);
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
        lifo: j.lifo ?? false,
        remove_on_complete: j.remove_on_complete ?? false,
        remove_on_fail: j.remove_on_fail ?? false,
        stall_timeout: j.stall_timeout,
        debounce_id: j.debounce_id,
        debounce_ttl: j.debounce_ttl,
        job_id: j.jobId,
        keep_completed_age: j.keepCompletedAge,
        keep_completed_count: j.keepCompletedCount,
      })),
    });
    return response.ids;
  }

  /**
   * Pull a job from a queue (blocking with server-side timeout)
   * @param queue Queue name
   * @param timeout Server-side timeout in ms (default 60s). Returns null if no job available within timeout.
   */
  async pull<T = unknown>(queue: string, timeout?: number): Promise<(Job & { data: T }) | null> {
    const serverTimeout = timeout ?? 60000;
    // Server-side timeout + small buffer for network latency
    const clientTimeout = serverTimeout + 5000;
    const response = await this.send<{ ok: boolean; job: Job | null }>(
      {
        cmd: 'PULL',
        queue,
        timeout: serverTimeout, // Pass timeout to server
      },
      clientTimeout
    );
    return response.job as (Job & { data: T }) | null;
  }

  /**
   * Pull multiple jobs from a queue
   * @param queue Queue name
   * @param count Number of jobs to pull
   * @param timeout Optional server-side timeout in ms (default: 60s)
   */
  async pullBatch<T = unknown>(
    queue: string,
    count: number,
    timeout?: number
  ): Promise<Array<Job & { data: T }>> {
    const serverTimeout = timeout ?? 60000;
    const clientTimeout = serverTimeout + 5000;
    const response = await this.send<{ ok: boolean; jobs: Job[] }>(
      {
        cmd: 'PULLB',
        queue,
        count,
        timeout: serverTimeout,
      },
      clientTimeout
    );
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
   * Wait for a job to complete and return its result.
   * This is useful for synchronous workflows where you need to wait
   * for a job to finish before continuing.
   *
   * @param jobId - Job ID to wait for
   * @param timeout - Optional timeout in milliseconds (default: 30000)
   * @returns The job result, or null if job completed without result
   * @throws Error if job fails or times out
   *
   * @example
   * ```typescript
   * // Push a job and wait for it to complete
   * const job = await client.push('process', { data: 'value' });
   * const result = await client.finished(job.id);
   * console.log('Job completed with result:', result);
   *
   * // With custom timeout
   * const result = await client.finished(job.id, 60000); // 60 seconds
   * ```
   */
  async finished<T = unknown>(jobId: number, timeout?: number): Promise<T | null> {
    const waitTimeout = timeout ?? 30000;
    // Use a longer request timeout than the wait timeout to allow server to respond
    const requestTimeout = waitTimeout + 5000;

    const response = await this.send<{
      ok: boolean;
      result: T | null;
      error?: string;
    }>({
      cmd: 'WAITJOB',
      id: jobId,
      timeout: waitTimeout,
    }, requestTimeout);
    if (response.error) {
      throw new Error(response.error);
    }
    return response.result;
  }

  /**
   * Get a job by its custom ID.
   * Custom IDs are set using the `jobId` option when pushing a job.
   *
   * @param customId - The custom job ID
   * @returns Job with state, or null if not found
   *
   * @example
   * ```typescript
   * // Push a job with custom ID for idempotency
   * await client.push('process', { orderId: 123 }, { jobId: 'order-123' });
   *
   * // Later, retrieve the job by custom ID
   * const jobWithState = await client.getJobByCustomId('order-123');
   * if (jobWithState) {
   *   console.log('Job state:', jobWithState.state);
   * }
   * ```
   */
  async getJobByCustomId(customId: string): Promise<JobWithState | null> {
    const response = await this.send<{
      ok: boolean;
      job: Job | null;
      state: JobState | null;
    }>({
      cmd: 'GETJOBBYCUSTOMID',
      job_id: customId,
    });
    if (!response.job) return null;
    return { job: response.job, state: response.state! };
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
      progress: {
        id: number;
        progress: number;
        message?: string;
      };
    }>({
      cmd: 'GETPROGRESS',
      id: jobId,
    });
    return { progress: response.progress.progress, message: response.progress.message };
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

  /**
   * Purge all jobs from the dead letter queue.
   * This permanently removes all failed jobs from the DLQ.
   *
   * @param queue - Queue name
   * @returns Number of jobs purged
   *
   * @example
   * ```typescript
   * const purged = await client.purgeDlq('emails');
   * console.log(`Purged ${purged} failed jobs`);
   * ```
   */
  async purgeDlq(queue: string): Promise<number> {
    const response = await this.send<{ ok: boolean; count: number }>({
      cmd: 'PURGEDLQ',
      queue,
    });
    return response.count;
  }

  /**
   * Get multiple jobs by their IDs in a single call (batch status).
   * More efficient than calling getJob() multiple times.
   *
   * @param jobIds - Array of job IDs to retrieve
   * @returns Array of jobs with their states (only found jobs are returned)
   *
   * @example
   * ```typescript
   * const jobs = await client.getJobsBatch([1, 2, 3, 4, 5]);
   * for (const { job, state } of jobs) {
   *   console.log(`Job ${job.id}: ${state}`);
   * }
   * ```
   */
  async getJobsBatch(jobIds: number[]): Promise<JobWithState[]> {
    const response = await this.send<{
      ok: boolean;
      jobs: Array<{ job: Job; state: JobState }>;
    }>({
      cmd: 'GETJOBSBATCH',
      ids: jobIds,
    });
    return response.jobs.map((j) => ({ job: j.job, state: j.state }));
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
   * // Run every minute using cron schedule
   * await client.addCron('cleanup', {
   *   queue: 'maintenance',
   *   data: { task: 'cleanup' },
   *   schedule: '0 * * * * *', // Every minute at second 0
   * });
   *
   * // Run every 5 seconds using repeat_every
   * await client.addCron('heartbeat', {
   *   queue: 'health',
   *   data: { check: 'health' },
   *   repeat_every: 5000, // Every 5 seconds
   *   limit: 100, // Stop after 100 executions
   * });
   * ```
   */
  async addCron(name: string, options: CronOptions): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'CRON',
      name,
      queue: options.queue,
      data: options.data,
      schedule: options.schedule,
      repeat_every: options.repeat_every,
      priority: options.priority ?? 0,
      limit: options.limit,
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

  // ============== Job Logs (BullMQ-like) ==============

  /**
   * Add a log entry to a job
   *
   * @example
   * ```typescript
   * await client.log(jobId, 'Processing step 1', 'info');
   * await client.log(jobId, 'Warning: low memory', 'warn');
   * await client.log(jobId, 'Error: failed to connect', 'error');
   * ```
   */
  async log(
    jobId: number,
    message: string,
    level: 'info' | 'warn' | 'error' = 'info'
  ): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'LOG',
      job_id: jobId,
      message,
      level,
    });
  }

  /**
   * Get log entries for a job
   */
  async getLogs(jobId: number): Promise<JobLogEntry[]> {
    const response = await this.send<{ ok: boolean; logs: JobLogEntry[] }>({
      cmd: 'GETLOGS',
      job_id: jobId,
    });
    return response.logs;
  }

  // ============== Stalled Jobs Detection (BullMQ-like) ==============

  /**
   * Send a heartbeat for a job to prevent it from being marked as stalled.
   * Workers should call this periodically for long-running jobs.
   *
   * @example
   * ```typescript
   * // In your job processor
   * async function processJob(job) {
   *   for (const item of largeDataset) {
   *     await processItem(item);
   *     await client.heartbeat(job.id); // Keep job alive
   *   }
   * }
   * ```
   */
  async heartbeat(jobId: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'HEARTBEAT',
      job_id: jobId,
    });
  }

  // ============== Flows (Parent-Child Jobs) ==============

  /**
   * Push a flow (parent job with children).
   * The parent job will wait until all children complete before becoming ready.
   *
   * @example
   * ```typescript
   * const flow = await client.pushFlow(
   *   'reports',
   *   { reportType: 'monthly' },
   *   [
   *     { queue: 'process', data: { section: 'sales' } },
   *     { queue: 'process', data: { section: 'marketing' } },
   *     { queue: 'process', data: { section: 'operations' } },
   *   ]
   * );
   * console.log('Parent job:', flow.parent_id);
   * console.log('Child jobs:', flow.children_ids);
   * ```
   */
  async pushFlow<T = unknown>(
    queue: string,
    parentData: T,
    children: FlowChild[],
    options: FlowOptions = {}
  ): Promise<FlowResult> {
    const response = await this.send<{
      ok: boolean;
      parent_id: number;
      children_ids: number[];
    }>({
      cmd: 'FLOW',
      queue,
      parent_data: parentData,
      children,
      priority: options.priority ?? 0,
      delay: options.delay,
      ttl: options.ttl,
      timeout: options.timeout,
      max_attempts: options.max_attempts,
      backoff: options.backoff,
      unique_key: options.unique_key,
      tags: options.tags,
    });
    return {
      parent_id: response.parent_id,
      children_ids: response.children_ids,
    };
  }

  /**
   * Get children job IDs for a parent job in a flow
   */
  async getChildren(jobId: number): Promise<number[]> {
    const response = await this.send<{ ok: boolean; children_ids: number[] }>({
      cmd: 'GETCHILDREN',
      parent_id: jobId,
    });
    return response.children_ids;
  }

  // ============== BullMQ Advanced Features ==============

  /**
   * Get jobs filtered by queue and/or state with pagination.
   *
   * @example
   * ```typescript
   * // Get all waiting jobs
   * const { jobs, total } = await client.getJobs({ state: 'waiting' });
   *
   * // Get failed jobs from a specific queue
   * const { jobs } = await client.getJobs({ queue: 'emails', state: 'failed' });
   *
   * // Paginate results
   * const { jobs, total } = await client.getJobs({ limit: 10, offset: 20 });
   * ```
   */
  async getJobs(options: {
    queue?: string;
    state?: JobState;
    limit?: number;
    offset?: number;
  } = {}): Promise<{ jobs: JobWithState[]; total: number }> {
    const response = await this.send<{
      ok: boolean;
      jobs: Array<{ job: Job; state: JobState }>;
      total: number;
    }>({
      cmd: 'GETJOBS',
      queue: options.queue,
      state: options.state,
      limit: options.limit ?? 100,
      offset: options.offset ?? 0,
    });
    return {
      jobs: response.jobs.map((j) => ({ job: j.job, state: j.state })),
      total: response.total,
    };
  }

  /**
   * Clean jobs older than grace period by state.
   *
   * @param queue - Queue name
   * @param grace - Grace period in ms (keep jobs newer than this)
   * @param state - Job state to clean: 'waiting', 'delayed', 'completed', 'failed'
   * @param limit - Optional max jobs to clean
   * @returns Number of jobs cleaned
   *
   * @example
   * ```typescript
   * // Clean completed jobs older than 1 hour
   * const cleaned = await client.clean('emails', 3600000, 'completed');
   *
   * // Clean at most 100 failed jobs older than 1 day
   * const cleaned = await client.clean('emails', 86400000, 'failed', 100);
   * ```
   */
  async clean(
    queue: string,
    grace: number,
    state: 'waiting' | 'delayed' | 'completed' | 'failed',
    limit?: number
  ): Promise<number> {
    const response = await this.send<{ ok: boolean; count: number }>({
      cmd: 'CLEAN',
      queue,
      grace,
      state,
      limit,
    });
    return response.count;
  }

  /**
   * Drain all waiting jobs from a queue.
   * Does NOT remove jobs that are processing or in DLQ.
   *
   * @param queue - Queue name
   * @returns Number of jobs drained
   *
   * @example
   * ```typescript
   * const drained = await client.drain('emails');
   * console.log(`Drained ${drained} jobs`);
   * ```
   */
  async drain(queue: string): Promise<number> {
    const response = await this.send<{ ok: boolean; count: number }>({
      cmd: 'DRAIN',
      queue,
    });
    return response.count;
  }

  /**
   * Remove ALL data for a queue: jobs, DLQ, cron jobs, unique keys, queue state.
   * Use with caution - this is destructive!
   *
   * @param queue - Queue name
   * @returns Total number of items removed
   *
   * @example
   * ```typescript
   * const removed = await client.obliterate('test-queue');
   * console.log(`Removed ${removed} items`);
   * ```
   */
  async obliterate(queue: string): Promise<number> {
    const response = await this.send<{ ok: boolean; count: number }>({
      cmd: 'OBLITERATE',
      queue,
    });
    return response.count;
  }

  /**
   * Change the priority of a job.
   * Works for jobs in any state (waiting, delayed, processing, DLQ).
   *
   * @param jobId - Job ID
   * @param priority - New priority (higher = processed first)
   *
   * @example
   * ```typescript
   * // Increase priority of a job
   * await client.changePriority(jobId, 100);
   *
   * // Lower priority
   * await client.changePriority(jobId, -10);
   * ```
   */
  async changePriority(jobId: number, priority: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'CHANGEPRIORITY',
      id: jobId,
      priority,
    });
  }

  /**
   * Move a job from processing back to delayed state.
   * Useful for "snoozing" a job or deferring it for later.
   *
   * @param jobId - Job ID (must be in processing state)
   * @param delay - Delay in milliseconds before job becomes ready again
   *
   * @example
   * ```typescript
   * // Delay a job for 5 minutes
   * await client.moveToDelayed(jobId, 300000);
   *
   * // Delay for 1 hour
   * await client.moveToDelayed(jobId, 3600000);
   * ```
   */
  async moveToDelayed(jobId: number, delay: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'MOVETODELAYED',
      id: jobId,
      delay,
    });
  }

  /**
   * Promote a delayed job to waiting state immediately.
   * The job becomes ready to process without waiting for its scheduled time.
   *
   * @param jobId - Job ID (must be in delayed state)
   *
   * @example
   * ```typescript
   * // Job was scheduled for later, but we want it now
   * await client.promote(jobId);
   * ```
   */
  async promote(jobId: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'PROMOTE',
      id: jobId,
    });
  }

  /**
   * Update the data payload of a job.
   * Works for jobs in any state (waiting, delayed, processing, DLQ).
   *
   * @param jobId - Job ID
   * @param data - New data payload
   *
   * @example
   * ```typescript
   * // Update job data
   * await client.update(jobId, { email: 'new@example.com', retry: true });
   * ```
   */
  async update<T = unknown>(jobId: number, data: T): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'UPDATEJOB',
      id: jobId,
      data,
    });
  }

  /**
   * Discard a job - immediately move it to DLQ without retrying.
   * Use when you know a job should not be processed.
   *
   * @param jobId - Job ID (must be in waiting, delayed, or processing state)
   *
   * @example
   * ```typescript
   * // Skip retries and move directly to DLQ
   * await client.discard(jobId);
   * ```
   */
  async discard(jobId: number): Promise<void> {
    await this.send<{ ok: boolean }>({
      cmd: 'DISCARD',
      id: jobId,
    });
  }

  /**
   * Check if a queue is paused.
   *
   * @param queue - Queue name
   * @returns true if queue is paused, false otherwise
   *
   * @example
   * ```typescript
   * if (await client.isPaused('emails')) {
   *   console.log('Email queue is paused');
   * }
   * ```
   */
  async isPaused(queue: string): Promise<boolean> {
    const response = await this.send<{ ok: boolean; paused: boolean }>({
      cmd: 'ISPAUSED',
      queue,
    });
    return response.paused;
  }

  /**
   * Get the total count of jobs in a queue (waiting + delayed).
   *
   * @param queue - Queue name
   * @returns Total job count
   *
   * @example
   * ```typescript
   * const total = await client.count('emails');
   * console.log(`${total} jobs in queue`);
   * ```
   */
  async count(queue: string): Promise<number> {
    const response = await this.send<{ ok: boolean; count: number }>({
      cmd: 'COUNT',
      queue,
    });
    return response.count;
  }

  /**
   * Get detailed job counts by state for a queue.
   *
   * @param queue - Queue name
   * @returns Object with counts for each state
   *
   * @example
   * ```typescript
   * const counts = await client.getJobCounts('emails');
   * console.log(`Waiting: ${counts.waiting}`);
   * console.log(`Active: ${counts.active}`);
   * console.log(`Delayed: ${counts.delayed}`);
   * console.log(`Failed: ${counts.failed}`);
   * ```
   */
  async getJobCounts(queue: string): Promise<{
    waiting: number;
    active: number;
    delayed: number;
    completed: number;
    failed: number;
  }> {
    const response = await this.send<{
      ok: boolean;
      waiting: number;
      active: number;
      delayed: number;
      completed: number;
      failed: number;
    }>({
      cmd: 'GETJOBCOUNTS',
      queue,
    });
    return {
      waiting: response.waiting,
      active: response.active,
      delayed: response.delayed,
      completed: response.completed,
      failed: response.failed,
    };
  }

  // ============== Event Subscriptions ==============

  /**
   * Subscribe to real-time events via SSE.
   * Returns an EventSubscriber that can be used to listen for events.
   *
   * @param queue - Optional queue to filter events
   * @returns EventSubscriber instance
   *
   * @example
   * ```typescript
   * const events = client.subscribe();
   *
   * events.on('completed', (e) => {
   *   console.log(`Job ${e.jobId} completed`);
   * });
   *
   * events.on('failed', (e) => {
   *   console.log(`Job ${e.jobId} failed: ${e.error}`);
   * });
   *
   * await events.connect();
   *
   * // Later...
   * events.close();
   * ```
   *
   * @example
   * ```typescript
   * // Subscribe to specific queue
   * const events = client.subscribe('emails');
   * events.on('completed', handler);
   * await events.connect();
   * ```
   */
  subscribe(queue?: string): import('./events').EventSubscriber {
    const { EventSubscriber } = require('./events');
    return new EventSubscriber({
      host: this.options.host,
      httpPort: this.options.httpPort,
      token: this.options.token,
      queue,
      type: 'sse',
    });
  }

  /**
   * Subscribe to real-time events via WebSocket.
   * Lower latency than SSE, supports bidirectional communication.
   *
   * @param queue - Optional queue to filter events
   * @returns EventSubscriber instance
   *
   * @example
   * ```typescript
   * const events = client.subscribeWs();
   * events.on('completed', (e) => console.log(e));
   * await events.connect();
   * ```
   */
  subscribeWs(queue?: string): import('./events').EventSubscriber {
    const { EventSubscriber } = require('./events');
    return new EventSubscriber({
      host: this.options.host,
      httpPort: this.options.httpPort,
      token: this.options.token,
      queue,
      type: 'websocket',
    });
  }
}

export default FlashQ;
