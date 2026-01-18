/**
 * Connection management for FlashQ client
 * Production-ready with auto-reconnect, validation, and proper error handling
 */
import * as net from 'net';
import { EventEmitter } from 'events';
import { encode, decode } from '@msgpack/msgpack';
import type { ClientOptions, ApiResponse } from '../types';

// ============== Constants ==============

/** Maximum job data size in bytes (1MB) */
export const MAX_JOB_DATA_SIZE = 1024 * 1024;

/** Maximum batch size allowed by the server */
export const MAX_BATCH_SIZE = 1000;

/** Valid queue name pattern */
const QUEUE_NAME_REGEX = /^[a-zA-Z0-9_.-]{1,256}$/;

/** Ultra-fast request ID generator (no crypto overhead) */
let requestIdCounter = 0;
const generateReqId = (): string => `r${++requestIdCounter}`;

// ============== Validation ==============

/**
 * Validate queue name to prevent injection attacks
 * @throws Error if queue name is invalid
 */
export function validateQueueName(queue: string): void {
  if (!queue || typeof queue !== 'string') {
    throw new Error('Queue name is required');
  }
  if (!QUEUE_NAME_REGEX.test(queue)) {
    throw new Error(
      `Invalid queue name: "${queue}". Must match pattern: alphanumeric, underscore, hyphen, dot (1-256 chars)`
    );
  }
}

/**
 * Validate job data size
 * @throws Error if data exceeds max size
 */
export function validateJobDataSize(data: unknown): void {
  const size = JSON.stringify(data).length;
  if (size > MAX_JOB_DATA_SIZE) {
    throw new Error(
      `Job data size (${size} bytes) exceeds maximum allowed (${MAX_JOB_DATA_SIZE} bytes)`
    );
  }
}

// ============== Types ==============

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'closed';

// ============== Connection Class ==============

/**
 * Base connection class for FlashQ.
 * Handles TCP/HTTP connection, binary protocol, request multiplexing, and auto-reconnect.
 */
export class FlashQConnection extends EventEmitter {
  protected _options: Required<ClientOptions>;
  private socket: net.Socket | null = null;
  private connectionState: ConnectionState = 'disconnected';
  private authenticated = false;
  private pendingRequests: Map<string, PendingRequest> = new Map();
  private responseQueue: Array<{
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
  }> = [];
  private buffer = '';
  private binaryBuffer: Buffer = Buffer.alloc(0);

  // Reconnect state
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private manualClose = false;

  constructor(options: ClientOptions = {}) {
    super();
    this._options = {
      host: options.host ?? 'localhost',
      port: options.port ?? 6789,
      httpPort: options.httpPort ?? 6790,
      socketPath: options.socketPath ?? '',
      token: options.token ?? '',
      timeout: options.timeout ?? 5000,
      useHttp: options.useHttp ?? false,
      useBinary: options.useBinary ?? false,
      autoReconnect: options.autoReconnect ?? true,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10,
      reconnectDelay: options.reconnectDelay ?? 1000,
      maxReconnectDelay: options.maxReconnectDelay ?? 30000,
    };

    // HTTP is stateless, so we're always "connected"
    if (this._options.useHttp) {
      this.connectionState = 'connected';
    }
  }

  /** Get client options (read-only) */
  get options(): Required<ClientOptions> {
    return this._options;
  }

  /**
   * Connect to FlashQ server.
   * Automatically called on first command if not connected.
   *
   * @example
   * ```typescript
   * const client = new FlashQ();
   * await client.connect();
   * ```
   */
  async connect(): Promise<void> {
    if (this._options.useHttp) {
      this.connectionState = 'connected';
      return;
    }

    if (this.connectionState === 'connected') {
      return;
    }

    if (this.connectionState === 'connecting') {
      // Wait for existing connection attempt
      return new Promise((resolve, reject) => {
        const onConnect = () => {
          this.removeListener('error', onError);
          resolve();
        };
        const onError = (err: Error) => {
          this.removeListener('connect', onConnect);
          reject(err);
        };
        this.once('connect', onConnect);
        this.once('error', onError);
      });
    }

    this.connectionState = 'connecting';
    this.manualClose = false;

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.connectionState = 'disconnected';
        reject(new Error('Connection timeout'));
      }, this._options.timeout);

      const connectionOptions = this._options.socketPath
        ? { path: this._options.socketPath }
        : { host: this._options.host, port: this._options.port };

      this.socket = net.createConnection(connectionOptions, async () => {
        clearTimeout(timeout);
        this.connectionState = 'connected';
        this.reconnectAttempts = 0;
        this.setupSocketHandlers();

        if (this._options.token) {
          try {
            await this.auth(this._options.token);
            this.authenticated = true;
          } catch (err) {
            this.socket?.destroy();
            this.connectionState = 'disconnected';
            reject(err);
            return;
          }
        }

        this.emit('connect');
        resolve();
      });

      this.socket.on('error', (err) => {
        clearTimeout(timeout);
        if (this.connectionState === 'connecting') {
          this.connectionState = 'disconnected';
          reject(err);
        }
      });
    });
  }

  /**
   * Attempt to reconnect with exponential backoff
   */
  private scheduleReconnect(): void {
    if (this.manualClose || !this._options.autoReconnect) {
      return;
    }

    if (
      this._options.maxReconnectAttempts > 0 &&
      this.reconnectAttempts >= this._options.maxReconnectAttempts
    ) {
      this.emit('reconnect_failed', new Error('Max reconnection attempts reached'));
      return;
    }

    this.connectionState = 'reconnecting';
    this.reconnectAttempts++;

    // Exponential backoff with jitter
    const baseDelay = Math.min(
      this._options.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
      this._options.maxReconnectDelay
    );
    const jitter = Math.random() * 0.3 * baseDelay;
    const delay = baseDelay + jitter;

    this.emit('reconnecting', { attempt: this.reconnectAttempts, delay });

    this.reconnectTimer = setTimeout(async () => {
      try {
        this.connectionState = 'disconnected';
        await this.connect();
        this.emit('reconnected');
      } catch {
        this.scheduleReconnect();
      }
    }, delay);
  }

  private setupSocketHandlers(): void {
    if (!this.socket) return;

    this.socket.on('data', (data) => {
      if (this._options.useBinary) {
        this.binaryBuffer = Buffer.concat([this.binaryBuffer, data]);
        this.processBinaryBuffer();
      } else {
        this.buffer += data.toString();
        this.processBuffer();
      }
    });

    this.socket.on('close', () => {
      const wasConnected = this.connectionState === 'connected';
      this.connectionState = 'disconnected';
      this.authenticated = false;
      this.emit('disconnect');

      // Auto-reconnect if enabled and not manually closed
      if (wasConnected && !this.manualClose && this._options.autoReconnect) {
        this.scheduleReconnect();
      }
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

  private processBinaryBuffer(): void {
    while (this.binaryBuffer.length >= 4) {
      const len = this.binaryBuffer.readUInt32BE(0);
      if (this.binaryBuffer.length < 4 + len) break;

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

  private handleResponse(response: Record<string, unknown>): void {
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
   * Close the connection to the server.
   *
   * @example
   * ```typescript
   * await client.close();
   * ```
   */
  async close(): Promise<void> {
    this.manualClose = true;
    this.connectionState = 'closed';

    // Cancel any pending reconnect
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    for (const [, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Connection closed'));
    }
    this.pendingRequests.clear();

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
   * Check if connected to the server.
   *
   * @returns true if connected
   */
  isConnected(): boolean {
    return this.connectionState === 'connected';
  }

  /**
   * Get current connection state.
   *
   * @returns Connection state
   */
  getConnectionState(): ConnectionState {
    return this.connectionState;
  }

  /**
   * Ping the server to check connection health.
   *
   * @returns true if server responds
   */
  async ping(): Promise<boolean> {
    try {
      const response = await this.send<{ ok: boolean; pong?: boolean }>({ cmd: 'PING' });
      return response.ok || response.pong === true;
    } catch {
      return false;
    }
  }

  /**
   * Authenticate with the server.
   *
   * @param token - Authentication token
   */
  async auth(token: string): Promise<void> {
    const response = await this.send<{ ok: boolean }>({
      cmd: 'AUTH',
      token,
    });
    if (!response.ok) {
      throw new Error('Authentication failed');
    }
    this._options.token = token;
    this.authenticated = true;
  }

  /**
   * Send a command to the server.
   * Auto-connects if not connected.
   *
   * @param command - Command object
   * @param customTimeout - Optional custom timeout
   * @returns Response from server
   */
  async send<T>(command: Record<string, unknown>, customTimeout?: number): Promise<T> {
    // Wait for reconnection if in progress
    if (this.connectionState === 'reconnecting') {
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          this.removeListener('reconnected', onReconnect);
          this.removeListener('reconnect_failed', onFailed);
          reject(new Error('Reconnection timeout'));
        }, this._options.timeout);

        const onReconnect = () => {
          clearTimeout(timeout);
          this.removeListener('reconnect_failed', onFailed);
          resolve();
        };
        const onFailed = (err: Error) => {
          clearTimeout(timeout);
          this.removeListener('reconnected', onReconnect);
          reject(err);
        };

        this.once('reconnected', onReconnect);
        this.once('reconnect_failed', onFailed);
      });
    }

    if (this.connectionState !== 'connected') {
      await this.connect();
    }

    if (this._options.useHttp) {
      return this.sendHttp<T>(command, customTimeout);
    }
    return this.sendTcp<T>(command, customTimeout);
  }

  private async sendTcp<T>(command: Record<string, unknown>, customTimeout?: number): Promise<T> {
    if (!this.socket || this.connectionState !== 'connected') {
      throw new Error('Not connected');
    }

    return new Promise((resolve, reject) => {
      const reqId = generateReqId();
      const timeoutMs = customTimeout ?? this._options.timeout;

      const timer = setTimeout(() => {
        this.pendingRequests.delete(reqId);
        reject(new Error('Request timeout'));
      }, timeoutMs);

      this.pendingRequests.set(reqId, {
        resolve: (value) => resolve(value as T),
        reject,
        timer,
      });

      if (this._options.useBinary) {
        const payload = { ...command, reqId };
        const encoded = encode(payload);
        const frame = Buffer.alloc(4 + encoded.length);
        frame.writeUInt32BE(encoded.length, 0);
        frame.set(encoded, 4);
        this.socket!.write(frame);
      } else {
        this.socket!.write(JSON.stringify({ ...command, reqId }) + '\n');
      }
    });
  }

  private async sendHttp<T>(command: Record<string, unknown>, customTimeout?: number): Promise<T> {
    const { cmd, ...params } = command;
    const baseUrl = `http://${this._options.host}:${this._options.httpPort}`;
    const timeout = customTimeout ?? this._options.timeout;
    const response = await this.httpRequest(baseUrl, cmd as string, params, timeout);
    return response as T;
  }

  private async httpRequest(
    baseUrl: string,
    cmd: string,
    params: Record<string, unknown>,
    timeout: number
  ): Promise<unknown> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this._options.token) {
      headers['Authorization'] = `Bearer ${this._options.token}`;
    }

    // URL-encode queue names to prevent injection
    const encodeQueue = (queue: unknown): string =>
      encodeURIComponent(String(queue));

    let url: string;
    let method: string;
    let body: string | undefined;

    switch (cmd) {
      case 'PUSH':
        url = `${baseUrl}/queues/${encodeQueue(params.queue)}/jobs`;
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
        url = `${baseUrl}/queues/${encodeQueue(params.queue)}/jobs?count=1`;
        method = 'GET';
        break;
      case 'PULLB':
        url = `${baseUrl}/queues/${encodeQueue(params.queue)}/jobs?count=${params.count}`;
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

    // Create abort controller for timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const res = await fetch(url, { method, headers, body, signal: controller.signal });
      const json = (await res.json()) as ApiResponse;

      if (!json.ok) {
        throw new Error(json.error ?? 'Unknown error');
      }

      const data = (json as { ok: boolean; data?: unknown }).data;

      if (cmd === 'PUSH' && data && typeof data === 'object') {
        return { ok: true, id: (data as { id: number }).id };
      }

      if ((cmd === 'PULL' || cmd === 'PULLB') && Array.isArray(data)) {
        if (data.length === 0) {
          return { ok: true, job: null };
        }
        return cmd === 'PULL'
          ? { ok: true, job: data[0] }
          : { ok: true, jobs: data };
      }

      if (cmd === 'STATS' && data && typeof data === 'object') {
        return { ok: true, ...data };
      }

      if (cmd === 'METRICS' && data && typeof data === 'object') {
        return { ok: true, ...data };
      }

      if (cmd === 'LISTQUEUES' && Array.isArray(data)) {
        return { ok: true, queues: data };
      }

      return json;
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error('HTTP request timeout');
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  }
}
