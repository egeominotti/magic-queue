/**
 * flashQ TypeScript SDK Types
 */

// ============== Job ==============

export interface Job<T = unknown> {
  id: number;
  queue: string;
  data: T;
  priority: number;
  attempts: number;
  created_at: number;
  run_at: number;
  started_at: number;
  max_attempts: number;
  backoff: number;
  ttl: number;
  timeout: number;
  progress: number;
  unique_key?: string;
  custom_id?: string;
  tags: string[];
  depends_on: number[];
  parent_id?: number;
  children_ids: number[];
  children_completed: number;
  lifo: boolean;
  remove_on_complete: boolean;
  remove_on_fail: boolean;
  last_heartbeat: number;
  stall_timeout: number;
  stall_count: number;
  keep_completed_age: number;
  keep_completed_count: number;
  completed_at: number;
}

export type JobState =
  | 'waiting'
  | 'delayed'
  | 'active'
  | 'completed'
  | 'failed';

export interface JobWithState<T = unknown> {
  job: Job<T>;
  state: JobState;
}

// ============== Options ==============

export interface PushOptions {
  /** Higher = processed first (default: 0) */
  priority?: number;
  /** Delay in ms before job is available */
  delay?: number;
  /** Max retry attempts (default: 0 = no retry) */
  max_attempts?: number;
  /** Backoff base in ms (exponential: backoff * 2^attempt) */
  backoff?: number;
  /** Job timeout in ms */
  timeout?: number;
  /** Time-to-live in ms (job expires after) */
  ttl?: number;
  /** Unique key for deduplication */
  unique_key?: string;
  /** Custom ID for lookup */
  jobId?: string;
  /** Job IDs that must complete first */
  depends_on?: number[];
  /** Tags for filtering */
  tags?: string[];
  /** LIFO mode (stack) */
  lifo?: boolean;
  /** Remove from completed set immediately */
  remove_on_complete?: boolean;
  /** Remove from DLQ immediately */
  remove_on_fail?: boolean;
  /** Stall detection timeout in ms */
  stall_timeout?: number;
  /** Debounce ID for grouping */
  debounce_id?: string;
  /** Debounce window in ms */
  debounce_ttl?: number;
  /** Keep completed job result for this duration (ms) */
  keepCompletedAge?: number;
  /** Keep completed job in last N completed */
  keepCompletedCount?: number;
}

export interface WorkerOptions {
  /** Worker ID */
  id?: string;
  /** Parallel job processing (default: 10) */
  concurrency?: number;
  /** Jobs per batch (default: 100) */
  batchSize?: number;
  /** Heartbeat interval in ms (default: 1000) */
  heartbeatInterval?: number;
  /** Auto-ack on success (default: true) */
  autoAck?: boolean;
}

export interface ClientOptions {
  /** Server host (default: localhost) */
  host?: string;
  /** TCP port (default: 6789) */
  port?: number;
  /** HTTP port (default: 6790) */
  httpPort?: number;
  /** Unix socket path (alternative to TCP) */
  socketPath?: string;
  /** Auth token */
  token?: string;
  /** Connection timeout in ms (default: 5000) */
  timeout?: number;
  /** Use HTTP instead of TCP */
  useHttp?: boolean;
  /** Use binary (MessagePack) protocol */
  useBinary?: boolean;
}

// ============== Queue Info ==============

export interface QueueInfo {
  name: string;
  pending: number;
  processing: number;
  dlq: number;
  paused: boolean;
}

export interface QueueStats {
  queued: number;
  processing: number;
  delayed: number;
  dlq: number;
}

// ============== Cron ==============

export interface CronOptions {
  queue: string;
  data: unknown;
  /** Cron expression: "sec min hour day month weekday" */
  schedule?: string;
  /** Or repeat every N ms */
  repeat_every?: number;
  priority?: number;
  limit?: number;
}

export interface CronJob {
  name: string;
  queue: string;
  data: unknown;
  schedule?: string;
  repeat_every?: number;
  priority: number;
  next_run: number;
  executions: number;
  limit?: number;
}

// ============== Metrics ==============

export interface Metrics {
  total_pushed: number;
  total_completed: number;
  total_failed: number;
  jobs_per_second: number;
  avg_latency_ms: number;
  queues: QueueMetrics[];
}

export interface QueueMetrics {
  name: string;
  pending: number;
  processing: number;
  dlq: number;
}

// ============== Flow ==============

export interface FlowChild {
  queue: string;
  data: unknown;
  priority?: number;
  delay?: number;
}

export interface FlowResult {
  parent_id: number;
  children_ids: number[];
}

// ============== Internal ==============

export interface JobLogEntry {
  timestamp: number;
  message: string;
  level: 'info' | 'warn' | 'error';
}

export interface ApiResponse<T = unknown> {
  ok: boolean;
  data?: T;
  error?: string;
}

export type JobProcessor<T = unknown, R = unknown> = (
  job: Job<T>
) => R | Promise<R>;
