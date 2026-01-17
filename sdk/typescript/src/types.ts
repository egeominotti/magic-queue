/**
 * FlashQ TypeScript SDK Types
 */

// ============== Job Types ==============

export interface Job {
  id: number;
  queue: string;
  data: unknown;
  priority: number;
  created_at: number;
  run_at: number;
  started_at: number;
  attempts: number;
  max_attempts: number;
  backoff: number;
  ttl: number;
  timeout: number;
  unique_key?: string;
  depends_on: number[];
  progress: number;
  progress_msg?: string;
  tags: string[];
  lifo: boolean;
  // BullMQ-like features
  remove_on_complete: boolean;
  remove_on_fail: boolean;
  last_heartbeat: number;
  stall_timeout: number;
  stall_count: number;
  // Flow (Parent-Child) fields
  parent_id?: number;
  children_ids: number[];
  children_completed: number;
  // Custom ID and retention (BullMQ idempotency)
  custom_id?: string;
  keep_completed_age: number;
  keep_completed_count: number;
  completed_at: number;
}

export type JobState =
  | 'waiting'
  | 'delayed'
  | 'active'
  | 'completed'
  | 'failed'
  | 'waiting-children'
  | 'waiting-parent'
  | 'stalled';

export interface JobWithState {
  job: Job;
  state: JobState;
}

export interface JobInput {
  data: unknown;
  priority?: number;
  delay?: number;
  ttl?: number;
  timeout?: number;
  max_attempts?: number;
  backoff?: number;
  unique_key?: string;
  depends_on?: number[];
  tags?: string[];
  /** Custom job ID for idempotency */
  jobId?: string;
  /** Keep completed job result for N milliseconds */
  keepCompletedAge?: number;
  /** Keep job in last N completed jobs per queue */
  keepCompletedCount?: number;
}

// ============== Push Options ==============

export interface PushOptions {
  /** Higher priority = processed first (default: 0) */
  priority?: number;
  /** Delay in milliseconds before job becomes available */
  delay?: number;
  /** Time-to-live in milliseconds (job expires after this) */
  ttl?: number;
  /** Processing timeout in milliseconds */
  timeout?: number;
  /** Maximum retry attempts before moving to DLQ */
  max_attempts?: number;
  /** Base backoff in milliseconds (exponential: backoff * 2^attempts) */
  backoff?: number;
  /** Unique key for deduplication */
  unique_key?: string;
  /** Job IDs that must complete before this job runs */
  depends_on?: number[];
  /** Tags for categorization and filtering */
  tags?: string[];
  /** LIFO mode: last in, first out (default: false) */
  lifo?: boolean;
  /** Remove job from storage immediately on completion (default: false) */
  remove_on_complete?: boolean;
  /** Don't store job in DLQ on failure (default: false) */
  remove_on_fail?: boolean;
  /** Stall detection timeout in ms (default: 30000, 0 = disabled) */
  stall_timeout?: number;
  /** Debounce identifier - prevents duplicate jobs with same ID within debounce_ttl */
  debounce_id?: string;
  /** Debounce window in milliseconds (default: 5000) */
  debounce_ttl?: number;
  /** Custom job ID for idempotency - returns existing job if ID already exists */
  jobId?: string;
  /** Keep completed job result for N milliseconds (default: 0 = use global setting) */
  keepCompletedAge?: number;
  /** Keep job in last N completed jobs per queue (default: 0 = use global setting) */
  keepCompletedCount?: number;
}

// ============== Queue Types ==============

export interface QueueInfo {
  name: string;
  pending: number;
  processing: number;
  dlq: number;
  paused: boolean;
  rate_limit?: number;
  concurrency_limit?: number;
}

export interface QueueStats {
  queued: number;
  processing: number;
  delayed: number;
  dlq: number;
}

// ============== Metrics Types ==============

export interface QueueMetrics {
  name: string;
  pending: number;
  processing: number;
  dlq: number;
  rate_limit?: number;
}

export interface Metrics {
  total_pushed: number;
  total_completed: number;
  total_failed: number;
  jobs_per_second: number;
  avg_latency_ms: number;
  queues: QueueMetrics[];
}

// ============== Cron Types ==============

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

export interface CronOptions {
  /** Queue to push jobs to */
  queue: string;
  /** Job data payload */
  data: unknown;
  /** Cron schedule (6-field: "sec min hour day month weekday") - either this or repeat_every is required */
  schedule?: string;
  /** Repeat every N milliseconds - either this or schedule is required */
  repeat_every?: number;
  /** Job priority (default: 0) */
  priority?: number;
  /** Maximum number of executions (optional) */
  limit?: number;
}

// ============== Worker Types ==============

export interface WorkerInfo {
  id: string;
  queues: string[];
  concurrency: number;
  last_heartbeat: number;
  jobs_processed: number;
}

export interface WorkerOptions {
  /** Unique worker ID (auto-generated if not provided) */
  id?: string;
  /** Number of concurrent jobs to process (default: 1) */
  concurrency?: number;
  /** Heartbeat interval in milliseconds (default: 30000) */
  heartbeatInterval?: number;
  /** Whether to auto-acknowledge jobs on success (default: true) */
  autoAck?: boolean;
}

// ============== Webhook Types ==============

export interface WebhookConfig {
  id: string;
  url: string;
  events: string[];
  queue?: string;
  secret?: string;
  created_at: number;
}

export interface WebhookOptions {
  /** URL to call when events occur */
  url: string;
  /** Events to subscribe to: "pushed", "completed", "failed", "progress" */
  events: string[];
  /** Filter to specific queue (optional) */
  queue?: string;
  /** Secret for HMAC signature verification */
  secret?: string;
}

// ============== Event Types ==============

export interface JobEvent {
  event_type: 'pushed' | 'completed' | 'failed' | 'progress' | 'timeout';
  queue: string;
  job_id: number;
  timestamp: number;
  data?: unknown;
  error?: string;
  progress?: number;
}

export type EventHandler = (event: JobEvent) => void | Promise<void>;

// ============== Response Types ==============

export interface ApiResponse<T = unknown> {
  ok: boolean;
  data?: T;
  error?: string;
}

export interface BatchResponse {
  ok: boolean;
  ids?: number[];
  count?: number;
  error?: string;
}

// ============== Client Options ==============

export interface ClientOptions {
  /** FlashQ server host (default: "localhost") */
  host?: string;
  /** TCP port (default: 6789) */
  port?: number;
  /** HTTP port for REST API (default: 6790) */
  httpPort?: number;
  /** Unix socket path (e.g., "/tmp/flashq.sock") - if set, ignores host/port */
  socketPath?: string;
  /** Authentication token */
  token?: string;
  /** Connection timeout in milliseconds (default: 5000) */
  timeout?: number;
  /** Use HTTP instead of TCP (default: false) */
  useHttp?: boolean;
  /** Use binary protocol (MessagePack) for 3-5x faster serialization (default: false) */
  useBinary?: boolean;
}

// ============== Processor Types ==============

export type JobProcessor<T = unknown, R = unknown> = (
  job: Job & { data: T }
) => R | Promise<R>;

export interface ProcessorOptions {
  /** Number of concurrent jobs (default: 1) */
  concurrency?: number;
  /** Auto-acknowledge on success (default: true) */
  autoAck?: boolean;
  /** Auto-fail on error (default: true) */
  autoFail?: boolean;
}

// ============== Job Logs Types ==============

export interface JobLogEntry {
  timestamp: number;
  message: string;
  level: 'info' | 'warn' | 'error';
}

// ============== Flow Types ==============

export interface FlowChild {
  queue: string;
  data: unknown;
  priority?: number;
  delay?: number;
  ttl?: number;
  timeout?: number;
  max_attempts?: number;
  backoff?: number;
  unique_key?: string;
  tags?: string[];
  lifo?: boolean;
  remove_on_complete?: boolean;
  remove_on_fail?: boolean;
  stall_timeout?: number;
}

export interface FlowOptions {
  /** Parent job priority (default: 0) */
  priority?: number;
  /** Parent job delay in milliseconds */
  delay?: number;
  /** Parent job TTL */
  ttl?: number;
  /** Parent job timeout */
  timeout?: number;
  /** Parent job max attempts */
  max_attempts?: number;
  /** Parent job backoff */
  backoff?: number;
  /** Parent job unique key */
  unique_key?: string;
  /** Parent job tags */
  tags?: string[];
}

export interface FlowResult {
  parent_id: number;
  children_ids: number[];
}
