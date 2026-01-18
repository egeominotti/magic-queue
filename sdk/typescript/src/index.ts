/**
 * flashQ - Official TypeScript SDK
 *
 * High-performance job queue with the simplest API.
 *
 * @example
 * ```typescript
 * // Zero-config worker (recommended)
 * import { defineWorker } from 'flashq';
 *
 * export default defineWorker('emails', async (job, ctx) => {
 *   ctx.log('Sending email...');
 *   ctx.progress(50, 'Connecting...');
 *   await sendEmail(job.data);
 *   return { sent: true };
 * });
 * // Run: bun run worker.ts
 * ```
 *
 * @example
 * ```typescript
 * // Queue API
 * import { Queue } from 'flashq';
 *
 * const emails = new Queue('emails');
 * await emails.add({ to: 'user@example.com' });
 * ```
 *
 * @example
 * ```typescript
 * // Low-level client
 * import { FlashQ } from 'flashq';
 *
 * const client = new FlashQ();
 * await client.add('emails', { to: 'user@example.com' });
 * ```
 *
 * @packageDocumentation
 */

// Zero-config Worker API (recommended)
export { defineWorker, defineWorkers, NonRetryableError, RetryableError } from './define';
export type {
  JobContext,
  JobHandler,
  DefineWorkerOptions,
  WorkerInstance,
  WorkerEvents,
  LogLevel,
} from './define';

// High-level API
export { Queue } from './queue';

// Low-level API
export { FlashQ, FlashQ as default } from './client';
export { Worker } from './worker';

// Bun-Optimized API (30% faster with native TCP)
export { BunFlashQ } from './bun-client';
export { BunWorker } from './bun-worker';

// Sandboxed Processors
export { SandboxedWorker, createProcessor } from './sandbox';
export type { SandboxOptions } from './sandbox';

// Advanced Features
export {
  // Circuit Breaker
  CircuitBreaker,
  // Retry with Jitter
  retry,
  calculateBackoff,
  // Workflows
  Workflows,
  // Namespace API
  FlashQClient,
  JobsAPI,
  QueuesAPI,
  SchedulesAPI,
} from './advanced';
export type {
  CircuitState,
  CircuitBreakerOptions,
  RetryOptions,
  WorkflowJob,
  WorkflowDefinition,
  WorkflowInstance,
} from './advanced';

// Events (Real-time subscriptions)
export {
  EventSubscriber,
  createEventSubscriber,
  createWebSocketSubscriber,
  subscribeToEvents,
} from './events';
export type {
  EventType,
  JobEvent,
  EventSubscriberOptions,
  EventSubscriberEvents,
} from './events';

// Types
export * from './types';
export type {
  Job,
  JobState,
  JobWithState,
  PushOptions,
  QueueInfo,
  QueueStats,
  Metrics,
  CronJob,
  CronOptions,
  WorkerOptions,
  ClientOptions,
  JobProcessor,
  // BullMQ-like features
  JobLogEntry,
  FlowChild,
  FlowOptions,
  FlowResult,
} from './types';
