/**
 * flashQ - Official TypeScript SDK
 *
 * High-performance job queue with the simplest API.
 *
 * @example
 * ```typescript
 * // Simple usage (recommended)
 * import { Queue } from 'flashq';
 *
 * const emails = new Queue('emails');
 * await emails.add({ to: 'user@example.com' });
 *
 * emails.process(async (job) => {
 *   await sendEmail(job.data);
 * });
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

// High-level API (recommended)
export { Queue } from './queue';

// Low-level API
export { FlashQ, FlashQ as default } from './client';
export { Worker } from './worker';

// Sandboxed Processors
export { SandboxedWorker, createProcessor } from './sandbox';
export type { SandboxOptions } from './sandbox';

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
