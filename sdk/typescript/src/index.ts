/**
 * flashQ - High-performance Job Queue (BullMQ-compatible)
 *
 * @example
 * ```typescript
 * import { Queue, Worker } from 'flashq';
 *
 * // Add jobs
 * const queue = new Queue('emails');
 * await queue.add('send', { to: 'user@example.com' });
 *
 * // Process jobs (auto-starts)
 * const worker = new Worker('emails', async (job) => {
 *   await sendEmail(job.data);
 *   return { sent: true };
 * });
 * ```
 *
 * @packageDocumentation
 */

// BullMQ-compatible API
export { Queue } from './queue';
export type { QueueOptions, JobOptions } from './queue';

export { Worker } from './worker';
export type { BullMQWorkerOptions } from './worker';

// Low-level API
export { FlashQ, FlashQ as default } from './client';

// Constants
export { MAX_BATCH_SIZE } from './client/core';

// Optional: Real-time events
export { EventSubscriber } from './events';

// Types
export type {
  Job,
  JobState,
  PushOptions,
  WorkerOptions,
  ClientOptions,
} from './types';
