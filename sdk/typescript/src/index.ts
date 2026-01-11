/**
 * MagicQueue - Official TypeScript/Node.js SDK
 *
 * High-performance job queue client for MagicQueue server.
 *
 * @packageDocumentation
 */

export { MagicQueue, MagicQueue as default } from './client';
export { Worker } from './worker';
export * from './types';

// Re-export commonly used types at top level
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
} from './types';
