/**
 * FlashQ - Official TypeScript/Node.js SDK
 *
 * High-performance job queue client for FlashQ server.
 *
 * @packageDocumentation
 */

export { FlashQ, FlashQ as default } from './client';
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
