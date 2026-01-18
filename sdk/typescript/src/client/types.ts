/**
 * Internal types for FlashQ client modules
 */
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
  JobLogEntry,
  FlowChild,
  FlowResult,
} from '../types';

/**
 * Interface for the internal send method.
 * Used by all modules to communicate with the server.
 */
export interface IFlashQClient {
  /** Send a command to the server */
  send<T>(command: Record<string, unknown>, customTimeout?: number): Promise<T>;
  /** Client options */
  readonly options: Required<ClientOptions>;
}

/**
 * Flow options for parent job
 */
export interface FlowOptions {
  /** Job priority (higher = processed first) */
  priority?: number;
}

// Re-export types for convenience
export type {
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
  JobLogEntry,
  FlowChild,
  FlowResult,
};
