/**
 * FlashQ Client
 *
 * High-performance job queue client with auto-connect.
 *
 * @example
 * ```typescript
 * import { FlashQ } from 'flashq';
 *
 * const client = new FlashQ();
 * const job = await client.add('emails', { to: 'user@example.com' });
 * ```
 *
 * @module
 */

// Re-export from modular structure
export { FlashQ, FlashQ as default } from './client/index';
