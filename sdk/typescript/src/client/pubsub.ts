/**
 * Pub/Sub operations for FlashQ client.
 * Redis-like publish/subscribe messaging.
 */

import type { FlashQConnection } from './connection';

// Response types
interface PubResponse {
  ok: boolean;
  receivers: number;
  error?: string;
}

interface SubResponse {
  ok: boolean;
  channels: string[];
  error?: string;
}

interface ChannelsResponse {
  ok: boolean;
  channels: string[];
  error?: string;
}

interface NumsubResponse {
  ok: boolean;
  counts: Array<[string, number]>;
  error?: string;
}

/**
 * Publish a message to a channel.
 * @returns Number of subscribers that received the message.
 */
export async function publish(
  client: FlashQConnection,
  channel: string,
  message: unknown
): Promise<number> {
  const response = await client.send<PubResponse>({ cmd: 'PUB', channel, message });
  if (!response.ok) {
    throw new Error(response.error || 'Publish failed');
  }
  return response.receivers;
}

/**
 * Subscribe to channels.
 * @returns List of subscribed channels.
 */
export async function subscribe(
  client: FlashQConnection,
  channels: string[]
): Promise<string[]> {
  const response = await client.send<SubResponse>({ cmd: 'SUB', channels });
  if (!response.ok) {
    throw new Error(response.error || 'Subscribe failed');
  }
  return response.channels;
}

/**
 * Subscribe to patterns (e.g., "events:*").
 * @returns List of subscribed patterns.
 */
export async function psubscribe(
  client: FlashQConnection,
  patterns: string[]
): Promise<string[]> {
  const response = await client.send<SubResponse>({ cmd: 'PSUB', patterns });
  if (!response.ok) {
    throw new Error(response.error || 'Pattern subscribe failed');
  }
  return response.channels;
}

/**
 * Unsubscribe from channels.
 * @returns List of unsubscribed channels.
 */
export async function unsubscribe(
  client: FlashQConnection,
  channels: string[]
): Promise<string[]> {
  const response = await client.send<SubResponse>({ cmd: 'UNSUB', channels });
  if (!response.ok) {
    throw new Error(response.error || 'Unsubscribe failed');
  }
  return response.channels;
}

/**
 * Unsubscribe from patterns.
 * @returns List of unsubscribed patterns.
 */
export async function punsubscribe(
  client: FlashQConnection,
  patterns: string[]
): Promise<string[]> {
  const response = await client.send<SubResponse>({ cmd: 'PUNSUB', patterns });
  if (!response.ok) {
    throw new Error(response.error || 'Pattern unsubscribe failed');
  }
  return response.channels;
}

/**
 * List active channels.
 * @param pattern - Optional glob pattern to filter channels
 * @returns Array of active channel names
 */
export async function channels(
  client: FlashQConnection,
  pattern?: string
): Promise<string[]> {
  const response = await client.send<ChannelsResponse>({ cmd: 'PUBSUBCHANNELS', pattern });
  if (!response.ok) {
    throw new Error(response.error || 'Channels list failed');
  }
  return response.channels;
}

/**
 * Get subscriber counts for channels.
 * @returns Array of [channel, count] tuples
 */
export async function numsub(
  client: FlashQConnection,
  channelNames: string[]
): Promise<Array<[string, number]>> {
  const response = await client.send<NumsubResponse>({ cmd: 'PUBSUBNUMSUB', channels: channelNames });
  if (!response.ok) {
    throw new Error(response.error || 'Numsub failed');
  }
  return response.counts;
}
