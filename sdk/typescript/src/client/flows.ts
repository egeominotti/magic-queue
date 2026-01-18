/**
 * Flow operations (parent-child job dependencies)
 */
import type { IFlashQClient, FlowChild, FlowResult, FlowOptions } from './types';

/**
 * Push a flow (parent job with children).
 * The parent job waits until all children complete before becoming ready.
 *
 * @param client - FlashQ client instance
 * @param queue - Parent queue name
 * @param parentData - Parent job data
 * @param children - Array of child jobs
 * @param options - Flow options
 * @returns Parent and children IDs
 *
 * @example
 * ```typescript
 * const flow = await client.pushFlow(
 *   'reports',
 *   { reportType: 'monthly' },
 *   [
 *     { queue: 'process', data: { section: 'sales' } },
 *     { queue: 'process', data: { section: 'marketing' } },
 *     { queue: 'process', data: { section: 'operations' } },
 *   ]
 * );
 * console.log('Parent:', flow.parent_id);
 * console.log('Children:', flow.children_ids);
 * ```
 */
export async function pushFlow<T = unknown>(
  client: IFlashQClient,
  queue: string,
  parentData: T,
  children: FlowChild[],
  options: FlowOptions = {}
): Promise<FlowResult> {
  const response = await client.send<{
    ok: boolean;
    parent_id: number;
    children_ids: number[];
  }>({
    cmd: 'FLOW',
    queue,
    data: parentData,
    children,
    priority: options.priority ?? 0,
  });
  return {
    parent_id: response.parent_id,
    children_ids: response.children_ids,
  };
}

/**
 * Get children job IDs for a parent job in a flow.
 *
 * @param client - FlashQ client instance
 * @param jobId - Parent job ID
 * @returns Array of children job IDs
 *
 * @example
 * ```typescript
 * const childIds = await client.getChildren(parentJobId);
 * for (const id of childIds) {
 *   const child = await client.getJob(id);
 *   console.log(`Child ${id}: ${child?.state}`);
 * }
 * ```
 */
export async function getChildren(
  client: IFlashQClient,
  jobId: number
): Promise<number[]> {
  const response = await client.send<{ ok: boolean; children_ids: number[] }>({
    cmd: 'GETCHILDREN',
    parent_id: jobId,
  });
  return response.children_ids;
}
