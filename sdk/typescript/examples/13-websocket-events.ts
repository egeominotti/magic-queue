/**
 * WebSocket Real-Time Events Example
 *
 * Demonstrates subscribing to real-time job events via WebSocket.
 *
 * Run: bun run examples/13-websocket-events.ts
 *
 * Prerequisites:
 *   - Start server with HTTP enabled: HTTP=1 cargo run --release
 */

import { FlashQ } from '../src';

interface JobEvent {
  event_type: 'pushed' | 'completed' | 'failed' | 'progress' | 'timeout';
  queue: string;
  job_id: number;
  timestamp: number;
  data?: unknown;
  error?: string;
  progress?: number;
}

async function main() {
  const WS_URL = 'ws://localhost:6790/ws';
  const WS_URL_QUEUE = 'ws://localhost:6790/ws/example-queue'; // Filter by queue

  console.log('===================================');
  console.log('WebSocket Real-Time Events');
  console.log('===================================\n');

  // Connect to TCP for pushing jobs
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('TCP Client connected\n');

  // Connect to WebSocket for events
  console.log('Connecting to WebSocket...');

  const ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    console.log('WebSocket connected to', WS_URL);
    console.log('Listening for events...\n');
  };

  ws.onmessage = (event) => {
    const data = JSON.parse(event.data as string) as JobEvent;
    const time = new Date(data.timestamp).toISOString().split('T')[1].slice(0, 8);

    switch (data.event_type) {
      case 'pushed':
        console.log(`[${time}] PUSHED - Job ${data.job_id} to queue "${data.queue}"`);
        break;
      case 'completed':
        console.log(`[${time}] COMPLETED - Job ${data.job_id} in queue "${data.queue}"`);
        break;
      case 'failed':
        console.log(`[${time}] FAILED - Job ${data.job_id}: ${data.error}`);
        break;
      case 'progress':
        console.log(`[${time}] PROGRESS - Job ${data.job_id}: ${data.progress}%`);
        break;
      case 'timeout':
        console.log(`[${time}] TIMEOUT - Job ${data.job_id}`);
        break;
      default:
        console.log(`[${time}] EVENT:`, data);
    }
  };

  ws.onerror = (error) => {
    console.error('WebSocket error:', error);
  };

  ws.onclose = () => {
    console.log('\nWebSocket disconnected');
  };

  // Wait for WebSocket connection
  await new Promise<void>((resolve) => {
    const check = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        clearInterval(check);
        resolve();
      }
    }, 100);
  });

  // Push some jobs to trigger events
  console.log('\nPushing jobs to trigger events...\n');

  // Push jobs
  await client.push('example-queue', { task: 'task-1' });
  await client.push('example-queue', { task: 'task-2' });
  await client.push('other-queue', { task: 'task-3' });

  // Pull and ack to trigger completed events
  const job1 = await client.pull('example-queue');
  if (job1) {
    await client.ack(job1.id, { result: 'done' });
  }

  // Wait for events
  await sleep(2000);

  // === WebSocket with Token Authentication ===
  console.log('\n===================================');
  console.log('WebSocket with Authentication');
  console.log('===================================\n');

  // When server has AUTH_TOKENS set, use token in query string:
  // const authWs = new WebSocket('ws://localhost:6790/ws?token=your-secret-token');

  console.log('To use authenticated WebSocket:');
  console.log('  1. Start server with: AUTH_TOKENS=secret cargo run');
  console.log('  2. Connect with: ws://localhost:6790/ws?token=secret\n');

  // === Queue-Specific Events ===
  console.log('===================================');
  console.log('Queue-Specific Events');
  console.log('===================================\n');

  console.log('Connect to specific queue events:');
  console.log('  ws://localhost:6790/ws/my-queue-name\n');

  const queueWs = new WebSocket(WS_URL_QUEUE);

  queueWs.onopen = () => {
    console.log(`Connected to queue-specific WebSocket`);
    console.log('Only events from "example-queue" will be received\n');
  };

  queueWs.onmessage = (event) => {
    const data = JSON.parse(event.data as string) as JobEvent;
    console.log(`[Queue WS] ${data.event_type}: Job ${data.job_id}`);
  };

  // Wait for connection
  await new Promise<void>((resolve) => {
    const check = setInterval(() => {
      if (queueWs.readyState === WebSocket.OPEN) {
        clearInterval(check);
        resolve();
      }
    }, 100);
  });

  // Push to filtered queue
  console.log('Pushing to example-queue (should appear)...');
  await client.push('example-queue', { task: 'filtered-1' });

  console.log('Pushing to other-queue (should NOT appear)...');
  await client.push('other-queue', { task: 'filtered-2' });

  await sleep(1000);

  // Cleanup
  ws.close();
  queueWs.close();
  await client.close();

  console.log('\nDone!');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);
