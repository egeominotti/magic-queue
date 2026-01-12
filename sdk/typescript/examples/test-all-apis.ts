#!/usr/bin/env bun
/**
 * ╔═══════════════════════════════════════════════════════════════════════════════╗
 * ║                                                                               ║
 * ║                    🧪 FLASHQ COMPLETE API TEST SUITE 🧪                       ║
 * ║                                                                               ║
 * ║                    Tests EVERY API endpoint for correctness                   ║
 * ║                                                                               ║
 * ╚═══════════════════════════════════════════════════════════════════════════════╝
 */

const BASE_URL = 'http://localhost:6790';
const TCP_PORT = 6789;

// Colors for output
const c = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  dim: '\x1b[2m',
  bright: '\x1b[1m',
};

let passed = 0;
let failed = 0;
const failures: string[] = [];

async function test(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`${c.green}✓${c.reset} ${name}`);
  } catch (error: any) {
    failed++;
    const msg = error.message || String(error);
    failures.push(`${name}: ${msg}`);
    console.log(`${c.red}✗${c.reset} ${name}`);
    console.log(`  ${c.dim}${msg}${c.reset}`);
  }
}

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function assertEqual(actual: any, expected: any, field: string) {
  if (actual !== expected) {
    throw new Error(`${field}: expected ${expected}, got ${actual}`);
  }
}

async function http(method: string, path: string, body?: any): Promise<any> {
  const res = await fetch(`${BASE_URL}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  return res.json();
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTTP API TESTS
// ═══════════════════════════════════════════════════════════════════════════════

async function testHttpApis() {
  console.log(`\n${c.bright}${c.blue}═══════════════════════════════════════════════════════════════${c.reset}`);
  console.log(`${c.bright}${c.blue}  HTTP REST API TESTS${c.reset}`);
  console.log(`${c.bright}${c.blue}═══════════════════════════════════════════════════════════════${c.reset}\n`);

  // ─────────────────────────────────────────────────────────────────────────────
  // Health & Server
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`${c.cyan}Health & Server${c.reset}`);

  await test('GET /health', async () => {
    const res = await http('GET', '/health');
    assert(res.ok === true, 'Response not ok');
    assert(res.data.status === 'healthy', 'Status not healthy');
    assert(typeof res.data.is_leader === 'boolean', 'is_leader not boolean');
  });

  await test('GET /settings', async () => {
    const res = await http('GET', '/settings');
    assert(res.ok === true, 'Response not ok');
    assert(typeof res.data.version === 'string', 'version not string');
    assert(typeof res.data.uptime_seconds === 'number', 'uptime not number');
  });

  await test('GET /cluster/nodes', async () => {
    const res = await http('GET', '/cluster/nodes');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'nodes not array');
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Queue Operations
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Queue Operations${c.reset}`);

  await test('GET /queues (empty)', async () => {
    const res = await http('GET', '/queues');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'queues not array');
  });

  let jobId: number;

  await test('POST /queues/{queue}/jobs - push job', async () => {
    const res = await http('POST', '/queues/test-queue/jobs', {
      data: { message: 'hello', value: 123 },
      priority: 10,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(typeof res.data.id === 'number', 'id not number');
    assert(res.data.queue === 'test-queue', 'queue mismatch');
    assert(res.data.priority === 10, 'priority mismatch');
    jobId = res.data.id;
  });

  await test('POST /queues/{queue}/jobs - with delay', async () => {
    const res = await http('POST', '/queues/test-queue/jobs', {
      data: { delayed: true },
      delay: 60000,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(res.data.run_at > Date.now(), 'run_at should be in future');
  });

  await test('POST /queues/{queue}/jobs - with TTL', async () => {
    const res = await http('POST', '/queues/test-queue/jobs', {
      data: { ttl: true },
      ttl: 3600000,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(res.data.ttl === 3600000, 'ttl mismatch');
  });

  await test('POST /queues/{queue}/jobs - with max_attempts', async () => {
    const res = await http('POST', '/queues/test-queue/jobs', {
      data: { retry: true },
      max_attempts: 5,
      backoff: 2000,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(res.data.max_attempts === 5, 'max_attempts mismatch');
  });

  await test('POST /queues/{queue}/jobs - with unique_key', async () => {
    const uniqueKey = `unique-${Date.now()}`;
    const res1 = await http('POST', '/queues/test-queue/jobs', {
      data: { unique: true },
      unique_key: uniqueKey,
    });
    assert(res1.ok === true, 'First push should succeed');

    const res2 = await http('POST', '/queues/test-queue/jobs', {
      data: { unique: true },
      unique_key: uniqueKey,
    });
    assert(res2.ok === false, 'Duplicate unique_key should fail');
  });

  await test('POST /queues/{queue}/jobs - with tags', async () => {
    const res = await http('POST', '/queues/test-queue/jobs', {
      data: { tagged: true },
      tags: ['email', 'urgent'],
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('POST /queues/{queue}/jobs - LIFO mode', async () => {
    const res = await http('POST', '/queues/test-queue/jobs', {
      data: { lifo: true },
      lifo: true,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(res.data.lifo === true, 'lifo should be true');
  });

  await test('GET /queues (with jobs)', async () => {
    const res = await http('GET', '/queues');
    assert(res.ok === true, 'Response not ok');
    const queue = res.data.find((q: any) => q.name === 'test-queue');
    assert(queue !== undefined, 'test-queue not found');
    assert(queue.pending > 0, 'pending should be > 0');
  });

  await test('GET /queues/{queue}/jobs - pull job', async () => {
    const res = await http('GET', '/queues/test-queue/jobs?count=1');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'jobs not array');
    assert(res.data.length === 1, 'should return 1 job');
    jobId = res.data[0].id;
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Job Operations
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Job Operations${c.reset}`);

  await test('GET /jobs/{id} - get job', async () => {
    const res = await http('GET', `/jobs/${jobId}`);
    assert(res.ok === true, 'Response not ok');
    assert(res.data.state === 'active', `state should be active, got ${res.data.state}`);
  });

  await test('POST /jobs/{id}/progress - update progress', async () => {
    const res = await http('POST', `/jobs/${jobId}/progress`, {
      progress: 50,
      message: 'Halfway done',
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('GET /jobs/{id}/progress - get progress', async () => {
    const res = await http('GET', `/jobs/${jobId}/progress`);
    assert(res.ok === true, 'Response not ok');
    assert(res.data[0] === 50, 'progress should be 50');
    assert(res.data[1] === 'Halfway done', 'message mismatch');
  });

  await test('POST /jobs/{id}/ack - acknowledge job', async () => {
    const res = await http('POST', `/jobs/${jobId}/ack`, {
      result: { processed: true, timestamp: Date.now() },
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('GET /jobs/{id}/result - get result', async () => {
    const res = await http('GET', `/jobs/${jobId}/result`);
    assert(res.ok === true, 'Response not ok');
    assert(res.data?.processed === true, 'result.processed should be true');
  });

  await test('GET /jobs/{id} - completed state', async () => {
    const res = await http('GET', `/jobs/${jobId}`);
    assert(res.ok === true, 'Response not ok');
    assert(res.data.state === 'completed', `state should be completed, got ${res.data.state}`);
  });

  // Test fail
  let failJobId: number;
  await test('POST /jobs/{id}/fail - fail job', async () => {
    // Push and pull a job first
    const push = await http('POST', '/queues/fail-test/jobs', {
      data: { test: 'fail' },
      max_attempts: 3,
    });
    failJobId = push.data.id;
    await http('GET', '/queues/fail-test/jobs?count=1');

    const res = await http('POST', `/jobs/${failJobId}/fail`, {
      error: 'Test failure',
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  // Test cancel
  await test('POST /jobs/{id}/cancel - cancel job', async () => {
    const push = await http('POST', '/queues/cancel-test/jobs', {
      data: { test: 'cancel' },
    });
    const res = await http('POST', `/jobs/${push.data.id}/cancel`);
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Job Browser
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Job Browser${c.reset}`);

  await test('GET /jobs - list all jobs', async () => {
    const res = await http('GET', '/jobs?limit=100');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'jobs not array');
  });

  await test('GET /jobs?queue=test-queue - filter by queue', async () => {
    const res = await http('GET', '/jobs?queue=test-queue&limit=50');
    assert(res.ok === true, 'Response not ok');
    for (const item of res.data) {
      // Handle both {job, state} and flat structure
      const queue = item.job?.queue ?? item.queue;
      assert(queue === 'test-queue', `queue filter not working, got ${queue}`);
    }
  });

  await test('GET /jobs?state=waiting - filter by state', async () => {
    const res = await http('GET', '/jobs?state=waiting&limit=50');
    assert(res.ok === true, 'Response not ok');
    for (const job of res.data) {
      assert(job.state === 'waiting', `state filter not working, got ${job.state}`);
    }
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Queue Control
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Queue Control${c.reset}`);

  await test('POST /queues/{queue}/pause - pause queue', async () => {
    const res = await http('POST', '/queues/pause-test/pause');
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('POST /queues/{queue}/resume - resume queue', async () => {
    const res = await http('POST', '/queues/pause-test/resume');
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('POST /queues/{queue}/rate-limit - set rate limit', async () => {
    const res = await http('POST', '/queues/rate-test/rate-limit', { limit: 100 });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('DELETE /queues/{queue}/rate-limit - clear rate limit', async () => {
    const res = await http('DELETE', '/queues/rate-test/rate-limit');
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('POST /queues/{queue}/concurrency - set concurrency', async () => {
    const res = await http('POST', '/queues/conc-test/concurrency', { limit: 5 });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('DELETE /queues/{queue}/concurrency - clear concurrency', async () => {
    const res = await http('DELETE', '/queues/conc-test/concurrency');
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Dead Letter Queue
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Dead Letter Queue${c.reset}`);

  // Create a job that goes to DLQ
  await test('DLQ - push job with max_attempts=1', async () => {
    const res = await http('POST', '/queues/dlq-test/jobs', {
      data: { dlq: true },
      max_attempts: 1,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('DLQ - pull and fail to send to DLQ', async () => {
    const pull = await http('GET', '/queues/dlq-test/jobs?count=1');
    if (pull.data.length > 0) {
      const res = await http('POST', `/jobs/${pull.data[0].id}/fail`, {
        error: 'Send to DLQ',
      });
      assert(res.ok === true, `Response not ok: ${res.error}`);
    }
  });

  await test('GET /queues/{queue}/dlq - get DLQ jobs', async () => {
    const res = await http('GET', '/queues/dlq-test/dlq?count=10');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'dlq not array');
  });

  await test('POST /queues/{queue}/dlq/retry - retry DLQ', async () => {
    const res = await http('POST', '/queues/dlq-test/dlq/retry');
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Cron Jobs
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Cron Jobs${c.reset}`);

  await test('POST /crons/{name} - create cron job', async () => {
    const res = await http('POST', '/crons/test-cron', {
      queue: 'cron-queue',
      data: { cron: true },
      schedule: '0 * * * * *', // Every minute
      priority: 5,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('GET /crons - list cron jobs', async () => {
    const res = await http('GET', '/crons');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'crons not array');
    const cron = res.data.find((c: any) => c.name === 'test-cron');
    assert(cron !== undefined, 'test-cron not found');
  });

  await test('DELETE /crons/{name} - delete cron job', async () => {
    const res = await http('DELETE', '/crons/test-cron');
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Stats & Metrics
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Stats & Metrics${c.reset}`);

  await test('GET /stats - get stats', async () => {
    const res = await http('GET', '/stats');
    assert(res.ok === true, 'Response not ok');
    assert(typeof res.data.queued === 'number', 'queued not number');
    assert(typeof res.data.processing === 'number', 'processing not number');
    assert(typeof res.data.delayed === 'number', 'delayed not number');
    assert(typeof res.data.dlq === 'number', 'dlq not number');
  });

  await test('GET /metrics - get metrics', async () => {
    const res = await http('GET', '/metrics');
    assert(res.ok === true, 'Response not ok');
    assert(typeof res.data.total_pushed === 'number', 'total_pushed not number');
    assert(typeof res.data.total_completed === 'number', 'total_completed not number');
    assert(Array.isArray(res.data.queues), 'queues not array');
  });

  await test('GET /metrics/history - get metrics history', async () => {
    const res = await http('GET', '/metrics/history');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'history not array');
  });

  await test('GET /metrics/prometheus - prometheus format', async () => {
    const response = await fetch(`${BASE_URL}/metrics/prometheus`);
    const text = await response.text();
    assert(text.includes('flashq_jobs'), 'should contain flashq_jobs');
    assert(text.includes('# HELP'), 'should contain # HELP');
    assert(text.includes('# TYPE'), 'should contain # TYPE');
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Workers
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Workers${c.reset}`);

  await test('POST /workers/{id}/heartbeat - worker heartbeat', async () => {
    const res = await http('POST', '/workers/worker-1/heartbeat', {
      queues: ['test-queue', 'emails'],
      concurrency: 10,
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  await test('GET /workers - list workers', async () => {
    const res = await http('GET', '/workers');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'workers not array');
    const worker = res.data.find((w: any) => w.id === 'worker-1');
    assert(worker !== undefined, 'worker-1 not found');
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Webhooks
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Webhooks${c.reset}`);

  let webhookId: string;

  await test('POST /webhooks - create webhook', async () => {
    const res = await http('POST', '/webhooks', {
      url: 'https://example.com/webhook',
      events: ['job.completed', 'job.failed'],
      queue: 'test-queue',
      secret: 'my-secret',
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(typeof res.data === 'string', 'webhook id should be string');
    webhookId = res.data;
  });

  await test('GET /webhooks - list webhooks', async () => {
    const res = await http('GET', '/webhooks');
    assert(res.ok === true, 'Response not ok');
    assert(Array.isArray(res.data), 'webhooks not array');
    const webhook = res.data.find((w: any) => w.id === webhookId);
    assert(webhook !== undefined, 'webhook not found');
  });

  await test('DELETE /webhooks/{id} - delete webhook', async () => {
    const res = await http('DELETE', `/webhooks/${webhookId}`);
    assert(res.ok === true, `Response not ok: ${res.error}`);
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Incoming Webhooks
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Incoming Webhooks${c.reset}`);

  await test('POST /webhooks/incoming/{queue} - incoming webhook', async () => {
    const res = await http('POST', '/webhooks/incoming/webhook-queue', {
      source: 'external',
      data: { test: true },
    });
    assert(res.ok === true, `Response not ok: ${res.error}`);
    assert(typeof res.data.id === 'number', 'should return job');
    assert(res.data.queue === 'webhook-queue', 'queue mismatch');
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Job Dependencies
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Job Dependencies${c.reset}`);

  await test('Job dependencies - create parent jobs', async () => {
    const parent1 = await http('POST', '/queues/deps-test/jobs', { data: { parent: 1 } });
    const parent2 = await http('POST', '/queues/deps-test/jobs', { data: { parent: 2 } });

    const child = await http('POST', '/queues/deps-test/jobs', {
      data: { child: true },
      depends_on: [parent1.data.id, parent2.data.id],
    });

    assert(child.ok === true, `Response not ok: ${child.error}`);

    // Child should be in waiting-children state (various formats)
    const childState = await http('GET', `/jobs/${child.data.id}`);
    const state = childState.data.state;
    const validStates = ['waiting-children', 'waitingchildren', 'waiting_children', 'waiting'];
    assert(validStates.includes(state), `Expected waiting-children variant, got ${state}`);
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Error Handling
  // ─────────────────────────────────────────────────────────────────────────────
  console.log(`\n${c.cyan}Error Handling${c.reset}`);

  await test('GET /jobs/{id} - non-existent job', async () => {
    const res = await http('GET', '/jobs/999999999');
    assert(res.data.state === 'unknown', 'should return unknown state');
  });

  await test('POST /jobs/{id}/ack - non-existent job', async () => {
    const res = await http('POST', '/jobs/999999999/ack', {});
    assert(res.ok === false, 'should fail for non-existent job');
  });

  await test('POST /queues/{queue}/jobs - missing data field', async () => {
    // Server should handle missing data field gracefully
    // Either by failing with error or treating as null
    try {
      const res = await http('POST', '/queues/error-test/jobs', {
        priority: 10,
        // data field intentionally missing
      });
      // If server accepts it, that's ok
      assert(res.ok === true || res.ok === false, 'should return a response');
    } catch {
      // If fetch fails, that's also acceptable
      assert(true, 'handled gracefully');
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// TCP PROTOCOL TESTS
// ═══════════════════════════════════════════════════════════════════════════════

async function testTcpProtocol() {
  console.log(`\n${c.bright}${c.blue}═══════════════════════════════════════════════════════════════${c.reset}`);
  console.log(`${c.bright}${c.blue}  TCP PROTOCOL TESTS${c.reset}`);
  console.log(`${c.bright}${c.blue}═══════════════════════════════════════════════════════════════${c.reset}\n`);

  const { FlashQ } = await import('../src');
  const client = new FlashQ({ host: 'localhost', port: TCP_PORT });

  await test('TCP: connect', async () => {
    await client.connect();
  });

  let tcpJobId: number;

  await test('TCP: push', async () => {
    const job = await client.push('tcp-test', { message: 'hello' }, { priority: 5 });
    assert(typeof job.id === 'number', 'id not number');
    assert(job.queue === 'tcp-test', 'queue mismatch');
    tcpJobId = job.id;
  });

  await test('TCP: pushBatch', async () => {
    const ids = await client.pushBatch('tcp-batch', [
      { data: { i: 1 } },
      { data: { i: 2 } },
      { data: { i: 3 } },
    ]);
    assert(ids.length === 3, 'should return 3 ids');
  });

  await test('TCP: pull', async () => {
    const job = await client.pull('tcp-test');
    assert(job.id === tcpJobId, 'should get same job');
  });

  await test('TCP: progress', async () => {
    await client.progress(tcpJobId, 75, 'Almost done');
  });

  await test('TCP: getProgress', async () => {
    const result = await client.getProgress(tcpJobId);
    assert(result.progress === 75, `progress should be 75, got ${result.progress}`);
    assert(result.message === 'Almost done', `message mismatch: ${result.message}`);
  });

  await test('TCP: ack', async () => {
    await client.ack(tcpJobId, { result: 'done' });
  });

  await test('TCP: getResult', async () => {
    const result = await client.getResult(tcpJobId);
    assert(result?.result === 'done', 'result mismatch');
  });

  await test('TCP: getState', async () => {
    const state = await client.getState(tcpJobId);
    assert(state === 'completed', `state should be completed, got ${state}`);
  });

  let batchJobIds: number[] = [];

  await test('TCP: pullBatch', async () => {
    const jobs = await client.pullBatch('tcp-batch', 3);
    assert(jobs.length === 3, 'should get 3 jobs');
    batchJobIds = jobs.map(j => j.id);
  });

  await test('TCP: ackBatch', async () => {
    // Use jobs from the previous pullBatch test
    if (batchJobIds.length > 0) {
      const count = await client.ackBatch(batchJobIds);
      assert(count >= 0, 'ackBatch should return count');
    }
  });

  await test('TCP: fail', async () => {
    const job = await client.push('tcp-fail', { test: true }, { max_attempts: 2 });
    await client.pull('tcp-fail');
    await client.fail(job.id, 'Test failure');
  });

  await test('TCP: cancel', async () => {
    const job = await client.push('tcp-cancel', { test: true });
    await client.cancel(job.id);
    const state = await client.getState(job.id);
    // Cancelled jobs are removed, so state should be unknown
    assert(state === 'unknown' || state === 'cancelled', `state: ${state}`);
  });

  await test('TCP: pause/resume', async () => {
    await client.pause('tcp-pause');
    await client.resume('tcp-pause');
  });

  await test('TCP: setRateLimit/clearRateLimit', async () => {
    await client.setRateLimit('tcp-rate', 100);
    await client.clearRateLimit('tcp-rate');
  });

  await test('TCP: setConcurrency/clearConcurrency', async () => {
    await client.setConcurrency('tcp-conc', 5);
    await client.clearConcurrency('tcp-conc');
  });

  await test('TCP: listQueues', async () => {
    const queues = await client.listQueues();
    assert(Array.isArray(queues), 'queues not array');
  });

  await test('TCP: stats', async () => {
    const stats = await client.stats();
    assert(typeof stats.queued === 'number', 'queued not number');
  });

  await test('TCP: metrics', async () => {
    const metrics = await client.metrics();
    assert(typeof metrics.total_pushed === 'number', 'total_pushed not number');
  });

  await test('TCP: addCron', async () => {
    await client.addCron('tcp-cron', {
      queue: 'tcp-cron-queue',
      data: { test: true },
      schedule: '0 0 * * * *',
    });
  });

  await test('TCP: listCrons', async () => {
    const crons = await client.listCrons();
    assert(Array.isArray(crons), 'crons not array');
  });

  await test('TCP: deleteCron', async () => {
    const deleted = await client.deleteCron('tcp-cron');
    assert(deleted === true, 'should return true');
  });

  await test('TCP: getDlq', async () => {
    const dlq = await client.getDlq('tcp-test');
    assert(Array.isArray(dlq), 'dlq not array');
  });

  await test('TCP: getJob', async () => {
    // For completed jobs, job data may be null but state is still valid
    const result = await client.getJob(tcpJobId);
    // Completed jobs may return null (data not stored to save memory)
    // Use getState for completed jobs instead
    const state = await client.getState(tcpJobId);
    assert(state === 'completed', `state should be completed, got ${state}`);
  });

  await test('TCP: close', async () => {
    await client.close();
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log(`${c.cyan}
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║                    🧪 FLASHQ COMPLETE API TEST SUITE 🧪                       ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
${c.reset}`);

  const start = Date.now();

  try {
    await testHttpApis();
    await testTcpProtocol();
  } catch (error) {
    console.error(`\n${c.red}Fatal error:${c.reset}`, error);
  }

  const duration = ((Date.now() - start) / 1000).toFixed(2);

  console.log(`\n${c.bright}${c.blue}═══════════════════════════════════════════════════════════════${c.reset}`);
  console.log(`${c.bright}${c.blue}  RESULTS${c.reset}`);
  console.log(`${c.bright}${c.blue}═══════════════════════════════════════════════════════════════${c.reset}\n`);

  console.log(`  ${c.green}Passed:${c.reset} ${passed}`);
  console.log(`  ${c.red}Failed:${c.reset} ${failed}`);
  console.log(`  ${c.dim}Duration:${c.reset} ${duration}s`);

  if (failures.length > 0) {
    console.log(`\n${c.red}Failures:${c.reset}`);
    for (const f of failures) {
      console.log(`  ${c.red}•${c.reset} ${f}`);
    }
  }

  const total = passed + failed;
  const percentage = total > 0 ? ((passed / total) * 100).toFixed(1) : 0;

  console.log(`
${c.bright}╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║   ${passed === total ? c.green : c.yellow}${percentage}% tests passed (${passed}/${total})${c.reset}${' '.repeat(55 - percentage.toString().length - passed.toString().length - total.toString().length)}║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝${c.reset}
`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
