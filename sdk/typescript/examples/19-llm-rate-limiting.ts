/**
 * LLM API Rate Limiting Example
 *
 * Demonstrates how to manage LLM API calls with:
 * - Rate limiting (tokens per minute)
 * - Concurrency control (parallel requests)
 * - Priority queuing (premium vs standard)
 * - Automatic retries with backoff
 *
 * Run: bun run examples/19-llm-rate-limiting.ts
 */

import { Worker, FlashQ } from '../src';

const QUEUE_NAME = 'llm-requests';

interface LLMRequest {
  prompt: string;
  model: 'gpt-4' | 'gpt-3.5';
  userId: string;
  premium: boolean;
}

interface LLMResponse {
  text: string;
  tokens: number;
  latency: number;
}

// Simulated LLM API call
async function callLLMAPI(request: LLMRequest): Promise<LLMResponse> {
  const latency = request.model === 'gpt-4' ? 200 : 50;
  await new Promise(r => setTimeout(r, latency));

  // Simulate occasional rate limit errors
  if (Math.random() < 0.1) {
    throw new Error('Rate limit exceeded');
  }

  return {
    text: `Response to: ${request.prompt.slice(0, 20)}...`,
    tokens: Math.floor(request.prompt.length * 1.5),
    latency,
  };
}

async function main() {
  const client = new FlashQ();
  await client.connect();
  await client.obliterate(QUEUE_NAME);

  console.log('=== LLM Rate Limiting Example ===\n');

  // Configure rate limiting: 100 requests per minute
  await client.setRateLimit(QUEUE_NAME, 100);

  // Configure concurrency: max 5 parallel API calls
  await client.setConcurrency(QUEUE_NAME, 5);

  console.log('Rate limit: 100 requests/min');
  console.log('Concurrency: 5 parallel requests\n');

  // Track stats
  let completed = 0;
  let totalTokens = 0;
  let totalLatency = 0;
  const startTime = Date.now();

  // Create worker with retry configuration
  const worker = new Worker<LLMRequest, LLMResponse>(QUEUE_NAME, async (job) => {
    const result = await callLLMAPI(job.data);
    return result;
  }, { concurrency: 5 });

  worker.on('completed', (_job, result) => {
    completed++;
    totalTokens += result.tokens;
    totalLatency += result.latency;
    if (completed % 10 === 0) {
      console.log(`Processed: ${completed} requests, ${totalTokens} tokens`);
    }
  });

  worker.on('failed', (job, error) => {
    console.log(`Request failed for user ${job.data.userId}: ${error.message}`);
  });

  await new Promise(r => setTimeout(r, 500));

  // Queue various requests with different priorities
  console.log('Queueing requests...\n');
  const jobIds: number[] = [];

  // Premium users get higher priority (10)
  for (let i = 0; i < 5; i++) {
    const job = await client.push(QUEUE_NAME, {
      prompt: `Premium request ${i}: Analyze this important document...`,
      model: 'gpt-4',
      userId: `premium-${i}`,
      premium: true,
    }, {
      priority: 10,
      max_attempts: 3,
      backoff: 2000,
    });
    jobIds.push(job.id);
  }

  // Standard users get normal priority (5)
  for (let i = 0; i < 20; i++) {
    const job = await client.push(QUEUE_NAME, {
      prompt: `Standard request ${i}: Quick question about...`,
      model: 'gpt-3.5',
      userId: `user-${i}`,
      premium: false,
    }, {
      priority: 5,
      max_attempts: 3,
      backoff: 1000,
    });
    jobIds.push(job.id);
  }

  console.log(`Queued ${jobIds.length} requests (5 premium, 20 standard)\n`);
  console.log('Processing...');

  // Wait for completion
  while (completed < 25) {
    await new Promise(r => setTimeout(r, 100));
  }

  const elapsed = Date.now() - startTime;

  console.log('\n=== Results ===');
  console.log(`Total requests: ${completed}`);
  console.log(`Total tokens: ${totalTokens}`);
  console.log(`Avg latency: ${Math.round(totalLatency / completed)}ms`);
  console.log(`Total time: ${elapsed}ms`);
  console.log(`Throughput: ${(completed / (elapsed / 1000)).toFixed(1)} req/sec`);

  // Check rate limit stats
  const stats = await client.stats();
  console.log('\n=== Queue Stats ===');
  console.log(JSON.stringify(stats, null, 2));

  await worker.close();
  await client.obliterate(QUEUE_NAME);
  await client.close();

  console.log('\n=== Complete ===');
}

main().catch(console.error);
