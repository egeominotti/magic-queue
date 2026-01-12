/**
 * gRPC Client Example
 *
 * Demonstrates using the gRPC interface for high-performance job operations.
 *
 * Run: bun run examples/14-grpc-client.ts
 *
 * Prerequisites:
 *   - Start server with gRPC enabled: GRPC=1 cargo run --release
 *   - Install: bun add @grpc/grpc-js @grpc/proto-loader
 *
 * Note: gRPC provides better performance than TCP/HTTP for high-throughput scenarios
 *       and supports streaming for real-time job processing.
 */

// @ts-ignore - grpc packages may not be installed
import * as grpc from '@grpc/grpc-js';
// @ts-ignore
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

const GRPC_PORT = 6791;
const PROTO_PATH = path.resolve(__dirname, '../../../server/proto/queue.proto');

interface Job {
  id: string;
  queue: string;
  data: Buffer;
  priority: number;
  createdAt: string;
  runAt: string;
  startedAt: string;
  attempts: number;
  maxAttempts: number;
  backoff: string;
  ttl: string;
  timeout: string;
  uniqueKey?: string;
  dependsOn: string[];
  progress: number;
  progressMsg?: string;
}

interface StatsResponse {
  queued: string;
  processing: string;
  delayed: string;
  dlq: string;
}

async function main() {
  console.log('===================================');
  console.log('gRPC Client Example');
  console.log('===================================\n');

  // Load proto definition
  console.log('Loading proto definition...');
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
  const flashq = protoDescriptor.flashq as any;

  // Create client
  const client = new flashq.QueueService(
    `localhost:${GRPC_PORT}`,
    grpc.credentials.createInsecure()
  );

  console.log(`Connected to gRPC server on port ${GRPC_PORT}\n`);

  // Promisify client methods
  const push = promisify(client.Push.bind(client));
  const pushBatch = promisify(client.PushBatch.bind(client));
  const pull = promisify(client.Pull.bind(client));
  const pullBatch = promisify(client.PullBatch.bind(client));
  const ack = promisify(client.Ack.bind(client));
  const ackBatch = promisify(client.AckBatch.bind(client));
  const fail = promisify(client.Fail.bind(client));
  const getState = promisify(client.GetState.bind(client));
  const stats = promisify(client.Stats.bind(client));

  try {
    // === Basic Operations ===
    console.log('===================================');
    console.log('Basic gRPC Operations');
    console.log('===================================\n');

    // Push a job
    console.log('Pushing a job...');
    const pushResponse = await push({
      queue: 'grpc-queue',
      data: Buffer.from(JSON.stringify({ task: 'grpc-task-1', value: 42 })),
      priority: 10,
      max_attempts: 3,
      backoff_ms: 5000,
    });
    console.log(`  Pushed job ID: ${pushResponse.id}\n`);

    // Get stats
    const statsResponse: StatsResponse = await stats({});
    console.log('Queue Stats:');
    console.log(`  Queued: ${statsResponse.queued}`);
    console.log(`  Processing: ${statsResponse.processing}`);
    console.log(`  Delayed: ${statsResponse.delayed}`);
    console.log(`  DLQ: ${statsResponse.dlq}\n`);

    // Pull the job
    console.log('Pulling job...');
    const job: Job = await pull({ queue: 'grpc-queue' });
    const jobData = JSON.parse(job.data.toString());
    console.log(`  Got job ${job.id}:`);
    console.log(`  Data: ${JSON.stringify(jobData)}`);
    console.log(`  Priority: ${job.priority}\n`);

    // Acknowledge the job
    console.log('Acknowledging job...');
    const ackResponse = await ack({
      id: job.id,
      result: Buffer.from(JSON.stringify({ processed: true })),
    });
    console.log(`  Ack result: ${ackResponse.ok}\n`);

    // === Batch Operations ===
    console.log('===================================');
    console.log('Batch gRPC Operations');
    console.log('===================================\n');

    // Push batch
    console.log('Pushing batch of 10 jobs...');
    const jobs = Array.from({ length: 10 }, (_, i) => ({
      data: Buffer.from(JSON.stringify({ task: `batch-${i}` })),
      priority: i,
    }));

    const batchResponse = await pushBatch({
      queue: 'grpc-batch-queue',
      jobs,
    });
    console.log(`  Pushed ${batchResponse.ids.length} jobs`);
    console.log(`  IDs: ${batchResponse.ids.slice(0, 5).join(', ')}...\n`);

    // Pull batch
    console.log('Pulling batch of 5 jobs...');
    const pullBatchResponse = await pullBatch({
      queue: 'grpc-batch-queue',
      count: 5,
    });
    console.log(`  Pulled ${pullBatchResponse.jobs.length} jobs\n`);

    // Ack batch
    const idsToAck = pullBatchResponse.jobs.map((j: Job) => j.id);
    console.log('Acknowledging batch...');
    const ackBatchResponse = await ackBatch({ ids: idsToAck });
    console.log(`  Acked ${ackBatchResponse.acked} jobs\n`);

    // === Job State ===
    console.log('===================================');
    console.log('Job State Tracking');
    console.log('===================================\n');

    // Push and check state
    const stateJob = await push({
      queue: 'grpc-state-queue',
      data: Buffer.from(JSON.stringify({ test: 'state' })),
    });

    const stateResponse = await getState({ id: stateJob.id });
    console.log(`Job ${stateJob.id} state: ${stateResponse.state}\n`);

    // === Streaming (Server-Side) ===
    console.log('===================================');
    console.log('gRPC Streaming');
    console.log('===================================\n');

    console.log('Streaming jobs from queue...');
    console.log('(Push jobs to grpc-stream-queue to see them here)\n');

    // Push some jobs first
    for (let i = 0; i < 3; i++) {
      await push({
        queue: 'grpc-stream-queue',
        data: Buffer.from(JSON.stringify({ stream: i })),
      });
    }

    // Stream jobs
    const stream = client.StreamJobs({
      queue: 'grpc-stream-queue',
      batch_size: 1,
    });

    let streamCount = 0;
    const maxStreamJobs = 3;

    await new Promise<void>((resolve) => {
      stream.on('data', (job: Job) => {
        const data = JSON.parse(job.data.toString());
        console.log(`  [Stream] Job ${job.id}: ${JSON.stringify(data)}`);
        streamCount++;

        // Ack the job
        client.Ack({ id: job.id }, () => {});

        if (streamCount >= maxStreamJobs) {
          stream.cancel();
          resolve();
        }
      });

      stream.on('error', (err: Error) => {
        if (err.message.includes('CANCELLED')) {
          resolve();
        } else {
          console.error('Stream error:', err.message);
          resolve();
        }
      });

      stream.on('end', resolve);
    });

    console.log('\n===================================');
    console.log('gRPC Performance Notes');
    console.log('===================================\n');

    console.log('gRPC advantages over TCP/HTTP:');
    console.log('  - Binary protocol (protobuf) - smaller payloads');
    console.log('  - HTTP/2 multiplexing - better connection reuse');
    console.log('  - Streaming support - real-time job processing');
    console.log('  - Strong typing via proto definitions');
    console.log('  - Ideal for high-throughput worker processes\n');

    console.log('To enable gRPC server:');
    console.log('  GRPC=1 cargo run --release');
    console.log('  GRPC=1 GRPC_PORT=6791 cargo run --release\n');

  } catch (error) {
    console.error('Error:', error);
  }

  // Close client
  client.close();
  console.log('Done!');
}

function promisify<T>(fn: Function): (...args: any[]) => Promise<T> {
  return (...args: any[]) =>
    new Promise((resolve, reject) => {
      fn(...args, (err: Error | null, response: T) => {
        if (err) reject(err);
        else resolve(response);
      });
    });
}

main().catch(console.error);
