/**
 * Job State Tracking Example
 *
 * Demonstrates monitoring job lifecycle through all states.
 *
 * Run: npx ts-node examples/11-job-state-tracking.ts
 */

import { MagicQueue, Worker, JobState } from '../src';

async function main() {
  const client = new MagicQueue({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  const QUEUE = 'state-tracking';

  try {
    console.log('═══════════════════════════════════════');
    console.log('📊 JOB STATE LIFECYCLE');
    console.log('═══════════════════════════════════════\n');

    // Create a job with delay to observe delayed state
    console.log('Creating delayed job (2 second delay)...');
    const delayedJob = await client.push(
      QUEUE,
      { type: 'delayed-task' },
      { delay: 2000 }
    );
    console.log(`Job ID: ${delayedJob.id}\n`);

    // Track state changes
    const trackState = async (jobId: number, label: string): Promise<void> => {
      const state = await client.getState(jobId);
      const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
      console.log(`   [${timestamp}] ${label}: ${state}`);
    };

    // Initial state (should be 'delayed')
    await trackState(delayedJob.id, 'After push');

    // Wait for delay to pass
    console.log('\nWaiting for delay (2 seconds)...');
    await sleep(2500);
    await trackState(delayedJob.id, 'After delay');

    // Start worker
    const worker = new Worker(
      QUEUE,
      async (job) => {
        console.log('\n   Worker processing job...');
        await sleep(1000);
        return { completed: true };
      },
      { host: 'localhost', port: 6789 }
    );

    worker.on('active', async (job) => {
      await trackState(job.id, 'Worker active');
    });

    worker.on('completed', async (job) => {
      await trackState(job.id, 'After ack  ');
    });

    await worker.start();
    await sleep(3000);
    await worker.stop();

    // ===== JOB WITH DEPENDENCIES =====
    console.log('\n═══════════════════════════════════════');
    console.log('🔗 DEPENDENCY STATE TRACKING');
    console.log('═══════════════════════════════════════\n');

    // Create parent job
    const parentJob = await client.push(QUEUE, { type: 'parent' });
    console.log(`Parent job: ${parentJob.id}`);

    // Create child job (depends on parent)
    const childJob = await client.push(
      QUEUE,
      { type: 'child' },
      { depends_on: [parentJob.id] }
    );
    console.log(`Child job: ${childJob.id} (depends on ${parentJob.id})\n`);

    // Check initial states
    await trackState(parentJob.id, 'Parent initial');
    await trackState(childJob.id, 'Child initial '); // Should be 'waiting-children'

    // Start worker to process parent
    const depWorker = new Worker(
      QUEUE,
      async () => {
        await sleep(500);
        return { done: true };
      },
      { host: 'localhost', port: 6789 }
    );

    await depWorker.start();
    await sleep(2000);

    // Check states after parent completes
    await trackState(parentJob.id, 'Parent after ');
    await trackState(childJob.id, 'Child after  ');

    await sleep(2000);
    await depWorker.stop();

    // ===== FULL JOB INFO =====
    console.log('\n═══════════════════════════════════════');
    console.log('📋 FULL JOB INFO (getJob)');
    console.log('═══════════════════════════════════════\n');

    // Create a new job to inspect
    const inspectJob = await client.push(QUEUE, {
      type: 'inspection',
      data: { foo: 'bar' },
    });

    const jobInfo = await client.getJob(inspectJob.id);
    if (jobInfo) {
      console.log('Job details:');
      console.log(`   ID: ${jobInfo.job.id}`);
      console.log(`   Queue: ${jobInfo.job.queue}`);
      console.log(`   State: ${jobInfo.state}`);
      console.log(`   Priority: ${jobInfo.job.priority}`);
      console.log(`   Created: ${new Date(jobInfo.job.created_at).toISOString()}`);
      console.log(`   Run at: ${new Date(jobInfo.job.run_at).toISOString()}`);
      console.log(`   Attempts: ${jobInfo.job.attempts}`);
      console.log(`   Data: ${JSON.stringify(jobInfo.job.data)}`);
    }

    // ===== STATE REFERENCE =====
    console.log('\n═══════════════════════════════════════');
    console.log('📖 JOB STATE REFERENCE');
    console.log('═══════════════════════════════════════');
    console.log(`
    State             | Description
    ------------------|------------------------------------
    waiting           | In queue, ready to be processed
    delayed           | Scheduled for future execution
    active            | Currently being processed
    completed         | Successfully finished
    failed            | In DLQ after max attempts
    waiting-children  | Waiting for dependencies
    `);

  } finally {
    await client.close();
    console.log('👋 Disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);
