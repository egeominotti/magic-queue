/**
 * Worker Example
 *
 * Demonstrates how to create a worker that processes jobs.
 *
 * Run: npx ts-node examples/02-worker.ts
 */

import { MagicQueue, Worker } from '../src';

interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

interface EmailResult {
  sent: boolean;
  sentAt: string;
}

async function main() {
  // First, push some jobs
  const client = new MagicQueue({ host: 'localhost', port: 6789 });
  await client.connect();

  console.log('📤 Pushing 5 email jobs...');
  for (let i = 1; i <= 5; i++) {
    await client.push<EmailJob>('emails', {
      to: `user${i}@example.com`,
      subject: `Test Email ${i}`,
      body: `This is test email number ${i}`,
    });
  }
  await client.close();

  // Create worker
  const worker = new Worker<EmailJob, EmailResult>(
    'emails',
    async (job) => {
      console.log(`\n📧 Processing email to: ${job.data.to}`);
      console.log(`   Subject: ${job.data.subject}`);

      // Simulate email sending
      await sleep(500);

      console.log(`   ✅ Email sent!`);

      return {
        sent: true,
        sentAt: new Date().toISOString(),
      };
    },
    {
      concurrency: 2, // Process 2 jobs at a time
      host: 'localhost',
      port: 6789,
    }
  );

  // Event handlers
  worker.on('ready', () => {
    console.log('\n🚀 Worker ready, waiting for jobs...');
  });

  worker.on('active', (job) => {
    console.log(`⚡ Job ${job.id} started`);
  });

  worker.on('completed', (job, result) => {
    console.log(`✅ Job ${job.id} completed:`, result);
  });

  worker.on('failed', (job, error) => {
    console.log(`❌ Job ${job.id} failed:`, error);
  });

  worker.on('error', (error) => {
    console.error('Worker error:', error);
  });

  // Start worker
  await worker.start();

  // Run for 10 seconds then stop
  setTimeout(async () => {
    console.log('\n⏹️  Stopping worker...');
    await worker.stop();
    console.log('👋 Worker stopped');
    process.exit(0);
  }, 10000);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);
