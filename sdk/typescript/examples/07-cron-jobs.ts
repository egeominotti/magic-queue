/**
 * Cron Jobs Example
 *
 * Demonstrates scheduled recurring jobs with full cron expression support.
 *
 * Run: npx ts-node examples/07-cron-jobs.ts
 */

import { MagicQueue, Worker } from '../src';

async function main() {
  const client = new MagicQueue({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  try {
    // List existing crons
    console.log('📋 Current cron jobs:');
    let crons = await client.listCrons();
    if (crons.length === 0) {
      console.log('   (none)\n');
    } else {
      crons.forEach((c) => {
        console.log(`   - ${c.name}: ${c.schedule} → ${c.queue}`);
      });
      console.log('');
    }

    // Create cron jobs with different schedules
    console.log('➕ Creating cron jobs...\n');

    // 1. Simple interval (every N seconds)
    await client.addCron('heartbeat', {
      queue: 'cron-queue',
      data: { type: 'heartbeat', message: 'ping' },
      schedule: '*/5', // Every 5 seconds
    });
    console.log('   ✅ heartbeat: every 5 seconds (*/5)');

    // 2. Full 6-field cron (sec min hour day month weekday)
    await client.addCron('every-minute', {
      queue: 'cron-queue',
      data: { type: 'minute-task' },
      schedule: '0 * * * * *', // At second 0 of every minute
    });
    console.log('   ✅ every-minute: at second 0 of every minute');

    // 3. Hourly task
    await client.addCron('hourly-report', {
      queue: 'cron-queue',
      data: { type: 'report', period: 'hourly' },
      schedule: '0 0 * * * *', // At minute 0 of every hour
      priority: 10,
    });
    console.log('   ✅ hourly-report: at minute 0 of every hour (priority: 10)');

    // 4. Daily task
    await client.addCron('daily-cleanup', {
      queue: 'cron-queue',
      data: { type: 'cleanup', period: 'daily' },
      schedule: '0 0 3 * * *', // Every day at 3:00 AM
    });
    console.log('   ✅ daily-cleanup: every day at 3:00 AM');

    // 5. Weekday task
    await client.addCron('weekday-report', {
      queue: 'cron-queue',
      data: { type: 'report', period: 'weekday' },
      schedule: '0 30 9 * * 1-5', // Mon-Fri at 9:30 AM
    });
    console.log('   ✅ weekday-report: Mon-Fri at 9:30 AM');

    // 6. Weekend task
    await client.addCron('weekend-backup', {
      queue: 'cron-queue',
      data: { type: 'backup', period: 'weekend' },
      schedule: '0 0 2 * * 0,6', // Sat & Sun at 2:00 AM
    });
    console.log('   ✅ weekend-backup: Sat & Sun at 2:00 AM');

    // 7. Monthly task
    await client.addCron('monthly-invoice', {
      queue: 'cron-queue',
      data: { type: 'invoice', period: 'monthly' },
      schedule: '0 0 9 1 * *', // 1st of every month at 9:00 AM
    });
    console.log('   ✅ monthly-invoice: 1st of every month at 9:00 AM\n');

    // List all crons
    console.log('📋 All cron jobs:');
    crons = await client.listCrons();
    crons.forEach((c) => {
      const nextRun = new Date(c.next_run).toISOString();
      console.log(`   ${c.name}:`);
      console.log(`      Schedule: ${c.schedule}`);
      console.log(`      Queue: ${c.queue}`);
      console.log(`      Priority: ${c.priority}`);
      console.log(`      Next run: ${nextRun}`);
      console.log('');
    });

    // Create a worker to process cron jobs
    console.log('🚀 Starting worker to process cron jobs...');
    console.log('   (Will run for 15 seconds)\n');

    const worker = new Worker(
      'cron-queue',
      async (job) => {
        const data = job.data as { type: string; message?: string };
        console.log(`   ⚡ Cron job executed: ${data.type}`);
        return { executed: true, timestamp: new Date().toISOString() };
      },
      { host: 'localhost', port: 6789 }
    );

    await worker.start();

    // Run for 15 seconds
    await sleep(15000);
    await worker.stop();

    // Cleanup: delete all cron jobs
    console.log('\n🧹 Cleaning up cron jobs...');
    for (const cron of crons) {
      await client.deleteCron(cron.name);
      console.log(`   Deleted: ${cron.name}`);
    }

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);
