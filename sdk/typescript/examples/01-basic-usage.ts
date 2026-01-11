/**
 * Basic Usage Example
 *
 * Demonstrates basic push/pull/ack operations.
 *
 * Run: npx ts-node examples/01-basic-usage.ts
 */

import { MagicQueue } from '../src';

async function main() {
  // Create client
  const client = new MagicQueue({
    host: 'localhost',
    port: 6789,
  });

  try {
    // Connect to server
    await client.connect();
    console.log('✅ Connected to MagicQueue');

    // Push a job
    const job = await client.push('example-queue', {
      message: 'Hello, MagicQueue!',
      timestamp: Date.now(),
    });
    console.log(`✅ Pushed job: ${job.id}`);

    // Pull the job
    const pulled = await client.pull('example-queue');
    console.log(`✅ Pulled job: ${pulled.id}`);
    console.log(`   Data: ${JSON.stringify(pulled.data)}`);

    // Acknowledge the job
    await client.ack(pulled.id);
    console.log(`✅ Acknowledged job: ${pulled.id}`);

    // Check stats
    const stats = await client.stats();
    console.log(`\n📊 Queue Stats:`);
    console.log(`   Queued: ${stats.queued}`);
    console.log(`   Processing: ${stats.processing}`);
    console.log(`   Delayed: ${stats.delayed}`);
    console.log(`   DLQ: ${stats.dlq}`);

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

main().catch(console.error);
