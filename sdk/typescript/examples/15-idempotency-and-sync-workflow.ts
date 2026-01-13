/**
 * Idempotency and Synchronous Workflow Example
 *
 * This example demonstrates:
 * 1. Custom Job IDs for idempotent job creation
 * 2. The finished() method for synchronous workflows
 * 3. Lookup jobs by custom ID
 * 4. Combining both patterns for robust order processing
 *
 * Run: bun run examples/15-idempotency-and-sync-workflow.ts
 */

import { FlashQ } from '../src';

// Simulated order data
interface Order {
  orderId: string;
  customerId: string;
  items: Array<{ sku: string; quantity: number; price: number }>;
  total: number;
}

// Simulated processing result
interface ProcessingResult {
  status: 'completed' | 'failed';
  trackingNumber?: string;
  shippedAt?: number;
  error?: string;
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('Connected to FlashQ\n');

  // Clean up from previous runs
  await client.obliterate('order-processing');
  await client.obliterate('slow-queue');

  // ============================================================
  // SECTION 1: Idempotent Job Creation
  // ============================================================
  console.log('='.repeat(60));
  console.log('SECTION 1: Idempotent Job Creation');
  console.log('='.repeat(60));
  console.log('\nProblem: Duplicate order submissions from retries or UI bugs');
  console.log('Solution: Use custom jobId based on order ID\n');

  const order: Order = {
    orderId: 'ORD-2024-001234',
    customerId: 'CUST-5678',
    items: [
      { sku: 'WIDGET-A', quantity: 2, price: 29.99 },
      { sku: 'GADGET-B', quantity: 1, price: 49.99 },
    ],
    total: 109.97,
  };

  // First submission - creates the job
  console.log('1. First order submission...');
  const job1 = await client.push<Order>(
    'order-processing',
    order,
    { jobId: `order-${order.orderId}` } // Custom ID based on order ID
  );
  console.log(`   Created job: ID=${job1.id}, customId=${job1.custom_id}`);

  // Simulate: User clicks submit again (duplicate)
  console.log('\n2. User clicks submit again (duplicate attempt)...');
  const job2 = await client.push<Order>(
    'order-processing',
    order,
    { jobId: `order-${order.orderId}` }
  );
  console.log(`   Returned existing job: ID=${job2.id}`);
  console.log(`   Same job? ${job1.id === job2.id ? 'YES (idempotent!)' : 'NO (bug!)'}`);

  // Simulate: Webhook retry hits our endpoint
  console.log('\n3. Webhook retry creates another duplicate...');
  const job3 = await client.push<Order>(
    'order-processing',
    order,
    { jobId: `order-${order.orderId}` }
  );
  console.log(`   Still same job: ID=${job3.id}`);

  // ============================================================
  // SECTION 2: Lookup by Custom ID
  // ============================================================
  console.log('\n' + '='.repeat(60));
  console.log('SECTION 2: Lookup by Custom ID');
  console.log('='.repeat(60));
  console.log('\nUse case: Customer asks "What is the status of my order?"\n');

  console.log(`Looking up order ${order.orderId}...`);
  const found = await client.getJobByCustomId(`order-${order.orderId}`);

  if (found) {
    console.log(`   Found! Internal ID: ${found.job.id}`);
    console.log(`   State: ${found.state}`);
    console.log(`   Created: ${new Date(found.job.created_at).toISOString()}`);
  } else {
    console.log('   Not found!');
  }

  // Non-existent order
  console.log('\nLooking up non-existent order ORD-9999...');
  const notFound = await client.getJobByCustomId('order-ORD-9999');
  console.log(`   Result: ${notFound === null ? 'null (as expected)' : 'unexpected!'}`);

  // ============================================================
  // SECTION 3: Synchronous Workflow with finished()
  // ============================================================
  console.log('\n' + '='.repeat(60));
  console.log('SECTION 3: Synchronous Workflow with finished()');
  console.log('='.repeat(60));
  console.log('\nUse case: API endpoint that waits for job completion\n');

  // Use a separate client for the worker to avoid TCP response interleaving
  const workerClient = new FlashQ({ host: 'localhost', port: 6789 });
  await workerClient.connect();

  // Clean up previous job
  const pulled1 = await workerClient.pull('order-processing');
  await workerClient.ack(pulled1.id);

  // Push a new job
  const syncOrder: Order = {
    orderId: 'ORD-2024-SYNC',
    customerId: 'CUST-9999',
    items: [{ sku: 'INSTANT-ITEM', quantity: 1, price: 9.99 }],
    total: 9.99,
  };

  console.log('1. Push job and start waiting for completion...');
  const syncJob = await client.push<Order>(
    'order-processing',
    syncOrder,
    { jobId: `order-${syncOrder.orderId}` }
  );
  console.log(`   Job ${syncJob.id} created`);

  // Start waiting in the background
  const waitPromise = client.finished<ProcessingResult>(syncJob.id, 10000);
  console.log('   Started waiting for completion (10s timeout)...');

  // Small delay to ensure WAITJOB is sent first
  await sleep(50);

  // Simulate worker processing in parallel (on separate connection)
  console.log('\n2. Simulating worker processing...');
  const workerJob = await workerClient.pull<Order>('order-processing');
  console.log(`   Worker picked up job ${workerJob.id}`);

  await sleep(500); // Simulate processing time
  console.log('   Processing order...');

  await sleep(500);
  console.log('   Generating tracking number...');

  const result: ProcessingResult = {
    status: 'completed',
    trackingNumber: 'TRACK-' + Math.random().toString(36).substring(7).toUpperCase(),
    shippedAt: Date.now(),
  };

  await workerClient.ack(workerJob.id, result);
  console.log(`   Worker completed with result: ${JSON.stringify(result)}`);

  // Now the wait resolves
  console.log('\n3. Waiting for finished() to resolve...');
  const finalResult = await waitPromise;
  console.log(`   Got result: ${JSON.stringify(finalResult)}`);

  await workerClient.close();

  // ============================================================
  // SECTION 4: Timeout Handling
  // ============================================================
  console.log('\n' + '='.repeat(60));
  console.log('SECTION 4: Timeout Handling');
  console.log('='.repeat(60));
  console.log('\nUse case: Don\'t wait forever if processing is slow\n');

  const slowJob = await client.push('slow-queue', { task: 'slow' });
  console.log(`1. Created slow job ${slowJob.id}`);
  console.log('2. Waiting with 500ms timeout (job won\'t complete in time)...');

  try {
    await client.finished(slowJob.id, 500);
    console.log('   ERROR: Should have timed out!');
  } catch (e: any) {
    console.log(`   Caught timeout: ${e.message}`);
  }

  // Clean up
  const slow = await client.pull('slow-queue');
  await client.ack(slow.id);

  // ============================================================
  // SECTION 5: Real-World Pattern - API Endpoint
  // ============================================================
  console.log('\n' + '='.repeat(60));
  console.log('SECTION 5: Real-World Pattern - API Endpoint');
  console.log('='.repeat(60));
  console.log('\nComplete pattern for order creation endpoint:\n');

  async function createOrder(order: Order): Promise<{ jobId: number; result?: ProcessingResult }> {
    const customId = `order-${order.orderId}`;

    // Check if order already exists
    const existing = await client.getJobByCustomId(customId);
    if (existing) {
      console.log(`   Order ${order.orderId} already submitted (job ${existing.job.id})`);

      if (existing.state === 'completed') {
        // Already done - get result
        const result = await client.getResult<ProcessingResult>(existing.job.id);
        return { jobId: existing.job.id, result: result ?? undefined };
      } else if (existing.state === 'active') {
        // Being processed - wait for it
        console.log('   Order is being processed, waiting...');
        const result = await client.finished<ProcessingResult>(existing.job.id, 30000);
        return { jobId: existing.job.id, result: result ?? undefined };
      } else {
        // Waiting/delayed - return job ID, don't wait
        return { jobId: existing.job.id };
      }
    }

    // New order - create job
    const job = await client.push<Order>('order-processing', order, {
      jobId: customId,
      keepCompletedAge: 86400000, // Keep result for 24 hours
    });
    console.log(`   Created new order job ${job.id}`);

    // Optional: Wait for processing
    // const result = await client.finished<ProcessingResult>(job.id, 30000);
    // return { jobId: job.id, result };

    return { jobId: job.id };
  }

  // Simulate API calls
  const testOrder: Order = {
    orderId: 'ORD-API-TEST',
    customerId: 'API-CUST',
    items: [{ sku: 'TEST', quantity: 1, price: 1.0 }],
    total: 1.0,
  };

  console.log('Call 1: First request');
  const result1 = await createOrder(testOrder);
  console.log(`   Result: jobId=${result1.jobId}\n`);

  console.log('Call 2: Duplicate request (idempotent)');
  const result2 = await createOrder(testOrder);
  console.log(`   Result: jobId=${result2.jobId}`);
  console.log(`   Same job? ${result1.jobId === result2.jobId ? 'YES' : 'NO'}\n`);

  // Process the job
  const apiJob = await client.pull('order-processing');
  await client.ack(apiJob.id, { status: 'completed', trackingNumber: 'API-TRACK-123' });

  console.log('Call 3: Request after completion');
  const result3 = await createOrder(testOrder);
  console.log(`   Result: jobId=${result3.jobId}, result=${JSON.stringify(result3.result)}`);

  // ============================================================
  // Cleanup
  // ============================================================
  console.log('\n' + '='.repeat(60));
  console.log('CLEANUP');
  console.log('='.repeat(60));

  await client.obliterate('order-processing');
  await client.obliterate('slow-queue');
  console.log('\nQueues cleaned up');

  await client.close();
  console.log('Disconnected\n');
}

main().catch(console.error);
