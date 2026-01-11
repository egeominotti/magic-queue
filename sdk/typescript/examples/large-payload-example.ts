/**
 * Large Payload Example
 *
 * Demonstrates handling jobs with large JSON payloads (up to 1MB limit).
 * Tests various payload sizes and structures.
 */

import { MagicQueue } from '../src/index';

const HOST = process.env.MQ_HOST || 'localhost';
const PORT = parseInt(process.env.MQ_PORT || '6789');

interface LargeDataPayload {
  id: string;
  timestamp: number;
  size: number;
  data: string;
  metadata: {
    type: string;
    compressed: boolean;
    chunks: number;
  };
}

interface ArrayPayload {
  items: Array<{
    id: number;
    name: string;
    value: number;
    tags: string[];
  }>;
  total: number;
}

interface NestedPayload {
  level1: {
    level2: {
      level3: {
        level4: {
          level5: {
            data: string;
            value: number;
          };
        };
      };
    };
  };
}

async function main() {
  const client = new MagicQueue({ host: HOST, port: PORT });
  await client.connect();

  console.log('Connected to MagicQueue\n');
  console.log('='.repeat(60));
  console.log('   Large Payload Examples');
  console.log('='.repeat(60));

  const queue = 'large-payload-demo';

  // ============================================
  // Example 1: Large String Data (500KB)
  // ============================================
  console.log('\n1. Large String Data (500KB)');
  console.log('-'.repeat(40));

  const largeString = 'x'.repeat(500_000);
  const payload1: LargeDataPayload = {
    id: 'large-string-001',
    timestamp: Date.now(),
    size: largeString.length,
    data: largeString,
    metadata: {
      type: 'raw-string',
      compressed: false,
      chunks: 1
    }
  };

  const job1 = await client.push(queue, payload1);
  console.log(`   Pushed job ${job1.id} with ${payload1.size} bytes`);

  const pulled1 = await client.pull<LargeDataPayload>(queue);
  console.log(`   Pulled job ${pulled1.id}`);
  console.log(`   Data integrity: ${pulled1.data.data.length === 500_000 ? 'OK' : 'FAILED'}`);
  await client.ack(pulled1.id);
  console.log(`   Acknowledged`);

  // ============================================
  // Example 2: Large Array (10,000 items)
  // ============================================
  console.log('\n2. Large Array (10,000 items)');
  console.log('-'.repeat(40));

  const items = Array.from({ length: 10_000 }, (_, i) => ({
    id: i,
    name: `Item ${i}`,
    value: Math.random() * 1000,
    tags: [`tag-${i % 10}`, `category-${i % 5}`]
  }));

  const payload2: ArrayPayload = {
    items,
    total: items.length
  };

  const serializedSize = JSON.stringify(payload2).length;
  console.log(`   Payload size: ${(serializedSize / 1024).toFixed(2)} KB`);

  const job2 = await client.push(queue, payload2);
  console.log(`   Pushed job ${job2.id} with ${items.length} items`);

  const pulled2 = await client.pull<ArrayPayload>(queue);
  console.log(`   Pulled job ${pulled2.id}`);
  console.log(`   Items count: ${pulled2.data.items.length}`);
  console.log(`   First item: ${JSON.stringify(pulled2.data.items[0])}`);
  console.log(`   Last item: ${JSON.stringify(pulled2.data.items[pulled2.data.items.length - 1])}`);
  await client.ack(pulled2.id);
  console.log(`   Acknowledged`);

  // ============================================
  // Example 3: Deeply Nested Structure
  // ============================================
  console.log('\n3. Deeply Nested Structure');
  console.log('-'.repeat(40));

  const deepData = 'Deep nested value - '.repeat(1000);
  const payload3: NestedPayload = {
    level1: {
      level2: {
        level3: {
          level4: {
            level5: {
              data: deepData,
              value: 42
            }
          }
        }
      }
    }
  };

  const job3 = await client.push(queue, payload3);
  console.log(`   Pushed job ${job3.id} with 5 nesting levels`);

  const pulled3 = await client.pull<NestedPayload>(queue);
  console.log(`   Pulled job ${pulled3.id}`);
  console.log(`   Deep value: ${pulled3.data.level1.level2.level3.level4.level5.value}`);
  console.log(`   Deep data length: ${pulled3.data.level1.level2.level3.level4.level5.data.length}`);
  await client.ack(pulled3.id);
  console.log(`   Acknowledged`);

  // ============================================
  // Example 4: Wide Object (1000 keys)
  // ============================================
  console.log('\n4. Wide Object (1000 keys)');
  console.log('-'.repeat(40));

  const wideObject: Record<string, any> = {};
  for (let i = 0; i < 1000; i++) {
    wideObject[`field_${i.toString().padStart(4, '0')}`] = {
      index: i,
      value: `Value for field ${i}`,
      nested: { a: i * 2, b: i * 3 }
    };
  }

  const job4 = await client.push(queue, wideObject);
  console.log(`   Pushed job ${job4.id} with ${Object.keys(wideObject).length} keys`);

  const pulled4 = await client.pull<Record<string, any>>(queue);
  console.log(`   Pulled job ${pulled4.id}`);
  console.log(`   Keys count: ${Object.keys(pulled4.data).length}`);
  console.log(`   Sample key: field_0500 = ${JSON.stringify(pulled4.data.field_0500)}`);
  await client.ack(pulled4.id);
  console.log(`   Acknowledged`);

  // ============================================
  // Example 5: Binary-like Data (Base64)
  // ============================================
  console.log('\n5. Binary-like Data (Base64 encoded, 200KB)');
  console.log('-'.repeat(40));

  // Simulate binary data as base64
  const binarySize = 200_000;
  const binaryData = Buffer.alloc(binarySize).fill(0xAB).toString('base64');

  const payload5 = {
    type: 'binary',
    encoding: 'base64',
    originalSize: binarySize,
    encodedSize: binaryData.length,
    data: binaryData
  };

  const job5 = await client.push(queue, payload5);
  console.log(`   Pushed job ${job5.id}`);
  console.log(`   Original size: ${binarySize} bytes`);
  console.log(`   Encoded size: ${binaryData.length} bytes`);

  const pulled5 = await client.pull<typeof payload5>(queue);
  console.log(`   Pulled job ${pulled5.id}`);

  // Verify data integrity
  const decoded = Buffer.from(pulled5.data.data, 'base64');
  const isValid = decoded.length === binarySize && decoded.every(b => b === 0xAB);
  console.log(`   Data integrity: ${isValid ? 'OK' : 'FAILED'}`);
  await client.ack(pulled5.id);
  console.log(`   Acknowledged`);

  // ============================================
  // Example 6: Maximum Size Test (~900KB)
  // ============================================
  console.log('\n6. Maximum Size Test (~900KB)');
  console.log('-'.repeat(40));

  const maxData = 'M'.repeat(900_000);
  const payload6 = {
    id: 'max-size-test',
    data: maxData,
    size: maxData.length
  };

  try {
    const job6 = await client.push(queue, payload6);
    console.log(`   Pushed job ${job6.id} with ${maxData.length} bytes`);

    const pulled6 = await client.pull<typeof payload6>(queue);
    console.log(`   Pulled job ${pulled6.id}`);
    console.log(`   Data integrity: ${pulled6.data.data.length === 900_000 ? 'OK' : 'FAILED'}`);
    await client.ack(pulled6.id);
    console.log(`   Acknowledged`);
  } catch (error: any) {
    console.log(`   Error: ${error.message}`);
  }

  // ============================================
  // Example 7: Over Limit Test (should fail)
  // ============================================
  console.log('\n7. Over Limit Test (>1MB, should be rejected)');
  console.log('-'.repeat(40));

  const oversizedData = 'X'.repeat(1_100_000); // 1.1MB
  const payload7 = {
    id: 'oversized-test',
    data: oversizedData
  };

  try {
    await client.push(queue, payload7);
    console.log(`   ERROR: Should have been rejected!`);
  } catch (error: any) {
    console.log(`   Correctly rejected: ${error.message}`);
  }

  // ============================================
  // Example 8: Batch with Large Payloads
  // ============================================
  console.log('\n8. Batch Push with Large Payloads');
  console.log('-'.repeat(40));

  const batchJobs = Array.from({ length: 10 }, (_, i) => ({
    data: {
      batchId: i,
      payload: 'B'.repeat(50_000), // 50KB each
      timestamp: Date.now()
    }
  }));

  const totalBatchSize = JSON.stringify(batchJobs).length;
  console.log(`   Total batch size: ${(totalBatchSize / 1024).toFixed(2)} KB`);

  const ids = await client.pushBatch(queue, batchJobs);
  console.log(`   Pushed ${ids.length} jobs: [${ids.slice(0, 3).join(', ')}...]`);

  // Pull and verify all
  const pulledBatch = await client.pullBatch(queue, 10);
  console.log(`   Pulled ${pulledBatch.length} jobs`);

  let allValid = true;
  for (const job of pulledBatch) {
    const data = job.data as { payload: string };
    if (data.payload.length !== 50_000) {
      allValid = false;
      break;
    }
  }
  console.log(`   All payloads valid: ${allValid ? 'YES' : 'NO'}`);

  await client.ackBatch(pulledBatch.map(j => j.id));
  console.log(`   Acknowledged all`);

  // ============================================
  // Summary
  // ============================================
  console.log('\n' + '='.repeat(60));
  console.log('   Summary');
  console.log('='.repeat(60));
  console.log(`
   - Maximum job size: 1MB (1,048,576 bytes)
   - Large strings: Supported up to limit
   - Large arrays: Supported (tested 10,000 items)
   - Deep nesting: Supported (tested 5+ levels)
   - Wide objects: Supported (tested 1,000 keys)
   - Binary data: Use Base64 encoding
   - Batch operations: Supported with large payloads
   - Oversized payloads: Correctly rejected
  `);

  await client.close();
  console.log('Disconnected from MagicQueue');
}

main().catch(console.error);
