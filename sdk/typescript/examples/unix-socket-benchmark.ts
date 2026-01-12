/**
 * FlashQ: Unix Socket Benchmark
 *
 * Confronta con risultati TCP precedenti:
 * - TCP Single: ~6,000 jobs/sec
 * - TCP Batch: ~666,000 jobs/sec
 * - TCP Pull+Ack: ~185,000 jobs/sec
 */

import { FlashQ } from '../src/index';

const UNIX_SOCKET = '/tmp/flashq.sock';

const CONFIG = {
  BATCH_SIZE: 500,
  BATCHES: 20,
};

async function main() {
  console.log('═'.repeat(60));
  console.log('  Unix Socket Benchmark');
  console.log('═'.repeat(60));
  console.log('\n🔗 Unix Socket (/tmp/flashq.sock)');
  console.log('─'.repeat(50));

  const client = new FlashQ({ socketPath: UNIX_SOCKET });
  await client.connect();
  console.log('  ✓ Connected\n');

  const queue = `unix-bench-${Date.now()}`;

  // Single push
  console.log('  Testing single push (2000 jobs)...');
  let start = Date.now();
  for (let i = 0; i < 2000; i++) {
    await client.push(queue, { i });
  }
  const singlePush = Math.round(2000 / ((Date.now() - start) / 1000));
  console.log(`    Result: ${singlePush.toLocaleString()} jobs/sec\n`);

  // Batch push
  console.log(`  Testing batch push (${CONFIG.BATCHES} x ${CONFIG.BATCH_SIZE} = ${CONFIG.BATCHES * CONFIG.BATCH_SIZE} jobs)...`);
  start = Date.now();
  for (let b = 0; b < CONFIG.BATCHES; b++) {
    const jobs = Array.from({ length: CONFIG.BATCH_SIZE }, (_, i) => ({ data: { b, i } }));
    await client.pushBatch(queue, jobs);
  }
  const totalBatch = CONFIG.BATCHES * CONFIG.BATCH_SIZE;
  const batchPush = Math.round(totalBatch / ((Date.now() - start) / 1000));
  console.log(`    Result: ${batchPush.toLocaleString()} jobs/sec\n`);

  // Pull + ack
  console.log('  Testing pull + ack (5000 jobs)...');
  start = Date.now();
  let processed = 0;
  while (processed < 5000) {
    const jobs = await client.pullBatch(queue, 100);
    if (jobs.length === 0) break;
    await client.ackBatch(jobs.map(j => j.id));
    processed += jobs.length;
  }
  const pullAck = Math.round(processed / ((Date.now() - start) / 1000));
  console.log(`    Result: ${pullAck.toLocaleString()} jobs/sec\n`);

  await client.close();

  // Comparison with TCP
  console.log('═'.repeat(60));
  console.log('  CONFRONTO Unix Socket vs TCP');
  console.log('═'.repeat(60));

  const tcpResults = { single: 6061, batch: 666667, pull: 185185 };

  console.log('\n  Operazione        TCP         Unix Socket   Speedup');
  console.log('  ' + '─'.repeat(55));

  const singleSpeedup = (singlePush / tcpResults.single).toFixed(2);
  const batchSpeedup = (batchPush / tcpResults.batch).toFixed(2);
  const pullSpeedup = (pullAck / tcpResults.pull).toFixed(2);

  console.log(`  Single Push    ${tcpResults.single.toLocaleString().padStart(10)}   ${singlePush.toLocaleString().padStart(10)}     ${singleSpeedup}x`);
  console.log(`  Batch Push     ${tcpResults.batch.toLocaleString().padStart(10)}   ${batchPush.toLocaleString().padStart(10)}     ${batchSpeedup}x`);
  console.log(`  Pull + Ack     ${tcpResults.pull.toLocaleString().padStart(10)}   ${pullAck.toLocaleString().padStart(10)}     ${pullSpeedup}x`);

  console.log('\n  ' + '─'.repeat(55));

  const avgSpeedup = ((parseFloat(singleSpeedup) + parseFloat(batchSpeedup) + parseFloat(pullSpeedup)) / 3).toFixed(2);

  if (parseFloat(avgSpeedup) > 1) {
    console.log(`\n  🏆 Unix Socket è ${avgSpeedup}x più veloce in media!`);
  } else if (parseFloat(avgSpeedup) < 1) {
    console.log(`\n  🏆 TCP è ${(1/parseFloat(avgSpeedup)).toFixed(2)}x più veloce in media!`);
  } else {
    console.log('\n  🤝 Prestazioni simili!');
  }

  console.log('\n' + '═'.repeat(60));
}

main().catch(console.error);
