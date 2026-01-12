/**
 * MagicQueue Protocol Comparison Benchmark
 *
 * Confronta: TCP vs HTTP vs gRPC
 */

import { MagicQueue } from '../src/index';

const HOST = process.env.MQ_HOST || 'localhost';
const TCP_PORT = parseInt(process.env.MQ_PORT || '6789');
const HTTP_PORT = 6790;

const CONFIG = {
  JOBS: 10_000,
  BATCH_SIZE: 500,
  BATCHES: 20,
};

interface BenchResult {
  protocol: string;
  singlePush: number;
  batchPush: number;
  pullAck: number;
}

async function benchmarkTCP(): Promise<BenchResult> {
  console.log('\n🔌 TCP Protocol (porta 6789)');
  console.log('─'.repeat(40));

  const client = new MagicQueue({ host: HOST, port: TCP_PORT });
  await client.connect();
  const queue = `tcp-bench-${Date.now()}`;

  // Single push
  let start = Date.now();
  for (let i = 0; i < 1000; i++) {
    await client.push(queue, { i });
  }
  const singlePush = Math.round(1000 / ((Date.now() - start) / 1000));
  console.log(`  Single push: ${singlePush.toLocaleString()} jobs/sec`);

  // Batch push
  start = Date.now();
  for (let b = 0; b < CONFIG.BATCHES; b++) {
    const jobs = Array.from({ length: CONFIG.BATCH_SIZE }, (_, i) => ({ data: { b, i } }));
    await client.pushBatch(queue, jobs);
  }
  const batchPush = Math.round((CONFIG.BATCHES * CONFIG.BATCH_SIZE) / ((Date.now() - start) / 1000));
  console.log(`  Batch push:  ${batchPush.toLocaleString()} jobs/sec`);

  // Pull + ack
  start = Date.now();
  let processed = 0;
  while (processed < 5000) {
    const jobs = await client.pullBatch(queue, 100);
    if (jobs.length === 0) break;
    await client.ackBatch(jobs.map(j => j.id));
    processed += jobs.length;
  }
  const pullAck = Math.round(processed / ((Date.now() - start) / 1000));
  console.log(`  Pull + ack:  ${pullAck.toLocaleString()} jobs/sec`);

  await client.close();
  return { protocol: 'TCP', singlePush, batchPush, pullAck };
}

async function benchmarkHTTP(): Promise<BenchResult> {
  console.log('\n🌐 HTTP Protocol (porta 6790)');
  console.log('─'.repeat(40));

  const client = new MagicQueue({ host: HOST, port: TCP_PORT, httpPort: HTTP_PORT, useHttp: true });
  await client.connect();
  const queue = `http-bench-${Date.now()}`;

  // Single push
  let start = Date.now();
  for (let i = 0; i < 1000; i++) {
    await client.push(queue, { i });
  }
  const singlePush = Math.round(1000 / ((Date.now() - start) / 1000));
  console.log(`  Single push: ${singlePush.toLocaleString()} jobs/sec`);

  // Batch push via parallel single (HTTP SDK non supporta batch)
  start = Date.now();
  const batchPromises: Promise<any>[] = [];
  for (let i = 0; i < 2000; i++) {
    batchPromises.push(client.push(queue, { i }));
  }
  await Promise.all(batchPromises);
  const batchPush = Math.round(2000 / ((Date.now() - start) / 1000));
  console.log(`  Parallel:    ${batchPush.toLocaleString()} jobs/sec`);

  // Pull + ack (single)
  start = Date.now();
  let processed = 0;
  while (processed < 1000) {
    const job = await client.pull(queue);
    if (!job?.id) break;
    await client.ack(job.id);
    processed++;
  }
  const pullAck = Math.round(processed / ((Date.now() - start) / 1000));
  console.log(`  Pull + ack:  ${pullAck.toLocaleString()} jobs/sec`);

  await client.close();
  return { protocol: 'HTTP', singlePush, batchPush, pullAck };
}

async function benchmarkHTTPParallel(): Promise<BenchResult> {
  console.log('\n🚀 HTTP Parallel (10 connessioni)');
  console.log('─'.repeat(40));

  const queue = `http-parallel-${Date.now()}`;

  // Single push parallel (10 connections x 200 jobs each)
  let start = Date.now();
  const singlePromises = Array.from({ length: 10 }, async () => {
    const client = new MagicQueue({ host: HOST, httpPort: HTTP_PORT, useHttp: true });
    await client.connect();
    const pushes = Array.from({ length: 200 }, (_, i) => client.push(queue, { i }));
    await Promise.all(pushes);
    await client.close();
  });
  await Promise.all(singlePromises);
  const singlePush = Math.round(2000 / ((Date.now() - start) / 1000));
  console.log(`  Parallel:    ${singlePush.toLocaleString()} jobs/sec`);

  // More parallel pushes (simulate batch)
  start = Date.now();
  const batchPromises = Array.from({ length: 10 }, async () => {
    const client = new MagicQueue({ host: HOST, httpPort: HTTP_PORT, useHttp: true });
    await client.connect();
    const pushes = Array.from({ length: 500 }, (_, i) => client.push(queue, { i }));
    await Promise.all(pushes);
    await client.close();
  });
  await Promise.all(batchPromises);
  const batchPush = Math.round(5000 / ((Date.now() - start) / 1000));
  console.log(`  Mass push:   ${batchPush.toLocaleString()} jobs/sec`);

  // Pull + ack parallel
  start = Date.now();
  let totalProcessed = 0;
  const pullPromises = Array.from({ length: 10 }, async () => {
    const client = new MagicQueue({ host: HOST, httpPort: HTTP_PORT, useHttp: true });
    await client.connect();
    let processed = 0;
    while (totalProcessed < 2000 && processed < 200) {
      const job = await client.pull(queue);
      if (!job?.id) break;
      await client.ack(job.id);
      processed++;
      totalProcessed++;
    }
    await client.close();
    return processed;
  });
  await Promise.all(pullPromises);
  const pullAck = Math.round(totalProcessed / ((Date.now() - start) / 1000));
  console.log(`  Pull + ack:  ${pullAck.toLocaleString()} jobs/sec`);

  return { protocol: 'HTTP Parallel', singlePush, batchPush, pullAck };
}

async function benchmarkRawFetch(): Promise<BenchResult> {
  console.log('\n⚡ Raw HTTP Fetch (no SDK overhead)');
  console.log('─'.repeat(40));

  const baseUrl = `http://${HOST}:${HTTP_PORT}`;
  const queue = `raw-bench-${Date.now()}`;

  // Single push
  let start = Date.now();
  for (let i = 0; i < 1000; i++) {
    await fetch(`${baseUrl}/queues/${queue}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: { i } })
    });
  }
  const singlePush = Math.round(1000 / ((Date.now() - start) / 1000));
  console.log(`  Single push: ${singlePush.toLocaleString()} jobs/sec`);

  // Batch push (simulate with multiple parallel requests)
  start = Date.now();
  const batchRequests = Array.from({ length: CONFIG.BATCHES }, (_, b) =>
    fetch(`${baseUrl}/queues/${queue}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: { batch: b, items: CONFIG.BATCH_SIZE } })
    })
  );
  await Promise.all(batchRequests);
  // Note: questo non è vero batch, ma parallel single push
  const batchPush = Math.round(CONFIG.BATCHES / ((Date.now() - start) / 1000));
  console.log(`  Parallel req: ${batchPush.toLocaleString()} req/sec`);

  // Pull
  start = Date.now();
  let processed = 0;
  while (processed < 1000) {
    const res = await fetch(`${baseUrl}/queues/${queue}/jobs?count=100`);
    const data = await res.json() as any;
    if (!data.ok || !data.data?.length) break;
    for (const job of data.data) {
      await fetch(`${baseUrl}/jobs/${job.id}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
    }
    processed += data.data.length;
  }
  const pullAck = Math.round(processed / ((Date.now() - start) / 1000));
  console.log(`  Pull + ack:  ${pullAck.toLocaleString()} jobs/sec`);

  return { protocol: 'Raw Fetch', singlePush, batchPush, pullAck };
}

async function main() {
  console.log('═'.repeat(60));
  console.log('  MagicQueue Protocol Comparison');
  console.log('═'.repeat(60));

  const results: BenchResult[] = [];

  results.push(await benchmarkTCP());
  results.push(await benchmarkHTTP());
  results.push(await benchmarkHTTPParallel());
  results.push(await benchmarkRawFetch());

  // Summary table
  console.log('\n' + '═'.repeat(60));
  console.log('  RISULTATI COMPARATIVI');
  console.log('═'.repeat(60));
  console.log('\n  Protocollo         Single     Batch      Pull+Ack');
  console.log('  ' + '─'.repeat(55));

  for (const r of results) {
    console.log(`  ${r.protocol.padEnd(18)} ${r.singlePush.toLocaleString().padStart(8)}   ${r.batchPush.toLocaleString().padStart(8)}   ${r.pullAck.toLocaleString().padStart(8)}`);
  }

  // Winner
  const batchWinner = results.reduce((a, b) => a.batchPush > b.batchPush ? a : b);
  const pullWinner = results.reduce((a, b) => a.pullAck > b.pullAck ? a : b);

  console.log('\n  🏆 Batch Push Winner: ' + batchWinner.protocol);
  console.log('  🏆 Pull+Ack Winner:  ' + pullWinner.protocol);

  console.log('\n' + '═'.repeat(60));
}

main().catch(console.error);
