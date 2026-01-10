import { Queue, Worker, Job } from "../src";

const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 5000;
const WORKER_BATCH_SIZE = 50;

async function benchmark() {
  console.log("=== MagicQueue ULTRA Benchmark ===\n");
  console.log(`Jobs: ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Push batch size: ${BATCH_SIZE.toLocaleString()}`);
  console.log(`Worker batch size: ${WORKER_BATCH_SIZE}\n`);

  let completed = 0;

  // Worker con batch processing
  const worker = new Worker(
    "benchmark",
    async (_job: Job) => {
      completed++;
    },
    { batchSize: WORKER_BATCH_SIZE }
  );

  const workerPromise = worker.start();
  await new Promise((r) => setTimeout(r, 300));

  // === BATCH PUSH ===
  const queue = new Queue("benchmark");
  await queue.connect();

  console.log("--- Batch Push ---");
  const pushStart = performance.now();

  const batches = Math.ceil(TOTAL_JOBS / BATCH_SIZE);
  for (let b = 0; b < batches; b++) {
    const batchJobs = [];
    const start = b * BATCH_SIZE;
    const end = Math.min(start + BATCH_SIZE, TOTAL_JOBS);

    for (let i = start; i < end; i++) {
      batchJobs.push({ data: { i }, priority: 0 });
    }

    await queue.pushBatch(batchJobs);

    if ((b + 1) % 10 === 0) {
      process.stdout.write(`\rPushed: ${((b + 1) * BATCH_SIZE).toLocaleString()}`);
    }
  }

  const pushEnd = performance.now();
  const pushDuration = (pushEnd - pushStart) / 1000;
  const pushThroughput = TOTAL_JOBS / pushDuration;

  console.log(`\rPush duration: ${pushDuration.toFixed(2)}s                    `);
  console.log(`Push throughput: ${pushThroughput.toLocaleString(undefined, { maximumFractionDigits: 0 })} jobs/sec\n`);

  await queue.close();

  // === PROCESSING ===
  console.log("--- Processing (batch pull + batch ack) ---");
  const processingStart = performance.now();

  const progressInterval = setInterval(() => {
    const pct = ((completed / TOTAL_JOBS) * 100).toFixed(1);
    const elapsed = (performance.now() - processingStart) / 1000;
    const rate = completed / elapsed;
    process.stdout.write(`\rProcessed: ${completed.toLocaleString()} (${pct}%) - ${rate.toLocaleString(undefined, { maximumFractionDigits: 0 })}/s`);
  }, 500);

  while (completed < TOTAL_JOBS) {
    await new Promise((r) => setTimeout(r, 100));
  }

  clearInterval(progressInterval);

  const processEnd = performance.now();
  const processDuration = (processEnd - processingStart) / 1000;
  const processThroughput = TOTAL_JOBS / processDuration;

  console.log(`\rProcess duration: ${processDuration.toFixed(2)}s                                        `);
  console.log(`Process throughput: ${processThroughput.toLocaleString(undefined, { maximumFractionDigits: 0 })} jobs/sec\n`);

  // === TOTALE ===
  const totalDuration = (processEnd - pushStart) / 1000;
  console.log("=== RESULTS ===");
  console.log(`Total: ${totalDuration.toFixed(2)}s`);
  console.log(`Throughput: ${(TOTAL_JOBS / totalDuration).toLocaleString(undefined, { maximumFractionDigits: 0 })} jobs/sec`);

  await worker.stop();
  process.exit(0);
}

benchmark().catch(console.error);
