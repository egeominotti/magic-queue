import { Queue, Worker, Job } from "../src";

const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 5000;
const WORKER_BATCH_SIZE = 50;

// CPU intensive work simulation
function cpuIntensiveWork(data: any): number {
  let result = 0;

  // 1. JSON serialization/deserialization
  const json = JSON.stringify(data);
  const parsed = JSON.parse(json);

  // 2. Math operations
  for (let i = 0; i < 100; i++) {
    result += Math.sqrt(i * parsed.i) * Math.sin(i);
    result = Math.abs(result % 1000000);
  }

  // 3. String operations
  const str = "benchmark_" + data.i.toString().repeat(10);
  result += str.length;

  // 4. Array operations
  const arr = Array.from({ length: 50 }, (_, i) => i * data.i);
  result += arr.reduce((a, b) => a + b, 0);

  return result;
}

async function benchmark() {
  console.log("=== MagicQueue CPU INTENSIVE Benchmark ===\n");
  console.log(`Jobs: ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Push batch size: ${BATCH_SIZE.toLocaleString()}`);
  console.log(`Worker batch size: ${WORKER_BATCH_SIZE}`);
  console.log(`CPU work: JSON + Math + String + Array ops\n`);

  let completed = 0;
  let totalWork = 0;

  // Worker con CPU intensive processing
  const worker = new Worker(
    "benchmark",
    async (job: Job) => {
      totalWork += cpuIntensiveWork(job.data);
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
      batchJobs.push({ data: { i, payload: "test_data_" + i }, priority: 0 });
    }

    await queue.pushBatch(batchJobs);
  }

  const pushEnd = performance.now();
  const pushDuration = (pushEnd - pushStart) / 1000;
  const pushThroughput = TOTAL_JOBS / pushDuration;

  console.log(`Push duration: ${pushDuration.toFixed(2)}s`);
  console.log(`Push throughput: ${pushThroughput.toLocaleString(undefined, { maximumFractionDigits: 0 })} jobs/sec\n`);

  await queue.close();

  // === PROCESSING ===
  console.log("--- Processing (CPU intensive) ---");
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
  console.log(`Work done: ${totalWork.toLocaleString()}`);

  await worker.stop();
  process.exit(0);
}

benchmark().catch(console.error);
