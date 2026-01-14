import { Worker } from '../src';

let count = 0;
const start = Date.now();

const worker = new Worker(
  'test',
  async () => {
    console.log('hello');
    count++;
    if (count === 10000) {
      console.log(`\nDone! Processed ${count} jobs in ${Date.now() - start}ms`);
      process.exit(0);
    }
    return { ok: true };
  },
  {
    id: 'worker-1',
    host: 'localhost',
    port: 6789,
    concurrency: 100,
    // heartbeatInterval: 500 (default)
  }
);

await worker.start();
console.log('Worker started, waiting for jobs...\n');
