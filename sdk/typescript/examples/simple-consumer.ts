import { Worker } from '../src';

let count = 0;
const start = Date.now();
const TARGET = 10000;

const worker = new Worker(
  'test',
  async () => {
    console.log('hello');
    return { ok: true };
  },
  {
    id: 'worker-1',
    host: 'localhost',
    port: 6789,
    concurrency: 100,
  }
);

// Count AFTER ACK completes (via completed event)
worker.on('completed', () => {
  count++;
  if (count === TARGET) {
    console.log(`\nDone! Processed ${count} jobs in ${Date.now() - start}ms`);
    worker.stop().then(() => process.exit(0));
  }
});

await worker.start();
console.log('Worker started, waiting for jobs...\n');
