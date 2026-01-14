import { FlashQ } from '../src';

const client = new FlashQ({ host: 'localhost', port: 6789 });
await client.connect();

// Reset server
await fetch('http://localhost:6790/server/reset', { method: 'POST' });

console.log('Pushing 10,000 jobs...');
const start = Date.now();

for (let i = 0; i < 10; i++) {
  const jobs = [];
  for (let j = 0; j < 1000; j++) {
    jobs.push({ data: { id: i * 1000 + j } });
  }
  await client.pushBatch('test', jobs);
}

console.log(`Done in ${Date.now() - start}ms`);
await client.close();
