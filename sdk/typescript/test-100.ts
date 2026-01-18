import { FlashQ } from "./src";

const client = new FlashQ({ host: "localhost", port: 6789, httpPort: 6790, useHttp: true });

async function main() {
  await client.connect();
  console.log("Connected to flashQ (HTTP mode)");

  const queue = "test-100-jobs";

  // Push 100 jobs one by one via HTTP
  console.log("\n📤 Pushing 100 jobs...");
  const start = Date.now();

  for (let i = 0; i < 100; i++) {
    await client.push(queue, { task: `job-${i}`, value: Math.random() }, { priority: Math.floor(Math.random() * 10) });
    if ((i + 1) % 20 === 0) {
      console.log(`   Pushed ${i + 1}/100`);
    }
  }
  console.log(`✅ Pushed 100 jobs in ${Date.now() - start}ms`);

  // Process all jobs via HTTP
  console.log("\n⚙️  Processing jobs...");
  const processStart = Date.now();
  let processed = 0;

  while (processed < 100) {
    const job = await client.pull(queue);
    if (job) {
      await client.ack(job.id);
      processed++;
      if (processed % 20 === 0) {
        console.log(`   Processed ${processed}/100`);
      }
    }
  }

  console.log(`✅ Processed 100 jobs in ${Date.now() - processStart}ms`);
  console.log(`\n📊 Total time: ${Date.now() - start}ms`);

  // Check stats
  const stats = await client.stats();
  console.log("\n📈 Stats:", JSON.stringify(stats, null, 2));

  await client.close();
}

main().catch(console.error);
