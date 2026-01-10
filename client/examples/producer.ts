import { Queue } from "../src";

const queue = new Queue("emails");

async function main() {
  console.log("Connecting to MagicQueue server...");
  await queue.connect();
  console.log("Connected!");

  // Invia alcuni job
  for (let i = 1; i <= 5; i++) {
    const jobId = await queue.push({
      to: `user${i}@example.com`,
      subject: `Test email ${i}`,
      body: `This is test email number ${i}`,
    });
    console.log(`Pushed job ${jobId}`);
  }

  await queue.close();
  console.log("Done!");
}

main().catch(console.error);
