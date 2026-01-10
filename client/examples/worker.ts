import { Worker, Job } from "../src";

interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

const worker = new Worker<EmailJob>(
  "emails",
  async (job: Job<EmailJob>) => {
    console.log(`Sending email to ${job.data.to}...`);
    console.log(`  Subject: ${job.data.subject}`);

    // Simula invio email
    await new Promise((resolve) => setTimeout(resolve, 500));

    console.log(`  Email sent!`);
  },
  { concurrency: 2 }
);

// Gestisci SIGINT per graceful shutdown
process.on("SIGINT", async () => {
  console.log("\nShutting down...");
  await worker.stop();
  process.exit(0);
});

console.log("Starting worker...");
worker.start().catch(console.error);
