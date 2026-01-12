/**
 * Real-World Example: Email Queue System
 *
 * A complete example showing how to build a production-ready
 * email queue with templates, retries, and monitoring.
 *
 * Run: npx ts-node examples/12-real-world-email-queue.ts
 */

import { FlashQ, Worker } from '../src';

// ============== Types ==============

interface EmailJob {
  template: 'welcome' | 'password-reset' | 'order-confirmation' | 'newsletter';
  to: string;
  subject: string;
  variables: Record<string, string>;
  attachments?: string[];
}

interface EmailResult {
  messageId: string;
  sentAt: string;
  provider: string;
}

// ============== Email Templates ==============

const EMAIL_TEMPLATES: Record<string, string> = {
  welcome: `
    Hello {{name}},

    Welcome to our service! We're excited to have you.

    Best regards,
    The Team
  `,
  'password-reset': `
    Hi {{name}},

    Click here to reset your password: {{resetLink}}

    This link expires in 1 hour.
  `,
  'order-confirmation': `
    Thank you for your order #{{orderId}}!

    Total: {{total}}
    Shipping to: {{address}}

    Track your order: {{trackingLink}}
  `,
  newsletter: `
    {{content}}

    Unsubscribe: {{unsubscribeLink}}
  `,
};

// ============== Mock Email Provider ==============

async function sendEmail(email: EmailJob): Promise<EmailResult> {
  // Simulate email sending with occasional failures
  await sleep(Math.random() * 500 + 200);

  if (Math.random() < 0.1) {
    throw new Error('SMTP connection failed');
  }

  // Render template
  let body = EMAIL_TEMPLATES[email.template] || '';
  for (const [key, value] of Object.entries(email.variables)) {
    body = body.replace(new RegExp(`{{${key}}}`, 'g'), value);
  }

  console.log(`      📧 Sending to: ${email.to}`);
  console.log(`      Subject: ${email.subject}`);

  return {
    messageId: `msg-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    sentAt: new Date().toISOString(),
    provider: 'mock-smtp',
  };
}

// ============== Main ==============

async function main() {
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected to FlashQ\n');

  const QUEUE = 'emails';

  try {
    console.log('═══════════════════════════════════════');
    console.log('📬 EMAIL QUEUE SYSTEM');
    console.log('═══════════════════════════════════════\n');

    // Set up rate limiting (max 10 emails/sec to avoid spam filters)
    await client.setRateLimit(QUEUE, 10);
    console.log('🚦 Rate limit set: 10 emails/second\n');

    // ===== PRODUCER: Queue emails =====
    console.log('📤 Queueing emails...\n');

    // Welcome email (high priority)
    await client.push<EmailJob>(
      QUEUE,
      {
        template: 'welcome',
        to: 'newuser@example.com',
        subject: 'Welcome to Our Service!',
        variables: { name: 'John' },
      },
      {
        priority: 100, // High priority
        max_attempts: 5,
        backoff: 30000, // 30s between retries
        unique_key: 'welcome-newuser@example.com', // Prevent duplicate welcome emails
      }
    );
    console.log('   ✅ Welcome email queued (priority: high)');

    // Password reset (urgent)
    await client.push<EmailJob>(
      QUEUE,
      {
        template: 'password-reset',
        to: 'user@example.com',
        subject: 'Reset Your Password',
        variables: {
          name: 'Jane',
          resetLink: 'https://example.com/reset?token=abc123',
        },
      },
      {
        priority: 200, // Highest priority
        ttl: 3600000, // Expires in 1 hour
        max_attempts: 3,
        backoff: 10000,
      }
    );
    console.log('   ✅ Password reset email queued (priority: urgent)');

    // Order confirmations (batch)
    const orders = [
      { email: 'customer1@example.com', orderId: '1001', total: '$99.99' },
      { email: 'customer2@example.com', orderId: '1002', total: '$149.99' },
      { email: 'customer3@example.com', orderId: '1003', total: '$49.99' },
    ];

    const orderJobs = orders.map((order) => ({
      data: {
        template: 'order-confirmation' as const,
        to: order.email,
        subject: `Order Confirmation #${order.orderId}`,
        variables: {
          orderId: order.orderId,
          total: order.total,
          address: '123 Main St',
          trackingLink: `https://track.example.com/${order.orderId}`,
        },
      },
      priority: 50,
      max_attempts: 5,
      backoff: 60000,
    }));

    await client.pushBatch(QUEUE, orderJobs);
    console.log(`   ✅ ${orderJobs.length} order confirmation emails queued (batch)`);

    // Newsletter (low priority, can be delayed)
    await client.push<EmailJob>(
      QUEUE,
      {
        template: 'newsletter',
        to: 'subscriber@example.com',
        subject: 'Weekly Newsletter',
        variables: {
          content: 'This week in tech...',
          unsubscribeLink: 'https://example.com/unsubscribe',
        },
      },
      {
        priority: 1, // Low priority
        delay: 5000, // Send after 5 seconds
        max_attempts: 2,
      }
    );
    console.log('   ✅ Newsletter queued (priority: low, delayed 5s)\n');

    // ===== CONSUMER: Process emails =====
    console.log('🚀 Starting email worker (3 concurrent)...\n');

    let sent = 0;
    let failed = 0;

    const worker = new Worker<EmailJob, EmailResult>(
      QUEUE,
      async (job) => {
        console.log(`\n   📧 Processing email ${job.id}:`);
        console.log(`      Template: ${job.data.template}`);

        // Update progress
        await worker.updateProgress(job.id, 10, 'Preparing email');

        // Validate email
        if (!job.data.to.includes('@')) {
          throw new Error('Invalid email address');
        }
        await worker.updateProgress(job.id, 30, 'Email validated');

        // Send email
        await worker.updateProgress(job.id, 50, 'Sending...');
        const result = await sendEmail(job.data);
        await worker.updateProgress(job.id, 100, 'Sent!');

        return result;
      },
      {
        host: 'localhost',
        port: 6789,
        concurrency: 3,
      }
    );

    worker.on('completed', (job, result) => {
      sent++;
      console.log(`   ✅ Email sent: ${(result as EmailResult).messageId}`);
    });

    worker.on('failed', (job, error) => {
      failed++;
      console.log(`   ❌ Email failed: ${(error as Error).message}`);
    });

    await worker.start();

    // Wait for processing
    await sleep(10000);

    // ===== MONITORING =====
    console.log('\n═══════════════════════════════════════');
    console.log('📊 EMAIL QUEUE METRICS');
    console.log('═══════════════════════════════════════\n');

    const stats = await client.stats();
    const metrics = await client.metrics();

    console.log(`   Emails sent: ${sent}`);
    console.log(`   Emails failed: ${failed}`);
    console.log(`   Queue depth: ${stats.queued}`);
    console.log(`   In DLQ: ${stats.dlq}`);
    console.log(`   Avg latency: ${metrics.avg_latency_ms.toFixed(2)}ms`);

    // Check DLQ
    if (stats.dlq > 0) {
      console.log('\n   ⚠️  Failed emails in DLQ:');
      const dlqJobs = await client.getDlq(QUEUE);
      for (const job of dlqJobs) {
        const data = job.data as EmailJob;
        console.log(`      - ${data.to}: ${job.progress_msg}`);
      }

      console.log('\n   Would you like to retry? Use: client.retryDlq(QUEUE)');
    }

    await worker.stop();

    // Cleanup
    await client.clearRateLimit(QUEUE);

  } finally {
    await client.close();
    console.log('\n👋 Email system shutdown');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);
