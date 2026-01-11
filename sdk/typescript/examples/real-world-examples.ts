/**
 * Real-World Examples
 *
 * Practical examples of MagicQueue usage in production scenarios.
 */

import { MagicQueue } from '../src/index';

const HOST = process.env.MQ_HOST || 'localhost';
const PORT = parseInt(process.env.MQ_PORT || '6789');

// ============================================
// 1. E-Commerce Order Processing
// ============================================
interface OrderJob {
  orderId: string;
  customerId: string;
  items: Array<{
    sku: string;
    name: string;
    quantity: number;
    price: number;
  }>;
  shipping: {
    address: string;
    city: string;
    country: string;
    postalCode: string;
  };
  payment: {
    method: 'credit_card' | 'paypal' | 'bank_transfer';
    transactionId: string;
    amount: number;
    currency: string;
  };
  createdAt: string;
}

async function ecommerceExample(client: MagicQueue) {
  console.log('\n1. E-Commerce Order Processing');
  console.log('-'.repeat(50));

  const order: OrderJob = {
    orderId: 'ORD-2024-001234',
    customerId: 'CUST-5678',
    items: [
      { sku: 'LAPTOP-001', name: 'MacBook Pro 14"', quantity: 1, price: 1999.00 },
      { sku: 'CASE-001', name: 'Laptop Case', quantity: 1, price: 49.99 },
      { sku: 'CHARGER-001', name: 'USB-C Charger', quantity: 2, price: 29.99 }
    ],
    shipping: {
      address: 'Via Roma 123',
      city: 'Milano',
      country: 'IT',
      postalCode: '20100'
    },
    payment: {
      method: 'credit_card',
      transactionId: 'TXN-9876543210',
      amount: 2108.97,
      currency: 'EUR'
    },
    createdAt: new Date().toISOString()
  };

  // Push order with high priority and retry config
  const job = await client.push('orders', order, {
    priority: 10,
    max_attempts: 5,
    backoff: 5000, // 5s, 10s, 20s, 40s, 80s
    timeout: 60000 // 1 minute timeout
  });

  console.log(`   Order ${order.orderId} queued as job ${job.id}`);
  console.log(`   Total: ${order.payment.amount} ${order.payment.currency}`);
  console.log(`   Items: ${order.items.length}`);

  // Process order
  const pulled = await client.pull<OrderJob>('orders');
  console.log(`   Processing order ${pulled.data.orderId}...`);

  // Simulate order processing steps
  await client.progress(pulled.id, 25, 'Validating payment');
  await client.progress(pulled.id, 50, 'Reserving inventory');
  await client.progress(pulled.id, 75, 'Generating shipping label');
  await client.progress(pulled.id, 100, 'Order confirmed');

  await client.ack(pulled.id, {
    status: 'confirmed',
    trackingNumber: 'IT123456789',
    estimatedDelivery: '2024-01-15'
  });

  console.log(`   Order completed!`);
}

// ============================================
// 2. Email Campaign with Large Recipient List
// ============================================
interface EmailCampaignJob {
  campaignId: string;
  subject: string;
  htmlBody: string;
  textBody: string;
  from: { name: string; email: string };
  recipients: Array<{
    email: string;
    name: string;
    customFields: Record<string, string>;
  }>;
  scheduledAt?: string;
  tags: string[];
}

async function emailCampaignExample(client: MagicQueue) {
  console.log('\n2. Email Campaign (1000 recipients)');
  console.log('-'.repeat(50));

  // Generate 1000 recipients
  const recipients = Array.from({ length: 1000 }, (_, i) => ({
    email: `user${i}@example.com`,
    name: `User ${i}`,
    customFields: {
      firstName: `Name${i}`,
      company: `Company ${i % 100}`,
      plan: i % 3 === 0 ? 'premium' : 'basic'
    }
  }));

  const campaign: EmailCampaignJob = {
    campaignId: 'CAMP-2024-Q1-PROMO',
    subject: 'Special Offer: {{firstName}}, 30% off for {{company}}!',
    htmlBody: `
      <html>
        <body>
          <h1>Hello {{firstName}}!</h1>
          <p>As a valued {{plan}} customer at {{company}}, we're offering you an exclusive 30% discount.</p>
          <a href="https://example.com/promo?user={{email}}">Claim Your Discount</a>
        </body>
      </html>
    `,
    textBody: 'Hello {{firstName}}! Get 30% off at https://example.com/promo',
    from: { name: 'Marketing Team', email: 'marketing@example.com' },
    recipients,
    tags: ['promo', 'q1-2024', 'discount']
  };

  const payloadSize = JSON.stringify(campaign).length;
  console.log(`   Campaign payload: ${(payloadSize / 1024).toFixed(2)} KB`);
  console.log(`   Recipients: ${campaign.recipients.length}`);

  const job = await client.push('email-campaigns', campaign, {
    unique_key: campaign.campaignId, // Prevent duplicate sends
    timeout: 300000 // 5 minutes for large campaigns
  });

  console.log(`   Campaign queued as job ${job.id}`);

  // Process
  const pulled = await client.pull<EmailCampaignJob>('email-campaigns');
  const data = pulled.data;

  console.log(`   Sending to ${data.recipients.length} recipients...`);

  // Simulate batch sending with progress
  const batchSize = 100;
  for (let i = 0; i < data.recipients.length; i += batchSize) {
    const progress = Math.round((i / data.recipients.length) * 100);
    await client.progress(pulled.id, progress, `Sent ${i}/${data.recipients.length}`);
  }

  await client.ack(pulled.id, {
    sent: data.recipients.length,
    failed: 0,
    completedAt: new Date().toISOString()
  });

  console.log(`   Campaign sent successfully!`);
}

// ============================================
// 3. Video Transcoding Pipeline
// ============================================
interface VideoTranscodeJob {
  videoId: string;
  sourceUrl: string;
  sourceFormat: string;
  sourceSize: number;
  outputs: Array<{
    format: 'mp4' | 'webm' | 'hls';
    resolution: '1080p' | '720p' | '480p' | '360p';
    bitrate: number;
    codec: string;
  }>;
  webhookUrl: string;
  metadata: {
    title: string;
    duration: number;
    uploadedBy: string;
  };
}

async function videoTranscodingExample(client: MagicQueue) {
  console.log('\n3. Video Transcoding Pipeline');
  console.log('-'.repeat(50));

  const transcodeJob: VideoTranscodeJob = {
    videoId: 'VID-2024-ABCD1234',
    sourceUrl: 's3://videos-raw/upload-12345.mov',
    sourceFormat: 'mov',
    sourceSize: 2_500_000_000, // 2.5GB
    outputs: [
      { format: 'mp4', resolution: '1080p', bitrate: 8000, codec: 'h264' },
      { format: 'mp4', resolution: '720p', bitrate: 5000, codec: 'h264' },
      { format: 'mp4', resolution: '480p', bitrate: 2500, codec: 'h264' },
      { format: 'webm', resolution: '1080p', bitrate: 6000, codec: 'vp9' },
      { format: 'hls', resolution: '1080p', bitrate: 8000, codec: 'h264' }
    ],
    webhookUrl: 'https://api.example.com/webhooks/transcode-complete',
    metadata: {
      title: 'Product Demo Video',
      duration: 180, // 3 minutes
      uploadedBy: 'user@example.com'
    }
  };

  // Push with long timeout for video processing
  const job = await client.push('video-transcode', transcodeJob, {
    priority: 5,
    timeout: 3600000, // 1 hour
    max_attempts: 3,
    backoff: 60000 // 1 minute between retries
  });

  console.log(`   Video ${transcodeJob.videoId} queued as job ${job.id}`);
  console.log(`   Source: ${(transcodeJob.sourceSize / 1e9).toFixed(2)} GB`);
  console.log(`   Outputs: ${transcodeJob.outputs.length} variants`);

  // Process
  const pulled = await client.pull<VideoTranscodeJob>('video-transcode');
  const video = pulled.data;

  console.log(`   Transcoding ${video.metadata.title}...`);

  // Simulate transcoding each output
  const totalOutputs = video.outputs.length;
  for (let i = 0; i < totalOutputs; i++) {
    const output = video.outputs[i];
    const progress = Math.round(((i + 1) / totalOutputs) * 100);
    await client.progress(
      pulled.id,
      progress,
      `Encoding ${output.resolution} ${output.format}`
    );
  }

  await client.ack(pulled.id, {
    status: 'completed',
    outputs: video.outputs.map((o, i) => ({
      ...o,
      url: `s3://videos-processed/${video.videoId}/${o.resolution}.${o.format}`,
      size: Math.round(video.sourceSize * (o.bitrate / 8000) * 0.8)
    })),
    processingTime: 847 // seconds
  });

  console.log(`   Transcoding completed!`);
}

// ============================================
// 4. Data Import/Export (CSV Processing)
// ============================================
interface DataImportJob {
  importId: string;
  source: {
    type: 'csv' | 'json' | 'xlsx';
    url: string;
    encoding: string;
  };
  mapping: Record<string, string>;
  options: {
    skipHeader: boolean;
    delimiter: string;
    batchSize: number;
    validateEmail: boolean;
    deduplicateBy?: string;
  };
  destination: {
    table: string;
    mode: 'insert' | 'upsert' | 'replace';
  };
  notifyEmail: string;
}

async function dataImportExample(client: MagicQueue) {
  console.log('\n4. Data Import (CSV with 50K rows)');
  console.log('-'.repeat(50));

  const importJob: DataImportJob = {
    importId: 'IMP-2024-CONTACTS',
    source: {
      type: 'csv',
      url: 's3://imports/contacts-export-2024.csv',
      encoding: 'utf-8'
    },
    mapping: {
      'Email': 'email',
      'First Name': 'firstName',
      'Last Name': 'lastName',
      'Company': 'company',
      'Phone': 'phone',
      'Created Date': 'createdAt'
    },
    options: {
      skipHeader: true,
      delimiter: ',',
      batchSize: 1000,
      validateEmail: true,
      deduplicateBy: 'email'
    },
    destination: {
      table: 'contacts',
      mode: 'upsert'
    },
    notifyEmail: 'admin@example.com'
  };

  const job = await client.push('data-imports', importJob, {
    priority: 3,
    timeout: 600000, // 10 minutes
    max_attempts: 2
  });

  console.log(`   Import ${importJob.importId} queued as job ${job.id}`);
  console.log(`   Destination: ${importJob.destination.table}`);
  console.log(`   Mode: ${importJob.destination.mode}`);

  // Process
  const pulled = await client.pull<DataImportJob>('data-imports');
  const imp = pulled.data;

  console.log(`   Processing import...`);

  // Simulate processing 50K rows in batches
  const totalRows = 50000;
  const batchSize = imp.options.batchSize;

  for (let i = 0; i < totalRows; i += batchSize) {
    const progress = Math.round((i / totalRows) * 100);
    await client.progress(pulled.id, progress, `Processed ${i}/${totalRows} rows`);
  }

  await client.ack(pulled.id, {
    status: 'completed',
    stats: {
      totalRows: totalRows,
      imported: 48500,
      skipped: 1200,
      errors: 300,
      duplicates: 1500
    },
    completedAt: new Date().toISOString()
  });

  console.log(`   Import completed!`);
}

// ============================================
// 5. ML Model Inference Batch
// ============================================
interface MLInferenceJob {
  batchId: string;
  model: {
    name: string;
    version: string;
    endpoint: string;
  };
  inputs: Array<{
    id: string;
    features: number[];
    metadata?: Record<string, any>;
  }>;
  outputConfig: {
    format: 'json' | 'csv';
    destination: string;
    includeConfidence: boolean;
  };
}

async function mlInferenceExample(client: MagicQueue) {
  console.log('\n5. ML Model Inference (500 samples)');
  console.log('-'.repeat(50));

  // Generate 500 inference requests (staying under 1MB limit)
  const inputs = Array.from({ length: 500 }, (_, i) => ({
    id: `sample-${i.toString().padStart(5, '0')}`,
    features: Array.from({ length: 64 }, () => Math.round(Math.random() * 1000) / 1000), // 64-dim vector
    metadata: { source: 'batch-job' }
  }));

  const inferenceJob: MLInferenceJob = {
    batchId: 'BATCH-ML-2024-001',
    model: {
      name: 'fraud-detection-v2',
      version: '2.3.1',
      endpoint: 'https://ml.example.com/predict'
    },
    inputs,
    outputConfig: {
      format: 'json',
      destination: 's3://ml-results/batch-001.json',
      includeConfidence: true
    }
  };

  const payloadSize = JSON.stringify(inferenceJob).length;
  console.log(`   Payload size: ${(payloadSize / 1024).toFixed(2)} KB`);
  console.log(`   Samples: ${inputs.length}`);
  console.log(`   Features per sample: 64`);

  const job = await client.push('ml-inference', inferenceJob, {
    priority: 8,
    timeout: 300000 // 5 minutes
  });

  console.log(`   Batch ${inferenceJob.batchId} queued as job ${job.id}`);

  // Process
  const pulled = await client.pull<MLInferenceJob>('ml-inference');
  const batch = pulled.data;

  console.log(`   Running inference on ${batch.inputs.length} samples...`);

  // Simulate batch inference
  const batchSize = 100;
  for (let i = 0; i < batch.inputs.length; i += batchSize) {
    const progress = Math.round((i / batch.inputs.length) * 100);
    await client.progress(pulled.id, progress, `Processed ${i}/${batch.inputs.length}`);
  }

  await client.ack(pulled.id, {
    status: 'completed',
    predictions: batch.inputs.length,
    avgLatency: 12.5, // ms per sample
    outputUrl: batch.outputConfig.destination
  });

  console.log(`   Inference completed!`);
}

// ============================================
// 6. Report Generation
// ============================================
interface ReportJob {
  reportId: string;
  type: 'sales' | 'inventory' | 'analytics' | 'financial';
  parameters: {
    dateFrom: string;
    dateTo: string;
    groupBy: string[];
    filters: Record<string, any>;
    includeCharts: boolean;
  };
  format: 'pdf' | 'xlsx' | 'html';
  delivery: {
    method: 'email' | 's3' | 'webhook';
    destination: string;
  };
  requestedBy: {
    userId: string;
    email: string;
  };
}

async function reportGenerationExample(client: MagicQueue) {
  console.log('\n6. Report Generation');
  console.log('-'.repeat(50));

  const reportJob: ReportJob = {
    reportId: 'RPT-2024-Q1-SALES',
    type: 'sales',
    parameters: {
      dateFrom: '2024-01-01',
      dateTo: '2024-03-31',
      groupBy: ['region', 'product_category', 'month'],
      filters: {
        region: ['EU', 'NA', 'APAC'],
        minAmount: 1000
      },
      includeCharts: true
    },
    format: 'pdf',
    delivery: {
      method: 'email',
      destination: 'cfo@example.com'
    },
    requestedBy: {
      userId: 'USR-12345',
      email: 'analyst@example.com'
    }
  };

  const job = await client.push('reports', reportJob, {
    priority: 5,
    timeout: 180000, // 3 minutes
    max_attempts: 2
  });

  console.log(`   Report ${reportJob.reportId} queued as job ${job.id}`);
  console.log(`   Type: ${reportJob.type}`);
  console.log(`   Format: ${reportJob.format}`);
  console.log(`   Period: ${reportJob.parameters.dateFrom} to ${reportJob.parameters.dateTo}`);

  // Process
  const pulled = await client.pull<ReportJob>('reports');
  const report = pulled.data;

  console.log(`   Generating ${report.type} report...`);

  await client.progress(pulled.id, 20, 'Querying database');
  await client.progress(pulled.id, 50, 'Aggregating data');
  await client.progress(pulled.id, 70, 'Generating charts');
  await client.progress(pulled.id, 90, 'Rendering PDF');
  await client.progress(pulled.id, 100, 'Sending email');

  await client.ack(pulled.id, {
    status: 'delivered',
    pages: 24,
    fileSize: 2_450_000,
    deliveredAt: new Date().toISOString()
  });

  console.log(`   Report delivered!`);
}

// ============================================
// Main
// ============================================
async function main() {
  const client = new MagicQueue({ host: HOST, port: PORT });
  await client.connect();

  console.log('='.repeat(60));
  console.log('   Real-World MagicQueue Examples');
  console.log('='.repeat(60));

  await ecommerceExample(client);
  await emailCampaignExample(client);
  await videoTranscodingExample(client);
  await dataImportExample(client);
  await mlInferenceExample(client);
  await reportGenerationExample(client);

  console.log('\n' + '='.repeat(60));
  console.log('   All examples completed!');
  console.log('='.repeat(60));

  await client.close();
}

main().catch(console.error);
