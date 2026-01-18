/**
 * AI Image Processing Pipeline
 *
 * Demonstrates image processing workflow:
 * - Image upload and validation
 * - AI object detection
 * - Image captioning
 * - Result aggregation
 *
 * Run: bun run examples/20-image-processing.ts
 */

import { Worker, FlashQ } from '../src';

const DETECT_QUEUE = 'image-detect';
const CAPTION_QUEUE = 'image-caption';
const AGGREGATE_QUEUE = 'image-aggregate';

interface ImageJob {
  imageId: string;
  imageUrl: string;
  width: number;
  height: number;
}

interface DetectionResult {
  imageId: string;
  objects: Array<{ label: string; confidence: number; bbox: number[] }>;
}

interface CaptionResult {
  imageId: string;
  caption: string;
  confidence: number;
}

// Simulated object detection model (YOLO, etc.)
async function detectObjects(image: ImageJob): Promise<DetectionResult> {
  await new Promise(r => setTimeout(r, 100)); // GPU inference time
  return {
    imageId: image.imageId,
    objects: [
      { label: 'person', confidence: 0.95, bbox: [10, 20, 100, 200] },
      { label: 'car', confidence: 0.87, bbox: [150, 100, 300, 250] },
      { label: 'dog', confidence: 0.82, bbox: [50, 150, 120, 220] },
    ],
  };
}

// Simulated image captioning model (BLIP, etc.)
async function generateCaption(image: ImageJob): Promise<CaptionResult> {
  await new Promise(r => setTimeout(r, 150)); // Transformer inference
  return {
    imageId: image.imageId,
    caption: 'A person walking their dog near a parked car on a sunny day',
    confidence: 0.91,
  };
}

async function main() {
  const client = new FlashQ();
  await client.connect();

  // Clean up
  await client.obliterate(DETECT_QUEUE);
  await client.obliterate(CAPTION_QUEUE);
  await client.obliterate(AGGREGATE_QUEUE);

  console.log('=== AI Image Processing Pipeline ===\n');

  // Results storage
  const results: Map<string, { detection?: DetectionResult; caption?: CaptionResult }> = new Map();
  let processed = 0;

  // Detection worker (GPU-bound, limited concurrency)
  const detectWorker = new Worker<ImageJob, DetectionResult>(DETECT_QUEUE, async (job) => {
    console.log(`[Detection] Processing ${job.data.imageId}`);
    return await detectObjects(job.data);
  }, { concurrency: 2 }); // Limited by GPU memory

  detectWorker.on('completed', (_job, result) => {
    const existing = results.get(result.imageId) || {};
    existing.detection = result;
    results.set(result.imageId, existing);
    console.log(`[Detection] Found ${result.objects.length} objects in ${result.imageId}`);
  });

  // Caption worker
  const captionWorker = new Worker<ImageJob, CaptionResult>(CAPTION_QUEUE, async (job) => {
    console.log(`[Caption] Processing ${job.data.imageId}`);
    return await generateCaption(job.data);
  }, { concurrency: 2 });

  captionWorker.on('completed', (_job, result) => {
    const existing = results.get(result.imageId) || {};
    existing.caption = result;
    results.set(result.imageId, existing);
    console.log(`[Caption] Generated for ${result.imageId}`);
    processed++;
  });

  // Wait for workers
  await new Promise(r => setTimeout(r, 500));

  // Process batch of images
  const images: ImageJob[] = [
    { imageId: 'img-001', imageUrl: 'https://example.com/img1.jpg', width: 1920, height: 1080 },
    { imageId: 'img-002', imageUrl: 'https://example.com/img2.jpg', width: 1280, height: 720 },
    { imageId: 'img-003', imageUrl: 'https://example.com/img3.jpg', width: 800, height: 600 },
    { imageId: 'img-004', imageUrl: 'https://example.com/img4.jpg', width: 1920, height: 1080 },
    { imageId: 'img-005', imageUrl: 'https://example.com/img5.jpg', width: 640, height: 480 },
  ];

  console.log(`Processing ${images.length} images...\n`);

  // Submit both detection and caption jobs for each image
  const jobs: Array<{ detectId: number; captionId: number }> = [];
  for (const image of images) {
    const detectJob = await client.push(DETECT_QUEUE, image);
    const captionJob = await client.push(CAPTION_QUEUE, image);
    jobs.push({ detectId: detectJob.id, captionId: captionJob.id });
  }

  // Wait for all jobs to complete
  await Promise.all(jobs.flatMap(j => [
    client.finished(j.detectId),
    client.finished(j.captionId),
  ]));

  console.log('\n=== Final Results ===\n');

  for (const [imageId, result] of results) {
    console.log(`📷 ${imageId}:`);
    if (result.detection) {
      console.log(`   Objects: ${result.detection.objects.map(o => `${o.label} (${(o.confidence * 100).toFixed(0)}%)`).join(', ')}`);
    }
    if (result.caption) {
      console.log(`   Caption: "${result.caption.caption}"`);
    }
    console.log();
  }

  // Cleanup
  await detectWorker.close();
  await captionWorker.close();
  await client.obliterate(DETECT_QUEUE);
  await client.obliterate(CAPTION_QUEUE);
  await client.close();

  console.log('=== Pipeline Complete ===');
}

main().catch(console.error);
