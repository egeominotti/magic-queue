/**
 * Multi-Model Ensemble Pattern
 *
 * Demonstrates ensemble inference with:
 * - Multiple models running in parallel
 * - Result aggregation and voting
 * - Confidence-weighted averaging
 *
 * Run: bun run examples/21-model-ensemble.ts
 */

import { Worker, FlashQ } from '../src';

const MODEL_A_QUEUE = 'ensemble-model-a';
const MODEL_B_QUEUE = 'ensemble-model-b';
const MODEL_C_QUEUE = 'ensemble-model-c';

interface ClassificationRequest {
  id: string;
  text: string;
}

interface ClassificationResult {
  id: string;
  model: string;
  prediction: string;
  confidence: number;
  latency: number;
}

// Simulated sentiment analysis models
async function runModelA(text: string): Promise<{ prediction: string; confidence: number }> {
  await new Promise(r => setTimeout(r, 30)); // BERT-based
  const sentiment = text.includes('great') || text.includes('amazing') ? 'positive' :
                   text.includes('bad') || text.includes('terrible') ? 'negative' : 'neutral';
  return { prediction: sentiment, confidence: 0.85 + Math.random() * 0.1 };
}

async function runModelB(text: string): Promise<{ prediction: string; confidence: number }> {
  await new Promise(r => setTimeout(r, 20)); // RoBERTa-based
  const sentiment = text.includes('love') || text.includes('excellent') ? 'positive' :
                   text.includes('hate') || text.includes('awful') ? 'negative' : 'neutral';
  return { prediction: sentiment, confidence: 0.82 + Math.random() * 0.12 };
}

async function runModelC(text: string): Promise<{ prediction: string; confidence: number }> {
  await new Promise(r => setTimeout(r, 25)); // DistilBERT
  const sentiment = text.length > 50 ? 'positive' : text.length > 20 ? 'neutral' : 'negative';
  return { prediction: sentiment, confidence: 0.78 + Math.random() * 0.15 };
}

// Ensemble voting function
function ensembleVote(results: ClassificationResult[]): { prediction: string; confidence: number } {
  const votes: Record<string, { count: number; totalConf: number }> = {};

  for (const r of results) {
    if (!votes[r.prediction]) {
      votes[r.prediction] = { count: 0, totalConf: 0 };
    }
    votes[r.prediction].count++;
    votes[r.prediction].totalConf += r.confidence;
  }

  // Weighted voting by confidence
  let best = '';
  let bestScore = 0;
  for (const [pred, data] of Object.entries(votes)) {
    const score = data.count * (data.totalConf / data.count);
    if (score > bestScore) {
      bestScore = score;
      best = pred;
    }
  }

  return {
    prediction: best,
    confidence: votes[best].totalConf / votes[best].count,
  };
}

async function main() {
  const client = new FlashQ();
  await client.connect();

  // Clean up
  await client.obliterate(MODEL_A_QUEUE);
  await client.obliterate(MODEL_B_QUEUE);
  await client.obliterate(MODEL_C_QUEUE);

  console.log('=== Multi-Model Ensemble Example ===\n');

  // Results storage
  const modelResults: Map<string, ClassificationResult[]> = new Map();

  // Create workers for each model
  const workerA = new Worker<ClassificationRequest, ClassificationResult>(MODEL_A_QUEUE, async (job) => {
    const start = Date.now();
    const result = await runModelA(job.data.text);
    return { id: job.data.id, model: 'BERT', ...result, latency: Date.now() - start };
  }, { concurrency: 3 });

  const workerB = new Worker<ClassificationRequest, ClassificationResult>(MODEL_B_QUEUE, async (job) => {
    const start = Date.now();
    const result = await runModelB(job.data.text);
    return { id: job.data.id, model: 'RoBERTa', ...result, latency: Date.now() - start };
  }, { concurrency: 3 });

  const workerC = new Worker<ClassificationRequest, ClassificationResult>(MODEL_C_QUEUE, async (job) => {
    const start = Date.now();
    const result = await runModelC(job.data.text);
    return { id: job.data.id, model: 'DistilBERT', ...result, latency: Date.now() - start };
  }, { concurrency: 3 });

  // Collect results
  const collectResult = (result: ClassificationResult) => {
    const existing = modelResults.get(result.id) || [];
    existing.push(result);
    modelResults.set(result.id, existing);
  };

  workerA.on('completed', (_job, result) => collectResult(result));
  workerB.on('completed', (_job, result) => collectResult(result));
  workerC.on('completed', (_job, result) => collectResult(result));

  await new Promise(r => setTimeout(r, 500));

  // Test data
  const testCases: ClassificationRequest[] = [
    { id: 'test-1', text: 'This product is amazing! Great quality and fast shipping.' },
    { id: 'test-2', text: 'Terrible experience, awful customer service, would not recommend.' },
    { id: 'test-3', text: 'The item arrived on time. It works as expected.' },
    { id: 'test-4', text: 'I love this! Excellent value for money, highly recommended.' },
    { id: 'test-5', text: 'Bad quality, broke after one week. Very disappointed.' },
  ];

  console.log(`Processing ${testCases.length} texts through 3-model ensemble...\n`);

  // Submit to all three models
  const allJobs: Array<{ id: string; jobIds: number[] }> = [];
  for (const testCase of testCases) {
    const [jobA, jobB, jobC] = await Promise.all([
      client.push(MODEL_A_QUEUE, testCase),
      client.push(MODEL_B_QUEUE, testCase),
      client.push(MODEL_C_QUEUE, testCase),
    ]);
    allJobs.push({ id: testCase.id, jobIds: [jobA.id, jobB.id, jobC.id] });
  }

  // Wait for all models to complete
  await Promise.all(allJobs.flatMap(j => [
    client.finished(j.jobIds[0]),
    client.finished(j.jobIds[1]),
    client.finished(j.jobIds[2]),
  ]));

  console.log('=== Individual Model Results ===\n');

  for (const testCase of testCases) {
    const results = modelResults.get(testCase.id) || [];
    console.log(`📝 "${testCase.text.slice(0, 40)}..."`);
    for (const r of results) {
      console.log(`   ${r.model.padEnd(10)}: ${r.prediction.padEnd(8)} (${(r.confidence * 100).toFixed(1)}%, ${r.latency}ms)`);
    }
  }

  console.log('\n=== Ensemble Predictions ===\n');

  for (const testCase of testCases) {
    const results = modelResults.get(testCase.id) || [];
    const ensemble = ensembleVote(results);
    console.log(`"${testCase.text.slice(0, 30)}..."`);
    console.log(`   → Ensemble: ${ensemble.prediction} (${(ensemble.confidence * 100).toFixed(1)}% avg confidence)\n`);
  }

  // Cleanup
  await workerA.close();
  await workerB.close();
  await workerC.close();
  await client.obliterate(MODEL_A_QUEUE);
  await client.obliterate(MODEL_B_QUEUE);
  await client.obliterate(MODEL_C_QUEUE);
  await client.close();

  console.log('=== Complete ===');
}

main().catch(console.error);
