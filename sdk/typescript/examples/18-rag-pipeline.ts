/**
 * RAG (Retrieval Augmented Generation) Pipeline
 *
 * Complete RAG pipeline with:
 * - Document chunking and embedding
 * - Vector similarity search
 * - LLM response generation
 *
 * Run: bun run examples/18-rag-pipeline.ts
 */

import { Worker, FlashQ } from '../src';

const EMBED_QUEUE = 'rag-embed';
const SEARCH_QUEUE = 'rag-search';
const GENERATE_QUEUE = 'rag-generate';

// Simulated vector database
const vectorDB: Map<string, { text: string; embedding: number[] }> = new Map();

// Simulated embedding model
async function embed(text: string): Promise<number[]> {
  await new Promise(r => setTimeout(r, 10));
  // Simple hash-based pseudo-embedding for demo
  const hash = text.split('').reduce((a, c) => a + c.charCodeAt(0), 0);
  return Array(384).fill(0).map((_, i) => Math.sin(hash + i) * 0.5);
}

// Simulated vector search
async function vectorSearch(queryEmbed: number[], topK: number): Promise<string[]> {
  await new Promise(r => setTimeout(r, 5));
  // Return random chunks for demo
  const keys = Array.from(vectorDB.keys());
  return keys.slice(0, Math.min(topK, keys.length)).map(k => vectorDB.get(k)!.text);
}

// Simulated LLM generation
async function generateResponse(context: string[], query: string): Promise<string> {
  await new Promise(r => setTimeout(r, 50));
  return `Based on ${context.length} retrieved documents about "${query.slice(0, 30)}...", the answer is: [Generated response here]`;
}

async function main() {
  const client = new FlashQ();
  await client.connect();

  // Clean up
  await client.obliterate(EMBED_QUEUE);
  await client.obliterate(SEARCH_QUEUE);
  await client.obliterate(GENERATE_QUEUE);

  console.log('=== RAG Pipeline Example ===\n');

  // Step 1: Document Embedding Worker
  const embedWorker = new Worker(EMBED_QUEUE, async (job) => {
    const { docId, text } = job.data as { docId: string; text: string };
    const embedding = await embed(text);
    vectorDB.set(docId, { text, embedding });
    return { docId, dimensions: embedding.length };
  }, { concurrency: 5 });

  // Step 2: Search Worker
  const searchWorker = new Worker(SEARCH_QUEUE, async (job) => {
    const { query, topK } = job.data as { query: string; topK: number };
    const queryEmbed = await embed(query);
    const results = await vectorSearch(queryEmbed, topK);
    return { query, results };
  }, { concurrency: 2 });

  // Step 3: Generation Worker
  const generateWorker = new Worker(GENERATE_QUEUE, async (job) => {
    const { query, context } = job.data as { query: string; context: string[] };
    const response = await generateResponse(context, query);
    return { response };
  }, { concurrency: 1 }); // Limited concurrency for LLM

  // Wait for workers to start
  await new Promise(r => setTimeout(r, 500));

  // Index some documents
  console.log('Phase 1: Indexing documents...');
  const documents = [
    { docId: 'doc1', text: 'FlashQ is a high-performance job queue built in Rust.' },
    { docId: 'doc2', text: 'It supports priorities, delays, and rate limiting.' },
    { docId: 'doc3', text: 'Workers can process jobs concurrently with configurable limits.' },
    { docId: 'doc4', text: 'The queue persists to PostgreSQL for durability.' },
    { docId: 'doc5', text: 'It is compatible with BullMQ APIs for easy migration.' },
  ];

  const embedJobs = await Promise.all(
    documents.map(doc => client.push(EMBED_QUEUE, doc))
  );

  // Wait for embeddings
  await Promise.all(embedJobs.map(job => client.finished(job.id)));
  console.log(`Indexed ${documents.length} documents\n`);

  // Process a query
  console.log('Phase 2: Processing query...');
  const userQuery = 'How does FlashQ handle job priorities?';
  console.log(`Query: "${userQuery}"\n`);

  // Search for relevant documents
  const searchJob = await client.push(SEARCH_QUEUE, { query: userQuery, topK: 3 });
  const searchResult = await client.finished<{ query: string; results: string[] }>(searchJob.id);

  console.log('Retrieved context:');
  searchResult?.results.forEach((r, i) => console.log(`  ${i + 1}. ${r}`));

  // Generate response
  console.log('\nPhase 3: Generating response...');
  const genJob = await client.push(GENERATE_QUEUE, { query: userQuery, context: searchResult?.results || [] });
  const genResult = await client.finished<{ response: string }>(genJob.id);

  console.log('\n=== Final Response ===');
  console.log(genResult?.response);

  // Cleanup
  await embedWorker.close();
  await searchWorker.close();
  await generateWorker.close();
  await client.obliterate(EMBED_QUEUE);
  await client.obliterate(SEARCH_QUEUE);
  await client.obliterate(GENERATE_QUEUE);
  await client.close();

  console.log('\n=== Pipeline Complete ===');
}

main().catch(console.error);
