# flashQ Performance Benchmarks

> **flashQ is 12.7x faster than BullMQ on average**

## Quick Results

| Scenario | flashQ | BullMQ | Speedup |
|----------|--------|--------|---------|
| Single Push | 21,034 jobs/sec | 4,938 jobs/sec | **4.3x** |
| Batch Push | 648,520 jobs/sec | 42,570 jobs/sec | **15.2x** |
| Processing | 293,967 jobs/sec | 13,407 jobs/sec | **21.9x** |
| High Throughput | 236,329 jobs/sec | 13,135 jobs/sec | **18.0x** |
| Large Payload (10KB) | 11,940 jobs/sec | 3,106 jobs/sec | **3.8x** |

## Extreme Scale Test

| Metric | Value |
|--------|-------|
| Total Jobs | **10,000,000** |
| Processing Rate | **245,863 jobs/sec** |
| Total Time | ~40 seconds |
| Errors | 0 |

## Why flashQ is Faster

### 1. Rust vs Node.js
- **No garbage collection pauses** - Predictable latency
- **Zero-cost abstractions** - No runtime overhead
- **Native async** - Tokio runtime with work-stealing scheduler

### 2. Architecture
- **In-memory first** - Jobs stored in memory, optional PostgreSQL persistence
- **No Redis dependency** - Direct TCP protocol, no intermediate broker
- **32 sharded queues** - Parallel access with minimal lock contention

### 3. Data Structures
- **DashMap** - Lock-free concurrent HashMap for O(1) job lookups
- **IndexedPriorityQueue** - O(log n) for cancel/update/promote (vs O(n))
- **CompactString** - Inline strings up to 24 chars (zero heap allocation)

### 4. Protocol
- **Binary protocol** - MessagePack support for 40% smaller payloads
- **Batch operations** - Native batch push/pull/ack for high throughput
- **Connection pooling** - Reuse TCP connections

### 5. Worker Design
- **Batch worker mode** - Pull 100 jobs at once (default)
- **Parallel ack** - Acknowledge jobs in batch
- **Smart polling** - Adaptive sleep to reduce CPU usage

## Benchmark Configuration

```
Platform: darwin arm64 (Apple Silicon)
Bun Version: 1.3.6
flashQ: cargo run --release (Rust 1.75+)
BullMQ: v5.66.5 with Redis 7.x
```

| Parameter | Value |
|-----------|-------|
| Single Push Jobs | 10,000 |
| Batch Push Jobs | 100,000 |
| Batch Size | 1,000 |
| Processing Jobs | 50,000 |
| Concurrency | 10 workers |
| Large Payload Size | 10KB |

## How to Reproduce

```bash
# 1. Start flashQ server
cd server && HTTP=1 cargo run --release

# 2. Start Redis (for BullMQ comparison)
docker run -d -p 6379:6379 redis:alpine

# 3. Run benchmarks
cd benchmarks && bun install && bun run benchmark.ts

# 4. Run extreme scale test (10M jobs)
cd sdk/typescript && bun run examples/test-10k-jobs.ts
```

## Comparison with Other Systems

| Queue System | Language | Throughput | Notes |
|--------------|----------|------------|-------|
| **flashQ** | Rust | **245,000+ jobs/sec** | In-memory + PostgreSQL |
| BullMQ | Node.js/Redis | ~15,000 jobs/sec | Redis bottleneck |
| Sidekiq | Ruby/Redis | ~10,000 jobs/sec | Ruby GIL limits |
| Celery | Python/Redis | ~5,000 jobs/sec | Python overhead |
| RabbitMQ | Erlang | ~30,000 msg/sec | Message broker |
| Kafka | Java | ~100,000 msg/sec | Streaming platform |

## API Compatibility

flashQ provides a BullMQ-compatible API, making migration easy:

```typescript
// BullMQ
const queue = new Queue('emails');
await queue.add('send', { to: 'user@example.com' });

// flashQ (same API!)
const client = new FlashQ();
await client.push('emails', { to: 'user@example.com' });
```

## License

MIT
