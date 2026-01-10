# MagicQueue

A high-performance job queue system built from scratch with **Rust** and **TypeScript/Bun**.

No Redis. No dependencies. Pure performance.

## Performance

| Metric | Throughput |
|--------|------------|
| Push (batch) | **1,900,000+ jobs/sec** |
| Processing (no-op) | **280,000+ jobs/sec** |
| Processing (CPU work) | **196,000+ jobs/sec** |

*Benchmarked with 100k jobs on Apple Silicon*

### Optimizations

- **mimalloc** - High-performance memory allocator
- **parking_lot** - Faster locks than std
- **Atomic u64 IDs** - No UUID overhead
- **32 shards** - Minimized lock contention
- **LTO** - Link-Time Optimization for smaller, faster binary

## Features

- **Batch Operations** - Push/Pull/Ack thousands of jobs in a single request
- **Job Priorities** - Higher priority jobs execute first
- **Delayed Jobs** - Schedule jobs to run in the future
- **Persistence** - Optional WAL (Write-Ahead Log) for durability
- **Unix Socket** - Lower latency than TCP
- **Sharded Architecture** - 32 shards reduce lock contention

## Quick Start

### Start the Server

```bash
cd server
cargo run --release
```

Server listens on port `6789` by default.

### Install Client

```bash
cd client
bun install
```

### Producer Example

```typescript
import { Queue } from "./src";

const queue = new Queue("emails");

// Single push
await queue.push({ to: "user@example.com", subject: "Hello" });

// With priority (higher = more urgent)
await queue.push({ task: "urgent" }, { priority: 10 });

// Delayed job (executes in 5 seconds)
await queue.push({ task: "reminder" }, { delay: 5000 });

// Batch push (fastest)
await queue.pushBatch([
  { data: { to: "a@test.com" } },
  { data: { to: "b@test.com" }, priority: 5 },
  { data: { to: "c@test.com" }, delay: 10000 },
]);
```

### Worker Example

```typescript
import { Worker } from "./src";

const worker = new Worker(
  "emails",
  async (job) => {
    console.log("Processing:", job.data);
    // Job auto-ACKed on success
    // Job auto-FAILed on error (returned to queue)
  },
  { batchSize: 50 } // Process 50 jobs per batch
);

await worker.start();
```

## Server Options

```bash
# Standard TCP (port 6789)
cargo run --release

# With persistence (WAL)
PERSIST=1 cargo run --release

# Unix socket (lower latency)
UNIX_SOCKET=1 cargo run --release

# Custom port
PORT=7000 cargo run --release

# All options
PERSIST=1 UNIX_SOCKET=1 cargo run --release
```

## Protocol

JSON over TCP/Unix socket, newline-delimited.

### Commands

```json
// Push single job
{"cmd": "PUSH", "queue": "jobs", "data": {...}, "priority": 0, "delay": null}

// Batch push
{"cmd": "PUSHB", "queue": "jobs", "jobs": [{"data": {...}, "priority": 0}]}

// Pull single job (blocking)
{"cmd": "PULL", "queue": "jobs"}

// Batch pull
{"cmd": "PULLB", "queue": "jobs", "count": 50}

// Acknowledge job
{"cmd": "ACK", "id": "job-id"}

// Batch acknowledge
{"cmd": "ACKB", "ids": ["id1", "id2", "id3"]}

// Fail job (returns to queue)
{"cmd": "FAIL", "id": "job-id", "error": "reason"}

// Get stats
{"cmd": "STATS"}
```

## Architecture

```
                    ┌─────────────────────────────┐
                    │      MagicQueue Server      │
                    │           (Rust)            │
┌─────────┐        ├─────────────────────────────┤
│Producer │──TCP──▶│  32 Sharded Queues          │
│  (Bun)  │        │  ├─ BinaryHeap (priority)   │
└─────────┘        │  ├─ Delayed job support     │
                   │  └─ Processing tracker      │
┌─────────┐        ├─────────────────────────────┤
│ Worker  │◀─TCP──▶│  Optional WAL persistence   │
│  (Bun)  │        └─────────────────────────────┘
└─────────┘
```

## Benchmarks

Run the benchmark:

```bash
# Terminal 1
cd server && cargo run --release

# Terminal 2
cd client && bun run examples/benchmark.ts
```

## Project Structure

```
magic-queue/
├── server/                 # Rust server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # TCP/Unix socket server
│       ├── protocol.rs     # Commands & responses
│       └── queue.rs        # Sharded queue manager
│
└── client/                 # TypeScript client
    ├── package.json
    └── src/
        ├── client.ts       # TCP connection
        ├── queue.ts        # Producer API
        └── worker.ts       # Consumer API
```

## License

MIT
