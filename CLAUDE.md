# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this codebase.

## Project Overview

MagicQueue is a high-performance job queue system with:
- **Server**: Rust async TCP/Unix socket server using Tokio
- **Client**: TypeScript library for Bun runtime

## Key Commands

### Server (Rust)

```bash
cd server

# Development
cargo run

# Production (optimized)
cargo run --release

# With persistence
PERSIST=1 cargo run --release

# With Unix socket
UNIX_SOCKET=1 cargo run --release
```

### Client (TypeScript/Bun)

```bash
cd client

# Install dependencies
bun install

# Run examples
bun run examples/producer.ts
bun run examples/worker.ts
bun run examples/benchmark.ts
```

## Architecture

### Server Components

- `main.rs` - TCP/Unix socket server, connection handling
- `protocol.rs` - Command/Response types, JSON serialization
- `queue.rs` - Sharded queue manager with BinaryHeap for priority

### Client Components

- `client.ts` - Low-level TCP connection
- `queue.ts` - Producer API (push, pushBatch, pushMany)
- `worker.ts` - Consumer API with batch processing

## Protocol

JSON over TCP, newline-delimited. Commands:

| Command | Description |
|---------|-------------|
| PUSH | Single job push |
| PUSHB | Batch push |
| PULL | Single job pull (blocking) |
| PULLB | Batch pull |
| ACK | Acknowledge job |
| ACKB | Batch acknowledge |
| FAIL | Fail job (returns to queue) |
| STATS | Get queue statistics |

## Performance Optimizations

1. **Sharding** - 16 shards reduce lock contention
2. **Batch operations** - Reduce round-trips
3. **BinaryHeap** - O(log n) priority queue
4. **Buffer pooling** - 64KB read/write buffers
5. **TCP_NODELAY** - Disable Nagle's algorithm

## Common Tasks

### Adding a new command

1. Add variant to `Command` enum in `protocol.rs`
2. Add response type if needed in `Response` enum
3. Handle in `process_command()` in `main.rs`
4. Implement logic in `queue.rs`
5. Add client method in TypeScript

### Modifying queue behavior

- Priority logic: `Ord` impl for `Job` in `protocol.rs`
- Delayed jobs: `is_ready()` method and `delayed_promoter()` task
- Persistence: `WalEvent` and `write_wal()` in `queue.rs`

## Testing

```bash
# Terminal 1: Start server
cd server && cargo run --release

# Terminal 2: Run benchmark
cd client && bun run examples/benchmark.ts
```

Expected results: ~170k+ jobs/sec end-to-end throughput.
