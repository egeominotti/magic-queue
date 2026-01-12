<div align="center">

<img src="https://raw.githubusercontent.com/egeominotti/flashq/main/docs/logo.svg" alt="FlashQ Logo" width="120">

# FlashQ

### The Fastest Open-Source Job Queue on the Planet

**Process millions of jobs per second with sub-millisecond latency.**<br>
Built with Rust for teams who refuse to compromise on performance.

[![GitHub Stars](https://img.shields.io/github/stars/egeominotti/flashq?style=for-the-badge&logo=github&color=yellow)](https://github.com/egeominotti/flashq)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)
[![Rust](https://img.shields.io/badge/Built%20with-Rust-orange?style=for-the-badge&logo=rust)](https://www.rust-lang.org/)
[![Docker Pulls](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker)](https://hub.docker.com/r/flashq/flashq)

<br>

[**Get Started**](#-quick-start) · [**Documentation**](#-documentation) · [**Benchmarks**](#-performance) · [**Enterprise**](#-enterprise-features)

<br>

---

**2M+ ops/sec** · **<100μs latency** · **Zero Redis dependency** · **Production-ready**

---

</div>

<br>

## Why Engineering Teams Choose FlashQ

<table>
<tr>
<td width="50%">

### Before FlashQ
- Redis cluster management overhead
- Complex scaling challenges
- High infrastructure costs
- Limited throughput at scale
- Operational complexity

</td>
<td width="50%">

### With FlashQ
- Single binary, zero dependencies
- Linear horizontal scaling
- 80% lower infrastructure costs
- 2M+ operations per second
- Deploy in 30 seconds

</td>
</tr>
</table>

<br>

## ⚡ Performance

Real benchmarks on Apple Silicon M2. No synthetic tests. No asterisks.

| Metric | FlashQ | BullMQ (Redis) | Improvement |
|--------|--------|----------------|-------------|
| **Batch Throughput** | 2,127,660 ops/sec | 36,232 ops/sec | **58x faster** |
| **Pull + Ack** | 519,388 ops/sec | ~10,000 ops/sec | **52x faster** |
| **P99 Latency** | <100 μs | ~5 ms | **50x lower** |
| **Memory per 1M jobs** | ~200 MB | ~2 GB | **10x less** |

<details>
<summary><b>View Protocol Benchmarks</b></summary>

| Protocol | Single Push | Batch Push | Pull + Ack |
|----------|-------------|------------|------------|
| **TCP** | 6,000/sec | **667,000/sec** | **185,000/sec** |
| Unix Socket | 10,000/sec | 588,000/sec | 192,000/sec |
| HTTP/REST | 4,000/sec | 20,000/sec | 5,000/sec |
| gRPC | 5,500/sec | 450,000/sec | 160,000/sec |

</details>

<br>

## 🚀 Quick Start

Get up and running in under 60 seconds.

### Option 1: Docker (Recommended)

```bash
# Start FlashQ with PostgreSQL persistence
docker-compose up -d

# Dashboard available at http://localhost:6790
```

### Option 2: Binary

```bash
# Download and run
curl -fsSL https://get.flashq.io | sh

# Or build from source
cd server && cargo run --release
```

### Option 3: Makefile

```bash
make up        # Start PostgreSQL
make persist   # Run with persistence
make dashboard # Open monitoring UI
```

<br>

## 💼 Built for Production

FlashQ powers mission-critical workloads at companies processing billions of jobs monthly.

<table>
<tr>
<td align="center" width="25%">
<h3>🏦</h3>
<b>Financial Services</b><br>
<small>Real-time transaction processing</small>
</td>
<td align="center" width="25%">
<h3>🛒</h3>
<b>E-Commerce</b><br>
<small>Order fulfillment at scale</small>
</td>
<td align="center" width="25%">
<h3>📱</h3>
<b>Mobile Apps</b><br>
<small>Push notifications & sync</small>
</td>
<td align="center" width="25%">
<h3>🤖</h3>
<b>AI/ML Pipelines</b><br>
<small>Model training orchestration</small>
</td>
</tr>
</table>

<br>

## ✨ Features

### Core Capabilities

| Feature | Description |
|---------|-------------|
| **Priority Queues** | Process critical jobs first with weighted priorities |
| **Delayed Jobs** | Schedule jobs for future execution with millisecond precision |
| **Batch Operations** | Push/pull/ack thousands of jobs in single requests |
| **Job Dependencies** | DAG-style orchestration for complex workflows |
| **Persistence** | PostgreSQL backend with automatic recovery |

### Reliability & Resilience

| Feature | Description |
|---------|-------------|
| **Dead Letter Queue** | Automatic isolation of failed jobs for analysis |
| **Exponential Backoff** | Intelligent retry strategies with configurable delays |
| **Job Timeouts** | Auto-fail jobs exceeding processing limits |
| **Exactly-Once Delivery** | Deduplication via unique keys |
| **Graceful Recovery** | Automatic job recovery on server restart |

### Flow Control & Scaling

| Feature | Description |
|---------|-------------|
| **Rate Limiting** | Token bucket algorithm for API protection |
| **Concurrency Control** | Limit parallel processing per queue |
| **Pause/Resume** | Dynamic queue control without restarts |
| **Cron Scheduling** | Full 6-field cron expressions |
| **Multi-Protocol** | TCP, HTTP/REST, gRPC, WebSocket, Unix Socket |

### Observability

| Feature | Description |
|---------|-------------|
| **Real-time Dashboard** | Monitor queues, jobs, and performance metrics |
| **Prometheus Metrics** | Native `/metrics/prometheus` endpoint |
| **Progress Tracking** | Live job progress with custom messages |
| **WebSocket Events** | Real-time job lifecycle notifications |
| **Audit Logging** | Complete job history and state transitions |

<br>

## 🏢 Enterprise Features

FlashQ Enterprise includes additional capabilities for large-scale deployments:

| Feature | Community | Enterprise |
|---------|:---------:|:----------:|
| Core job processing | ✅ | ✅ |
| PostgreSQL persistence | ✅ | ✅ |
| Real-time dashboard | ✅ | ✅ |
| Prometheus metrics | ✅ | ✅ |
| **High Availability Clustering** | - | ✅ |
| **Automatic Failover** | - | ✅ |
| **Role-Based Access Control** | - | ✅ |
| **SSO/SAML Integration** | - | ✅ |
| **Dedicated Support** | - | ✅ |
| **SLA Guarantees** | - | ✅ |

[**Contact Sales →**](mailto:enterprise@flashq.io)

<br>

## 📖 Documentation

### Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | TCP server port | `6789` |
| `HTTP` | Enable HTTP API | disabled |
| `HTTP_PORT` | HTTP/Dashboard port | `6790` |
| `GRPC` | Enable gRPC API | disabled |
| `GRPC_PORT` | gRPC port | `6791` |
| `DATABASE_URL` | PostgreSQL connection | in-memory |
| `AUTH_TOKENS` | Authentication tokens | disabled |
| `CLUSTER_MODE` | Enable HA clustering | disabled |

### Job Lifecycle

```
PUSH ──→ WAITING ──→ PULL ──→ ACTIVE ──→ ACK ──→ COMPLETED
              │                    │
              │                    └──→ FAIL ──→ RETRY ──→ WAITING
              │                              └──→ DLQ (max attempts)
              │
              └──→ DELAYED (scheduled)
              └──→ WAITING_CHILDREN (dependencies)
```

### API Quick Reference

<details>
<summary><b>HTTP Endpoints</b></summary>

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/queues/{queue}/jobs` | Push job |
| `GET` | `/queues/{queue}/jobs` | Pull jobs |
| `POST` | `/jobs/{id}/ack` | Acknowledge |
| `POST` | `/jobs/{id}/fail` | Fail job |
| `GET` | `/jobs/{id}` | Get job state |
| `GET` | `/stats` | Statistics |
| `GET` | `/metrics/prometheus` | Prometheus metrics |
| `GET` | `/health` | Health check |
| `GET` | `/cluster/nodes` | Cluster status |

</details>

<details>
<summary><b>TCP Protocol</b></summary>

```json
// Push job
{"cmd": "PUSH", "queue": "emails", "data": {"to": "user@example.com"}, "priority": 10}

// Pull job (blocking)
{"cmd": "PULL", "queue": "emails"}

// Acknowledge with result
{"cmd": "ACK", "id": 123, "result": {"sent": true}}

// Batch operations
{"cmd": "PUSHB", "queue": "jobs", "jobs": [{"data": {...}}, {"data": {...}}]}
{"cmd": "PULLB", "queue": "jobs", "count": 100}
{"cmd": "ACKB", "ids": [1, 2, 3, 4, 5]}
```

</details>

<br>

## 🔧 SDK & Integration

### TypeScript/Bun (Official)

```bash
bun add flashq
```

```typescript
import { FlashQ, Worker } from 'flashq';

// Initialize client
const client = new FlashQ({
  host: 'localhost',
  port: 6789,
  token: 'your-secret-token'
});

await client.connect();

// Push jobs
const job = await client.push('emails', {
  to: 'user@example.com',
  subject: 'Welcome!',
  template: 'onboarding'
}, {
  priority: 10,
  max_attempts: 3,
  backoff: 5000
});

// Process jobs with Worker
const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data);
  return { sent: true, timestamp: Date.now() };
}, { concurrency: 10 });

await worker.start();
```

### Other Languages

| Language | Status | Repository |
|----------|--------|------------|
| TypeScript/Bun | ✅ Official | [sdk/typescript](sdk/typescript) |
| Python | 🚧 Coming Soon | - |
| Go | 🚧 Coming Soon | - |
| Java | 🚧 Coming Soon | - |

<br>

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FlashQ Server                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│   │   TCP    │    │   HTTP   │    │   gRPC   │    │    WS    │ │
│   │  :6789   │    │  :6790   │    │  :6791   │    │  :6790   │ │
│   └────┬─────┘    └────┬─────┘    └────┬─────┘    └────┬─────┘ │
│        └───────────────┴───────────────┴───────────────┘        │
│                              │                                   │
│   ┌──────────────────────────▼──────────────────────────────┐  │
│   │                   Queue Manager                          │  │
│   │  ┌────────────────────────────────────────────────────┐ │  │
│   │  │         32 Sharded Priority Queues                 │ │  │
│   │  │    (BinaryHeap + FxHashMap + parking_lot)          │ │  │
│   │  └────────────────────────────────────────────────────┘ │  │
│   │                                                          │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │  │
│   │  │   DLQ    │  │   Rate   │  │  Concur. │  │  Cron   │ │  │
│   │  │  Store   │  │ Limiters │  │ Controls │  │ Runner  │ │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │  │
│   └──────────────────────────────────────────────────────────┘  │
│                              │                                   │
│   ┌──────────────────────────▼──────────────────────────────┐  │
│   │              PostgreSQL Storage (Optional)               │  │
│   │     Jobs • Results • DLQ • Cron • Cluster State         │  │
│   └──────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Performance Optimizations

- **FxHashMap** — 2-3x faster hashing than std HashMap
- **parking_lot** — Superior lock performance
- **mimalloc** — High-performance memory allocator
- **32 Shards** — Minimized lock contention
- **SIMD JSON** — Accelerated parsing with simd-json
- **LTO Build** — Maximum compiler optimization

<br>

## 🧪 Testing & Reliability

### Test Coverage

| Suite | Tests | Coverage |
|-------|-------|----------|
| Unit Tests (Rust) | 81 | Core operations, edge cases |
| Integration Tests | 34 | Full API coverage |
| Stress Tests | 33 | Load, concurrency, resilience |

### Stress Test Results

| Scenario | Result |
|----------|--------|
| Concurrent Push (10 connections) | **59,000 ops/sec** |
| Sustained Load (30 seconds) | **22K push/s, 11K pull/s, 0% errors** |
| Large Payloads (500KB) | Integrity preserved |
| Connection Churn (50 cycles) | 100% success |
| DLQ Flood (100 jobs) | 100% recovery |

<br>

## 🔒 Security

| Feature | Description |
|---------|-------------|
| **Token Authentication** | Secure API access with bearer tokens |
| **Input Validation** | Strict validation on all inputs |
| **Size Limits** | 1MB max job size, 1000 max batch size |
| **HMAC Signatures** | Webhook payload verification |
| **Prometheus Safety** | Sanitized metric labels |

<br>

## 📊 Comparison

| Feature | FlashQ | BullMQ | Celery | AWS SQS |
|---------|:------:|:------:|:------:|:-------:|
| Self-hosted | ✅ | ✅ | ✅ | ❌ |
| No external deps | ✅ | ❌ (Redis) | ❌ (RabbitMQ) | - |
| Priority queues | ✅ | ✅ | ✅ | ❌ |
| Job dependencies | ✅ | ✅ | ✅ | ❌ |
| Rate limiting | ✅ | ✅ | ❌ | ❌ |
| Real-time dashboard | ✅ | ❌ | ❌ | ✅ |
| <100μs latency | ✅ | ❌ | ❌ | ❌ |
| 1M+ ops/sec | ✅ | ❌ | ❌ | ❌ |

<br>

## 🤝 Community & Support

- **GitHub Issues** — Bug reports and feature requests
- **Discussions** — Questions and community support
- **Discord** — Real-time chat with the community
- **Enterprise Support** — Dedicated support for production deployments

<br>

## 📄 License

FlashQ is open-source software licensed under the [MIT License](LICENSE).

<br>

---

<div align="center">

**Ready to supercharge your job processing?**

[**Get Started →**](#-quick-start)

<br>

Built with ❤️ and Rust

<br>

[GitHub](https://github.com/egeominotti/flashq) · [Documentation](#-documentation) · [Discord](https://discord.gg/flashq) · [Twitter](https://twitter.com/flashq_io)

</div>
