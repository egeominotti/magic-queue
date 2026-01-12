# flashQ API Reference

Complete API documentation for HTTP REST, gRPC, and TCP Protocol.

## Table of Contents

- [HTTP REST API](#http-rest-api)
- [gRPC API](#grpc-api)
- [TCP Protocol](#tcp-protocol)
- [WebSocket Events](#websocket-events)
- [SSE Events](#sse-events)

---

## HTTP REST API

Base URL: `http://localhost:6790`

All responses follow the format:
```json
{
  "ok": true,
  "data": { ... },
  "error": null
}
```

---

### Queue Operations

#### List Queues
```http
GET /queues
```

**Response:**
```json
{
  "ok": true,
  "data": [
    {
      "name": "emails",
      "pending": 150,
      "processing": 5,
      "dlq": 2,
      "paused": false
    }
  ]
}
```

---

#### Push Job
```http
POST /queues/{queue}/jobs
Content-Type: application/json

{
  "data": { "email": "user@example.com", "template": "welcome" },
  "priority": 10,
  "delay": 5000,
  "ttl": 3600000,
  "timeout": 30000,
  "max_attempts": 3,
  "backoff": 1000,
  "unique_key": "user-123-welcome",
  "depends_on": [1, 2, 3],
  "tags": ["email", "marketing"],
  "lifo": false
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | object | required | Job payload (max 1MB) |
| `priority` | integer | 0 | Higher = processed first |
| `delay` | integer | 0 | Delay in ms before job is available |
| `ttl` | integer | null | Time-to-live in ms |
| `timeout` | integer | null | Processing timeout in ms |
| `max_attempts` | integer | 1 | Max retry attempts |
| `backoff` | integer | 1000 | Base backoff for retries (ms) |
| `unique_key` | string | null | Deduplication key |
| `depends_on` | array | null | Job IDs that must complete first |
| `tags` | array | null | Tags for filtering |
| `lifo` | boolean | false | Last-in-first-out mode |

**Response:**
```json
{
  "ok": true,
  "data": {
    "id": 12345,
    "queue": "emails",
    "data": { "email": "user@example.com" },
    "priority": 10,
    "created_at": 1704067200000,
    "run_at": 1704067205000,
    "attempts": 0,
    "max_attempts": 3
  }
}
```

---

#### Pull Jobs
```http
GET /queues/{queue}/jobs?count=10
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `count` | integer | 1 | Number of jobs to pull (max 1000) |

**Response:**
```json
{
  "ok": true,
  "data": [
    {
      "id": 12345,
      "queue": "emails",
      "data": { "email": "user@example.com" },
      "priority": 10,
      "created_at": 1704067200000,
      "started_at": 1704067210000,
      "attempts": 1
    }
  ]
}
```

---

#### Pause Queue
```http
POST /queues/{queue}/pause
```

---

#### Resume Queue
```http
POST /queues/{queue}/resume
```

---

#### Get Dead Letter Queue
```http
GET /queues/{queue}/dlq?count=100
```

---

#### Retry DLQ Jobs
```http
POST /queues/{queue}/dlq/retry
```

Moves all jobs from DLQ back to the queue for reprocessing.

---

#### Set Rate Limit
```http
POST /queues/{queue}/rate-limit
Content-Type: application/json

{
  "limit": 100
}
```

Limits queue to N jobs per second.

---

#### Clear Rate Limit
```http
DELETE /queues/{queue}/rate-limit
```

---

#### Set Concurrency Limit
```http
POST /queues/{queue}/concurrency
Content-Type: application/json

{
  "limit": 5
}
```

Limits concurrent processing to N jobs.

---

#### Clear Concurrency Limit
```http
DELETE /queues/{queue}/concurrency
```

---

### Job Operations

#### List Jobs (Browser)
```http
GET /jobs?queue=emails&state=active&limit=100&offset=0
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `queue` | string | null | Filter by queue name |
| `state` | string | null | Filter by state: `waiting`, `delayed`, `active`, `completed`, `failed`, `waiting-children` |
| `limit` | integer | 100 | Max jobs to return |
| `offset` | integer | 0 | Pagination offset |

---

#### Get Job
```http
GET /jobs/{id}
```

**Response:**
```json
{
  "ok": true,
  "data": {
    "id": 12345,
    "queue": "emails",
    "data": { "email": "user@example.com" },
    "priority": 10,
    "created_at": 1704067200000,
    "state": "active",
    "result": null
  }
}
```

---

#### Acknowledge Job
```http
POST /jobs/{id}/ack
Content-Type: application/json

{
  "result": { "sent": true, "message_id": "abc123" }
}
```

Marks job as completed. Optional result is stored.

---

#### Fail Job
```http
POST /jobs/{id}/fail
Content-Type: application/json

{
  "error": "SMTP connection failed"
}
```

Retries job with exponential backoff, or moves to DLQ if max_attempts reached.

---

#### Cancel Job
```http
POST /jobs/{id}/cancel
```

Cancels a pending job (not yet processing).

---

#### Update Progress
```http
POST /jobs/{id}/progress
Content-Type: application/json

{
  "progress": 75,
  "message": "Processing step 3 of 4"
}
```

---

#### Get Progress
```http
GET /jobs/{id}/progress
```

**Response:**
```json
{
  "ok": true,
  "data": [75, "Processing step 3 of 4"]
}
```

---

#### Get Result
```http
GET /jobs/{id}/result
```

Returns the result stored when job was acknowledged.

---

### Cron Jobs

#### List Cron Jobs
```http
GET /crons
```

**Response:**
```json
{
  "ok": true,
  "data": [
    {
      "name": "daily-cleanup",
      "queue": "maintenance",
      "schedule": "0 0 3 * * *",
      "data": { "action": "cleanup" },
      "priority": 0,
      "next_run": 1704067200000
    }
  ]
}
```

---

#### Create Cron Job
```http
POST /crons/{name}
Content-Type: application/json

{
  "queue": "maintenance",
  "data": { "action": "cleanup" },
  "schedule": "0 0 3 * * *",
  "priority": 0
}
```

**Schedule Format:** 6-field cron expression
```
┌──────────── second (0-59)
│ ┌────────── minute (0-59)
│ │ ┌──────── hour (0-23)
│ │ │ ┌────── day of month (1-31)
│ │ │ │ ┌──── month (1-12)
│ │ │ │ │ ┌── day of week (0-6, Sun=0)
│ │ │ │ │ │
* * * * * *
```

**Examples:**
- `0 0 * * * *` - Every hour
- `0 30 9 * * 1-5` - 9:30 AM weekdays
- `0 0 0 1 * *` - First day of month

---

#### Delete Cron Job
```http
DELETE /crons/{name}
```

---

### Stats & Metrics

#### Get Stats
```http
GET /stats
```

**Response:**
```json
{
  "ok": true,
  "data": {
    "queued": 1500,
    "processing": 25,
    "delayed": 100,
    "dlq": 5
  }
}
```

---

#### Get Metrics
```http
GET /metrics
```

**Response:**
```json
{
  "ok": true,
  "data": {
    "total_pushed": 1000000,
    "total_completed": 999000,
    "total_failed": 500,
    "jobs_per_second": 1500.5,
    "avg_latency_ms": 45.2,
    "queues": [
      {
        "name": "emails",
        "pending": 150,
        "processing": 5,
        "dlq": 2
      }
    ]
  }
}
```

---

#### Get Metrics History
```http
GET /metrics/history
```

Returns last 60 data points (5 minutes at 5s intervals).

---

#### Prometheus Metrics
```http
GET /metrics/prometheus
```

**Response:**
```
# HELP flashq_jobs_total Total number of jobs
# TYPE flashq_jobs_total counter
flashq_jobs_pushed_total 1000000
flashq_jobs_completed_total 999000
flashq_jobs_failed_total 500

# HELP flashq_jobs_current Current number of jobs by state
# TYPE flashq_jobs_current gauge
flashq_jobs_current{state="queued"} 1500
flashq_jobs_current{state="processing"} 25
flashq_jobs_current{state="delayed"} 100
flashq_jobs_current{state="dlq"} 5

# HELP flashq_throughput_per_second Jobs processed per second
# TYPE flashq_throughput_per_second gauge
flashq_throughput_per_second 1500.50

# HELP flashq_queue_jobs Queue job counts
# TYPE flashq_queue_jobs gauge
flashq_queue_jobs{queue="emails",state="pending"} 150
flashq_queue_jobs{queue="emails",state="processing"} 5
```

---

### Workers

#### List Workers
```http
GET /workers
```

**Response:**
```json
{
  "ok": true,
  "data": [
    {
      "id": "worker-1",
      "queues": ["emails", "notifications"],
      "concurrency": 10,
      "last_heartbeat": 1704067200000,
      "jobs_processed": 5000
    }
  ]
}
```

---

#### Worker Heartbeat
```http
POST /workers/{id}/heartbeat
Content-Type: application/json

{
  "queues": ["emails", "notifications"],
  "concurrency": 10
}
```

---

### Webhooks

#### List Webhooks
```http
GET /webhooks
```

---

#### Create Webhook
```http
POST /webhooks
Content-Type: application/json

{
  "url": "https://example.com/webhook",
  "events": ["job.completed", "job.failed"],
  "queue": "emails",
  "secret": "my-secret-key"
}
```

**Events:**
- `job.completed` - Job finished successfully
- `job.failed` - Job failed (retry or DLQ)
- `job.pushed` - New job added
- `*` - All events

**Webhook Payload:**
```json
{
  "event": "job.completed",
  "queue": "emails",
  "job_id": 12345,
  "timestamp": 1704067200000,
  "data": { ... },
  "error": null
}
```

**Signature Header:**
```
X-FlashQ-Signature: <HMAC-SHA256 of body using secret>
```

---

#### Delete Webhook
```http
DELETE /webhooks/{id}
```

---

#### Incoming Webhook (Push Job)
```http
POST /webhooks/incoming/{queue}
Content-Type: application/json

{
  "any": "data",
  "from": "external-service"
}
```

Creates a job with the request body as data.

---

### Server Management

#### Get Settings
```http
GET /settings
```

**Response:**
```json
{
  "ok": true,
  "data": {
    "version": "0.1.0",
    "tcp_port": 6789,
    "http_port": 6790,
    "database_connected": true,
    "database_url": "postgres://user:****@localhost/flashq",
    "auth_enabled": true,
    "auth_token_count": 2,
    "uptime_seconds": 86400
  }
}
```

---

#### Shutdown Server
```http
POST /server/shutdown
```

---

#### Restart Server
```http
POST /server/restart
```

---

### Health & Cluster

#### Health Check
```http
GET /health
```

**Response:**
```json
{
  "ok": true,
  "data": {
    "status": "healthy",
    "node_id": "node-1",
    "is_leader": true,
    "cluster_enabled": true,
    "postgres_connected": true
  }
}
```

---

#### List Cluster Nodes
```http
GET /cluster/nodes
```

**Response:**
```json
{
  "ok": true,
  "data": [
    {
      "node_id": "node-1",
      "host": "192.168.1.10",
      "port": 6790,
      "is_leader": true,
      "last_heartbeat": 1704067200000,
      "started_at": 1704000000000
    },
    {
      "node_id": "node-2",
      "host": "192.168.1.11",
      "port": 6790,
      "is_leader": false,
      "last_heartbeat": 1704067199000,
      "started_at": 1704000100000
    }
  ]
}
```

---

## gRPC API

Server: `localhost:6791`

### Service Definition

```protobuf
service QueueService {
  // Unary RPCs
  rpc Push(PushRequest) returns (PushResponse);
  rpc PushBatch(PushBatchRequest) returns (PushBatchResponse);
  rpc Pull(PullRequest) returns (Job);
  rpc PullBatch(PullBatchRequest) returns (PullBatchResponse);
  rpc Ack(AckRequest) returns (AckResponse);
  rpc AckBatch(AckBatchRequest) returns (AckBatchResponse);
  rpc Fail(FailRequest) returns (FailResponse);
  rpc GetState(GetStateRequest) returns (GetStateResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);

  // Streaming RPCs
  rpc StreamJobs(StreamJobsRequest) returns (stream Job);
  rpc ProcessJobs(stream JobResult) returns (stream Job);
}
```

---

### Push

```protobuf
message PushRequest {
  string queue = 1;
  bytes data = 2;              // JSON payload
  int32 priority = 3;
  uint64 delay_ms = 4;
  uint64 ttl_ms = 5;
  uint64 timeout_ms = 6;
  uint32 max_attempts = 7;
  uint64 backoff_ms = 8;
  optional string unique_key = 9;
  repeated uint64 depends_on = 10;
  bool lifo = 11;
}

message PushResponse {
  bool ok = 1;
  uint64 id = 2;
}
```

**Example (Node.js):**
```javascript
const response = await client.push({
  queue: 'emails',
  data: Buffer.from(JSON.stringify({ email: 'user@example.com' })),
  priority: 10,
  maxAttempts: 3,
});
console.log('Job ID:', response.id);
```

---

### PushBatch

```protobuf
message PushBatchRequest {
  string queue = 1;
  repeated JobInput jobs = 2;
}

message PushBatchResponse {
  bool ok = 1;
  repeated uint64 ids = 2;
}
```

---

### Pull

```protobuf
message PullRequest {
  string queue = 1;
}

message Job {
  uint64 id = 1;
  string queue = 2;
  bytes data = 3;
  int32 priority = 4;
  uint64 created_at = 5;
  uint64 run_at = 6;
  uint64 started_at = 7;
  uint32 attempts = 8;
  uint32 max_attempts = 9;
  uint64 backoff = 10;
  uint64 ttl = 11;
  uint64 timeout = 12;
  optional string unique_key = 13;
  repeated uint64 depends_on = 14;
  uint32 progress = 15;
  optional string progress_msg = 16;
  bool lifo = 17;
}
```

---

### Ack

```protobuf
message AckRequest {
  uint64 id = 1;
  optional bytes result = 2;
}

message AckResponse {
  bool ok = 1;
}
```

---

### Fail

```protobuf
message FailRequest {
  uint64 id = 1;
  optional string error = 2;
}

message FailResponse {
  bool ok = 1;
}
```

---

### StreamJobs (Server Streaming)

```protobuf
message StreamJobsRequest {
  string queue = 1;
  uint32 batch_size = 2;    // Jobs per batch
  uint32 prefetch = 3;      // Buffer size
}
```

**Example (Node.js):**
```javascript
const stream = client.streamJobs({ queue: 'emails', batchSize: 10 });

for await (const job of stream) {
  console.log('Received job:', job.id);
  // Process job...
  await client.ack({ id: job.id });
}
```

---

### ProcessJobs (Bidirectional Streaming)

```protobuf
message JobResult {
  uint64 id = 1;
  bool success = 2;
  optional bytes result = 3;
  optional string error = 4;
}
```

**Example (Node.js):**
```javascript
const call = client.processJobs();

call.on('data', async (job) => {
  try {
    const result = await processJob(job);
    call.write({ id: job.id, success: true, result: Buffer.from(JSON.stringify(result)) });
  } catch (err) {
    call.write({ id: job.id, success: false, error: err.message });
  }
});
```

---

### Job States

```protobuf
enum JobState {
  JOB_STATE_UNKNOWN = 0;
  JOB_STATE_WAITING = 1;
  JOB_STATE_DELAYED = 2;
  JOB_STATE_ACTIVE = 3;
  JOB_STATE_COMPLETED = 4;
  JOB_STATE_FAILED = 5;
  JOB_STATE_WAITING_CHILDREN = 6;
}
```

---

## TCP Protocol

Server: `localhost:6789`

JSON-based protocol over TCP. Each command is a single JSON line terminated by `\n`.

### Authentication

```json
{"cmd":"AUTH","token":"your-secret-token"}
```

Response:
```json
{"ok":true}
```

---

### Commands

#### PUSH
```json
{
  "cmd": "PUSH",
  "queue": "emails",
  "data": {"email": "user@example.com"},
  "priority": 10,
  "delay": 5000,
  "ttl": 3600000,
  "timeout": 30000,
  "max_attempts": 3,
  "backoff": 1000,
  "unique_key": "user-123",
  "depends_on": [1, 2],
  "tags": ["email"],
  "lifo": false
}
```

Response:
```json
{"ok":true,"id":12345}
```

---

#### PUSHB (Batch Push)
```json
{
  "cmd": "PUSHB",
  "queue": "emails",
  "jobs": [
    {"data": {"email": "a@example.com"}},
    {"data": {"email": "b@example.com"}, "priority": 5}
  ]
}
```

Response:
```json
{"ok":true,"ids":[12345,12346]}
```

---

#### PULL
```json
{"cmd":"PULL","queue":"emails"}
```

Response:
```json
{
  "ok": true,
  "job": {
    "id": 12345,
    "queue": "emails",
    "data": {"email": "user@example.com"},
    "priority": 10,
    "created_at": 1704067200000
  }
}
```

---

#### PULLB (Batch Pull)
```json
{"cmd":"PULLB","queue":"emails","count":10}
```

---

#### ACK
```json
{"cmd":"ACK","id":12345,"result":{"sent":true}}
```

---

#### ACKB (Batch Ack)
```json
{"cmd":"ACKB","ids":[12345,12346,12347]}
```

---

#### FAIL
```json
{"cmd":"FAIL","id":12345,"error":"Connection timeout"}
```

---

#### CANCEL
```json
{"cmd":"CANCEL","id":12345}
```

---

#### PROGRESS
```json
{"cmd":"PROGRESS","id":12345,"progress":75,"message":"Step 3/4"}
```

---

#### GETPROGRESS
```json
{"cmd":"GETPROGRESS","id":12345}
```

---

#### GETJOB
```json
{"cmd":"GETJOB","id":12345}
```

---

#### GETSTATE
```json
{"cmd":"GETSTATE","id":12345}
```

---

#### GETRESULT
```json
{"cmd":"GETRESULT","id":12345}
```

---

#### DLQ
```json
{"cmd":"DLQ","queue":"emails","count":100}
```

---

#### RETRYDLQ
```json
{"cmd":"RETRYDLQ","queue":"emails","id":12345}
```

---

#### RATELIMIT
```json
{"cmd":"RATELIMIT","queue":"emails","limit":100}
```

---

#### RATELIMITCLEAR
```json
{"cmd":"RATELIMITCLEAR","queue":"emails"}
```

---

#### SETCONCURRENCY
```json
{"cmd":"SETCONCURRENCY","queue":"emails","limit":5}
```

---

#### CLEARCONCURRENCY
```json
{"cmd":"CLEARCONCURRENCY","queue":"emails"}
```

---

#### PAUSE
```json
{"cmd":"PAUSE","queue":"emails"}
```

---

#### RESUME
```json
{"cmd":"RESUME","queue":"emails"}
```

---

#### LISTQUEUES
```json
{"cmd":"LISTQUEUES"}
```

---

#### CRON
```json
{
  "cmd": "CRON",
  "name": "daily-cleanup",
  "queue": "maintenance",
  "data": {"action": "cleanup"},
  "schedule": "0 0 3 * * *",
  "priority": 0
}
```

---

#### CRONDELETE
```json
{"cmd":"CRONDELETE","name":"daily-cleanup"}
```

---

#### CRONLIST
```json
{"cmd":"CRONLIST"}
```

---

#### STATS
```json
{"cmd":"STATS"}
```

---

#### METRICS
```json
{"cmd":"METRICS"}
```

---

## WebSocket Events

Connect: `ws://localhost:6790/ws?token=your-token`

Queue-specific: `ws://localhost:6790/ws/{queue}?token=your-token`

### Event Format

```json
{
  "event_type": "job.completed",
  "queue": "emails",
  "job_id": 12345,
  "timestamp": 1704067200000,
  "data": { ... },
  "error": null
}
```

### Event Types

| Event | Description |
|-------|-------------|
| `job.pushed` | New job added to queue |
| `job.started` | Job processing started |
| `job.completed` | Job finished successfully |
| `job.failed` | Job failed (will retry or DLQ) |
| `job.progress` | Job progress updated |

---

## SSE Events

Connect: `GET /events`

Queue-specific: `GET /events/{queue}`

### Event Format

```
event: job.completed
data: {"event_type":"job.completed","queue":"emails","job_id":12345}
```

---

## Error Handling

All APIs return errors in the format:

```json
{
  "ok": false,
  "data": null,
  "error": "Job 12345 not found"
}
```

### Common Errors

| Error | Description |
|-------|-------------|
| `Job X not found` | Job doesn't exist or already processed |
| `Queue paused` | Queue is paused, jobs not being processed |
| `Rate limited` | Queue rate limit exceeded |
| `Authentication required` | Missing or invalid auth token |
| `Invalid queue name` | Queue name contains invalid characters |
| `Payload too large` | Job data exceeds 1MB limit |
| `Duplicate job` | Job with same unique_key already exists |

---

## Authentication

### HTTP

Add header:
```
Authorization: Bearer your-token
```

### WebSocket

Add query parameter:
```
ws://localhost:6790/ws?token=your-token
```

### TCP

Send AUTH command first:
```json
{"cmd":"AUTH","token":"your-token"}
```

### gRPC

Add metadata:
```javascript
const metadata = new grpc.Metadata();
metadata.add('authorization', 'Bearer your-token');
client.push(request, metadata, callback);
```

---

## Rate Limits

| Resource | Limit |
|----------|-------|
| Job payload size | 1 MB |
| Batch size (push/pull/ack) | 1000 jobs |
| Queue name length | 256 characters |
| Cron schedule length | 256 characters |
| Interned queue names | 10,000 unique |

---

## Performance Tips

1. **Use batch operations** - `PUSHB`, `PULLB`, `ACKB` are 10-100x faster than single operations
2. **Use gRPC streaming** - For high-throughput workers, use `StreamJobs` or `ProcessJobs`
3. **Set appropriate timeouts** - Avoid zombie jobs with `timeout` parameter
4. **Use priorities** - Critical jobs should have higher priority
5. **Enable connection pooling** - Reuse TCP/gRPC connections
