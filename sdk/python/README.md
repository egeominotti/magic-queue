# FlashQ Python SDK

High-performance async job queue client for Python.

## Installation

```bash
pip install flashq

# With HTTP support
pip install flashq[http]
```

## Quick Start

```python
import asyncio
from flashq import FlashQ

async def main():
    async with FlashQ() as client:
        # Push a job
        job = await client.push("emails", {
            "to": "user@example.com",
            "subject": "Hello!"
        })
        print(f"Created job {job.id}")

        # Get stats
        stats = await client.stats()
        print(f"Queued: {stats.queued}")

asyncio.run(main())
```

## Worker Example

```python
import asyncio
from flashq import Worker

worker = Worker("emails", concurrency=5)

@worker.process
async def handle_email(job):
    print(f"Sending email to {job.data['to']}")
    # ... send email ...
    return {"sent": True}

asyncio.run(worker.run())
```

## Features

### Push Jobs

```python
from flashq import FlashQ, PushOptions

async with FlashQ() as client:
    # Simple push
    job = await client.push("queue", {"data": "value"})

    # With options
    job = await client.push("queue", {"data": "value"}, PushOptions(
        priority=10,          # Higher = processed first
        delay=5000,           # Delay in ms
        max_attempts=3,       # Retry attempts
        backoff=1000,         # Backoff base (exponential)
        timeout=30000,        # Processing timeout
        ttl=3600000,          # Time-to-live
        unique_key="key123",  # Deduplication
        depends_on=[1, 2],    # Job dependencies
        tags=["important"]    # Tags for filtering
    ))

    # Batch push
    ids = await client.push_batch("queue", [
        {"data": {"task": 1}},
        {"data": {"task": 2}, "priority": 5},
    ])
```

### Pull & Process Jobs

```python
async with FlashQ() as client:
    # Pull single job (blocking)
    job = await client.pull("queue")

    # Pull batch
    jobs = await client.pull_batch("queue", count=10)

    # Acknowledge completion
    await client.ack(job.id, result={"success": True})

    # Fail job (will retry or go to DLQ)
    await client.fail(job.id, error="Something went wrong")
```

### Job Progress

```python
async with FlashQ() as client:
    # Update progress
    await client.progress(job.id, 50, "Halfway done")

    # Get progress
    prog = await client.get_progress(job.id)
    print(f"Progress: {prog.progress}% - {prog.message}")
```

### Queue Control

```python
async with FlashQ() as client:
    # Pause/Resume
    await client.pause("queue")
    await client.resume("queue")

    # Rate limiting (jobs per second)
    await client.set_rate_limit("queue", 100)
    await client.clear_rate_limit("queue")

    # Concurrency limiting
    await client.set_concurrency("queue", 5)
    await client.clear_concurrency("queue")

    # List queues
    queues = await client.list_queues()
    for q in queues:
        print(f"{q.name}: {q.pending} pending, {q.processing} processing")
```

### Dead Letter Queue

```python
async with FlashQ() as client:
    # Get failed jobs
    failed = await client.get_dlq("queue", count=100)

    # Retry all DLQ jobs
    count = await client.retry_dlq("queue")

    # Retry specific job
    await client.retry_dlq("queue", job_id=123)
```

### Cron Jobs

```python
from flashq import CronOptions

async with FlashQ() as client:
    # Add cron job (runs every minute)
    await client.add_cron("cleanup", CronOptions(
        queue="maintenance",
        data={"task": "cleanup"},
        schedule="0 * * * * *",  # sec min hour day month weekday
        priority=5
    ))

    # List cron jobs
    crons = await client.list_crons()

    # Delete cron job
    await client.delete_cron("cleanup")
```

### Metrics

```python
async with FlashQ() as client:
    # Basic stats
    stats = await client.stats()
    print(f"Queued: {stats.queued}")
    print(f"Processing: {stats.processing}")
    print(f"Delayed: {stats.delayed}")
    print(f"DLQ: {stats.dlq}")

    # Detailed metrics
    metrics = await client.metrics()
    print(f"Total pushed: {metrics.total_pushed}")
    print(f"Jobs/sec: {metrics.jobs_per_second}")
    print(f"Avg latency: {metrics.avg_latency_ms}ms")
```

## Worker Pool

Process multiple queues with different handlers:

```python
from flashq import WorkerPool

async def handle_emails(job):
    print(f"Email: {job.data}")

async def handle_notifications(job):
    print(f"Notification: {job.data}")

pool = WorkerPool(host="localhost", port=6789)
pool.add_worker("emails", handler=handle_emails, concurrency=5)
pool.add_worker("notifications", handler=handle_notifications, concurrency=3)

await pool.run()
```

## Connection Options

```python
from flashq import FlashQ

# TCP (default)
client = FlashQ(host="localhost", port=6789)

# With authentication
client = FlashQ(host="localhost", port=6789, token="secret")

# HTTP mode (requires aiohttp)
client = FlashQ(host="localhost", http_port=6790, use_http=True)

# Unix socket
client = FlashQ(socket_path="/tmp/flashq.sock")
```

## License

MIT
