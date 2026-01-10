# MagicQueue Python SDK

High-performance Python client for MagicQueue server.

## Installation

```bash
pip install magicqueue
```

## Quick Start

### Producer

```python
from magicqueue import Queue

# Connect to server
queue = Queue("emails")

# Push a job
job_id = queue.push({"to": "user@example.com", "subject": "Hello"})

# With options
job_id = queue.push(
    {"task": "process"},
    priority=10,           # Higher = more urgent
    delay=5000,            # Run after 5 seconds
    ttl=60000,             # Expire after 60 seconds
    max_attempts=3,        # Retry up to 3 times
    backoff=1000,          # Exponential backoff starting at 1s
    unique_key="task-123", # Deduplication key
)

# Batch push
ids = queue.push_batch([
    {"data": {"to": "a@test.com"}},
    {"data": {"to": "b@test.com"}, "priority": 5},
])

# Close connection
queue.close()
```

### Worker

```python
from magicqueue import Worker, Job, JobContext

def handler(job: Job, ctx: JobContext):
    print(f"Processing job {job.id}: {job.data}")

    # Update progress
    ctx.update_progress(50, "Half done")

    # Do work...

    ctx.update_progress(100, "Complete")

# Start worker
worker = Worker("emails", handler, batch_size=50)
worker.start()
```

### Context Manager

```python
from magicqueue import Queue, Worker

# Queue with context manager
with Queue("emails") as queue:
    queue.push({"task": "test"})

# Worker with context manager
with Worker("emails", handler) as worker:
    worker.start()
```

## Features

### Dead Letter Queue

```python
# Jobs that fail max_attempts times go to DLQ
job_id = queue.push({"task": "risky"}, max_attempts=3, backoff=1000)

# View DLQ
dlq_jobs = queue.get_dlq(count=10)

# Retry all DLQ jobs
retried = queue.retry_dlq()

# Retry specific job
queue.retry_dlq(job_id=123)
```

### Job Dependencies

```python
# Job B runs only after Job A completes
job_a = queue.push({"step": "first"})
job_b = queue.push({"step": "second"}, depends_on=[job_a])
```

### Cron Jobs

```python
# Run every 60 seconds
queue.add_cron("cleanup", "*/60", {"task": "cleanup"})

# List cron jobs
crons = queue.list_crons()

# Delete cron
queue.delete_cron("cleanup")
```

### Rate Limiting

```python
# Limit to 100 jobs/second
queue.set_rate_limit(100)

# Clear rate limit
queue.clear_rate_limit()
```

### Metrics & Stats

```python
# Basic stats
stats = queue.stats()
print(f"Pending: {stats.queued}, Processing: {stats.processing}, DLQ: {stats.dlq}")

# Detailed metrics
metrics = queue.metrics()
print(f"Throughput: {metrics.jobs_per_second}/s, Avg latency: {metrics.avg_latency_ms}ms")
```

### Job Cancellation

```python
job_id = queue.push({"task": "maybe"}, delay=10000)
queue.cancel(job_id)
```

## Unix Socket

For lower latency:

```python
queue = Queue("emails", unix_socket="/tmp/magic-queue.sock")
```

## License

MIT
