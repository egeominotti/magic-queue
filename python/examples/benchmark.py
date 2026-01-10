#!/usr/bin/env python3
"""Benchmark for MagicQueue Python SDK."""

import time
import threading
from magicqueue import Queue, Worker, Job, JobContext

TOTAL_JOBS = 10000
BATCH_SIZE = 1000
WORKER_BATCH_SIZE = 50

completed = 0
lock = threading.Lock()


def handler(job: Job, ctx: JobContext):
    global completed
    # Minimal work
    _ = job.data
    with lock:
        completed += 1


def main():
    global completed

    print("=== MagicQueue Python Benchmark ===\n")
    print(f"Jobs: {TOTAL_JOBS:,}")
    print(f"Push batch size: {BATCH_SIZE:,}")
    print(f"Worker batch size: {WORKER_BATCH_SIZE}")
    print()

    # Start worker in background thread
    worker = Worker("benchmark", handler, batch_size=WORKER_BATCH_SIZE)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()
    time.sleep(0.3)

    # Batch push
    print("--- Batch Push ---")
    queue = Queue("benchmark")
    queue.connect()

    push_start = time.time()

    for batch_start in range(0, TOTAL_JOBS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, TOTAL_JOBS)
        jobs = [{"data": {"i": i}} for i in range(batch_start, batch_end)]
        queue.push_batch(jobs)

    push_duration = time.time() - push_start
    push_throughput = TOTAL_JOBS / push_duration

    print(f"Push duration: {push_duration:.2f}s")
    print(f"Push throughput: {push_throughput:,.0f} jobs/sec")
    print()

    queue.close()

    # Wait for processing
    print("--- Processing ---")
    process_start = time.time()

    while completed < TOTAL_JOBS:
        pct = (completed / TOTAL_JOBS) * 100
        elapsed = time.time() - process_start
        rate = completed / elapsed if elapsed > 0 else 0
        print(f"\rProcessed: {completed:,} ({pct:.1f}%) - {rate:,.0f}/s", end="", flush=True)
        time.sleep(0.5)

    process_duration = time.time() - process_start
    process_throughput = TOTAL_JOBS / process_duration

    print(f"\rProcess duration: {process_duration:.2f}s                    ")
    print(f"Process throughput: {process_throughput:,.0f} jobs/sec")
    print()

    # Total
    total_duration = time.time() - push_start
    print("=== RESULTS ===")
    print(f"Total: {total_duration:.2f}s")
    print(f"Throughput: {TOTAL_JOBS / total_duration:,.0f} jobs/sec")

    worker.stop()


if __name__ == "__main__":
    main()
