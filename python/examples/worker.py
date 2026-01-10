#!/usr/bin/env python3
"""Example worker for MagicQueue."""

import time
from magicqueue import Worker, Job, JobContext


def handler(job: Job, ctx: JobContext):
    """Process a job."""
    print(f"Processing job {job.id}: {job.data}")

    # Simulate work with progress updates
    for i in range(5):
        time.sleep(0.1)
        progress = (i + 1) * 20
        ctx.update_progress(progress, f"Step {i + 1}/5")

    print(f"Completed job {job.id}")


def main():
    worker = Worker(
        "demo",
        handler,
        batch_size=10,
        concurrency=4,
    )

    print("Starting worker... Press Ctrl+C to stop")

    try:
        worker.start()
    except KeyboardInterrupt:
        print("\nStopping worker...")
        worker.stop()


if __name__ == "__main__":
    main()
