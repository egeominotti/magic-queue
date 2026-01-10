#!/usr/bin/env python3
"""Example producer for MagicQueue."""

from magicqueue import Queue


def main():
    with Queue("demo") as queue:
        # Simple push
        job_id = queue.push({"message": "Hello, World!"})
        print(f"Pushed job {job_id}")

        # With priority
        job_id = queue.push({"task": "urgent"}, priority=10)
        print(f"Pushed urgent job {job_id}")

        # Delayed job
        job_id = queue.push({"task": "later"}, delay=5000)
        print(f"Pushed delayed job {job_id}")

        # With retry options
        job_id = queue.push(
            {"task": "retry-test"},
            max_attempts=3,
            backoff=1000,  # Exponential backoff
        )
        print(f"Pushed job with retry {job_id}")

        # Batch push
        ids = queue.push_many(
            [{"i": i} for i in range(100)],
            priority=0,
        )
        print(f"Pushed batch of {len(ids)} jobs")

        # Stats
        stats = queue.stats()
        print(f"Stats: queued={stats.queued}, processing={stats.processing}")


if __name__ == "__main__":
    main()
