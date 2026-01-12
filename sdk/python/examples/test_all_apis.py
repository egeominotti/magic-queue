#!/usr/bin/env python3
"""
FlashQ Python SDK - Comprehensive API Test

Tests all SDK functionality against a running FlashQ server.

Usage:
    python examples/test_all_apis.py

Requirements:
    - FlashQ server running on localhost:6789 (TCP) and localhost:6790 (HTTP)
    - Run with: HTTP=1 cargo run --release
"""
import asyncio
import sys
import time
from pathlib import Path

# Add parent directory to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent))

from flashq import (
    FlashQ, PushOptions, CronOptions, JobState,
    FlashQError, ConnectionError, TimeoutError
)


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    async def test(self, name: str, coro):
        """Run a single test"""
        try:
            await coro
            self.passed += 1
            print(f"  ✓ {name}")
        except AssertionError as e:
            self.failed += 1
            self.errors.append((name, str(e)))
            print(f"  ✗ {name}: {e}")
        except Exception as e:
            self.failed += 1
            self.errors.append((name, str(e)))
            print(f"  ✗ {name}: {type(e).__name__}: {e}")

    def summary(self):
        """Print test summary"""
        total = self.passed + self.failed
        print()
        print("=" * 60)
        if self.failed == 0:
            print(f"  ✓ All {total} tests passed!")
        else:
            print(f"  {self.passed}/{total} tests passed, {self.failed} failed")
            print()
            print("  Failed tests:")
            for name, error in self.errors:
                print(f"    - {name}: {error}")
        print("=" * 60)
        return self.failed == 0


async def main():
    print("FlashQ Python SDK - API Tests")
    print("=" * 60)

    runner = TestRunner()

    # ==================== TCP Tests ====================
    print("\n[TCP Protocol Tests]")

    client = FlashQ(host="localhost", port=6789)

    # Connection
    await runner.test("connect", test_connect(client))

    # Core Operations
    job_id = None

    async def test_push():
        nonlocal job_id
        job = await client.push("test-queue", {"message": "hello"})
        job_id = job.id
        assert job.id > 0, f"Expected job ID > 0, got {job.id}"
        assert job.queue == "test-queue"

    await runner.test("push", test_push())

    async def test_push_with_options():
        job = await client.push("test-queue", {"task": "process"}, PushOptions(
            priority=10,
            max_attempts=3,
            tags=["important"]
        ))
        assert job.priority == 10

    await runner.test("push with options", test_push_with_options())

    async def test_push_batch():
        ids = await client.push_batch("batch-queue", [
            {"data": {"n": 1}},
            {"data": {"n": 2}},
            {"data": {"n": 3}},
        ])
        assert len(ids) == 3, f"Expected 3 IDs, got {len(ids)}"

    await runner.test("push_batch", test_push_batch())

    async def test_pull_batch():
        jobs = await client.pull_batch("batch-queue", 3)
        assert len(jobs) == 3, f"Expected 3 jobs, got {len(jobs)}"

    await runner.test("pull_batch", test_pull_batch())

    async def test_ack_batch():
        # Push some jobs first
        ids = await client.push_batch("ack-batch-queue", [
            {"data": {"n": 1}},
            {"data": {"n": 2}},
        ])
        jobs = await client.pull_batch("ack-batch-queue", 2)
        job_ids = [j.id for j in jobs]
        count = await client.ack_batch(job_ids)
        assert count >= 0

    await runner.test("ack_batch", test_ack_batch())

    async def test_pull_and_ack():
        # Push a job
        job = await client.push("pull-test", {"value": 42})
        # Pull it
        pulled = await client.pull("pull-test")
        assert pulled.data["value"] == 42
        # Ack it
        await client.ack(pulled.id, result={"processed": True})

    await runner.test("pull and ack", test_pull_and_ack())

    async def test_fail():
        job = await client.push("fail-test", {"will": "fail"}, PushOptions(max_attempts=2))
        pulled = await client.pull("fail-test")
        await client.fail(pulled.id, "Test failure")

    await runner.test("fail", test_fail())

    async def test_progress():
        job = await client.push("progress-test", {"task": "long"})
        pulled = await client.pull("progress-test")
        await client.progress(pulled.id, 50, "Halfway there")
        prog = await client.get_progress(pulled.id)
        assert prog.progress == 50, f"Expected 50, got {prog.progress}"
        assert prog.message == "Halfway there"
        await client.ack(pulled.id)

    await runner.test("progress", test_progress())

    async def test_cancel():
        job = await client.push("cancel-test", {"will": "cancel"})
        await client.cancel(job.id)

    await runner.test("cancel", test_cancel())

    async def test_get_state():
        job = await client.push("state-test", {"data": 1})
        state = await client.get_state(job.id)
        assert state in [JobState.WAITING, JobState.DELAYED], f"Unexpected state: {state}"

    await runner.test("get_state", test_get_state())

    async def test_get_result():
        job = await client.push("result-test", {"data": 1})
        pulled = await client.pull("result-test")
        await client.ack(pulled.id, result={"answer": 42})
        result = await client.get_result(pulled.id)
        assert result["answer"] == 42

    await runner.test("get_result", test_get_result())

    # Queue Control
    async def test_pause_resume():
        await client.pause("pause-test")
        await client.resume("pause-test")

    await runner.test("pause/resume", test_pause_resume())

    async def test_rate_limit():
        await client.set_rate_limit("rate-test", 100)
        await client.clear_rate_limit("rate-test")

    await runner.test("rate_limit", test_rate_limit())

    async def test_concurrency():
        await client.set_concurrency("conc-test", 5)
        await client.clear_concurrency("conc-test")

    await runner.test("concurrency", test_concurrency())

    async def test_list_queues():
        queues = await client.list_queues()
        assert isinstance(queues, list)
        assert len(queues) > 0

    await runner.test("list_queues", test_list_queues())

    # DLQ
    async def test_dlq():
        # Create a job that will fail
        job = await client.push("dlq-test", {"fail": True}, PushOptions(max_attempts=1))
        pulled = await client.pull("dlq-test")
        await client.fail(pulled.id, "Intentional failure")
        # Get DLQ jobs
        dlq_jobs = await client.get_dlq("dlq-test")
        assert isinstance(dlq_jobs, list)

    await runner.test("dlq", test_dlq())

    async def test_retry_dlq():
        count = await client.retry_dlq("dlq-test")
        assert count >= 0

    await runner.test("retry_dlq", test_retry_dlq())

    # Cron
    async def test_add_cron():
        await client.add_cron("test-cron", CronOptions(
            queue="cron-queue",
            data={"scheduled": True},
            schedule="0 0 * * * *",  # Every hour
            priority=5
        ))

    await runner.test("add_cron", test_add_cron())

    async def test_list_crons():
        crons = await client.list_crons()
        assert isinstance(crons, list)
        # Find our cron
        found = any(c.name == "test-cron" for c in crons)
        assert found, "test-cron not found in cron list"

    await runner.test("list_crons", test_list_crons())

    async def test_delete_cron():
        result = await client.delete_cron("test-cron")
        assert result is True

    await runner.test("delete_cron", test_delete_cron())

    # Stats & Metrics
    async def test_stats():
        stats = await client.stats()
        assert stats.queued >= 0
        assert stats.processing >= 0
        assert stats.delayed >= 0
        assert stats.dlq >= 0

    await runner.test("stats", test_stats())

    async def test_metrics():
        metrics = await client.metrics()
        assert metrics.total_pushed >= 0
        assert metrics.total_completed >= 0
        assert metrics.jobs_per_second >= 0

    await runner.test("metrics", test_metrics())

    # Job Dependencies
    async def test_dependencies():
        # Create parent job
        parent = await client.push("deps-queue", {"type": "parent"})
        # Create child that depends on parent
        child = await client.push("deps-queue", {"type": "child"}, PushOptions(
            depends_on=[parent.id]
        ))
        # Child should be waiting for parent
        state = await client.get_state(child.id)
        assert state in [JobState.WAITING, JobState.WAITING_CHILDREN, "waitingchildren"]

    await runner.test("dependencies", test_dependencies())

    # Unique Jobs
    async def test_unique_key():
        key = f"unique-{time.time()}"
        job1 = await client.push("unique-queue", {"n": 1}, PushOptions(unique_key=key))
        # Second push with same key should either return same ID or raise Duplicate error
        try:
            job2 = await client.push("unique-queue", {"n": 2}, PushOptions(unique_key=key))
            # If no error, IDs should match (deduplicated)
            assert job1.id == job2.id, f"Expected deduplication: {job1.id} != {job2.id}"
        except FlashQError as e:
            # Duplicate error is expected behavior for deduplication
            assert "Duplicate" in str(e), f"Unexpected error: {e}"

    await runner.test("unique_key deduplication", test_unique_key())

    # Close connection
    await client.close()

    # ==================== HTTP Tests ====================
    print("\n[HTTP Protocol Tests]")

    try:
        import aiohttp
        http_client = FlashQ(host="localhost", http_port=6790, use_http=True)

        await runner.test("HTTP connect", test_connect(http_client))

        async def test_http_push():
            job = await http_client.push("http-test", {"via": "http"})
            assert job.id > 0

        await runner.test("HTTP push", test_http_push())

        async def test_http_stats():
            stats = await http_client.stats()
            assert stats.queued >= 0

        await runner.test("HTTP stats", test_http_stats())

        async def test_http_list_queues():
            queues = await http_client.list_queues()
            assert isinstance(queues, list)

        await runner.test("HTTP list_queues", test_http_list_queues())

        await http_client.close()

    except ImportError:
        print("  (HTTP tests skipped - aiohttp not installed)")

    # Summary
    success = runner.summary()
    sys.exit(0 if success else 1)


async def test_connect(client: FlashQ):
    """Test connection"""
    await client.connect()
    assert client.is_connected(), "Client should be connected"


if __name__ == "__main__":
    asyncio.run(main())
