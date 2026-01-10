from typing import Any, Dict, List, Optional, TypeVar
from dataclasses import dataclass
from .client import MagicQueueClient

T = TypeVar("T")


@dataclass
class CronJob:
    name: str
    queue: str
    data: Any
    schedule: str
    priority: int
    next_run: int


@dataclass
class QueueMetrics:
    name: str
    pending: int
    processing: int
    dlq: int
    rate_limit: Optional[int]


@dataclass
class Metrics:
    total_pushed: int
    total_completed: int
    total_failed: int
    jobs_per_second: float
    avg_latency_ms: float
    queues: List[QueueMetrics]


@dataclass
class Stats:
    queued: int
    processing: int
    delayed: int
    dlq: int


class Queue:
    """Producer class for pushing jobs to a queue."""

    def __init__(
        self,
        queue_name: str,
        host: str = "localhost",
        port: int = 6789,
        unix_socket: Optional[str] = None,
    ):
        self.queue_name = queue_name
        self._client = MagicQueueClient(host, port, unix_socket)
        self._auto_connect = True

    def connect(self) -> None:
        """Connect to the server."""
        self._client.connect()

    def close(self) -> None:
        """Close the connection."""
        self._client.close()

    def _ensure_connected(self) -> None:
        if not self._client.is_connected() and self._auto_connect:
            self.connect()

    def push(
        self,
        data: Any,
        priority: int = 0,
        delay: Optional[int] = None,
        ttl: Optional[int] = None,
        max_attempts: Optional[int] = None,
        backoff: Optional[int] = None,
        unique_key: Optional[str] = None,
        depends_on: Optional[List[int]] = None,
    ) -> int:
        """
        Push a job to the queue.

        Args:
            data: Job payload (any JSON-serializable data)
            priority: Job priority (higher = more urgent)
            delay: Delay in ms before job becomes available
            ttl: Time-to-live in ms (job expires after this)
            max_attempts: Max retry attempts before going to DLQ
            backoff: Base backoff in ms for exponential retry
            unique_key: Deduplication key
            depends_on: List of job IDs this job depends on

        Returns:
            Job ID
        """
        self._ensure_connected()

        cmd = {
            "cmd": "PUSH",
            "queue": self.queue_name,
            "data": data,
            "priority": priority,
        }

        if delay is not None:
            cmd["delay"] = delay
        if ttl is not None:
            cmd["ttl"] = ttl
        if max_attempts is not None:
            cmd["max_attempts"] = max_attempts
        if backoff is not None:
            cmd["backoff"] = backoff
        if unique_key is not None:
            cmd["unique_key"] = unique_key
        if depends_on is not None:
            cmd["depends_on"] = depends_on

        response = self._client.send(cmd)

        if not response.get("ok"):
            raise Exception(response.get("error", "Push failed"))

        return response["id"]

    def push_batch(self, jobs: List[Dict[str, Any]]) -> List[int]:
        """
        Push multiple jobs in a single request.

        Args:
            jobs: List of job dicts with keys: data, priority, delay, ttl, etc.

        Returns:
            List of job IDs
        """
        self._ensure_connected()

        formatted_jobs = []
        for job in jobs:
            formatted = {
                "data": job.get("data"),
                "priority": job.get("priority", 0),
            }
            for key in ["delay", "ttl", "max_attempts", "backoff", "unique_key", "depends_on"]:
                if key in job and job[key] is not None:
                    formatted[key] = job[key]
            formatted_jobs.append(formatted)

        response = self._client.send({
            "cmd": "PUSHB",
            "queue": self.queue_name,
            "jobs": formatted_jobs,
        })

        return response["ids"]

    def push_many(
        self,
        items: List[Any],
        priority: int = 0,
        delay: Optional[int] = None,
        ttl: Optional[int] = None,
        max_attempts: Optional[int] = None,
        backoff: Optional[int] = None,
    ) -> List[int]:
        """Push multiple items with the same options."""
        jobs = [
            {
                "data": item,
                "priority": priority,
                "delay": delay,
                "ttl": ttl,
                "max_attempts": max_attempts,
                "backoff": backoff,
            }
            for item in items
        ]
        return self.push_batch(jobs)

    def cancel(self, job_id: int) -> None:
        """Cancel a pending job."""
        self._ensure_connected()
        response = self._client.send({"cmd": "CANCEL", "id": job_id})
        if not response.get("ok"):
            raise Exception(response.get("error", "Cancel failed"))

    def get_progress(self, job_id: int) -> Dict[str, Any]:
        """Get progress of a job."""
        self._ensure_connected()
        response = self._client.send({"cmd": "GETPROGRESS", "id": job_id})
        return {
            "progress": response["progress"]["progress"],
            "message": response["progress"]["message"],
        }

    def get_dlq(self, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get jobs from the dead letter queue."""
        self._ensure_connected()
        cmd = {"cmd": "DLQ", "queue": self.queue_name}
        if count is not None:
            cmd["count"] = count
        response = self._client.send(cmd)
        return response["jobs"]

    def retry_dlq(self, job_id: Optional[int] = None) -> int:
        """Retry jobs from the dead letter queue."""
        self._ensure_connected()
        cmd = {"cmd": "RETRYDLQ", "queue": self.queue_name}
        if job_id is not None:
            cmd["id"] = job_id
        response = self._client.send(cmd)
        return response["ids"][0]

    def stats(self) -> Stats:
        """Get queue statistics."""
        self._ensure_connected()
        response = self._client.send({"cmd": "STATS"})
        return Stats(
            queued=response["queued"],
            processing=response["processing"],
            delayed=response["delayed"],
            dlq=response["dlq"],
        )

    def metrics(self) -> Metrics:
        """Get detailed metrics."""
        self._ensure_connected()
        response = self._client.send({"cmd": "METRICS"})
        m = response["metrics"]
        return Metrics(
            total_pushed=m["total_pushed"],
            total_completed=m["total_completed"],
            total_failed=m["total_failed"],
            jobs_per_second=m["jobs_per_second"],
            avg_latency_ms=m["avg_latency_ms"],
            queues=[
                QueueMetrics(
                    name=q["name"],
                    pending=q["pending"],
                    processing=q["processing"],
                    dlq=q["dlq"],
                    rate_limit=q["rate_limit"],
                )
                for q in m["queues"]
            ],
        )

    def set_rate_limit(self, limit: int) -> None:
        """Set rate limit (jobs per second) for this queue."""
        self._ensure_connected()
        self._client.send({
            "cmd": "RATELIMIT",
            "queue": self.queue_name,
            "limit": limit,
        })

    def clear_rate_limit(self) -> None:
        """Clear rate limit for this queue."""
        self._ensure_connected()
        self._client.send({
            "cmd": "RATELIMITCLEAR",
            "queue": self.queue_name,
        })

    def add_cron(
        self,
        name: str,
        schedule: str,
        data: Any,
        priority: int = 0,
    ) -> None:
        """
        Add a cron job.

        Args:
            name: Unique name for the cron job
            schedule: Cron expression (e.g., "*/60" for every 60 seconds)
            data: Job payload
            priority: Job priority
        """
        self._ensure_connected()
        self._client.send({
            "cmd": "CRON",
            "name": name,
            "queue": self.queue_name,
            "data": data,
            "schedule": schedule,
            "priority": priority,
        })

    def delete_cron(self, name: str) -> None:
        """Delete a cron job."""
        self._ensure_connected()
        response = self._client.send({"cmd": "CRONDELETE", "name": name})
        if not response.get("ok"):
            raise Exception(response.get("error", "Cron delete failed"))

    def list_crons(self) -> List[CronJob]:
        """List all cron jobs."""
        self._ensure_connected()
        response = self._client.send({"cmd": "CRONLIST"})
        return [
            CronJob(
                name=c["name"],
                queue=c["queue"],
                data=c["data"],
                schedule=c["schedule"],
                priority=c["priority"],
                next_run=c["next_run"],
            )
            for c in response["crons"]
        ]

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
