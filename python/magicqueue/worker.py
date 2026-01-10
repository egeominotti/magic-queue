import time
import traceback
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from .client import MagicQueueClient


@dataclass
class Job:
    id: int
    queue: str
    data: Any
    priority: int
    created_at: int
    run_at: int
    attempts: int
    max_attempts: int
    progress: int


class JobContext:
    """Context passed to job handlers for progress updates."""

    def __init__(self, job: Job, client: MagicQueueClient):
        self.job = job
        self._client = client

    def update_progress(self, progress: int, message: Optional[str] = None) -> None:
        """Update job progress (0-100)."""
        self._client.send({
            "cmd": "PROGRESS",
            "id": self.job.id,
            "progress": min(100, max(0, progress)),
            "message": message,
        })


JobHandler = Callable[[Job, JobContext], None]


class Worker:
    """Worker class for processing jobs from a queue."""

    def __init__(
        self,
        queue_name: str,
        handler: JobHandler,
        host: str = "localhost",
        port: int = 6789,
        unix_socket: Optional[str] = None,
        concurrency: int = 1,
        batch_size: int = 10,
    ):
        self.queue_name = queue_name
        self.handler = handler
        self._client = MagicQueueClient(host, port, unix_socket)
        self.concurrency = concurrency
        self.batch_size = batch_size
        self._running = False

    def start(self) -> None:
        """Start processing jobs."""
        self._client.connect()
        self._running = True

        print(f'Worker started: queue="{self.queue_name}" concurrency={self.concurrency} batch_size={self.batch_size}')

        if self.batch_size > 1:
            self._batch_process_loop()
        else:
            self._process_loop()

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self._client.close()

    def _parse_job(self, data: Dict[str, Any]) -> Job:
        return Job(
            id=data["id"],
            queue=data["queue"],
            data=data["data"],
            priority=data["priority"],
            created_at=data["created_at"],
            run_at=data["run_at"],
            attempts=data.get("attempts", 0),
            max_attempts=data.get("max_attempts", 0),
            progress=data.get("progress", 0),
        )

    def _batch_process_loop(self) -> None:
        while self._running:
            try:
                response = self._client.send({
                    "cmd": "PULLB",
                    "queue": self.queue_name,
                    "count": self.batch_size,
                })

                jobs_data = response.get("jobs", [])
                if not jobs_data:
                    continue

                jobs = [self._parse_job(j) for j in jobs_data]
                success_ids: List[int] = []
                failed_jobs: List[Dict[str, Any]] = []

                # Process jobs in parallel
                with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                    futures = {
                        executor.submit(self._process_job, job): job
                        for job in jobs
                    }

                    for future in as_completed(futures):
                        job = futures[future]
                        try:
                            future.result()
                            success_ids.append(job.id)
                        except Exception as e:
                            failed_jobs.append({
                                "id": job.id,
                                "error": str(e),
                            })

                # Acknowledge successful jobs
                if success_ids:
                    self._client.send({"cmd": "ACKB", "ids": success_ids})

                # Report failed jobs
                for failed in failed_jobs:
                    self._client.send({
                        "cmd": "FAIL",
                        "id": failed["id"],
                        "error": failed["error"],
                    })

            except Exception as e:
                if self._running:
                    print(f"Worker error: {e}")
                    traceback.print_exc()
                    time.sleep(1)

    def _process_loop(self) -> None:
        while self._running:
            try:
                response = self._client.send({
                    "cmd": "PULL",
                    "queue": self.queue_name,
                })

                job = self._parse_job(response["job"])

                try:
                    self._process_job(job)
                    self._client.send({"cmd": "ACK", "id": job.id})
                except Exception as e:
                    self._client.send({
                        "cmd": "FAIL",
                        "id": job.id,
                        "error": str(e),
                    })

            except Exception as e:
                if self._running:
                    time.sleep(1)

    def _process_job(self, job: Job) -> None:
        ctx = JobContext(job, self._client)
        self.handler(job, ctx)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
