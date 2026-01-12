"""
FlashQ Python SDK Worker

Worker class for processing jobs from queues.

Example:
    >>> from flashq import Worker
    >>>
    >>> worker = Worker("emails")
    >>>
    >>> @worker.process
    >>> async def handle_email(job):
    ...     print(f"Sending email to {job.data['to']}")
    ...     return {"sent": True}
    >>>
    >>> await worker.run()
"""
import asyncio
import signal
import uuid
from typing import Any, Callable, Dict, List, Optional, Union, Awaitable
from functools import wraps

from .client import FlashQ
from .types import Job, ClientOptions


JobHandler = Callable[[Job], Union[Any, Awaitable[Any]]]


class Worker:
    """
    FlashQ Worker

    Processes jobs from one or more queues.

    Example:
        >>> worker = Worker("emails", concurrency=5)
        >>>
        >>> @worker.process
        >>> async def handler(job):
        ...     # Process the job
        ...     return {"result": "success"}
        >>>
        >>> await worker.run()
    """

    def __init__(
        self,
        *queues: str,
        host: str = "localhost",
        port: int = 6789,
        http_port: int = 6790,
        token: Optional[str] = None,
        concurrency: int = 1,
        auto_ack: bool = True,
        auto_fail: bool = True,
        worker_id: Optional[str] = None
    ):
        """
        Initialize worker

        Args:
            *queues: Queue names to process
            host: FlashQ server host
            port: TCP port
            http_port: HTTP port
            token: Authentication token
            concurrency: Number of concurrent jobs
            auto_ack: Auto-acknowledge successful jobs
            auto_fail: Auto-fail jobs on exception
            worker_id: Unique worker ID (auto-generated if not provided)
        """
        self.queues = list(queues) if queues else ["default"]
        self.host = host
        self.port = port
        self.http_port = http_port
        self.token = token
        self.concurrency = concurrency
        self.auto_ack = auto_ack
        self.auto_fail = auto_fail
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

        self._handlers: Dict[str, JobHandler] = {}
        self._default_handler: Optional[JobHandler] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._client: Optional[FlashQ] = None
        self._jobs_processed = 0
        self._semaphore: Optional[asyncio.Semaphore] = None

    def process(
        self,
        func: Optional[JobHandler] = None,
        *,
        queue: Optional[str] = None
    ):
        """
        Decorator to register a job handler

        Args:
            func: Handler function
            queue: Specific queue to handle (None for all queues)

        Example:
            >>> @worker.process
            >>> async def handler(job):
            ...     return process_job(job)

            >>> @worker.process(queue="emails")
            >>> async def email_handler(job):
            ...     return send_email(job.data)
        """
        def decorator(f: JobHandler) -> JobHandler:
            if queue:
                self._handlers[queue] = f
            else:
                self._default_handler = f
            return f

        if func is not None:
            return decorator(func)
        return decorator

    def on(self, queue: str, handler: JobHandler) -> None:
        """
        Register a handler for a specific queue

        Args:
            queue: Queue name
            handler: Handler function
        """
        self._handlers[queue] = handler

    async def run(self) -> None:
        """
        Start the worker

        Runs until stopped with Ctrl+C or worker.stop()
        """
        self._running = True
        self._semaphore = asyncio.Semaphore(self.concurrency)

        # Set up signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass

        # Create client
        self._client = FlashQ(
            host=self.host,
            port=self.port,
            http_port=self.http_port,
            token=self.token
        )
        await self._client.connect()

        print(f"Worker {self.worker_id} started")
        print(f"  Queues: {', '.join(self.queues)}")
        print(f"  Concurrency: {self.concurrency}")

        try:
            # Start worker tasks for each queue
            for queue in self.queues:
                for _ in range(self.concurrency):
                    task = asyncio.create_task(self._process_queue(queue))
                    self._tasks.append(task)

            # Wait for all tasks
            await asyncio.gather(*self._tasks, return_exceptions=True)

        except asyncio.CancelledError:
            pass
        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """Stop the worker gracefully"""
        print(f"\nWorker {self.worker_id} stopping...")
        self._running = False

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

    async def _cleanup(self) -> None:
        """Clean up resources"""
        if self._client:
            await self._client.close()
            self._client = None

        print(f"Worker {self.worker_id} stopped. Jobs processed: {self._jobs_processed}")

    async def _process_queue(self, queue: str) -> None:
        """Process jobs from a queue"""
        while self._running:
            try:
                async with self._semaphore:
                    if not self._running:
                        break

                    # Pull job
                    job = await self._client.pull(queue)
                    if not job:
                        continue

                    # Process job
                    await self._process_job(job, queue)

            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log error but keep running
                print(f"Error in worker loop: {e}")
                await asyncio.sleep(1)

    async def _process_job(self, job: Job, queue: str) -> None:
        """Process a single job"""
        handler = self._handlers.get(queue) or self._default_handler

        if not handler:
            print(f"No handler for queue '{queue}', job {job.id}")
            if self.auto_fail:
                await self._client.fail(job.id, "No handler registered")
            return

        try:
            # Call handler
            result = handler(job)
            if asyncio.iscoroutine(result):
                result = await result

            # Auto-acknowledge
            if self.auto_ack:
                await self._client.ack(job.id, result)

            self._jobs_processed += 1

        except Exception as e:
            error_msg = str(e)
            print(f"Job {job.id} failed: {error_msg}")

            # Auto-fail
            if self.auto_fail:
                await self._client.fail(job.id, error_msg)

    @property
    def jobs_processed(self) -> int:
        """Number of jobs processed"""
        return self._jobs_processed

    @property
    def is_running(self) -> bool:
        """Whether the worker is running"""
        return self._running


class WorkerPool:
    """
    Pool of workers for processing multiple queues

    Example:
        >>> pool = WorkerPool()
        >>> pool.add_worker("emails", handler=send_email, concurrency=5)
        >>> pool.add_worker("notifications", handler=send_notification, concurrency=3)
        >>> await pool.run()
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6789,
        token: Optional[str] = None
    ):
        self.host = host
        self.port = port
        self.token = token
        self._workers: List[Worker] = []

    def add_worker(
        self,
        queue: str,
        handler: JobHandler,
        concurrency: int = 1
    ) -> Worker:
        """
        Add a worker for a queue

        Args:
            queue: Queue name
            handler: Job handler function
            concurrency: Number of concurrent jobs

        Returns:
            Created worker
        """
        worker = Worker(
            queue,
            host=self.host,
            port=self.port,
            token=self.token,
            concurrency=concurrency
        )
        worker.process(handler)
        self._workers.append(worker)
        return worker

    async def run(self) -> None:
        """Run all workers"""
        if not self._workers:
            raise ValueError("No workers added")

        await asyncio.gather(*[w.run() for w in self._workers])

    async def stop(self) -> None:
        """Stop all workers"""
        await asyncio.gather(*[w.stop() for w in self._workers])
