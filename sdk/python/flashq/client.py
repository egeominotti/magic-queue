"""
FlashQ Python SDK Client

High-performance job queue client with TCP and HTTP support.

Example:
    >>> from flashq import FlashQ
    >>>
    >>> async with FlashQ() as client:
    ...     job = await client.push("emails", {"to": "user@example.com"})
    ...     print(f"Created job {job.id}")
"""
import asyncio
import json
import socket
import time
from typing import Any, Dict, List, Optional, TypeVar, Generic
from contextlib import asynccontextmanager

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

from .types import (
    Job, JobState, JobWithState, PushOptions, QueueInfo, QueueStats,
    Metrics, QueueMetrics, CronJob, CronOptions, ClientOptions, Progress
)


T = TypeVar('T')


class FlashQError(Exception):
    """FlashQ client error"""
    pass


class ConnectionError(FlashQError):
    """Connection error"""
    pass


class TimeoutError(FlashQError):
    """Timeout error"""
    pass


class AuthenticationError(FlashQError):
    """Authentication error"""
    pass


class FlashQ:
    """
    FlashQ Client

    High-performance job queue client with auto-connect support.

    Example:
        >>> client = FlashQ()
        >>> await client.connect()
        >>> job = await client.push("emails", {"to": "user@example.com"})
        >>> await client.close()

    Context manager example:
        >>> async with FlashQ() as client:
        ...     job = await client.push("emails", {"to": "user@example.com"})
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6789,
        http_port: int = 6790,
        socket_path: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 5.0,
        use_http: bool = False
    ):
        self.options = ClientOptions(
            host=host,
            port=port,
            http_port=http_port,
            socket_path=socket_path,
            token=token,
            timeout=timeout,
            use_http=use_http
        )
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._authenticated = False
        self._lock = asyncio.Lock()
        self._http_session: Optional[Any] = None

    async def __aenter__(self) -> "FlashQ":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ============== Connection Management ==============

    async def connect(self) -> None:
        """Connect to FlashQ server"""
        if self._connected:
            return

        if self.options.use_http:
            if not HAS_AIOHTTP:
                raise ImportError("aiohttp required for HTTP mode: pip install aiohttp")
            self._http_session = aiohttp.ClientSession()
            self._connected = True
            return

        try:
            if self.options.socket_path:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(self.options.socket_path),
                    timeout=self.options.timeout
                )
            else:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.options.host, self.options.port),
                    timeout=self.options.timeout
                )

            self._connected = True

            # Authenticate if token provided
            if self.options.token:
                await self.auth(self.options.token)

        except asyncio.TimeoutError:
            raise ConnectionError("Connection timeout")
        except Exception as e:
            raise ConnectionError(f"Failed to connect: {e}")

    async def close(self) -> None:
        """Close the connection"""
        self._connected = False

        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None

        if self._http_session:
            await self._http_session.close()
            self._http_session = None

    def is_connected(self) -> bool:
        """Check if connected"""
        return self._connected

    # ============== Internal Methods ==============

    async def _send(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send command and receive response"""
        if not self._connected:
            await self.connect()

        if self.options.use_http:
            return await self._send_http(command)
        return await self._send_tcp(command)

    async def _send_tcp(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send command via TCP"""
        if not self._writer or not self._reader:
            raise ConnectionError("Not connected")

        async with self._lock:
            try:
                # Send command
                data = json.dumps(command) + "\n"
                self._writer.write(data.encode())
                await self._writer.drain()

                # Read response
                response_data = await asyncio.wait_for(
                    self._reader.readline(),
                    timeout=self.options.timeout
                )

                if not response_data:
                    raise ConnectionError("Connection closed")

                response = json.loads(response_data.decode())

                if response.get("ok") is False:
                    raise FlashQError(response.get("error", "Unknown error"))

                return response

            except asyncio.TimeoutError:
                raise TimeoutError("Request timeout")
            except json.JSONDecodeError as e:
                raise FlashQError(f"Invalid response: {e}")

    async def _send_http(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send command via HTTP"""
        if not self._http_session:
            raise ConnectionError("HTTP session not initialized")

        cmd = command.pop("cmd", None)
        base_url = f"http://{self.options.host}:{self.options.http_port}"
        headers = {"Content-Type": "application/json"}

        if self.options.token:
            headers["Authorization"] = f"Bearer {self.options.token}"

        url, method, body = self._map_command_to_http(base_url, cmd, command)

        async with self._http_session.request(method, url, headers=headers, json=body) as resp:
            data = await resp.json()
            if not data.get("ok"):
                raise FlashQError(data.get("error", "Unknown error"))
            return data

    def _map_command_to_http(
        self, base_url: str, cmd: str, params: Dict[str, Any]
    ) -> tuple:
        """Map TCP command to HTTP endpoint"""
        if cmd == "PUSH":
            return (
                f"{base_url}/queues/{params['queue']}/jobs",
                "POST",
                {k: v for k, v in params.items() if k != "queue" and v is not None}
            )
        elif cmd == "PULL":
            return f"{base_url}/queues/{params['queue']}/jobs?count=1", "GET", None
        elif cmd == "PULLB":
            return f"{base_url}/queues/{params['queue']}/jobs?count={params['count']}", "GET", None
        elif cmd == "ACK":
            return f"{base_url}/jobs/{params['id']}/ack", "POST", {"result": params.get("result")}
        elif cmd == "FAIL":
            return f"{base_url}/jobs/{params['id']}/fail", "POST", {"error": params.get("error")}
        elif cmd == "STATS":
            return f"{base_url}/stats", "GET", None
        elif cmd == "METRICS":
            return f"{base_url}/metrics", "GET", None
        elif cmd == "LISTQUEUES":
            return f"{base_url}/queues", "GET", None
        else:
            raise FlashQError(f"HTTP not supported for command: {cmd}")

    # ============== Authentication ==============

    async def auth(self, token: str) -> None:
        """Authenticate with the server"""
        response = await self._send_tcp({"cmd": "AUTH", "token": token})
        if not response.get("ok"):
            raise AuthenticationError("Authentication failed")
        self.options.token = token
        self._authenticated = True

    # ============== Core Operations ==============

    async def push(
        self,
        queue: str,
        data: Any,
        options: Optional[PushOptions] = None
    ) -> Job:
        """
        Push a job to a queue

        Args:
            queue: Queue name
            data: Job data payload
            options: Push options (priority, delay, etc.)

        Returns:
            Created job

        Example:
            >>> job = await client.push("emails", {"to": "user@example.com"})
            >>> job = await client.push("tasks", {"type": "cleanup"}, PushOptions(priority=10))
        """
        opts = options or PushOptions()

        response = await self._send({
            "cmd": "PUSH",
            "queue": queue,
            "data": data,
            "priority": opts.priority,
            "delay": opts.delay,
            "ttl": opts.ttl,
            "timeout": opts.timeout,
            "max_attempts": opts.max_attempts,
            "backoff": opts.backoff,
            "unique_key": opts.unique_key,
            "depends_on": opts.depends_on,
            "tags": opts.tags,
        })

        # HTTP returns {"data": job}, TCP returns {"id": id}
        job_id = response.get("id")
        if job_id is None and "data" in response:
            job_data = response["data"]
            return self._parse_job(job_data)

        now = int(time.time() * 1000)
        return Job(
            id=job_id,
            queue=queue,
            data=data,
            priority=opts.priority,
            created_at=now,
            run_at=now + (opts.delay or 0),
            depends_on=opts.depends_on or [],
            tags=opts.tags or []
        )

    async def add(
        self,
        queue: str,
        data: Any,
        options: Optional[PushOptions] = None
    ) -> Job:
        """Alias for push()"""
        return await self.push(queue, data, options)

    async def push_batch(
        self,
        queue: str,
        jobs: List[Dict[str, Any]]
    ) -> List[int]:
        """
        Push multiple jobs to a queue

        Args:
            queue: Queue name
            jobs: List of job dicts with 'data' and optional push options

        Returns:
            List of created job IDs
        """
        response = await self._send({
            "cmd": "PUSHB",
            "queue": queue,
            "jobs": [
                {
                    "data": j.get("data"),
                    "priority": j.get("priority", 0),
                    "delay": j.get("delay"),
                    "ttl": j.get("ttl"),
                    "timeout": j.get("timeout"),
                    "max_attempts": j.get("max_attempts"),
                    "backoff": j.get("backoff"),
                    "unique_key": j.get("unique_key"),
                    "depends_on": j.get("depends_on"),
                    "tags": j.get("tags"),
                }
                for j in jobs
            ]
        })
        return response.get("ids", [])

    async def pull(self, queue: str) -> Job:
        """
        Pull a job from a queue (blocking)

        Args:
            queue: Queue name

        Returns:
            Job to process
        """
        response = await self._send({"cmd": "PULL", "queue": queue})
        job_data = response.get("job", {})
        return self._parse_job(job_data)

    async def pull_batch(self, queue: str, count: int) -> List[Job]:
        """
        Pull multiple jobs from a queue

        Args:
            queue: Queue name
            count: Number of jobs to pull

        Returns:
            List of jobs
        """
        response = await self._send({"cmd": "PULLB", "queue": queue, "count": count})
        return [self._parse_job(j) for j in response.get("jobs", [])]

    async def ack(self, job_id: int, result: Any = None) -> None:
        """
        Acknowledge a job as completed

        Args:
            job_id: Job ID
            result: Optional result data
        """
        await self._send({"cmd": "ACK", "id": job_id, "result": result})

    async def ack_batch(self, job_ids: List[int]) -> int:
        """
        Acknowledge multiple jobs

        Args:
            job_ids: List of job IDs

        Returns:
            Count of acknowledged jobs
        """
        response = await self._send({"cmd": "ACKB", "ids": job_ids})
        ids = response.get("ids", [])
        return ids[0] if ids else 0

    async def fail(self, job_id: int, error: Optional[str] = None) -> None:
        """
        Fail a job (will retry or move to DLQ)

        Args:
            job_id: Job ID
            error: Error message
        """
        await self._send({"cmd": "FAIL", "id": job_id, "error": error})

    # ============== Job Management ==============

    async def get_job(self, job_id: int) -> Optional[JobWithState]:
        """
        Get a job with its current state

        Args:
            job_id: Job ID

        Returns:
            Job with state or None if not found
        """
        response = await self._send({"cmd": "GETJOB", "id": job_id})
        job_data = response.get("job")
        if not job_data:
            return None
        return JobWithState(
            job=self._parse_job(job_data),
            state=JobState(response.get("state", "waiting"))
        )

    async def get_state(self, job_id: int) -> Optional[JobState]:
        """
        Get job state only

        Args:
            job_id: Job ID

        Returns:
            Job state or None
        """
        response = await self._send({"cmd": "GETSTATE", "id": job_id})
        state = response.get("state")
        return JobState(state) if state else None

    async def get_result(self, job_id: int) -> Any:
        """
        Get job result

        Args:
            job_id: Job ID

        Returns:
            Job result or None
        """
        response = await self._send({"cmd": "GETRESULT", "id": job_id})
        return response.get("result")

    async def cancel(self, job_id: int) -> None:
        """
        Cancel a pending job

        Args:
            job_id: Job ID
        """
        await self._send({"cmd": "CANCEL", "id": job_id})

    async def progress(
        self,
        job_id: int,
        progress: int,
        message: Optional[str] = None
    ) -> None:
        """
        Update job progress

        Args:
            job_id: Job ID
            progress: Progress value (0-100)
            message: Optional progress message
        """
        await self._send({
            "cmd": "PROGRESS",
            "id": job_id,
            "progress": max(0, min(100, progress)),
            "message": message
        })

    async def get_progress(self, job_id: int) -> Progress:
        """
        Get job progress

        Args:
            job_id: Job ID

        Returns:
            Progress object
        """
        response = await self._send({"cmd": "GETPROGRESS", "id": job_id})
        prog = response.get("progress", {})
        return Progress(
            progress=prog.get("progress", 0),
            message=prog.get("message")
        )

    # ============== Dead Letter Queue ==============

    async def get_dlq(self, queue: str, count: int = 100) -> List[Job]:
        """
        Get jobs from the dead letter queue

        Args:
            queue: Queue name
            count: Maximum number of jobs to return

        Returns:
            List of failed jobs
        """
        response = await self._send({"cmd": "DLQ", "queue": queue, "count": count})
        return [self._parse_job(j) for j in response.get("jobs", [])]

    async def retry_dlq(self, queue: str, job_id: Optional[int] = None) -> int:
        """
        Retry jobs from DLQ

        Args:
            queue: Queue name
            job_id: Optional specific job ID to retry

        Returns:
            Number of jobs retried
        """
        response = await self._send({"cmd": "RETRYDLQ", "queue": queue, "id": job_id})
        ids = response.get("ids", [])
        return ids[0] if ids else 0

    # ============== Queue Control ==============

    async def pause(self, queue: str) -> None:
        """Pause a queue"""
        await self._send({"cmd": "PAUSE", "queue": queue})

    async def resume(self, queue: str) -> None:
        """Resume a paused queue"""
        await self._send({"cmd": "RESUME", "queue": queue})

    async def set_rate_limit(self, queue: str, limit: int) -> None:
        """
        Set rate limit for a queue

        Args:
            queue: Queue name
            limit: Jobs per second
        """
        await self._send({"cmd": "RATELIMIT", "queue": queue, "limit": limit})

    async def clear_rate_limit(self, queue: str) -> None:
        """Clear rate limit for a queue"""
        await self._send({"cmd": "RATELIMITCLEAR", "queue": queue})

    async def set_concurrency(self, queue: str, limit: int) -> None:
        """
        Set concurrency limit for a queue

        Args:
            queue: Queue name
            limit: Maximum concurrent jobs
        """
        await self._send({"cmd": "SETCONCURRENCY", "queue": queue, "limit": limit})

    async def clear_concurrency(self, queue: str) -> None:
        """Clear concurrency limit for a queue"""
        await self._send({"cmd": "CLEARCONCURRENCY", "queue": queue})

    async def list_queues(self) -> List[QueueInfo]:
        """
        List all queues

        Returns:
            List of queue info
        """
        response = await self._send({"cmd": "LISTQUEUES"})
        return [
            QueueInfo(
                name=q.get("name", ""),
                pending=q.get("pending", 0),
                processing=q.get("processing", 0),
                dlq=q.get("dlq", 0),
                paused=q.get("paused", False),
                rate_limit=q.get("rate_limit"),
                concurrency_limit=q.get("concurrency_limit")
            )
            for q in response.get("queues", [])
        ]

    # ============== Cron Jobs ==============

    async def add_cron(self, name: str, options: CronOptions) -> None:
        """
        Add a cron job

        Args:
            name: Unique cron job name
            options: Cron job options

        Example:
            >>> await client.add_cron("cleanup", CronOptions(
            ...     queue="maintenance",
            ...     data={"task": "cleanup"},
            ...     schedule="0 * * * * *"  # Every minute
            ... ))
        """
        await self._send({
            "cmd": "CRON",
            "name": name,
            "queue": options.queue,
            "data": options.data,
            "schedule": options.schedule,
            "priority": options.priority
        })

    async def delete_cron(self, name: str) -> bool:
        """
        Delete a cron job

        Args:
            name: Cron job name

        Returns:
            True if deleted
        """
        response = await self._send({"cmd": "CRONDELETE", "name": name})
        return response.get("ok", False)

    async def list_crons(self) -> List[CronJob]:
        """
        List all cron jobs

        Returns:
            List of cron jobs
        """
        response = await self._send({"cmd": "CRONLIST"})
        return [
            CronJob(
                name=c.get("name", ""),
                queue=c.get("queue", ""),
                data=c.get("data"),
                schedule=c.get("schedule", ""),
                priority=c.get("priority", 0),
                next_run=c.get("next_run", 0)
            )
            for c in response.get("crons", [])
        ]

    # ============== Stats & Metrics ==============

    async def stats(self) -> QueueStats:
        """
        Get queue statistics

        Returns:
            Queue statistics
        """
        response = await self._send({"cmd": "STATS"})
        return QueueStats(
            queued=response.get("queued", 0),
            processing=response.get("processing", 0),
            delayed=response.get("delayed", 0),
            dlq=response.get("dlq", 0)
        )

    async def metrics(self) -> Metrics:
        """
        Get detailed metrics

        Returns:
            Detailed metrics
        """
        response = await self._send({"cmd": "METRICS"})
        m = response.get("metrics", {})
        return Metrics(
            total_pushed=m.get("total_pushed", 0),
            total_completed=m.get("total_completed", 0),
            total_failed=m.get("total_failed", 0),
            jobs_per_second=m.get("jobs_per_second", 0.0),
            avg_latency_ms=m.get("avg_latency_ms", 0.0),
            queues=[
                QueueMetrics(
                    name=q.get("name", ""),
                    pending=q.get("pending", 0),
                    processing=q.get("processing", 0),
                    dlq=q.get("dlq", 0),
                    rate_limit=q.get("rate_limit")
                )
                for q in m.get("queues", [])
            ]
        )

    # ============== Helper Methods ==============

    def _parse_job(self, data: Dict[str, Any]) -> Job:
        """Parse job from response data"""
        return Job(
            id=data.get("id", 0),
            queue=data.get("queue", ""),
            data=data.get("data"),
            priority=data.get("priority", 0),
            created_at=data.get("created_at", 0),
            run_at=data.get("run_at", 0),
            started_at=data.get("started_at", 0),
            attempts=data.get("attempts", 0),
            max_attempts=data.get("max_attempts", 0),
            backoff=data.get("backoff", 0),
            ttl=data.get("ttl", 0),
            timeout=data.get("timeout", 0),
            unique_key=data.get("unique_key"),
            depends_on=data.get("depends_on", []),
            progress=data.get("progress", 0),
            progress_msg=data.get("progress_msg"),
            tags=data.get("tags", [])
        )
