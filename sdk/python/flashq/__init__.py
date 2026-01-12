"""
FlashQ Python SDK

High-performance job queue client for Python.

Example:
    >>> from flashq import FlashQ
    >>>
    >>> async with FlashQ() as client:
    ...     # Push a job
    ...     job = await client.push("emails", {"to": "user@example.com"})
    ...
    ...     # Pull and process
    ...     job = await client.pull("emails")
    ...     await client.ack(job.id)

Worker Example:
    >>> from flashq import Worker
    >>>
    >>> worker = Worker("emails")
    >>>
    >>> @worker.process
    >>> async def handle(job):
    ...     print(f"Processing {job.id}")
    ...     return {"sent": True}
    >>>
    >>> await worker.run()
"""

__version__ = "0.1.0"
__author__ = "FlashQ Team"

from .client import (
    FlashQ,
    FlashQError,
    ConnectionError,
    TimeoutError,
    AuthenticationError,
)

from .worker import (
    Worker,
    WorkerPool,
)

from .types import (
    Job,
    JobState,
    JobWithState,
    PushOptions,
    QueueInfo,
    QueueStats,
    QueueMetrics,
    Metrics,
    CronJob,
    CronOptions,
    ClientOptions,
    Progress,
    WorkerInfo,
)

__all__ = [
    # Client
    "FlashQ",
    "FlashQError",
    "ConnectionError",
    "TimeoutError",
    "AuthenticationError",
    # Worker
    "Worker",
    "WorkerPool",
    # Types
    "Job",
    "JobState",
    "JobWithState",
    "PushOptions",
    "QueueInfo",
    "QueueStats",
    "QueueMetrics",
    "Metrics",
    "CronJob",
    "CronOptions",
    "ClientOptions",
    "Progress",
    "WorkerInfo",
]
