"""
FlashQ Python SDK Types
"""
from dataclasses import dataclass, field
from typing import Any, Optional, List, Literal
from enum import Enum


class JobState(str, Enum):
    """Job state enumeration"""
    WAITING = "waiting"
    DELAYED = "delayed"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    WAITING_CHILDREN = "waiting-children"

    @classmethod
    def _missing_(cls, value):
        """Handle server returning 'waitingchildren' without hyphen"""
        if value == "waitingchildren":
            return cls.WAITING_CHILDREN
        return None


@dataclass
class Job:
    """Job representation"""
    id: int
    queue: str
    data: Any
    priority: int = 0
    created_at: int = 0
    run_at: int = 0
    started_at: int = 0
    attempts: int = 0
    max_attempts: int = 0
    backoff: int = 0
    ttl: int = 0
    timeout: int = 0
    unique_key: Optional[str] = None
    depends_on: List[int] = field(default_factory=list)
    progress: int = 0
    progress_msg: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class JobWithState:
    """Job with its current state"""
    job: Job
    state: JobState


@dataclass
class PushOptions:
    """Options for pushing a job"""
    priority: int = 0
    delay: Optional[int] = None
    ttl: Optional[int] = None
    timeout: Optional[int] = None
    max_attempts: Optional[int] = None
    backoff: Optional[int] = None
    unique_key: Optional[str] = None
    depends_on: Optional[List[int]] = None
    tags: Optional[List[str]] = None


@dataclass
class QueueInfo:
    """Queue information"""
    name: str
    pending: int
    processing: int
    dlq: int
    paused: bool = False
    rate_limit: Optional[int] = None
    concurrency_limit: Optional[int] = None


@dataclass
class QueueStats:
    """Queue statistics"""
    queued: int
    processing: int
    delayed: int
    dlq: int


@dataclass
class QueueMetrics:
    """Metrics for a single queue"""
    name: str
    pending: int
    processing: int
    dlq: int
    rate_limit: Optional[int] = None


@dataclass
class Metrics:
    """Global metrics"""
    total_pushed: int
    total_completed: int
    total_failed: int
    jobs_per_second: float
    avg_latency_ms: float
    queues: List[QueueMetrics] = field(default_factory=list)


@dataclass
class CronJob:
    """Cron job definition"""
    name: str
    queue: str
    data: Any
    schedule: str
    priority: int
    next_run: int


@dataclass
class CronOptions:
    """Options for creating a cron job"""
    queue: str
    data: Any
    schedule: str
    priority: int = 0


@dataclass
class WorkerInfo:
    """Worker information"""
    id: str
    queues: List[str]
    concurrency: int
    last_heartbeat: int
    jobs_processed: int


@dataclass
class ClientOptions:
    """Client connection options"""
    host: str = "localhost"
    port: int = 6789
    http_port: int = 6790
    socket_path: Optional[str] = None
    token: Optional[str] = None
    timeout: float = 5.0
    use_http: bool = False


@dataclass
class Progress:
    """Job progress information"""
    progress: int
    message: Optional[str] = None
