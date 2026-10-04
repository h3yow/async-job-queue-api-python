"""Type definitions for the job queue (skeleton)."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # terminal, non-retryable
    DEAD = "dead"      # terminal, retries exhausted (dead letter)


class RejectReason(str, enum.Enum):
    QUEUE_FULL = "queue_full"
    DUPLICATE = "duplicate"  # informational: existing job returned


class RetryableError(Exception):
    """Error that allows the job to be retried with backoff."""


class NonRetryableError(Exception):
    """Error that sends the job straight to terminal failure."""


@dataclass
class Job:
    job_id: str
    idempotency_key: str
    job_type: str
    payload: dict
    status: JobStatus = JobStatus.PENDING
    attempt: int = 0
    max_attempts: int = 3
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class QueueConfig:
    max_concurrency: int = 2
    max_queue_size: int = 8
    base_backoff_seconds: float = 0.05
    max_backoff_seconds: float = 1.0
    poll_interval_seconds: float = 0.02
    recent_window_seconds: float = 60.0  # "recently rejecting/failing" window
