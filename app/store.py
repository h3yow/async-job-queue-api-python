"""SQLite-backed persistent job store (skeleton)."""
from __future__ import annotations

from app.models import Job


class JobStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def init_schema(self) -> None:
        raise NotImplementedError

    def insert_job(self, job: Job) -> Job | None:
        """Insert job; return existing job if idempotency key already exists."""
        raise NotImplementedError

    def get_job(self, job_id: str) -> Job | None:
        raise NotImplementedError

    def claim_next_pending(self) -> Job | None:
        raise NotImplementedError

    def complete_job(self, job_id: str, attempt: int, status: str, error: str | None) -> bool:
        raise NotImplementedError

    def reschedule_for_retry(self, job_id: str, attempt: int, error: str, run_at: float) -> bool:
        raise NotImplementedError

    def requeue_running_jobs(self) -> int:
        """Recovery: move interrupted RUNNING jobs back to PENDING."""
        raise NotImplementedError

    def pending_count(self) -> int:
        raise NotImplementedError

    def replay_dead(self, job_id: str) -> Job | None:
        raise NotImplementedError

    def failure_history(self, job_id: str) -> list[dict]:
        raise NotImplementedError
