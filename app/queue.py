"""Async scheduler / worker pool (skeleton)."""
from __future__ import annotations

from typing import Awaitable, Callable

from app.models import Job, QueueConfig
from app.store import JobStore

Handler = Callable[[Job], Awaitable[None]]


class JobQueue:
    def __init__(self, store: JobStore, config: QueueConfig) -> None:
        self.store = store
        self.config = config
        self.handlers: dict[str, Handler] = {}

    def register_handler(self, job_type: str, handler: Handler) -> None:
        self.handlers[job_type] = handler

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def submit(self, job_type: str, payload: dict, idempotency_key: str,
                     max_attempts: int | None = None) -> tuple[Job | None, str | None]:
        """Return (job, None) on accept, (existing, 'duplicate') on replay, (None, 'queue_full') on backpressure."""
        raise NotImplementedError
