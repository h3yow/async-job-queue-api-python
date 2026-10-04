"""Async scheduler / worker pool with bounded concurrency, backpressure,
retry with exponential backoff, and dead-letter handling."""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Awaitable, Callable

from app.models import (
    Job,
    JobStatus,
    NonRetryableError,
    QueueConfig,
    RejectReason,
    RetryableError,
)
from app.store import JobStore

logger = logging.getLogger("jobqueue")

Handler = Callable[[Job], Awaitable[None]]


class JobQueue:
    def __init__(self, store: JobStore, config: QueueConfig) -> None:
        self.store = store
        self.config = config
        self.handlers: dict[str, Handler] = {}
        self._workers: list[asyncio.Task] = []
        self._wakeup: asyncio.Event | None = None
        self._capacity_changed: asyncio.Event | None = None
        self._stopped = False

    def register_handler(self, job_type: str, handler: Handler) -> None:
        self.handlers[job_type] = handler

    # ---- lifecycle -------------------------------------------------------

    async def start(self) -> None:
        """Recover interrupted jobs, then spawn the worker pool."""
        recovered = self.store.requeue_running_jobs()
        if recovered:
            logger.info("recovery: requeued %d interrupted job(s)", recovered)
        self._stopped = False
        self._wakeup = asyncio.Event()
        self._capacity_changed = asyncio.Event()
        self._workers = [
            asyncio.create_task(self._worker_loop(i))
            for i in range(self.config.max_concurrency)
        ]

    async def stop(self) -> None:
        self._stopped = True
        if self._wakeup:
            self._wakeup.set()
        if self._capacity_changed:
            self._capacity_changed.set()  # unblock waiting submitters
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

    # ---- submission ------------------------------------------------------

    async def submit(self, job_type: str, payload: dict, idempotency_key: str,
                     max_attempts: int | None = None,
                     wait_timeout_seconds: float | None = None
                     ) -> tuple[Job | None, str | None]:
        """Returns (job, None) when accepted, (existing_job, 'duplicate') when
        the idempotency key was already submitted, (None, 'queue_full') when
        the capacity limit is reached (backpressure).

        With wait_timeout_seconds > 0, a full queue does not reject
        immediately: the submission waits up to that many seconds for a slot
        to free up and is then accepted automatically. If the wait times out,
        the submission is rejected with 'queue_full' and leaves no record
        behind, so the same idempotency key can be submitted again later."""
        loop = asyncio.get_event_loop()
        deadline = (loop.time() + wait_timeout_seconds
                    if wait_timeout_seconds and wait_timeout_seconds > 0 else None)
        waited = False
        while True:
            job = Job(
                job_id=uuid.uuid4().hex,
                idempotency_key=idempotency_key,
                job_type=job_type,
                payload=payload,
                max_attempts=max_attempts or 3,
            )
            if self._capacity_changed:
                self._capacity_changed.clear()
            stored, reason = self.store.try_accept_job(job, self.config.max_queue_size)
            if reason == RejectReason.DUPLICATE.value:
                logger.info("submit key=%s -> duplicate of job=%s (idempotent replay)",
                            idempotency_key, stored.job_id if stored else None)
                return stored, RejectReason.DUPLICATE.value
            if reason is None:
                if self._wakeup:
                    self._wakeup.set()
                logger.info("submit key=%s accepted as job=%s%s", idempotency_key,
                            job.job_id, " (after waiting for capacity)" if waited else "")
                return job, None
            # queue_full: either reject now, or wait for a slot to free up.
            if deadline is None or self._stopped:
                logger.info("submit key=%s job=%s rejected: %s (pending >= %d)",
                            idempotency_key, job.job_id, RejectReason.QUEUE_FULL.value,
                            self.config.max_queue_size)
                return None, RejectReason.QUEUE_FULL.value
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.info("submit key=%s rejected: %s (waited %.3fs, no slot freed)",
                            idempotency_key, RejectReason.QUEUE_FULL.value,
                            wait_timeout_seconds)
                return None, RejectReason.QUEUE_FULL.value
            if not waited:
                waited = True
                logger.info("submit key=%s queue full, waiting up to %.3fs for capacity",
                            idempotency_key, wait_timeout_seconds)
            try:
                await asyncio.wait_for(self._capacity_changed.wait(), remaining)
            except asyncio.TimeoutError:
                logger.info("submit key=%s rejected: %s (waited %.3fs, no slot freed)",
                            idempotency_key, RejectReason.QUEUE_FULL.value,
                            wait_timeout_seconds)
                return None, RejectReason.QUEUE_FULL.value

    # ---- read-only overview ------------------------------------------------

    def overview(self) -> dict:
        """Consistent read-only snapshot of queue state for operators.

        All numbers come from the store under its lock; derived values are
        clamped so available capacity never goes negative or above the limit."""
        counts = self.store.status_counts()
        pending = counts[JobStatus.PENDING.value]
        running = counts[JobStatus.RUNNING.value]
        available = min(self.config.max_queue_size,
                        max(0, self.config.max_queue_size - pending))
        window = self.config.recent_window_seconds
        recent_rejections = self.store.recent_rejection_count(window)
        recent_failures = self.store.recent_failure_count(window)
        return {
            "counts": counts,
            "concurrency": {"running": running,
                            "max": self.config.max_concurrency},
            "capacity": {"max_queue_size": self.config.max_queue_size,
                         "pending": pending,
                         "available": available},
            "backpressure": {
                "rejected_total": self.store.backpressure_rejected_total(),
                "recent_rejections": recent_rejections,
                "rejecting": recent_rejections > 0,
            },
            "failures": {"recent_failures": recent_failures,
                         "failing": recent_failures > 0},
            "recent_window_seconds": window,
        }

    # ---- worker internals ------------------------------------------------

    async def _worker_loop(self, worker_id: int) -> None:
        while not self._stopped:
            job = self.store.claim_next_pending()
            if job is None:
                self._wakeup.clear()
                try:
                    await asyncio.wait_for(
                        self._wakeup.wait(), self.config.poll_interval_seconds)
                except asyncio.TimeoutError:
                    pass
                continue
            # A pending slot just freed up: wake submissions waiting for
            # capacity (submit with wait_timeout_seconds).
            if self._capacity_changed:
                self._capacity_changed.set()
            await self._execute(worker_id, job)

    async def _execute(self, worker_id: int, job: Job) -> None:
        handler = self.handlers.get(job.job_type)
        attempt = job.attempt
        try:
            if handler is None:
                raise NonRetryableError(f"no handler for type {job.job_type!r}")
            await handler(job)
        except RetryableError as exc:
            reason = str(exc) or exc.__class__.__name__
            if attempt >= job.max_attempts:
                self.store.record_event(job.job_id, attempt, "dead", reason)
                self.store.complete_job(job.job_id, attempt, JobStatus.DEAD, reason)
                logger.info("job=%s attempt=%d decision=dead reason=%r "
                            "(retries exhausted, max_attempts=%d)",
                            job.job_id, attempt, reason, job.max_attempts)
            else:
                delay = self._backoff(attempt)
                self.store.record_event(job.job_id, attempt, "failed_retryable", reason)
                self.store.reschedule_for_retry(job.job_id, attempt, reason,
                                                time.time() + delay)
                logger.info("job=%s attempt=%d decision=retry reason=%r "
                            "(retryable, backoff=%.3fs)", job.job_id, attempt,
                            reason, delay)
        except NonRetryableError as exc:
            reason = str(exc) or exc.__class__.__name__
            self.store.record_event(job.job_id, attempt, "failed_non_retryable", reason)
            self.store.complete_job(job.job_id, attempt, JobStatus.FAILED, reason)
            logger.info("job=%s attempt=%d decision=fail reason=%r (non-retryable)",
                        job.job_id, attempt, reason)
        except Exception as exc:  # unknown errors are treated as retryable
            reason = f"{exc.__class__.__name__}: {exc}"
            if attempt >= job.max_attempts:
                self.store.record_event(job.job_id, attempt, "dead", reason)
                self.store.complete_job(job.job_id, attempt, JobStatus.DEAD, reason)
                logger.info("job=%s attempt=%d decision=dead reason=%r "
                            "(unknown error, retries exhausted)",
                            job.job_id, attempt, reason)
            else:
                delay = self._backoff(attempt)
                self.store.record_event(job.job_id, attempt, "failed_retryable", reason)
                self.store.reschedule_for_retry(job.job_id, attempt, reason,
                                                time.time() + delay)
                logger.info("job=%s attempt=%d decision=retry reason=%r "
                            "(unknown error treated as retryable, backoff=%.3fs)",
                            job.job_id, attempt, reason, delay)
        else:
            self.store.complete_job(job.job_id, attempt, JobStatus.SUCCEEDED, None)
            logger.info("job=%s attempt=%d decision=succeed (handler completed)",
                        job.job_id, attempt)

    def _backoff(self, attempt: int) -> float:
        delay = self.config.base_backoff_seconds * (2 ** (attempt - 1))
        return min(delay, self.config.max_backoff_seconds)
