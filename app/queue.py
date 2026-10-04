"""Async scheduler / worker pool with bounded concurrency, backpressure,
retry with exponential backoff, and dead-letter handling."""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
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

REJECTED_TOTAL_COUNTER = "backpressure_rejected_total"


class JobQueue:
    def __init__(self, store: JobStore, config: QueueConfig) -> None:
        self.store = store
        self.config = config
        self.handlers: dict[str, Handler] = {}
        self._workers: list[asyncio.Task] = []
        self._wakeup: asyncio.Event | None = None
        self._capacity: asyncio.Event | None = None
        self._stopped = False
        # In-memory sliding-window signals for the queue overview.
        self._recent_rejections: deque[float] = deque()
        self._recent_failures: deque[float] = deque()

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
        self._capacity = asyncio.Event()
        self._workers = [
            asyncio.create_task(self._worker_loop(i))
            for i in range(self.config.max_concurrency)
        ]

    async def stop(self) -> None:
        self._stopped = True
        if self._wakeup:
            self._wakeup.set()
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

    # ---- submission ------------------------------------------------------

    def _new_job(self, job_type: str, payload: dict, idempotency_key: str,
                 max_attempts: int | None) -> Job:
        return Job(
            job_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            job_type=job_type,
            payload=payload,
            max_attempts=max_attempts or 3,
        )

    def _try_accept(self, job: Job) -> tuple[Job | None, str | None]:
        stored, reason = self.store.try_accept_job(job, self.config.max_queue_size)
        if reason == RejectReason.DUPLICATE.value:
            logger.info("submit key=%s -> duplicate of job=%s (idempotent replay)",
                        job.idempotency_key, stored.job_id if stored else None)
            return stored, RejectReason.DUPLICATE.value
        if reason == RejectReason.QUEUE_FULL.value:
            return None, RejectReason.QUEUE_FULL.value
        if self._wakeup:
            self._wakeup.set()
        logger.info("submit key=%s accepted as job=%s",
                    job.idempotency_key, job.job_id)
        return job, None

    def _record_rejection(self, idempotency_key: str, waited: float) -> None:
        """A capacity rejection is NOT a job failure: no row, no failure
        event — only the cumulative counter and the recent-signal window."""
        total = self.store.incr_counter(REJECTED_TOTAL_COUNTER)
        self._recent_rejections.append(time.time())
        logger.info("submit key=%s rejected: %s (pending >= %d, waited=%.3fs, "
                    "total_rejected=%d) — retryable by resubmitting the same key",
                    idempotency_key, RejectReason.QUEUE_FULL.value,
                    self.config.max_queue_size, waited, total)

    async def submit(self, job_type: str, payload: dict, idempotency_key: str,
                     max_attempts: int | None = None,
                     wait_timeout_seconds: float | None = None
                     ) -> tuple[Job | None, str | None]:
        """Returns (job, None) when accepted, (existing_job, 'duplicate') when
        the idempotency key was already submitted, (None, 'queue_full') when
        the capacity limit is reached (backpressure).

        A queue_full rejection leaves no record behind: resubmitting the same
        idempotency key after capacity frees up is accepted and executed.

        With wait_timeout_seconds > 0 the call waits for a slot to free up
        (up to the timeout) instead of rejecting immediately; on timeout it
        still rejects with 'queue_full' and leaves no placeholder record."""
        job = self._new_job(job_type, payload, idempotency_key, max_attempts)
        accepted, reason = self._try_accept(job)
        if reason != RejectReason.QUEUE_FULL.value:
            return accepted, reason

        wait = wait_timeout_seconds or 0.0
        if wait <= 0 or self._capacity is None:
            self._record_rejection(idempotency_key, waited=0.0)
            return None, RejectReason.QUEUE_FULL.value

        deadline = time.monotonic() + wait
        logger.info("submit key=%s queue full, waiting up to %.3fs for capacity",
                    idempotency_key, wait)
        while True:
            # Clear BEFORE re-checking so a slot freed between the check and
            # the wait still wakes us (no missed wakeup).
            self._capacity.clear()
            accepted, reason = self._try_accept(job)
            if reason != RejectReason.QUEUE_FULL.value:
                if reason is None:
                    logger.info("submit key=%s accepted as job=%s after waiting",
                                idempotency_key, job.job_id)
                return accepted, reason
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                waited = wait
                self._record_rejection(idempotency_key, waited)
                return None, RejectReason.QUEUE_FULL.value
            try:
                await asyncio.wait_for(self._capacity.wait(), remaining)
            except asyncio.TimeoutError:
                pass  # deadline re-checked at the top of the loop

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
            # A pending slot just freed up — wake submissions waiting for
            # capacity (wait_timeout_seconds).
            if self._capacity:
                self._capacity.set()
            await self._execute(worker_id, job)

    def _record_failure_event(self, job_id: str, attempt: int, event: str,
                              reason: str) -> None:
        self.store.record_event(job_id, attempt, event, reason)
        self._recent_failures.append(time.time())

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
                self._record_failure_event(job.job_id, attempt, "dead", reason)
                self.store.complete_job(job.job_id, attempt, JobStatus.DEAD, reason)
                logger.info("job=%s attempt=%d decision=dead reason=%r "
                            "(retries exhausted, max_attempts=%d)",
                            job.job_id, attempt, reason, job.max_attempts)
            else:
                delay = self._backoff(attempt)
                self._record_failure_event(job.job_id, attempt, "failed_retryable", reason)
                self.store.reschedule_for_retry(job.job_id, attempt, reason,
                                                time.time() + delay)
                logger.info("job=%s attempt=%d decision=retry reason=%r "
                            "(retryable, backoff=%.3fs)", job.job_id, attempt,
                            reason, delay)
        except NonRetryableError as exc:
            reason = str(exc) or exc.__class__.__name__
            self._record_failure_event(job.job_id, attempt, "failed_non_retryable", reason)
            self.store.complete_job(job.job_id, attempt, JobStatus.FAILED, reason)
            logger.info("job=%s attempt=%d decision=fail reason=%r (non-retryable)",
                        job.job_id, attempt, reason)
        except Exception as exc:  # unknown errors are treated as retryable
            reason = f"{exc.__class__.__name__}: {exc}"
            if attempt >= job.max_attempts:
                self._record_failure_event(job.job_id, attempt, "dead", reason)
                self.store.complete_job(job.job_id, attempt, JobStatus.DEAD, reason)
                logger.info("job=%s attempt=%d decision=dead reason=%r "
                            "(unknown error, retries exhausted)",
                            job.job_id, attempt, reason)
            else:
                delay = self._backoff(attempt)
                self._record_failure_event(job.job_id, attempt, "failed_retryable", reason)
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

    # ---- read-only overview ----------------------------------------------

    @staticmethod
    def _window_count(events: deque[float], cutoff: float) -> int:
        while events and events[0] < cutoff:
            events.popleft()
        return len(events)

    def overview(self) -> dict:
        """Read-only, self-consistent snapshot for operators: per-status
        counts, concurrency/capacity limits, remaining capacity, cumulative
        backpressure rejections, and recent rejection/failure signals.

        Invariants: remaining_capacity is clamped to [0, max_queue_size];
        a full queue (recent rejections) is reported separately from real
        handler failures so the two are never conflated."""
        counts = self.store.count_by_status()
        pending = counts.get(JobStatus.PENDING.value, 0)
        remaining = min(max(self.config.max_queue_size - pending, 0),
                        self.config.max_queue_size)
        window = self.config.overview_window_seconds
        cutoff = time.time() - window
        rejections = self._window_count(self._recent_rejections, cutoff)
        failures = self._window_count(self._recent_failures, cutoff)
        threshold = self.config.overview_sustained_threshold
        return {
            "counts": {
                s.value: counts.get(s.value, 0) for s in JobStatus
            },
            "max_concurrency": self.config.max_concurrency,
            "max_queue_size": self.config.max_queue_size,
            "running": counts.get(JobStatus.RUNNING.value, 0),
            "remaining_capacity": remaining,
            "backpressure_rejected_total": self.store.get_counter(
                REJECTED_TOTAL_COUNTER),
            "recent": {
                "window_seconds": window,
                "rejections": rejections,
                "failures": failures,
                "sustained_rejections": rejections >= threshold,
                "sustained_failures": failures >= threshold,
            },
        }
