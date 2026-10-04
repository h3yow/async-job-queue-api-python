"""Backpressure is recoverable: a capacity rejection leaves no record, so the
same idempotency key can be resubmitted once capacity frees up, and the
rejection is never counted as a job failure."""
import asyncio
import logging

from app.models import JobStatus, QueueConfig
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


def _full_queue_config():
    return QueueConfig(max_concurrency=1, max_queue_size=2,
                       base_backoff_seconds=0.01, poll_interval_seconds=0.005,
                       recent_window_seconds=60.0)


async def test_rejected_key_can_be_resubmitted_after_capacity_frees(db_path):
    store, queue = make_queue(db_path, _full_queue_config())
    gate = asyncio.Event()
    executed = []

    async def slow(job):
        await gate.wait()
        executed.append(job.job_id)

    queue.register_handler("slow", slow)
    await queue.start()

    blocker, _ = await queue.submit("slow", {}, "blocker")
    await asyncio.sleep(0.05)  # worker claims blocker -> running
    assert store.get_job(blocker.job_id).status == JobStatus.RUNNING
    j1, _ = await queue.submit("slow", {}, "k1")
    j2, _ = await queue.submit("slow", {}, "k2")

    # Queue is full: the submission is rejected with a distinct reason.
    rejected, reason = await queue.submit("slow", {}, "retry-me")
    assert rejected is None and reason == "queue_full"
    log.info("key=retry-me rejected reason=%r (capacity, not a failure)", reason)

    # The rejection left no job record and no failure history behind.
    assert store.status_counts()[JobStatus.FAILED.value] == 0
    assert store.failure_history("nonexistent") == []
    assert store.backpressure_rejected_total() == 1

    # Capacity frees up; resubmitting the SAME key is accepted and executed.
    gate.set()
    await wait_for_status(store, j1.job_id, TERMINAL)
    job, reason2 = await queue.submit("slow", {}, "retry-me")
    assert reason2 is None, "same key must be accepted after rejection"
    assert job is not None
    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    assert job.job_id in executed
    log.info("key=retry-me resubmitted as job=%s -> %s (backpressure recovered)",
             job.job_id, final.status.value)

    # The whole episode counts as zero failed jobs.
    counts = store.status_counts()
    assert counts[JobStatus.FAILED.value] == 0
    assert counts[JobStatus.DEAD.value] == 0
    await queue.stop()
    store.close()


async def test_rejection_is_distinguishable_from_execution_failure(db_path):
    store, queue = make_queue(db_path, _full_queue_config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()
        if job.payload.get("fail"):
            from app.models import NonRetryableError
            raise NonRetryableError("real handler failure")

    queue.register_handler("slow", slow)
    await queue.start()

    blocker, _ = await queue.submit("slow", {}, "b0")
    await asyncio.sleep(0.05)
    await queue.submit("slow", {}, "b1")
    await queue.submit("slow", {}, "b2")

    rejected, reason = await queue.submit("slow", {}, "b3")
    assert rejected is None and reason == "queue_full"

    # A real failure looks completely different from a capacity rejection.
    gate.set()
    await wait_for_status(store, blocker.job_id, TERMINAL)
    failing, reason_f = await queue.submit("slow", {"fail": True}, "f1")
    assert reason_f is None, "capacity freed once blocker finished"
    final = await wait_for_status(store, failing.job_id, TERMINAL)
    assert final.status == JobStatus.FAILED
    assert final.last_error == "real handler failure"
    history = store.failure_history(failing.job_id)
    assert [h["event"] for h in history] == ["failed_non_retryable"]

    counts = store.status_counts()
    assert counts[JobStatus.FAILED.value] == 1, "only the real failure counts"
    assert store.backpressure_rejected_total() == 1
    log.info("job=%s failed (handler error) vs key=b3 rejected (capacity): "
             "failed_count=%d rejected_total=%d",
             failing.job_id, counts[JobStatus.FAILED.value],
             store.backpressure_rejected_total())
    await queue.stop()
    store.close()
