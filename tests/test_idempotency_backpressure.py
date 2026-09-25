"""Concurrent submission of the same idempotency key, and backpressure
rejection when the capacity limit is reached."""
import asyncio
import logging

from app.models import JobStatus
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


async def test_concurrent_same_idempotency_key(db_path, fast_config):
    store, queue = make_queue(db_path, fast_config)
    executions = []

    async def handler(job):
        executions.append(job.job_id)

    queue.register_handler("ok", handler)
    await queue.start()

    results = await asyncio.gather(*[
        queue.submit("ok", {"n": i}, "same-key") for i in range(20)
    ])
    accepted = [j for j, r in results if r is None]
    duplicates = [j for j, r in results if r == "duplicate"]
    assert len(accepted) == 1, "exactly one submission may be accepted"
    assert len(duplicates) == 19
    job = accepted[0]
    assert all(d.job_id == job.job_id for d in duplicates)
    log.info("job=%s accepted=1 duplicates=%d (idempotency key dedup)",
             job.job_id, len(duplicates))

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    assert executions == [job.job_id], "handler must run exactly once"
    assert final.attempt == 1
    await queue.stop()
    store.close()


async def test_backpressure_rejection_reason_distinguishable(db_path, tmp_path):
    from app.models import QueueConfig
    config = QueueConfig(max_concurrency=1, max_queue_size=2,
                         base_backoff_seconds=0.01, poll_interval_seconds=0.01)
    store, queue = make_queue(db_path, config)
    gate = asyncio.Event()
    started = []

    async def slow_handler(job):
        started.append(job.job_id)
        await gate.wait()  # hold the single worker

    queue.register_handler("slow", slow_handler)
    await queue.start()

    first, r0 = await queue.submit("slow", {}, "k0")
    assert r0 is None
    await asyncio.sleep(0.05)  # let the worker claim k0 -> running
    assert store.get_job(first.job_id).status == JobStatus.RUNNING

    # Fill the pending queue up to capacity (2 pending allowed).
    j1, r1 = await queue.submit("slow", {}, "k1")
    j2, r2 = await queue.submit("slow", {}, "k2")
    assert r1 is None and r2 is None

    # Next submission exceeds capacity -> backpressure with distinct reason.
    rejected, r3 = await queue.submit("slow", {}, "k3")
    assert rejected is None and r3 == "queue_full"
    log.info("job rejected reason=%r (distinct from 'duplicate' and handler errors)", r3)

    # The rejection reason is distinguishable from a duplicate submission.
    dup, r_dup = await queue.submit("slow", {}, "k1")
    assert r_dup == "duplicate" and dup.job_id == j1.job_id
    assert r3 != r_dup

    # The rejected job must never execute.
    gate.set()
    await wait_for_status(store, j1.job_id, TERMINAL)
    await wait_for_status(store, j2.job_id, TERMINAL)
    await asyncio.sleep(0.05)
    assert len(started) == 3  # k0, k1, k2 only
    await queue.stop()
    store.close()
