"""Optional wait-for-capacity on submit: a full queue does not reject
immediately when the caller budgets a wait; acceptance happens as soon as a
slot frees up, and a timed-out wait leaves no record behind."""
import asyncio
import logging
import time

from app.models import JobStatus, QueueConfig
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


def _config():
    return QueueConfig(max_concurrency=1, max_queue_size=2,
                       base_backoff_seconds=0.01, poll_interval_seconds=0.005,
                       recent_window_seconds=60.0)


async def _fill_queue(queue, store, gate):
    """Occupy the single worker and fill pending capacity (2)."""
    blocker, _ = await queue.submit("slow", {}, "blocker")
    await asyncio.sleep(0.05)
    assert store.get_job(blocker.job_id).status == JobStatus.RUNNING
    j1, _ = await queue.submit("slow", {}, "p1")
    j2, _ = await queue.submit("slow", {}, "p2")
    return blocker, j1, j2


async def test_wait_accepted_when_slot_frees_up(db_path):
    store, queue = make_queue(db_path, _config())
    gate = asyncio.Event()
    executed = []

    async def slow(job):
        await gate.wait()
        executed.append(job.job_id)

    queue.register_handler("slow", slow)
    await queue.start()
    blocker, j1, j2 = await _fill_queue(queue, store, gate)

    async def release_soon():
        await asyncio.sleep(0.1)
        gate.set()  # blocker finishes -> worker claims p1 -> a slot frees

    releaser = asyncio.create_task(release_soon())
    t0 = time.monotonic()
    job, reason = await queue.submit("slow", {}, "waiter",
                                     wait_timeout_seconds=5.0)
    waited = time.monotonic() - t0
    await releaser
    assert reason is None and job is not None, "must be accepted once a slot frees"
    assert waited >= 0.09, "it actually waited for capacity"
    log.info("key=waiter accepted after waiting %.3fs for capacity", waited)

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    assert job.job_id in executed
    await queue.stop()
    store.close()


async def test_wait_timeout_rejects_and_leaves_no_record(db_path):
    store, queue = make_queue(db_path, _config())
    gate = asyncio.Event()  # never released while we wait

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    await queue.start()
    blocker, j1, j2 = await _fill_queue(queue, store, gate)

    t0 = time.monotonic()
    job, reason = await queue.submit("slow", {}, "impatient",
                                     wait_timeout_seconds=0.15)
    waited = time.monotonic() - t0
    assert job is None and reason == "queue_full"
    assert 0.1 <= waited < 1.0, "waited roughly the budget, then gave up"
    log.info("key=impatient rejected after %.3fs wait: %s (no placeholder left)",
             waited, reason)

    # The timed-out submission left no record: no job row, no failure event,
    # and the same key is still free for a later submission.
    counts = store.status_counts()
    assert counts[JobStatus.FAILED.value] == 0
    assert store.backpressure_rejected_total() == 1

    gate.set()
    await wait_for_status(store, j1.job_id, TERMINAL)
    job2, reason2 = await queue.submit("slow", {}, "impatient")
    assert reason2 is None and job2 is not None, "same key accepted afterwards"
    final = await wait_for_status(store, job2.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    log.info("key=impatient resubmitted as job=%s -> %s",
             job2.job_id, final.status.value)
    await queue.stop()
    store.close()


async def test_concurrent_same_key_while_waiting_yields_single_job(db_path):
    store, queue = make_queue(db_path, _config())
    gate = asyncio.Event()
    executions = []

    async def slow(job):
        await gate.wait()
        executions.append(job.job_id)

    queue.register_handler("slow", slow)
    await queue.start()
    blocker, j1, j2 = await _fill_queue(queue, store, gate)

    async def release_soon():
        await asyncio.sleep(0.1)
        gate.set()

    releaser = asyncio.create_task(release_soon())
    # Many concurrent submissions of the SAME key, all waiting for capacity.
    results = await asyncio.gather(*[
        queue.submit("slow", {}, "shared-key", wait_timeout_seconds=5.0)
        for _ in range(10)
    ])
    await releaser
    accepted = [j for j, r in results if r is None]
    duplicates = [j for j, r in results if r == "duplicate"]
    rejected = [r for j, r in results if r == "queue_full"]
    assert len(accepted) == 1, "exactly one waiter may insert the job"
    assert len(duplicates) == 9 and not rejected
    job = accepted[0]
    assert all(d.job_id == job.job_id for d in duplicates)
    log.info("key=shared-key: accepted=1 duplicates=%d rejected=%d "
             "(single job per key even while waiting)",
             len(duplicates), len(rejected))

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    assert executions.count(job.job_id) == 1
    await queue.stop()
    store.close()


async def test_no_wait_keeps_immediate_reject_default(db_path):
    """Omitting wait_timeout_seconds preserves the old behavior: a full queue
    rejects immediately."""
    store, queue = make_queue(db_path, _config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    await queue.start()
    await _fill_queue(queue, store, gate)

    t0 = time.monotonic()
    job, reason = await queue.submit("slow", {}, "nowait")
    assert time.monotonic() - t0 < 0.1, "no waiting without the option"
    assert job is None and reason == "queue_full"
    log.info("key=nowait rejected immediately (default behavior preserved)")
    gate.set()
    await queue.stop()
    store.close()
