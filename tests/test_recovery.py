"""Crash / restart recovery: unfinished jobs must be resumed, not lost,
and terminal states must not be duplicated."""
import asyncio
import logging

from app.models import JobStatus, QueueConfig
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


async def test_restart_recovers_interrupted_and_pending_jobs(db_path):
    config = QueueConfig(max_concurrency=1, max_queue_size=8,
                         base_backoff_seconds=0.01, poll_interval_seconds=0.005)
    # --- first "process": one job interrupted mid-flight, one still pending
    store1, queue1 = make_queue(db_path, config)
    entered = asyncio.Event()

    async def blocking_handler(job):
        entered.set()
        await asyncio.sleep(60)  # simulate long work; will be "crashed"

    queue1.register_handler("block", blocking_handler)
    await queue1.start()
    running_job, _ = await queue1.submit("block", {}, "crash-key")
    await asyncio.wait_for(entered.wait(), 2)
    pending_job, _ = await queue1.submit("block", {}, "pending-key")
    assert store1.get_job(running_job.job_id).status == JobStatus.RUNNING

    # Simulate process kill: cancel workers WITHOUT graceful requeue.
    for w in queue1._workers:
        w.cancel()
    await asyncio.gather(*queue1._workers, return_exceptions=True)
    queue1._workers = []
    store1.close()
    log.info("simulated crash: job=%s left RUNNING, job=%s left PENDING",
             running_job.job_id, pending_job.job_id)

    # --- second "process": same DB file, jobs must be resumed
    store2, queue2 = make_queue(db_path, config)
    completed = []

    async def quick_handler(job):
        completed.append((job.job_id, job.attempt))

    queue2.register_handler("block", quick_handler)
    await queue2.start()

    final_running = await wait_for_status(store2, running_job.job_id, TERMINAL)
    final_pending = await wait_for_status(store2, pending_job.job_id, TERMINAL)
    assert final_running.status == JobStatus.SUCCEEDED
    assert final_pending.status == JobStatus.SUCCEEDED
    log.info("recovered job=%s attempt=%d decision=succeed; job=%s attempt=%d",
             final_running.job_id, final_running.attempt,
             final_pending.job_id, final_pending.attempt)

    # The interrupted job kept its attempt counter (monotonic, no reset).
    assert final_running.attempt == 2, "claim before crash counted as attempt 1"
    # Each job executed exactly once in the second process: no duplicate
    # terminal writes, no lost jobs.
    assert sorted(j for j, _ in completed) == sorted(
        [running_job.job_id, pending_job.job_id])
    assert len(completed) == 2
    await queue2.stop()
    store2.close()


async def test_restart_does_not_duplicate_completed_terminal_state(db_path):
    config = QueueConfig(max_concurrency=2, max_queue_size=8,
                         base_backoff_seconds=0.01, poll_interval_seconds=0.005)
    store1, queue1 = make_queue(db_path, config)

    async def handler(job):
        return None

    queue1.register_handler("ok", handler)
    await queue1.start()
    job, _ = await queue1.submit("ok", {}, "done-key")
    final = await wait_for_status(store1, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    await queue1.stop()
    store1.close()

    # Restart: a terminal job must stay terminal, untouched.
    store2, queue2 = make_queue(db_path, config)
    queue2.register_handler("ok", handler)
    await queue2.start()
    await asyncio.sleep(0.1)
    after = store2.get_job(job.job_id)
    assert after.status == JobStatus.SUCCEEDED
    assert after.attempt == 1, "no re-execution after restart"
    # Resubmitting the same key after restart is still a duplicate.
    dup, reason = await queue2.submit("ok", {}, "done-key")
    assert reason == "duplicate" and dup.job_id == job.job_id
    log.info("job=%s terminal state preserved across restart (attempt=%d)",
             job.job_id, after.attempt)
    await queue2.stop()
    store2.close()
