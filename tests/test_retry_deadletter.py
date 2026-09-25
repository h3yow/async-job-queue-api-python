"""Retryable vs non-retryable failure classification, backoff retry,
dead-letter after retry exhaustion, and dead-letter replay."""
import logging

from app.models import JobStatus, NonRetryableError, RetryableError
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


async def test_retryable_failure_retries_then_succeeds(db_path, fast_config):
    store, queue = make_queue(db_path, fast_config)
    attempts = []

    async def flaky(job):
        attempts.append(job.attempt)
        if len(attempts) < 3:
            raise RetryableError("transient downstream error")

    queue.register_handler("flaky", flaky)
    await queue.start()
    job, reason = await queue.submit("flaky", {}, "flaky-key", max_attempts=5)
    assert reason is None

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    assert final.attempt == 3, "two retries then success on attempt 3"
    assert attempts == [1, 2, 3]

    history = store.failure_history(job.job_id)
    retryable = [h for h in history if h["event"] == "failed_retryable"]
    assert len(retryable) == 2
    # Dedup semantics: no duplicate (attempt, reason) failure records.
    keys = [(h["attempt"], h["reason"]) for h in history]
    assert len(keys) == len(set(keys))
    log.info("job=%s attempts=%d history=%s (dedup verified)",
             job.job_id, final.attempt, keys)
    await queue.stop()
    store.close()


async def test_non_retryable_failure_goes_terminal_immediately(db_path, fast_config):
    store, queue = make_queue(db_path, fast_config)
    attempts = []

    async def bad(job):
        attempts.append(job.attempt)
        raise NonRetryableError("payload failed validation")

    queue.register_handler("bad", bad)
    await queue.start()
    job, _ = await queue.submit("bad", {}, "bad-key", max_attempts=5)

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.FAILED
    assert final.attempt == 1, "non-retryable errors must not be retried"
    assert final.last_error == "payload failed validation"
    assert attempts == [1]

    history = store.failure_history(job.job_id)
    assert [h["event"] for h in history] == ["failed_non_retryable"]
    log.info("job=%s attempt=%d decision=terminal-fail reason=%r",
             job.job_id, final.attempt, final.last_error)
    await queue.stop()
    store.close()


async def test_retry_exhaustion_goes_to_dead_letter(db_path, fast_config):
    store, queue = make_queue(db_path, fast_config)
    attempts = []

    async def always_fails(job):
        attempts.append(job.attempt)
        raise RetryableError("downstream still down")

    queue.register_handler("down", always_fails)
    await queue.start()
    job, _ = await queue.submit("down", {}, "down-key", max_attempts=3)

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.DEAD
    assert final.attempt == 3, "exactly max_attempts executions"
    assert attempts == [1, 2, 3]
    assert final.last_error == "downstream still down"

    history = store.failure_history(job.job_id)
    events = [h["event"] for h in history]
    assert events.count("failed_retryable") == 2
    assert events.count("dead") == 1, "exactly one dead-letter record"
    dead = [h for h in history if h["event"] == "dead"][0]
    assert dead["reason"] == "downstream still down", "failure reason preserved"
    log.info("job=%s attempts=%d decision=dead reason=%r (history=%s)",
             job.job_id, final.attempt, dead["reason"], events)
    await queue.stop()
    store.close()


async def test_dead_letter_replay_no_duplicate_terminal_or_side_effects(
        db_path, fast_config):
    store, queue = make_queue(db_path, fast_config)
    side_effects = []
    fail = {"on": True}

    async def handler(job):
        if fail["on"]:
            raise RetryableError("still broken")
        side_effects.append(("effect", job.job_id, job.attempt))

    queue.register_handler("work", handler)
    await queue.start()
    job, _ = await queue.submit("work", {}, "work-key", max_attempts=2)
    dead = await wait_for_status(store, job.job_id, {JobStatus.DEAD})
    assert dead.attempt == 2

    # Replaying a non-dead job is a no-op (idempotent guard) — verify first
    # with a second live job that is not dead.
    other, _ = await queue.submit("work", {}, "other-key", max_attempts=1)
    await wait_for_status(store, other.job_id, {JobStatus.DEAD})

    fail["on"] = False
    replayed = store.replay_dead(job.job_id)
    assert replayed is not None and replayed.status == JobStatus.PENDING
    # Double replay of the same (no longer dead) job is a no-op.
    assert store.replay_dead(job.job_id) is None
    log.info("job=%s replayed once; second replay no-op (idempotent)", job.job_id)

    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED

    # Side effects happened exactly once for the replayed run.
    effects_for_job = [e for e in side_effects if e[1] == job.job_id]
    assert len(effects_for_job) == 1, "no duplicate side effects after replay"

    # No duplicate terminal/dead records for the same attempt+reason.
    history = store.failure_history(job.job_id)
    keys = [(h["attempt"], h["event"], h["reason"]) for h in history]
    assert len(keys) == len(set(keys))
    assert [h["event"] for h in history].count("dead") == 1
    log.info("job=%s final=%s side_effects=%d history=%s",
             job.job_id, final.status.value, len(effects_for_job), keys)
    await queue.stop()
    store.close()
