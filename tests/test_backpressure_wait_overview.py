"""Recoverable backpressure (reject -> resubmit same key -> executed),
optional wait-for-capacity on submit (slot freed vs timeout), and the
read-only queue overview."""
import asyncio
import logging
import time

import httpx

from app.api import create_app
from app.models import JobStatus, NonRetryableError, QueueConfig
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


def tight_config(**kw):
    base = dict(max_concurrency=1, max_queue_size=1, base_backoff_seconds=0.01,
                max_backoff_seconds=0.05, poll_interval_seconds=0.005,
                overview_window_seconds=60.0, overview_sustained_threshold=3)
    base.update(kw)
    return QueueConfig(**base)


async def test_rejected_then_resubmit_same_key_executes(db_path):
    """Full queue -> 429-style rejection leaves no record; resubmitting the
    SAME idempotency key after capacity frees up is accepted and runs."""
    store, queue = make_queue(db_path, tight_config())
    ran = []
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()
        ran.append(job.job_id)

    queue.register_handler("slow", slow)
    await queue.start()

    k0, _ = await queue.submit("slow", {}, "k0")
    await asyncio.sleep(0.05)  # worker claims k0 -> running
    k1, r1 = await queue.submit("slow", {}, "k1")  # fills the single pending slot
    assert r1 is None

    rejected, r2 = await queue.submit("slow", {}, "k2")
    assert rejected is None and r2 == "queue_full"
    # The rejection left NO placeholder: the key is not 'duplicate', and no
    # failed/terminal record exists — capacity rejection is not a failure.
    again, r_again = await queue.submit("slow", {}, "k2")
    assert r_again == "queue_full", "still full, and not misreported as duplicate"
    counts = store.count_by_status()
    assert counts.get("failed", 0) == 0 and counts.get("dead", 0) == 0
    assert sum(counts.values()) == 2, "only k0 and k1 exist"
    log.info("key=k2 rejected reason=%r with no placeholder record (counts=%s)",
             r2, counts)

    gate.set()
    await wait_for_status(store, k0.job_id, TERMINAL)
    await wait_for_status(store, k1.job_id, TERMINAL)

    # Capacity recovered: the SAME key is now accepted and really executes.
    job, r3 = await queue.submit("slow", {}, "k2")
    assert r3 is None and job is not None
    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED and final.attempt == 1
    assert final.last_error is None
    assert store.failure_history(job.job_id) == [], \
        "a capacity-rejected-then-accepted job has no failure records"
    assert ran.count(job.job_id) == 1
    log.info("key=k2 resubmitted after capacity freed -> job=%s status=%s "
             "attempt=%d (same key, executed once)", job.job_id,
             final.status.value, final.attempt)
    await queue.stop()
    store.close()


async def test_wait_for_capacity_accepted_when_slot_frees(db_path):
    """wait_timeout_seconds: a waiting submission is admitted as soon as a
    pending slot frees up, then executes."""
    store, queue = make_queue(db_path, tight_config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    await queue.start()

    k0, _ = await queue.submit("slow", {}, "w0")
    await asyncio.sleep(0.05)
    k1, _ = await queue.submit("slow", {}, "w1")  # queue now full

    started = time.monotonic()
    waiter = asyncio.create_task(
        queue.submit("slow", {}, "w2", wait_timeout_seconds=2.0))
    await asyncio.sleep(0.05)
    assert not waiter.done(), "submission is parked waiting for capacity"
    gate.set()  # let k0 finish; worker claims w1 -> a pending slot frees
    job, reason = await waiter
    waited = time.monotonic() - started
    assert reason is None and job is not None
    assert waited < 2.0
    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    log.info("key=w2 admitted after %.3fs wait -> job=%s status=%s",
             waited, job.job_id, final.status.value)
    await queue.stop()
    store.close()


async def test_wait_for_capacity_times_out_without_placeholder(db_path):
    """wait_timeout_seconds elapses with no free slot -> queue_full, no
    record left behind, and the same key can be submitted again later."""
    store, queue = make_queue(db_path, tight_config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    await queue.start()

    k0, _ = await queue.submit("slow", {}, "t0")
    await asyncio.sleep(0.05)
    k1, _ = await queue.submit("slow", {}, "t1")  # full

    started = time.monotonic()
    job, reason = await queue.submit("slow", {}, "t2", wait_timeout_seconds=0.1)
    waited = time.monotonic() - started
    assert job is None and reason == "queue_full"
    assert waited >= 0.1
    # Timeout rejection left nothing behind.
    counts = store.count_by_status()
    assert sum(counts.values()) == 2
    assert counts.get("failed", 0) == 0
    log.info("key=t2 wait timed out after %.3fs reason=%r (no placeholder)",
             waited, reason)

    gate.set()
    await wait_for_status(store, k0.job_id, TERMINAL)
    await wait_for_status(store, k1.job_id, TERMINAL)
    job2, r2 = await queue.submit("slow", {}, "t2")
    assert r2 is None, "same key accepted after timeout rejection"
    final = await wait_for_status(store, job2.job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    await queue.stop()
    store.close()


async def test_concurrent_waiters_same_key_single_job(db_path):
    """Two concurrent waiting submissions with the same idempotency key:
    exactly one is admitted, the other observes the duplicate; one job,
    one execution."""
    store, queue = make_queue(db_path, tight_config())
    gate = asyncio.Event()
    ran = []

    async def slow(job):
        await gate.wait()
        ran.append(job.job_id)

    queue.register_handler("slow", slow)
    await queue.start()

    k0, _ = await queue.submit("slow", {}, "c0")
    await asyncio.sleep(0.05)
    k1, _ = await queue.submit("slow", {}, "c1")  # full

    waiters = [asyncio.create_task(
        queue.submit("slow", {}, "c2", wait_timeout_seconds=2.0))
        for _ in range(2)]
    await asyncio.sleep(0.05)
    gate.set()
    results = await asyncio.gather(*waiters)
    accepted = [j for j, r in results if r is None]
    duplicates = [j for j, r in results if r == "duplicate"]
    assert len(accepted) == 1 and len(duplicates) == 1
    assert duplicates[0].job_id == accepted[0].job_id
    final = await wait_for_status(store, accepted[0].job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED
    assert ran.count(accepted[0].job_id) == 1, "one key -> one job -> one run"
    log.info("key=c2 concurrent waiters: accepted=1 duplicate=1 job=%s",
             accepted[0].job_id)
    await queue.stop()
    store.close()


async def test_overview_counts_and_signals(db_path):
    """Overview numbers track job state transitions; backpressure rejections
    are counted separately from real handler failures; invariants hold."""
    config = tight_config(max_queue_size=2)
    store, queue = make_queue(db_path, config)
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    async def boom(job):
        raise NonRetryableError("bad payload")

    queue.register_handler("slow", slow)
    queue.register_handler("boom", boom)
    await queue.start()

    ov = queue.overview()
    assert ov["counts"] == {"pending": 0, "running": 0, "succeeded": 0,
                            "failed": 0, "dead": 0}
    assert ov["remaining_capacity"] == 2
    assert ov["max_concurrency"] == 1 and ov["max_queue_size"] == 2
    assert ov["backpressure_rejected_total"] == 0
    assert ov["recent"]["rejections"] == 0 and ov["recent"]["failures"] == 0
    assert ov["recent"]["sustained_rejections"] is False
    assert ov["recent"]["sustained_failures"] is False

    k0, _ = await queue.submit("slow", {}, "o0")
    await asyncio.sleep(0.05)  # running
    await queue.submit("slow", {}, "o1")
    await queue.submit("slow", {}, "o2")  # pending = 2 = capacity
    ov = queue.overview()
    assert ov["counts"]["pending"] == 2 and ov["counts"]["running"] == 1
    assert ov["remaining_capacity"] == 0
    assert 0 <= ov["remaining_capacity"] <= ov["max_queue_size"]

    # Three rejections -> sustained backpressure signal, but NOT failures.
    for i in range(3):
        job, reason = await queue.submit("slow", {}, f"o-rej-{i}")
        assert job is None and reason == "queue_full"
    ov = queue.overview()
    assert ov["backpressure_rejected_total"] == 3
    assert ov["recent"]["rejections"] == 3
    assert ov["recent"]["sustained_rejections"] is True
    assert ov["recent"]["failures"] == 0
    assert ov["counts"]["failed"] == 0 and ov["counts"]["dead"] == 0
    assert ov["remaining_capacity"] == 0, "clamped, never negative"
    log.info("overview under backpressure: %s (rejections != failures)",
             {k: ov[k] for k in ("counts", "remaining_capacity",
                                 "backpressure_rejected_total")})

    gate.set()
    await asyncio.sleep(0.1)
    ov = queue.overview()
    assert ov["counts"]["succeeded"] == 3
    assert ov["counts"]["pending"] == 0 and ov["counts"]["running"] == 0
    assert ov["remaining_capacity"] == 2

    # A real handler failure shows up as a failure, distinct from backpressure.
    fjob, _ = await queue.submit("boom", {}, "o-fail")
    final = await wait_for_status(store, fjob.job_id, TERMINAL)
    assert final.status == JobStatus.FAILED
    ov = queue.overview()
    assert ov["counts"]["failed"] == 1
    assert ov["recent"]["failures"] == 1
    assert ov["recent"]["sustained_failures"] is False
    assert ov["backpressure_rejected_total"] == 3, "failures don't touch it"
    log.info("overview after failure: counts=%s recent=%s",
             ov["counts"], ov["recent"])
    await queue.stop()
    store.close()


async def test_overview_endpoint_and_wait_via_http(db_path):
    """HTTP: GET /queue/overview is read-only and self-consistent; POST /jobs
    with wait_timeout_seconds waits then 202s, or 429s on timeout."""
    store, queue = make_queue(db_path, tight_config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    app = create_app(queue)
    await queue.start()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as client:
        r = await client.get("/queue/overview")
        assert r.status_code == 200
        assert r.json()["remaining_capacity"] == 1

        await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h0"})
        await asyncio.sleep(0.05)  # claimed -> running
        await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h1"})  # full

        # Waiting submission admitted once a slot frees -> 202.
        async def release_soon():
            await asyncio.sleep(0.05)
            gate.set()
        releaser = asyncio.create_task(release_soon())
        r = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h2",
            "wait_timeout_seconds": 2.0})
        assert r.status_code == 202
        assert r.json()["duplicate"] is False
        h2 = r.json()["job"]["job_id"]
        await releaser
        await wait_for_status(store, h2, TERMINAL)  # deterministic: drained

        # Refill and let a waiting submission time out -> 429, no record.
        gate.clear()
        r = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h3"})
        assert r.status_code == 202
        await asyncio.sleep(0.1)  # h3 claimed -> running (blocked on gate)
        r = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h4"})
        assert r.status_code == 202  # pending = 1 = capacity -> full
        h4 = r.json()["job"]["job_id"]
        r = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h5",
            "wait_timeout_seconds": 0.1})
        assert r.status_code == 429
        assert r.json()["error"] == "queue_full"
        log.info("HTTP wait-timeout rejection: 429 %s", r.json())

        ov = (await client.get("/queue/overview")).json()
        assert ov["backpressure_rejected_total"] == 1
        assert ov["counts"]["failed"] == 0
        assert 0 <= ov["remaining_capacity"] <= ov["max_queue_size"]

        # The timed-out key is still usable after capacity recovers.
        gate.set()
        await wait_for_status(store, h4, TERMINAL)  # queue fully drained
        r = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "h5"})
        assert r.status_code == 202
        final = await wait_for_status(
            store, r.json()["job"]["job_id"], TERMINAL)
        assert final.status == JobStatus.SUCCEEDED
        log.info("key=h5 resubmitted after 429 -> job=%s status=%s",
                 final.job_id, final.status.value)
    await queue.stop()
    store.close()
