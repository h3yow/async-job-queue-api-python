"""Read-only queue overview: per-status counts, concurrency/capacity,
cumulative backpressure rejections, and recent rejecting/failing signals stay
consistent while jobs move through the pipeline."""
import asyncio
import logging

import httpx

from app.api import create_app
from app.models import JobStatus, NonRetryableError, QueueConfig, RetryableError
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


def _config():
    return QueueConfig(max_concurrency=1, max_queue_size=2,
                       base_backoff_seconds=0.01, max_backoff_seconds=0.02,
                       poll_interval_seconds=0.005, recent_window_seconds=60.0)


def _assert_invariants(ov, config):
    counts = ov["counts"]
    for status in ("pending", "running", "succeeded", "failed", "dead"):
        assert counts[status] >= 0, f"negative count for {status}"
    cap = ov["capacity"]
    assert 0 <= cap["available"] <= cap["max_queue_size"]
    assert cap["available"] == max(0, cap["max_queue_size"] - cap["pending"])
    assert cap["max_queue_size"] == config.max_queue_size
    conc = ov["concurrency"]
    assert 0 <= conc["running"] <= conc["max"] == config.max_concurrency
    assert conc["running"] == counts["running"]
    assert ov["backpressure"]["rejected_total"] >= 0


async def test_overview_tracks_status_transitions(db_path):
    store, queue = make_queue(db_path, _config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    await queue.start()

    ov = queue.overview()
    _assert_invariants(ov, queue.config)
    assert ov["counts"]["pending"] == 0 and ov["counts"]["running"] == 0
    assert ov["capacity"]["available"] == 2
    assert ov["backpressure"]["rejected_total"] == 0
    assert ov["backpressure"]["rejecting"] is False
    assert ov["failures"]["failing"] is False

    blocker, _ = await queue.submit("slow", {}, "o0")
    await asyncio.sleep(0.05)  # claimed -> running
    await queue.submit("slow", {}, "o1")
    await queue.submit("slow", {}, "o2")

    ov = queue.overview()
    _assert_invariants(ov, queue.config)
    assert ov["counts"]["running"] == 1 and ov["counts"]["pending"] == 2
    assert ov["capacity"]["available"] == 0
    log.info("overview mid-flight: counts=%s capacity=%s",
             ov["counts"], ov["capacity"])

    # Full queue -> rejections are visible cumulatively and as "recently
    # rejecting", but do NOT show up as failures.
    for key in ("o3", "o4"):
        _, reason = await queue.submit("slow", {}, key)
        assert reason == "queue_full"
    ov = queue.overview()
    _assert_invariants(ov, queue.config)
    assert ov["backpressure"]["rejected_total"] == 2
    assert ov["backpressure"]["recent_rejections"] == 2
    assert ov["backpressure"]["rejecting"] is True
    assert ov["counts"]["failed"] == 0 and ov["failures"]["failing"] is False
    log.info("overview under backpressure: backpressure=%s failures=%s",
             ov["backpressure"], ov["failures"])

    gate.set()
    await asyncio.sleep(0.2)  # let o0/o1/o2 drain to succeeded
    ov = queue.overview()
    _assert_invariants(ov, queue.config)
    assert ov["counts"]["succeeded"] == 3
    assert ov["counts"]["pending"] == 0 and ov["counts"]["running"] == 0
    assert ov["capacity"]["available"] == 2
    log.info("overview after drain: counts=%s", ov["counts"])
    await queue.stop()
    store.close()


async def test_overview_distinguishes_failures_from_backpressure(db_path):
    store, queue = make_queue(db_path, _config())

    async def flaky(job):
        if job.payload.get("kind") == "retryable":
            raise RetryableError("transient")
        if job.payload.get("kind") == "fatal":
            raise NonRetryableError("bad payload")

    queue.register_handler("work", flaky)
    await queue.start()

    ok, _ = await queue.submit("work", {}, "ok1")
    await wait_for_status(store, ok.job_id, TERMINAL)
    bad, _ = await queue.submit("work", {"kind": "fatal"}, "bad1")
    await wait_for_status(store, bad.job_id, TERMINAL)
    dead, _ = await queue.submit("work", {"kind": "retryable"}, "dead1",
                                 max_attempts=2)
    await wait_for_status(store, dead.job_id, TERMINAL)

    ov = queue.overview()
    _assert_invariants(ov, queue.config)
    assert ov["counts"]["succeeded"] == 1
    assert ov["counts"]["failed"] == 1
    assert ov["counts"]["dead"] == 1
    # 1 retryable failure event + 1 dead event + 1 non-retryable event.
    assert ov["failures"]["recent_failures"] == 3
    assert ov["failures"]["failing"] is True
    assert ov["backpressure"]["rejected_total"] == 0
    assert ov["backpressure"]["rejecting"] is False
    log.info("overview with real failures: failures=%s backpressure=%s "
             "(clearly not a capacity problem)",
             ov["failures"], ov["backpressure"])
    await queue.stop()
    store.close()


async def test_overview_endpoint_via_http(db_path):
    store, queue = make_queue(db_path, _config())

    async def handler(job):
        return None

    queue.register_handler("echo", handler)
    app = create_app(queue)
    await queue.start()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as client:
        r = await client.post("/jobs", json={
            "job_type": "echo", "payload": {}, "idempotency_key": "http-ov"})
        assert r.status_code == 202
        job_id = r.json()["job"]["job_id"]
        await wait_for_status(store, job_id, TERMINAL)

        r = await client.get("/queue/overview")
        assert r.status_code == 200
        ov = r.json()
        _assert_invariants(ov, queue.config)
        assert ov["counts"]["succeeded"] == 1
        log.info("GET /queue/overview -> %s", ov)
    await queue.stop()
    store.close()


async def test_submit_with_wait_timeout_via_http(db_path):
    """The wait option is exposed on the submit API; a timed-out wait returns
    the same 429 contract as an immediate rejection."""
    store, queue = make_queue(db_path, _config())
    gate = asyncio.Event()

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    app = create_app(queue)
    await queue.start()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as client:
        await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "w0"})
        await asyncio.sleep(0.05)
        await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "w1"})
        await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "w2"})

        # Timed-out wait -> 429, same shape as the immediate rejection.
        r = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "w3",
            "wait_timeout_seconds": 0.1})
        assert r.status_code == 429
        assert r.json()["error"] == "queue_full"
        log.info("submit with wait_timeout via API: 429 error=%r",
                 r.json()["error"])

        # Capacity frees; the same key is accepted and runs to completion.
        gate.set()
        await asyncio.sleep(0.1)
        r2 = await client.post("/jobs", json={
            "job_type": "slow", "payload": {}, "idempotency_key": "w3",
            "wait_timeout_seconds": 2.0})
        assert r2.status_code == 202
        job_id = r2.json()["job"]["job_id"]
        final = await wait_for_status(store, job_id, TERMINAL)
        assert final.status == JobStatus.SUCCEEDED
        log.info("key=w3 accepted as job=%s after capacity freed -> %s",
                 job_id, final.status.value)
    await queue.stop()
    store.close()
