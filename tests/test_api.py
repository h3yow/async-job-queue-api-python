"""HTTP API surface: submit (202/200/429), status query with dedup
semantics, and dead-letter replay endpoint."""
import asyncio
import logging

import httpx
import pytest

from app.api import create_app
from app.models import JobStatus, QueueConfig, RetryableError
from tests.conftest import TERMINAL, make_queue, wait_for_status

log = logging.getLogger("test")


@pytest.fixture
async def client(db_path):
    config = QueueConfig(max_concurrency=1, max_queue_size=2,
                         base_backoff_seconds=0.01, poll_interval_seconds=0.005)
    store, queue = make_queue(db_path, config)

    async def handler(job):
        if job.payload.get("fail"):
            raise RetryableError("boom")

    queue.register_handler("echo", handler)
    app = create_app(queue)
    await queue.start()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as c:
        c.job_queue = queue
        yield c
    await queue.stop()
    store.close()


async def test_submit_query_and_duplicate(client):
    r = await client.post("/jobs", json={
        "job_type": "echo", "payload": {}, "idempotency_key": "api-key"})
    assert r.status_code == 202
    job_id = r.json()["job"]["job_id"]

    # Duplicate submission returns 200 with the same job, no re-execution.
    r2 = await client.post("/jobs", json={
        "job_type": "echo", "payload": {}, "idempotency_key": "api-key"})
    assert r2.status_code == 200
    assert r2.json()["duplicate"] is True
    assert r2.json()["job"]["job_id"] == job_id

    final = await wait_for_status(client.job_queue.store, job_id, TERMINAL)
    assert final.status == JobStatus.SUCCEEDED

    r3 = await client.get(f"/jobs/{job_id}")
    assert r3.status_code == 200
    body = r3.json()
    assert body["status"] == "succeeded" and body["terminal"] is True
    log.info("job=%s status=%s attempt=%d via API", job_id,
             body["status"], body["attempt"])


async def test_backpressure_returns_429_with_reason(client):
    gate = asyncio.Event()
    queue = client.job_queue

    async def slow(job):
        await gate.wait()

    queue.register_handler("slow", slow)
    await client.post("/jobs", json={
        "job_type": "slow", "payload": {}, "idempotency_key": "b0"})
    await asyncio.sleep(0.05)  # worker claims b0
    await client.post("/jobs", json={
        "job_type": "slow", "payload": {}, "idempotency_key": "b1"})
    await client.post("/jobs", json={
        "job_type": "slow", "payload": {}, "idempotency_key": "b2"})
    r = await client.post("/jobs", json={
        "job_type": "slow", "payload": {}, "idempotency_key": "b3"})
    assert r.status_code == 429
    assert r.json()["error"] == "queue_full"
    log.info("backpressure via API: 429 error=%r", r.json()["error"])
    gate.set()


async def test_dead_letter_replay_endpoint(client):
    r = await client.post("/jobs", json={
        "job_type": "echo", "payload": {"fail": True},
        "idempotency_key": "dead-key", "max_attempts": 2})
    job_id = r.json()["job"]["job_id"]
    dead = await wait_for_status(client.job_queue.store, job_id, {JobStatus.DEAD})
    assert dead.last_error == "boom"

    # Replay while still failing -> goes dead again; history stays deduped.
    rr = await client.post(f"/jobs/{job_id}/replay")
    assert rr.status_code == 200 and rr.json()["replayed"] is True
    await wait_for_status(client.job_queue.store, job_id, {JobStatus.DEAD})
    history = client.job_queue.store.failure_history(job_id)
    keys = [(h["attempt"], h["event"], h["reason"]) for h in history]
    assert len(keys) == len(set(keys)), "no duplicate failure records"

    # Replaying a job that is currently dead-but-claimed-again is guarded;
    # replaying an unknown job is a 404.
    r404 = await client.post("/jobs/nonexistent/replay")
    assert r404.status_code == 404
    log.info("job=%s replayed; history=%s (dedup verified)", job_id, keys)
