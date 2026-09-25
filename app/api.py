"""FastAPI wiring: submit / query / replay endpoints."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.models import Job, JobStatus, QueueConfig, RejectReason
from app.queue import JobQueue
from app.store import JobStore


class SubmitRequest(BaseModel):
    job_type: str = "echo"
    payload: dict = Field(default_factory=dict)
    idempotency_key: str
    max_attempts: int = 3


def _job_view(store: JobStore, job: Job) -> dict:
    return {
        "job_id": job.job_id,
        "idempotency_key": job.idempotency_key,
        "job_type": job.job_type,
        "status": job.status.value,
        "attempt": job.attempt,
        "max_attempts": job.max_attempts,
        "last_error": job.last_error,
        "terminal": job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD),
        "failure_history": store.failure_history(job.job_id),
    }


def create_app(queue: JobQueue | None = None, db_path: str = "jobs.db",
               config: QueueConfig | None = None) -> FastAPI:
    owns_queue = queue is None
    if owns_queue:
        store = JobStore(db_path)
        store.init_schema()
        queue = JobQueue(store, config or QueueConfig())

        async def _echo_handler(job: Job) -> None:
            return None

        queue.register_handler("echo", _echo_handler)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await queue.start()
        yield
        await queue.stop()

    app = FastAPI(title="async-job-queue", lifespan=lifespan)
    app.state.queue = queue

    @app.get("/")
    def root() -> dict:
        return {"service": "async-job-queue", "status": "ok"}

    @app.post("/jobs", status_code=202)
    async def submit_job(req: SubmitRequest):
        job, reason = await queue.submit(
            req.job_type, req.payload, req.idempotency_key, req.max_attempts)
        if reason == RejectReason.QUEUE_FULL.value:
            return JSONResponse(
                status_code=429,
                content={"error": RejectReason.QUEUE_FULL.value,
                         "detail": "queue capacity reached, retry later"},
            )
        if reason == RejectReason.DUPLICATE.value:
            # Idempotent replay: return the existing job, do not re-execute.
            return JSONResponse(status_code=200,
                                content={"duplicate": True,
                                         "job": _job_view(queue.store, job)})
        return {"duplicate": False, "job": _job_view(queue.store, job)}

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict:
        job = queue.store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_view(queue.store, job)

    @app.post("/jobs/{job_id}/replay")
    def replay_job(job_id: str) -> dict:
        job = queue.store.replay_dead(job_id)
        if job is None:
            current = queue.store.get_job(job_id)
            if current is None:
                raise HTTPException(status_code=404, detail="job not found")
            # Idempotent: replaying a non-dead job changes nothing.
            return {"replayed": False, "job": _job_view(queue.store, current)}
        if queue._wakeup:
            queue._wakeup.set()
        return {"replayed": True, "job": _job_view(queue.store, job)}

    return app
