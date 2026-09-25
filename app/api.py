"""FastAPI wiring (skeleton)."""
from __future__ import annotations

from fastapi import FastAPI

from app.queue import JobQueue


def create_app(queue: JobQueue | None = None) -> FastAPI:
    app = FastAPI(title="async-job-queue")
    app.state.queue = queue

    @app.get("/")
    def root() -> dict:
        return {"service": "async-job-queue", "status": "ok"}

    return app
