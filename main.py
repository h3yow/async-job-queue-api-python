"""FastAPI 入口：任务提交、查询与死信重放接口。"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from app.models import RejectReason, SubmitStatus, Task, TaskState
from app.service import QueueService
from app.store import TaskStore

DB_PATH = os.environ.get("QUEUE_DB_PATH", "queue.db")

store = TaskStore(DB_PATH)
service = QueueService(
    store,
    concurrency=int(os.environ.get("QUEUE_CONCURRENCY", "4")),
    max_queue_size=int(os.environ.get("QUEUE_MAX_QUEUE_SIZE", "100")),
    max_attempts=int(os.environ.get("QUEUE_MAX_ATTEMPTS", "3")),
    backoff_base=float(os.environ.get("QUEUE_BACKOFF_BASE", "0.5")),
)


# -- 演示 handler（本地验证用，无任何外部依赖） --------------------------------


async def echo_handler(payload: dict) -> dict:
    await asyncio.sleep(0)
    return {"echo": payload}


service.register_handler("echo", echo_handler)


@asynccontextmanager
async def lifespan(app: FastAPI):
    store.open()
    await service.start()
    yield
    await service.stop()
    store.close()


app = FastAPI(title="async-job-queue", lifespan=lifespan)


class SubmitRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    handler: str = Field(min_length=1)
    payload: dict = Field(default_factory=dict)


def task_view(task: Task) -> dict:
    return {
        "id": task.id,
        "idempotency_key": task.idempotency_key,
        "handler": task.handler,
        "state": task.state.value,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "result": task.result,
        "error": task.error,
    }


@app.get("/")
def read_root() -> dict:
    return {"Hello": "World"}


@app.post("/tasks", status_code=201)
async def submit_task(body: SubmitRequest, response: Response) -> dict:
    try:
        result = await service.submit(body.idempotency_key, body.handler, body.payload)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if result.status is SubmitStatus.REJECTED:
        # 背压拒绝：原因 queue_full 与其它失败可区分
        raise HTTPException(
            status_code=429,
            detail={"reason": RejectReason.QUEUE_FULL.value},
        )
    assert result.task is not None
    if result.status is SubmitStatus.DUPLICATE:
        response.status_code = 200
    return {"status": result.status.value, "task": task_view(result.task)}


@app.get("/tasks/{task_id}")
async def get_task(task_id: str) -> dict:
    task = await service.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    view = task_view(task)
    view["failures"] = await service.failure_history(task_id)
    return view


@app.post("/tasks/{task_id}/replay")
async def replay_task(task_id: str) -> dict:
    task = await service.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    if task.state is not TaskState.DEAD:
        raise HTTPException(status_code=409, detail="task is not in DEAD state")
    replayed = await service.replay_dead(task_id)
    assert replayed is not None
    return {"status": "replayed", "task": task_view(replayed)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=0)
