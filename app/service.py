"""队列服务：并发调度、背压、重试退避、死信与重放。"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Awaitable, Callable

from .models import (
    FatalError,
    RejectReason,
    RetryableError,
    SubmitResult,
    SubmitStatus,
    Task,
)
from .store import TaskStore

logger = logging.getLogger("queue.service")

Handler = Callable[[dict], Awaitable[object]]


class QueueService:
    """异步任务队列。

    - 并发上限：同时执行的任务数不超过 ``concurrency``。
    - 背压：PENDING 任务数达到 ``max_queue_size`` 时拒绝新提交，
      拒绝原因为 ``queue_full``，与其它失败可区分。
    - 重试：``RetryableError`` 按指数退避重试，超过 ``max_attempts``
      进入死信；``FatalError`` 直接进入死信。
    """

    def __init__(
        self,
        store: TaskStore,
        *,
        concurrency: int = 4,
        max_queue_size: int = 100,
        max_attempts: int = 3,
        backoff_base: float = 0.1,
        poll_interval: float = 0.02,
    ) -> None:
        self.store = store
        self.concurrency = concurrency
        self.max_queue_size = max_queue_size
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.poll_interval = poll_interval
        self._handlers: dict[str, Handler] = {}
        self._worker_task: asyncio.Task | None = None
        self._inflight: set[asyncio.Task] = set()
        self._stopped = asyncio.Event()
        self._stopped.set()

    def register_handler(self, name: str, handler: Handler) -> None:
        self._handlers[name] = handler

    async def start(self) -> None:
        """恢复未完成任务并启动调度循环（重启不丢任务、不产生重复终态）。"""
        recovered = self.store.recover_inflight()
        if recovered:
            logger.info("recovered %d inflight tasks after restart", recovered)
        self._stopped.clear()
        self._worker_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stopped.set()
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
            self._inflight.clear()

    async def submit(
        self, idempotency_key: str, handler: str, payload: dict
    ) -> SubmitResult:
        if handler not in self._handlers:
            raise KeyError(f"未注册的 handler: {handler}")
        if self.store.count_pending() >= self.max_queue_size:
            logger.info(
                "submit rejected key=%s reason=%s", idempotency_key, RejectReason.QUEUE_FULL
            )
            return SubmitResult(status=SubmitStatus.REJECTED, reason=RejectReason.QUEUE_FULL)
        now = time.time()
        task = Task(
            id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            handler=handler,
            payload=payload,
            max_attempts=self.max_attempts,
            next_run_at=now,
            created_at=now,
            updated_at=now,
        )
        existing = self.store.insert_if_absent(task)
        if existing is not None:
            logger.info(
                "submit deduplicated key=%s existing_task=%s state=%s",
                idempotency_key, existing.id, existing.state,
            )
            return SubmitResult(status=SubmitStatus.DUPLICATE, task=existing)
        logger.info("submit created task=%s key=%s", task.id, idempotency_key)
        return SubmitResult(status=SubmitStatus.CREATED, task=task)

    async def get_task(self, task_id: str) -> Task | None:
        return self.store.get(task_id)

    async def failure_history(self, task_id: str) -> list[dict]:
        return self.store.failure_history(task_id)

    async def replay_dead(self, task_id: str) -> Task | None:
        """死信重放：DEAD -> PENDING 重新调度。

        终态写入由存储层守卫保证幂等，重放不会产生重复终态；
        失败历史按 (task_id, attempt) 去重，不产生重复失败记录。
        """
        task = self.store.reset_for_replay(task_id)
        if task is not None:
            logger.info("replayed dead task=%s", task_id)
        return task

    # -- 内部 ----------------------------------------------------------------

    async def _run_loop(self) -> None:
        while not self._stopped.is_set():
            slots = self.concurrency - len(self._inflight)
            if slots > 0:
                for task in self.store.claim_due(slots, time.time()):
                    runner = asyncio.create_task(self._execute(task))
                    self._inflight.add(runner)
                    runner.add_done_callback(self._inflight.discard)
            await asyncio.sleep(self.poll_interval)

    async def _execute(self, task: Task) -> None:
        handler = self._handlers[task.handler]
        try:
            result = await handler(task.payload)
        except FatalError as exc:
            self.store.fail_dead(task.id, f"{type(exc).__name__}: {exc}")
            logger.info(
                "task=%s attempt=%d verdict=fatal error=%s",
                task.id, task.attempts, exc,
            )
        except RetryableError as exc:
            error = f"{type(exc).__name__}: {exc}"
            if task.attempts >= task.max_attempts:
                self.store.fail_dead(task.id, error)
                logger.info(
                    "task=%s attempt=%d verdict=dead (retry exhausted) error=%s",
                    task.id, task.attempts, exc,
                )
            else:
                delay = self.backoff_base * (2 ** (task.attempts - 1))
                self.store.fail_retryable(task.id, error, time.time() + delay)
                logger.info(
                    "task=%s attempt=%d verdict=retry backoff=%.3fs error=%s",
                    task.id, task.attempts, delay, exc,
                )
        except Exception as exc:  # 未分类异常按可重试处理
            error = f"{type(exc).__name__}: {exc}"
            if task.attempts >= task.max_attempts:
                self.store.fail_dead(task.id, error)
            else:
                delay = self.backoff_base * (2 ** (task.attempts - 1))
                self.store.fail_retryable(task.id, error, time.time() + delay)
            logger.info(
                "task=%s attempt=%d verdict=retry(unclassified) error=%s",
                task.id, task.attempts, exc,
            )
        else:
            self.store.complete(task.id, result)
            logger.info("task=%s attempt=%d verdict=succeeded", task.id, task.attempts)
