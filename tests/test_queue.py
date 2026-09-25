"""队列行为测试：并发幂等、背压、失败分类、死信重放、重启恢复。"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from app.models import (
    FatalError,
    RejectReason,
    RetryableError,
    SubmitStatus,
    TaskState,
)
from app.service import QueueService
from app.store import TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("test.queue")

POLL = 0.01
TIMEOUT = 5.0


def run(coro):
    return asyncio.run(coro)


async def wait_for(predicate, desc: str, timeout: float = TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(POLL)
    raise AssertionError(f"超时等待: {desc}")


def make_service(tmp_path, **kwargs) -> QueueService:
    store = TaskStore(str(tmp_path / "queue.db"))
    store.open()
    defaults = dict(concurrency=2, max_queue_size=4, max_attempts=3,
                    backoff_base=0.01, poll_interval=0.01)
    defaults.update(kwargs)
    return QueueService(store, **defaults)


# -- 1. 并发提交同一幂等键 -----------------------------------------------------


def test_concurrent_submit_same_idempotency_key(tmp_path):
    async def main():
        service = make_service(tmp_path)
        calls: list[dict] = []

        async def handler(payload: dict) -> dict:
            calls.append(payload)
            await asyncio.sleep(0.05)
            return {"ok": True}

        service.register_handler("h", handler)
        await service.start()
        results = await asyncio.gather(
            *(service.submit("same-key", "h", {"n": i}) for i in range(10))
        )
        created = [r for r in results if r.status is SubmitStatus.CREATED]
        duplicated = [r for r in results if r.status is SubmitStatus.DUPLICATE]
        assert len(created) == 1, "同一幂等键只能创建一次"
        assert len(duplicated) == 9
        task_id = created[0].task.id
        assert all(r.task.id == task_id for r in duplicated)

        await wait_for(
            lambda: service.store.get(task_id).state is TaskState.SUCCEEDED,
            "任务完成",
        )
        task = service.store.get(task_id)
        logger.info(
            "task=%s attempts=%d verdict=dedup 判定依据: handler 调用次数=%d 应为 1",
            task_id, task.attempts, len(calls),
        )
        assert len(calls) == 1, "幂等键去重：任务不得被重复执行"
        assert task.attempts == 1
        await service.stop()
        service.store.close()

    run(main())


# -- 2. 并发上限下的背压拒绝 ---------------------------------------------------


def test_backpressure_rejection_when_queue_full(tmp_path):
    async def main():
        service = make_service(tmp_path, max_queue_size=2, concurrency=1)
        gate = asyncio.Event()

        async def blocking(payload: dict) -> dict:
            await gate.wait()
            return {}

        service.register_handler("blocking", blocking)
        await service.start()

        first = await service.submit("k0", "blocking", {})
        assert first.status is SubmitStatus.CREATED
        # 等第一个任务进入 RUNNING，占住唯一并发槽
        await wait_for(
            lambda: service.store.get(first.task.id).state is TaskState.RUNNING,
            "首个任务运行中",
        )
        second = await service.submit("k1", "blocking", {})
        third = await service.submit("k2", "blocking", {})
        assert second.status is SubmitStatus.CREATED
        assert third.status is SubmitStatus.CREATED  # 队列达到上限 2

        rejected = await service.submit("k3", "blocking", {})
        logger.info(
            "task_key=k3 verdict=rejected 判定依据: reason=%s 应为 queue_full",
            rejected.reason,
        )
        assert rejected.status is SubmitStatus.REJECTED
        assert rejected.reason is RejectReason.QUEUE_FULL
        # 拒绝原因须与其它失败可区分
        assert rejected.reason.value == "queue_full"
        assert rejected.reason.value not in {s.value for s in TaskState}

        gate.set()
        await wait_for(
            lambda: all(
                service.store.get(t.task.id).state is TaskState.SUCCEEDED
                for t in (first, second, third)
            ),
            "全部任务完成",
        )
        await service.stop()
        service.store.close()

    run(main())


# -- 3. 可重试失败按退避重试 ---------------------------------------------------


def test_retryable_error_retries_then_succeeds(tmp_path):
    async def main():
        service = make_service(tmp_path, max_attempts=3)
        attempts: list[int] = []

        async def flaky(payload: dict) -> dict:
            attempts.append(1)
            if len(attempts) < 3:
                raise RetryableError("暂时性故障")
            return {"done": True}

        service.register_handler("flaky", flaky)
        await service.start()
        result = await service.submit("retry-key", "flaky", {})
        task_id = result.task.id
        await wait_for(
            lambda: service.store.get(task_id).state is TaskState.SUCCEEDED,
            "重试后成功",
        )
        task = service.store.get(task_id)
        history = service.store.failure_history(task_id)
        logger.info(
            "task=%s attempts=%d verdict=succeeded 判定依据: 失败记录=%d 条（按 attempt 去重）",
            task_id, task.attempts, len(history),
        )
        assert task.attempts == 3
        assert len(attempts) == 3
        assert len(history) == 2, "同一 attempt 的失败记录不得重复"
        assert [h["attempt"] for h in history] == [1, 2]
        assert all(h["kind"] == "retryable" for h in history)
        await service.stop()
        service.store.close()

    run(main())


# -- 4. 不可重试失败直接进死信 -------------------------------------------------


def test_fatal_error_goes_straight_to_dead(tmp_path):
    async def main():
        service = make_service(tmp_path, max_attempts=3)
        calls: list[int] = []

        async def bad(payload: dict) -> dict:
            calls.append(1)
            raise FatalError("参数非法，不可恢复")

        service.register_handler("bad", bad)
        await service.start()
        result = await service.submit("fatal-key", "bad", {})
        task_id = result.task.id
        await wait_for(
            lambda: service.store.get(task_id).state is TaskState.DEAD,
            "任务进入死信",
        )
        task = service.store.get(task_id)
        history = service.store.failure_history(task_id)
        logger.info(
            "task=%s attempts=%d verdict=dead 判定依据: FatalError 不重试, error=%s",
            task_id, task.attempts, task.error,
        )
        assert task.attempts == 1, "不可重试错误不得重试"
        assert len(calls) == 1
        assert "参数非法" in task.error, "死信须保留失败原因"
        assert len(history) == 1 and history[0]["kind"] == "fatal"
        await service.stop()
        service.store.close()

    run(main())


# -- 5. 重试耗尽进入死信 -------------------------------------------------------


def test_retry_exhaustion_enters_dead_letter(tmp_path):
    async def main():
        service = make_service(tmp_path, max_attempts=2)
        calls: list[int] = []

        async def always_fail(payload: dict) -> dict:
            calls.append(1)
            raise RetryableError("持续故障")

        service.register_handler("always_fail", always_fail)
        await service.start()
        result = await service.submit("exhaust-key", "always_fail", {})
        task_id = result.task.id
        await wait_for(
            lambda: service.store.get(task_id).state is TaskState.DEAD,
            "重试耗尽进入死信",
        )
        task = service.store.get(task_id)
        history = service.store.failure_history(task_id)
        logger.info(
            "task=%s attempts=%d verdict=dead 判定依据: 重试 %d 次后耗尽, error=%s",
            task_id, task.attempts, len(calls), task.error,
        )
        assert task.attempts == 2
        assert len(calls) == 2, "超过重试上限后不得再执行"
        assert "持续故障" in task.error
        assert len(history) == 2  # 每次 attempt 恰好一条，无重复
        await service.stop()
        service.store.close()

    run(main())


# -- 6. 死信重放：无重复终态、无重复副作用 --------------------------------------


def test_dead_letter_replay_no_duplicate_terminal(tmp_path):
    async def main():
        service = make_service(tmp_path, max_attempts=1)
        side_effects: list[str] = []

        async def fail_once(payload: dict) -> dict:
            side_effects.append("run")
            if len(side_effects) == 1:
                raise FatalError("首次失败")
            return {"recovered": True}

        service.register_handler("fail_once", fail_once)
        await service.start()
        result = await service.submit("replay-key", "fail_once", {})
        task_id = result.task.id
        await wait_for(
            lambda: service.store.get(task_id).state is TaskState.DEAD,
            "任务进入死信",
        )

        # 终态写入幂等：重复写终态必须是无操作
        assert service.store.fail_dead(task_id, "duplicate") is False
        assert service.store.complete(task_id, {}) is False

        replayed = await service.replay_dead(task_id)
        assert replayed is not None and replayed.state is TaskState.PENDING
        # 非 DEAD 状态不得重放
        assert await service.replay_dead(task_id) is None

        await wait_for(
            lambda: service.store.get(task_id).state is TaskState.SUCCEEDED,
            "重放后成功",
        )
        task = service.store.get(task_id)
        logger.info(
            "task=%s attempts=%d verdict=replayed 判定依据: 副作用次数=%d, 终态唯一",
            task_id, task.attempts, len(side_effects),
        )
        assert len(side_effects) == 2, "重放只触发一次新的执行"
        # 重放后再写终态仍是幂等无操作
        assert service.store.complete(task_id, {}) is False
        assert service.store.fail_dead(task_id, "late") is False
        final = service.store.get(task_id)
        assert final.state is TaskState.SUCCEEDED
        await service.stop()
        service.store.close()

    run(main())


# -- 7. 进程重启恢复 -----------------------------------------------------------


def test_restart_recovers_inflight_tasks(tmp_path):
    async def main():
        db = str(tmp_path / "queue.db")
        store1 = TaskStore(db)
        store1.open()
        svc1 = QueueService(store1, concurrency=2, max_attempts=3,
                            backoff_base=0.01, poll_interval=0.01)
        gate = asyncio.Event()

        async def blocking(payload: dict) -> dict:
            await gate.wait()
            return {}

        svc1.register_handler("blocking", blocking)
        await svc1.start()
        r1 = await svc1.submit("restart-1", "blocking", {})
        r2 = await svc1.submit("restart-2", "blocking", {})
        id1, id2 = r1.task.id, r2.task.id
        await wait_for(
            lambda: store1.get(id1).state is TaskState.RUNNING
            and store1.get(id2).state is TaskState.RUNNING,
            "两个任务均运行中",
        )

        # 模拟进程崩溃：调度循环与执行任务被直接终止，RUNNING 遗留在库中
        svc1._worker_task.cancel()
        for t in list(svc1._inflight):
            t.cancel()
        await asyncio.sleep(0.05)
        store1.close()

        # 重启：新进程加载同一数据库
        store2 = TaskStore(db)
        store2.open()
        svc2 = QueueService(store2, concurrency=2, max_attempts=3,
                            backoff_base=0.01, poll_interval=0.01)
        done: list[str] = []

        async def quick(payload: dict) -> dict:
            done.append(payload.get("k", ""))
            return {"ok": True}

        svc2.register_handler("blocking", quick)
        recovered = store2.recover_inflight()
        logger.info(
            "restart verdict=recovered 判定依据: 恢复 RUNNING 任务 %d 个", recovered
        )
        assert recovered == 2, "崩溃时 RUNNING 的任务须被恢复"
        await svc2.start()
        await wait_for(
            lambda: store2.get(id1).state is TaskState.SUCCEEDED
            and store2.get(id2).state is TaskState.SUCCEEDED,
            "重启后全部任务完成",
        )
        t1, t2 = store2.get(id1), store2.get(id2)
        logger.info(
            "task=%s attempts=%d verdict=succeeded; task=%s attempts=%d verdict=succeeded",
            id1, t1.attempts, id2, t2.attempts,
        )
        assert t1.state is TaskState.SUCCEEDED
        assert t2.state is TaskState.SUCCEEDED
        # 无重复终态：恢复任务只被执行到一次成功终态
        assert len(done) == 2
        # 状态未倒退：SUCCEEDED 后再次终态写入被拒绝
        assert store2.complete(id1, {}) is False
        assert store2.fail_dead(id2, "x") is False
        await svc2.stop()
        store2.close()

    run(main())


# -- 8. 状态查询无倒退、失败记录无重复 ------------------------------------------


def test_observed_states_are_monotonic(tmp_path):
    async def main():
        service = make_service(tmp_path, max_attempts=2)
        calls: list[int] = []

        async def flaky(payload: dict) -> dict:
            calls.append(1)
            if len(calls) < 2:
                raise RetryableError("一次暂时故障")
            return {"ok": True}

        service.register_handler("flaky", flaky)
        await service.start()
        result = await service.submit("observe-key", "flaky", {})
        task_id = result.task.id

        rank = {TaskState.PENDING: 0, TaskState.RUNNING: 1,
                TaskState.SUCCEEDED: 2, TaskState.DEAD: 2}
        seen: list[TaskState] = []
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            state = service.store.get(task_id).state
            if not seen or seen[-1] is not state:
                seen.append(state)
            if state is TaskState.SUCCEEDED:
                break
            await asyncio.sleep(0.001)
        logger.info(
            "task=%s verdict=monotonic 判定依据: 观测序列=%s",
            task_id, [s.value for s in seen],
        )
        assert seen[-1] is TaskState.SUCCEEDED
        # 允许 RUNNING->PENDING（重试退避），但终态之后不得再变
        terminal_idx = [i for i, s in enumerate(seen)
                        if s in (TaskState.SUCCEEDED, TaskState.DEAD)]
        assert terminal_idx == [len(seen) - 1], "终态只能出现在末尾且唯一"
        history = service.store.failure_history(task_id)
        attempts_seen = [h["attempt"] for h in history]
        assert len(attempts_seen) == len(set(attempts_seen)), "失败记录不得重复"
        await service.stop()
        service.store.close()

    run(main())
