"""任务状态机与数据模型定义（骨架：仅类型定义）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskState(str, enum.Enum):
    """任务状态。迁移方向单调：PENDING -> RUNNING -> SUCCEEDED/DEAD。

    可重试失败时任务从 RUNNING 回到 PENDING（attempts 递增），
    但终态（SUCCEEDED/DEAD）一旦写入不可再变更。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    DEAD = "DEAD"


TERMINAL_STATES: frozenset[TaskState] = frozenset({TaskState.SUCCEEDED, TaskState.DEAD})


class SubmitStatus(str, enum.Enum):
    """提交结果：新建 / 幂等去重命中 / 背压拒绝。"""

    CREATED = "created"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class RejectReason(str, enum.Enum):
    """拒绝原因，须与其它失败可区分。"""

    QUEUE_FULL = "queue_full"


class RetryableError(Exception):
    """可重试错误：任务将按退避策略重试。"""


class FatalError(Exception):
    """不可重试错误：任务直接进入死信。"""


@dataclass
class Task:
    id: str
    idempotency_key: str
    handler: str
    payload: dict
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    max_attempts: int = 3
    next_run_at: float = 0.0
    result: object = None
    error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class SubmitResult:
    status: SubmitStatus
    task: Task | None = None
    reason: RejectReason | None = None
