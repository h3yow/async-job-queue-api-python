"""SQLite 持久化存储：单调状态迁移、幂等键去重、失败记录去重。"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from .models import TERMINAL_STATES, Task, TaskState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    handler TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    next_run_at REAL NOT NULL,
    result TEXT,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS failures (
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    error TEXT NOT NULL,
    kind TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (task_id, attempt)
);
"""


class TaskStore:
    """基于 SQLite 的任务存储。

    - 状态迁移通过 ``WHERE state = ...`` 守卫保证单调且幂等：
      终态（SUCCEEDED/DEAD）一旦写入，后续同态写入为无操作。
    - 失败记录以 (task_id, attempt) 为主键，同一尝试次数不重复记录。
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def open(self) -> None:
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("TaskStore 尚未 open()")
        return self._conn

    # -- 写入 ---------------------------------------------------------------

    def insert_if_absent(self, task: Task) -> Task | None:
        """按幂等键插入；键已存在时返回既有任务（不重复执行），否则返回 None。"""
        conn = self._require_conn()
        with self._lock, conn:
            try:
                conn.execute(
                    """INSERT INTO tasks
                       (id, idempotency_key, handler, payload, state, attempts,
                        max_attempts, next_run_at, result, error, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        task.id,
                        task.idempotency_key,
                        task.handler,
                        json.dumps(task.payload),
                        task.state.value,
                        task.attempts,
                        task.max_attempts,
                        task.next_run_at,
                        None,
                        None,
                        task.created_at,
                        task.updated_at,
                    ),
                )
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (task.idempotency_key,),
                ).fetchone()
                return self._to_task(row)
        return None

    def count_pending(self) -> int:
        conn = self._require_conn()
        with self._lock:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM tasks WHERE state = ?",
                (TaskState.PENDING.value,),
            ).fetchone()
        return int(row["n"])

    def claim_due(self, limit: int, now: float) -> list[Task]:
        """原子领取到期的 PENDING 任务并置为 RUNNING，attempts 递增。"""
        conn = self._require_conn()
        claimed: list[Task] = []
        with self._lock, conn:
            rows = conn.execute(
                """SELECT id FROM tasks
                   WHERE state = ? AND next_run_at <= ?
                   ORDER BY created_at LIMIT ?""",
                (TaskState.PENDING.value, now, limit),
            ).fetchall()
            for row in rows:
                cur = conn.execute(
                    """UPDATE tasks SET state = ?, attempts = attempts + 1, updated_at = ?
                       WHERE id = ? AND state = ?""",
                    (TaskState.RUNNING.value, now, row["id"], TaskState.PENDING.value),
                )
                if cur.rowcount == 1:
                    task = self.get(row["id"])
                    if task is not None:
                        claimed.append(task)
        return claimed

    def complete(self, task_id: str, result: object) -> bool:
        """幂等终态写入：仅 RUNNING -> SUCCEEDED；已终态返回 False。"""
        conn = self._require_conn()
        with self._lock, conn:
            cur = conn.execute(
                """UPDATE tasks SET state = ?, result = ?, error = NULL, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (
                    TaskState.SUCCEEDED.value,
                    json.dumps(result),
                    time.time(),
                    task_id,
                    TaskState.RUNNING.value,
                ),
            )
        return cur.rowcount == 1

    def fail_retryable(self, task_id: str, error: str, next_run_at: float) -> bool:
        """RUNNING -> PENDING（等待退避重试），并按 attempt 去重记录失败。"""
        conn = self._require_conn()
        with self._lock, conn:
            task = self.get(task_id)
            if task is None or task.state in TERMINAL_STATES:
                return False
            cur = conn.execute(
                """UPDATE tasks SET state = ?, error = ?, next_run_at = ?, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (TaskState.PENDING.value, error, next_run_at, time.time(),
                 task_id, TaskState.RUNNING.value),
            )
            if cur.rowcount != 1:
                return False
            conn.execute(
                """INSERT OR IGNORE INTO failures (task_id, attempt, error, kind, created_at)
                   VALUES (?, ?, ?, 'retryable', ?)""",
                (task_id, task.attempts, error, time.time()),
            )
        return True

    def fail_dead(self, task_id: str, error: str) -> bool:
        """幂等终态写入：RUNNING/PENDING -> DEAD；已终态返回 False。"""
        conn = self._require_conn()
        with self._lock, conn:
            task = self.get(task_id)
            if task is None or task.state in TERMINAL_STATES:
                return False
            cur = conn.execute(
                """UPDATE tasks SET state = ?, error = ?, updated_at = ?
                   WHERE id = ? AND state IN (?, ?)""",
                (
                    TaskState.DEAD.value,
                    error,
                    time.time(),
                    task_id,
                    TaskState.RUNNING.value,
                    TaskState.PENDING.value,
                ),
            )
            if cur.rowcount != 1:
                return False
            conn.execute(
                """INSERT OR IGNORE INTO failures (task_id, attempt, error, kind, created_at)
                   VALUES (?, ?, ?, 'fatal', ?)""",
                (task_id, task.attempts, error, time.time()),
            )
        return True

    def recover_inflight(self) -> int:
        """重启恢复：遗留 RUNNING 任务退回 PENDING（立即到期），返回恢复数量。"""
        conn = self._require_conn()
        with self._lock, conn:
            cur = conn.execute(
                """UPDATE tasks SET state = ?, next_run_at = ?, updated_at = ?
                   WHERE state = ?""",
                (TaskState.PENDING.value, time.time(), time.time(),
                 TaskState.RUNNING.value),
            )
        return cur.rowcount

    def reset_for_replay(self, task_id: str) -> Task | None:
        """死信重放：DEAD -> PENDING，attempts 归零；非 DEAD 返回 None。"""
        conn = self._require_conn()
        with self._lock, conn:
            cur = conn.execute(
                """UPDATE tasks SET state = ?, attempts = 0, error = NULL,
                       next_run_at = ?, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (TaskState.PENDING.value, time.time(), time.time(),
                 task_id, TaskState.DEAD.value),
            )
            if cur.rowcount != 1:
                return None
        return self.get(task_id)

    # -- 查询 ---------------------------------------------------------------

    def get(self, task_id: str) -> Task | None:
        conn = self._require_conn()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return self._to_task(row) if row else None

    def failure_history(self, task_id: str) -> list[dict]:
        conn = self._require_conn()
        with self._lock:
            rows = conn.execute(
                """SELECT attempt, error, kind, created_at FROM failures
                   WHERE task_id = ? ORDER BY attempt""",
                (task_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _to_task(row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            handler=row["handler"],
            payload=json.loads(row["payload"]),
            state=TaskState(row["state"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            next_run_at=row["next_run_at"],
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
