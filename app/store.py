"""SQLite-backed persistent job store.

All state transitions are conditional UPDATEs guarded by the current
(status, attempt), which makes them monotonic and idempotent:
terminal states can be written exactly once per attempt holder.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from app.models import Job, JobStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    job_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    last_error TEXT,
    run_at REAL NOT NULL DEFAULT 0,
    replay_generation INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE (job_id, attempt, event, reason)
);
"""


class JobStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")

    def init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            idempotency_key=row["idempotency_key"],
            job_type=row["job_type"],
            payload=json.loads(row["payload"]),
            status=JobStatus(row["status"]),
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def insert_job(self, job: Job) -> tuple[Job | None, bool]:
        """Insert job. Returns (job, created). If the idempotency key already
        exists, returns (existing_job, False) and does not create a duplicate."""
        now = time.time()
        job.created_at = job.updated_at = now
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO jobs
                   (job_id, idempotency_key, job_type, payload, status, attempt,
                    max_attempts, last_error, run_at, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (job.job_id, job.idempotency_key, job.job_type,
                 json.dumps(job.payload), job.status.value, job.attempt,
                 job.max_attempts, job.last_error, 0.0, now, now),
            )
            if cur.rowcount == 1:
                return job, True
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?",
                (job.idempotency_key,),
            ).fetchone()
            return (self._row_to_job(row) if row else None), False

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            return self._row_to_job(row) if row else None

    def pending_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM jobs WHERE status = 'pending'"
            ).fetchone()
            return int(row["c"])

    def claim_next_pending(self, now: float | None = None) -> Job | None:
        """Atomically move one due PENDING job to RUNNING and bump its attempt."""
        now = now if now is not None else time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                """SELECT job_id FROM jobs
                   WHERE status = 'pending' AND run_at <= ?
                   ORDER BY created_at LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None
            cur = self._conn.execute(
                """UPDATE jobs SET status = 'running', attempt = attempt + 1,
                   updated_at = ?
                   WHERE job_id = ? AND status = 'pending'""",
                (now, row["job_id"]),
            )
            if cur.rowcount != 1:
                return None
            job_row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (row["job_id"],)
            ).fetchone()
            return self._row_to_job(job_row)

    def record_event(self, job_id: str, attempt: int, event: str, reason: str) -> None:
        """Append an event; dedup on (job_id, attempt, event, reason) so callers
        never observe the same failure reason recorded twice."""
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT OR IGNORE INTO job_events (job_id, attempt, event, reason, created_at)
                   VALUES (?,?,?,?,?)""",
                (job_id, attempt, event, reason, time.time()),
            )

    def complete_job(self, job_id: str, attempt: int, status: JobStatus,
                     error: str | None) -> bool:
        """Write a terminal state exactly once: only succeeds if the job is
        still RUNNING at the same attempt we claimed."""
        assert status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD)
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET status = ?, last_error = ?, updated_at = ?
                   WHERE job_id = ? AND attempt = ? AND status = 'running'""",
                (status.value, error, time.time(), job_id, attempt),
            )
            return cur.rowcount == 1

    def reschedule_for_retry(self, job_id: str, attempt: int, error: str,
                             run_at: float) -> bool:
        """RUNNING -> PENDING with a future run_at (backoff). Monotonic because
        attempt was already incremented at claim time and never decreases."""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET status = 'pending', last_error = ?, run_at = ?,
                   updated_at = ?
                   WHERE job_id = ? AND attempt = ? AND status = 'running'""",
                (error, run_at, time.time(), job_id, attempt),
            )
            return cur.rowcount == 1

    def requeue_running_jobs(self) -> int:
        """Crash recovery: jobs left RUNNING by a dead process go back to
        PENDING so they are resumed, not lost. Attempt count is preserved."""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET status = 'pending', updated_at = ?
                   WHERE status = 'running'""",
                (time.time(),),
            )
            return cur.rowcount

    def replay_dead(self, job_id: str) -> Job | None:
        """DEAD -> PENDING, resetting attempts and bumping replay_generation.
        Idempotent: a no-op unless the job is currently DEAD, so a replay can
        never produce duplicate terminal writes for the same dead-lettered run."""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE jobs SET status = 'pending', attempt = 0, last_error = NULL,
                   run_at = 0, replay_generation = replay_generation + 1, updated_at = ?
                   WHERE job_id = ? AND status = 'dead'""",
                (time.time(), job_id),
            )
            if cur.rowcount != 1:
                return None
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            return self._row_to_job(row)

    def failure_history(self, job_id: str) -> list[dict]:
        """Deduplicated, ordered failure records for the status query API."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT DISTINCT attempt, event, reason, created_at
                   FROM job_events WHERE job_id = ?
                   ORDER BY created_at, id""",
                (job_id,),
            ).fetchall()
            return [dict(r) for r in rows]
