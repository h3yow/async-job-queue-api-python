import asyncio
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import JobStatus, QueueConfig  # noqa: E402
from app.queue import JobQueue  # noqa: E402
from app.store import JobStore  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(message)s")


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "jobs.db")


@pytest.fixture
def fast_config():
    return QueueConfig(max_concurrency=2, max_queue_size=4,
                       base_backoff_seconds=0.01, max_backoff_seconds=0.05,
                       poll_interval_seconds=0.005)


def make_queue(path, config):
    store = JobStore(path)
    store.init_schema()
    return store, JobQueue(store, config)


async def wait_for_status(store: JobStore, job_id: str,
                          statuses: set[JobStatus], timeout: float = 5.0):
    """Poll until the job reaches one of the given statuses."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = store.get_job(job_id)
        if job is not None and job.status in statuses:
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"job {job_id} did not reach {statuses}; "
        f"current={store.get_job(job_id).status if store.get_job(job_id) else None}")


TERMINAL = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD}
