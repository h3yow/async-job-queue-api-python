from tests.conftest import TERMINAL, make_queue, wait_for_status


async def test_smoke_submit_and_succeed(db_path, fast_config):
    store, queue = make_queue(db_path, fast_config)
    ran = []

    async def handler(job):
        ran.append(job.job_id)

    queue.register_handler("ok", handler)
    await queue.start()
    job, reason = await queue.submit("ok", {}, "smoke-key")
    assert reason is None
    final = await wait_for_status(store, job.job_id, TERMINAL)
    assert final.status.value == "succeeded"
    assert ran == [job.job_id]
    await queue.stop()
    store.close()
