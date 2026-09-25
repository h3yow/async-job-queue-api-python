# async-job-queue-api-python

本地可运行的异步任务队列服务（FastAPI + SQLite），无任何外部服务、真实凭据或网络依赖。

## 任务状态机

```
                claim_due                handler 成功
  PENDING  ───────────────►  RUNNING  ───────────────►  SUCCEEDED（终态）
    ▲    ▲                    │
    │    │ fail_retryable     │ handler 抛 RetryableError
    │    └────────────────────┤   （attempts < max_attempts，指数退避后回到 PENDING）
    │                         │
    │    fail_dead            │ handler 抛 FatalError，或重试次数耗尽
    └─────────────────────────┴───────────────►  DEAD（终态）
         （DEAD 可经 replay 重置回 PENDING，attempts 归零）
```

- 状态迁移**单调且幂等**：所有迁移由存储层 `WHERE state = ...` 守卫，
  终态（`SUCCEEDED` / `DEAD`）一旦写入，后续任何终态写入都是无操作，
  调用方不会观察到状态倒退。
- 可重试失败时任务从 `RUNNING` 回到 `PENDING` 等待退避，这是唯一允许的"回退"，
  且 `attempts` 单调递增，终态不会被覆盖。
- 失败记录以 `(task_id, attempt)` 为主键去重，同一尝试次数不会产生重复失败记录。

## 幂等、重试与死信规则

- **幂等提交**：`idempotency_key` 唯一。重复提交返回 `status=duplicate` 及既有任务，
  任务只会被执行一次。
- **失败分类**：
  - `RetryableError`（及未分类异常）→ 可重试，按指数退避
    `delay = backoff_base * 2^(attempts-1)` 重新调度；
  - `FatalError` → 不可重试，直接进入死信。
- **死信**：重试次数达到 `max_attempts` 或发生 `FatalError` 时进入 `DEAD`，
  任务上保留最后一次失败原因（`error` 字段），完整失败历史可通过
  `GET /tasks/{id}` 的 `failures` 字段查询。
- **死信重放**：`POST /tasks/{id}/replay` 将 `DEAD` 任务重置为 `PENDING`
  （`attempts` 归零）。重放不会产生重复终态写入（存储层守卫），
  也不会产生重复失败记录（按 attempt 去重）。

## 并发与背压

- 同时执行的任务数不超过 `concurrency`。
- 当 `PENDING` 任务数达到 `max_queue_size` 时，新提交被**背压拒绝**：
  - 服务层返回 `SubmitResult(status=rejected, reason=queue_full)`；
  - HTTP 层返回 `429`，`detail.reason = "queue_full"`，
    与任务执行失败（任务状态机中的 `DEAD`/`error`）完全可区分。

## 崩溃恢复

任务状态持久化于 SQLite。进程重启时：

1. 遗留的 `RUNNING` 任务（崩溃时正在执行）被退回 `PENDING` 并立即到期；
2. 未到期的 `PENDING` 任务按原退避计划继续调度；
3. 已终态的任务不受影响，不会被重复执行或重复写入终态。

## 配置项

| 环境变量              | 默认值     | 说明                       |
|-----------------------|-----------|----------------------------|
| `QUEUE_DB_PATH`       | `queue.db`| SQLite 数据库路径          |
| `QUEUE_CONCURRENCY`   | `4`       | 并发执行上限               |
| `QUEUE_MAX_QUEUE_SIZE`| `100`     | PENDING 容量上限（背压阈值）|
| `QUEUE_MAX_ATTEMPTS`  | `3`       | 最大尝试次数（含首次）      |
| `QUEUE_BACKOFF_BASE`  | `0.5`     | 指数退避基数（秒）          |

## API

| 方法 | 路径                    | 说明                                       |
|------|-------------------------|--------------------------------------------|
| POST | `/tasks`                | 提交任务；201 created / 200 duplicate / 429 queue_full / 400 未知 handler |
| GET  | `/tasks/{id}`           | 查询任务状态与去重后的失败历史              |
| POST | `/tasks/{id}/replay`    | 死信重放；非 DEAD 返回 409                  |

提交体：`{"idempotency_key": "...", "handler": "echo", "payload": {...}}`
（内置 `echo` handler 用于本地验证；业务 handler 通过
`QueueService.register_handler` 注册。）

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -v # 运行全部行为测试
# 查看测试日志中的任务标识 / 尝试次数 / 判定依据：
uv run pytest tests/ -o log_cli=true --log-cli-level=INFO
```

测试覆盖：并发提交同一幂等键、并发上限下的背压拒绝、可重试与不可重试失败、
重试耗尽进死信、死信重放（无重复终态/副作用）、进程重启恢复、状态观测单调性。

启动服务本地试用：

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"demo-1","handler":"echo","payload":{"hi":1}}'
curl localhost:8000/tasks/<task_id>
```
