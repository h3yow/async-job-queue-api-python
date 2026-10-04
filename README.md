# async-job-queue-api-python

本地可运行的异步任务队列服务（FastAPI + SQLite + asyncio），不依赖任何外部
服务、真实凭据或网络资源。任务在并发提交、执行失败与进程重启等场景下的
状态变化可预期、可解释。

## 任务状态机

```
                 submit
                   │
                   ▼
        ┌──────────────────────┐
        │       PENDING        │◄─────────────────────┐
        └──────────┬───────────┘                      │
                   │ claim (attempt += 1)             │ retryable 失败且
                   ▼                                  │ attempt < max_attempts
        ┌──────────────────────┐    退避 run_at       │
        │       RUNNING        │──────────────────────┘
        └──────────┬───────────┘
                   │
     ┌─────────────┼──────────────────┬───────────────────┐
     │ 成功         │ 不可重试失败       │ 可重试失败且          │
     ▼             ▼                  │ attempt ≥ max_attempts
┌──────────┐  ┌──────────┐            ▼
│SUCCEEDED │  │  FAILED  │      ┌──────────┐   replay   ┌─────────┐
│ (terminal)│  │ (terminal)│     │   DEAD   │──────────► │ PENDING │
└──────────┘  └──────────┘      │ (terminal)│  (attempt  └─────────┘
                                └──────────┘   归零, generation+1)
```

- **单调且幂等**：所有迁移都是带 `(status, attempt)` 前置条件的条件 UPDATE，
  只有当前 attempt 的持有者能写入终态，因此终态（SUCCEEDED / FAILED / DEAD）
  每次运行**恰好写入一次**，重复写入会被静默拒绝。`attempt` 只增不减
  （replay 是唯一显式重置点，并伴随 `replay_generation` 递增）。
- **背压拒绝（可恢复）**：提交时若 pending 数达到容量上限，本次提交被
  拒绝（HTTP 429, `error=queue_full`）且**不留下任何任务记录**——没有
  job 行、没有失败事件，不计入失败统计。因此容量恢复后用**同一个幂等键**
  重新提交即可被正常受理并执行，无需更换新键。拒绝次数只累计到
  `backpressure_rejected_total` 计数器（持久化），供概览查询。
- **状态查询去重**：失败历史记录表以 `(job_id, attempt, event, reason)`
  唯一约束去重，调用方不会观察到同一原因的重复失败记录，也不会观察到
  状态倒退。背压拒绝不会出现在任何任务的失败历史中。

## 幂等、重试与死信规则

- **幂等键**：`idempotency_key` 上有唯一约束。相同键的重复提交返回既有任务
  （HTTP 200 + `duplicate: true`），不会重复执行。
- **失败分类**：handler 抛出 `RetryableError` → 按指数退避重试；
  抛出 `NonRetryableError` → 直接进入终态 `failed`，不重试；
  未知异常按可重试处理。
- **退避策略**：`delay = min(base_backoff_seconds * 2^(attempt-1),
  max_backoff_seconds)`。
- **死信**：可重试失败达到 `max_attempts` 后进入 `dead`，失败原因保留在
  `last_error` 与 `failure_history` 中。
- **死信重放**：`POST /jobs/{id}/replay` 仅当任务处于 `dead` 时生效
  （否则为幂等空操作），重置 attempt 并重新调度。重放不会产生重复终态
  写入；配合幂等 handler，也不会产生重复副作用。

## 并发与背压配置（`QueueConfig`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `max_concurrency` | 2 | worker 协程数，即同时执行的任务上限 |
| `max_queue_size` | 8 | pending 任务容量上限，达到即背压拒绝（HTTP 429, `error=queue_full`） |
| `base_backoff_seconds` | 0.05 | 指数退避基数 |
| `max_backoff_seconds` | 1.0 | 退避上限 |
| `poll_interval_seconds` | 0.02 | worker 空闲轮询间隔 |
| `recent_window_seconds` | 60 | 概览中"最近拒绝/失败"信号的统计窗口 |

## 进程重启恢复

任务状态持久化于 SQLite（WAL 模式）。进程启动时执行恢复：所有崩溃遗留的
`running` 任务被重新置为 `pending`（attempt 计数保留）并继续处理——不丢失、
不重复终态。已终态的任务在重启后保持原状，相同幂等键的再提交仍判为重复。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/jobs` | 提交任务。202 接受 / 200 幂等重复 / 429 背压拒绝 |
| `GET` | `/jobs/{job_id}` | 查询状态与去重后的失败历史 |
| `POST` | `/jobs/{job_id}/replay` | 死信重放（幂等） |
| `GET` | `/queue/overview` | 只读队列概览（运维用） |

`POST /jobs` 请求体在原有字段之外新增可选字段
`wait_timeout_seconds`（0–300 秒）：队列满时最多等待该时长，期间腾出
位置即自动受理；超时仍返回 429 且不留任何记录。不传则保持立即拒绝。

### 队列概览（`GET /queue/overview`）

```json
{
  "counts": {"pending": 2, "running": 1, "succeeded": 5, "failed": 0, "dead": 0},
  "concurrency": {"running": 1, "max": 2},
  "capacity": {"max_queue_size": 8, "pending": 2, "available": 6},
  "backpressure": {"rejected_total": 3, "recent_rejections": 3, "rejecting": true},
  "failures": {"recent_failures": 0, "failing": false},
  "recent_window_seconds": 60.0
}
```

- `counts`：各状态任务数（等待中 / 执行中 / 成功 / 失败 / 死信）。
- `concurrency`：当前执行中并发数与 worker 上限。
- `capacity`：pending 容量上限、当前 pending 数、还可受理数
  （`available = max(0, max_queue_size - pending)`，恒在 `[0, max_queue_size]` 内）。
- `backpressure`：`rejected_total` 为累计容量拒绝次数（持久化，重启保留）；
  `recent_rejections` / `rejecting` 表示最近窗口内是否持续被拒——
  **队列被打满**看这里。
- `failures`：最近窗口内的真实执行失败事件数（可重试/不可重试/死信）——
  **任务真的在失败**看这里，与背压拒绝严格区分。
- 所有数字在同一快照下计算，并发提交与执行过程中保持自洽
  （不会出现负数或剩余容量超过上限）。

## 兼容性说明

- 原有提交（不带 `wait_timeout_seconds`）、状态查询、死信重放的行为与
  响应格式完全不变；旧的 429 响应仍是 `{"error": "queue_full", ...}`。
- 行为变化仅一处：背压拒绝不再写入 `failed` 任务记录（旧版本会留下
  `last_error=queue_full` 的失败任务）。这意味着旧版本中被拒的幂等键
  无法复用，新版本可以——这正是"拒绝可恢复"语义。
- 存量数据库无需迁移：新增 `queue_meta` 表由 `init_schema()` 自动创建。

## 本地验证

```bash
# 安装依赖（使用 uv，索引已配置为镜像，无需外部凭据）
uv sync

# 运行全部测试（日志会打印任务标识、尝试次数与判定依据）
uv run pytest -q

# 本地启动服务
uv run python main.py

# 手动验证
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_type":"echo","payload":{"x":1},"idempotency_key":"demo-1"}'
curl -s localhost:8000/jobs/<job_id>

# 等待受理：队列满时最多等 5 秒，腾出位置即受理，超时返回 429
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_type":"echo","payload":{},"idempotency_key":"demo-2",
       "wait_timeout_seconds":5}'

# 队列概览（只读）
curl -s localhost:8000/queue/overview
```

测试覆盖：并发提交同一幂等键、并发上限下的背压拒绝（拒绝原因与重复提交、
handler 错误可区分）、背压拒绝后同键重提正常执行（拒绝可恢复）、等待受理
的"等到空位 / 等超时"两条路径与等待期间同键并发去重、队列概览数字随状态
迁移的准确性与自洽性（容量/并发不变量）、可重试与不可重试失败、退避重试与
死信、死信重放（无重复终态/副作用）、进程重启恢复（不丢失、不重复终态、
状态不倒退）。
