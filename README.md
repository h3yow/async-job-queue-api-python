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
- **背压拒绝（可恢复，非失败）**：提交时若 pending 数达到容量上限，请求被
  拒绝（HTTP 429, `error=queue_full`），且**不留下任何任务记录**——没有占位行、
  没有失败事件，查询接口里看不到这次拒绝。容量恢复后用**同一个幂等键**重新
  提交即可被正常受理并执行，无需换新键。因此"打满被拒"与"任务执行失败 /
  不可重试失败"是严格区分的两类结果：前者只计入背压计数，后者才进入
  `failed` / `dead` 与失败历史。
- **状态查询去重**：失败历史记录表以 `(job_id, attempt, event, reason)`
  唯一约束去重，调用方不会观察到同一原因的重复失败记录，也不会观察到
  状态倒退。

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
| `overview_window_seconds` | 60 | 概览"最近"信号的滑动窗口时长 |
| `overview_sustained_threshold` | 3 | 窗口内拒绝/失败达到该次数即判定为"持续" |

## 提交时等待空位（`wait_timeout_seconds`，可选）

默认行为不变：队列打满立即拒绝（429）。提交时可额外携带
`wait_timeout_seconds`（秒，≥0），表示最多等待多久：

- 等待期间只要有 worker 领走 pending 任务腾出位置，提交即被自动受理
  （HTTP 202）并开始执行；
- 等满超时仍无空位，按容量不足拒绝（429 `queue_full`），**不留下任何
  占位记录**，随后用同一幂等键可正常重新提交；
- 等待期间幂等语义不变：同一幂等键的并发等待提交最多只有一个被受理，
  其余观察到 `duplicate`，同一个键始终只对应一个任务、只执行一次。

## 队列概览（只读）

`GET /queue/overview` 返回运维用只读快照，不修改任何状态：

| 字段 | 含义 |
|---|---|
| `counts` | 各状态当前任务数：`pending` / `running` / `succeeded` / `failed` / `dead` |
| `max_concurrency` | 并发上限（worker 数） |
| `max_queue_size` | pending 容量上限 |
| `running` | 当前正在执行的任务数（实际并发） |
| `remaining_capacity` | 还能再收多少 pending，恒满足 `0 ≤ remaining ≤ max_queue_size` |
| `backpressure_rejected_total` | 累计背压拒绝次数（持久化，重启不丢） |
| `recent.window_seconds` | "最近"信号的统计窗口 |
| `recent.rejections` / `recent.failures` | 窗口内的背压拒绝次数 / 任务失败次数 |
| `recent.sustained_rejections` / `recent.sustained_failures` | 窗口内次数达到阈值则为 `true`，用于区分"队列被打满"与"任务真的在失败" |

背压拒绝只计入 `backpressure_rejected_total` 与 `recent.rejections`，不会
出现在 `counts.failed` / `recent.failures` 中；反之 handler 失败不影响
背压计数。并发提交与执行过程中各数字保持自洽（无负数、剩余容量不超上限）。

## 进程重启恢复

任务状态持久化于 SQLite（WAL 模式）。进程启动时执行恢复：所有崩溃遗留的
`running` 任务被重新置为 `pending`（attempt 计数保留）并继续处理——不丢失、
不重复终态。已终态的任务在重启后保持原状，相同幂等键的再提交仍判为重复。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/jobs` | 提交任务。202 接受 / 200 幂等重复 / 429 背压拒绝；可选 `wait_timeout_seconds` 等待空位 |
| `GET` | `/jobs/{job_id}` | 查询状态与去重后的失败历史 |
| `POST` | `/jobs/{job_id}/replay` | 死信重放（幂等） |
| `GET` | `/queue/overview` | 只读队列概览（各状态计数、容量、背压与失败信号） |

## 兼容性

原有调用方式完全不变：不带 `wait_timeout_seconds` 的提交仍是"打满立即
429"；`GET /jobs/{id}` 的响应结构不变；死信重放语义不变。唯一的语义收紧：
背压拒绝不再留下 `failed` 占位记录（此前依赖该记录判重的调用方应改为
直接重试同一幂等键——现在会被正常受理）。

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

# 提交时最多等待 5 秒空位；打满超时会得到 429，之后用同一键重提即可
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_type":"echo","payload":{},"idempotency_key":"demo-2","wait_timeout_seconds":5}'

# 只读队列概览
curl -s localhost:8000/queue/overview
```

测试覆盖：并发提交同一幂等键、并发上限下的背压拒绝（拒绝原因与重复提交、
handler 错误可区分）、**打满被拒后同一幂等键重提正常执行（拒绝不留记录、
不计失败）**、**等待受理在"等到空位"与"等超时"两条路径下的结果（含同键
并发等待只受理一个）**、**队列概览数字随状态变化的准确性与自洽性**、
可重试与不可重试失败、退避重试与死信、死信重放（无重复终态/副作用）、
进程重启恢复（不丢失、不重复终态、状态不倒退）。
