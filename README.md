# Rolling Quota API（多桶滚动配额判定服务）

基于 **Python + FastAPI + SQLite** 的本地配额判定服务：一次请求可携带多个桶键，
在**同一事务**中完成全部桶的检查与扣减——任一超限则全部不扣减，并返回受限桶与最早重试时间。

## 核心特性

- **滚动窗口**：每个桶独立配置限额与窗口（秒），窗口内用量随事件过期滚动释放
- **多级配额树**：桶可通过可选 `parent_key` 挂到父桶形成任意深度的配额树；
  消费叶桶时用量沿父链**同时计入全部祖先**并对各层限额逐一原子校验，
  一次请求含共享祖先的多个桶时祖先只累计各后代金额之和（不重复生成贡献）；
  更新父级在事务内拒绝自引用、缺失父桶与任何直接/间接环
- **原子扣减**：单事务内完成贡献链展开 + 全部桶（含祖先）检查 + 扣减，
  任一超限则整体不扣减（all-or-nothing）
- **幂等判定**：`request_id` 首次出现时固化载荷与判定；相同内容重放原样返回
  （响应头 `X-Idempotent-Replay: true`），内容变化返回 **409**
- **原子释放**：`POST /v1/quota/releases` 在单事务内撤销一次已放行消费对
  **整条贡献链**（直接桶 + 全部祖先）的用量贡献；同一消费只能由一个
  `release_id` 释放，重放原样返回
- **原子调整**：`POST /v1/quota/adjustments` 在单事务内先排除已放行且未释放
  请求的**原贡献链**、再按当前桶配置树校验目标桶列表，任一未知桶或超限则
  **原贡献链不变**，全部通过才按同一展开规则原子替换整条贡献链；
  `adjustment_id` 幂等，同载荷重放原样返回、改换载荷返回 **409**；调整之后
  释放撤销的是**调整后**的贡献链
- **预留生命周期**：`POST /v1/quota/reservations` 提交桶列表与 `expires_in`，
  在单个写事务内按当前配额树展开**整条贡献链并立即占用容量**，任一层不足则
  整体失败（**429**，零占用）；`.../{id}/confirm` 把生效预留转为正式消费且
  **不二次扣量**，`.../{id}/cancel` 释放整条贡献链；过期预留在后续相关操作中
  **惰性释放**；确认、取消与过期清理并发时**只能有一个终态**，重复请求幂等
  返回，改换载荷返回 **409**；审计链记录首次创建成功及最终状态变更
- **并发安全**：SQLite `BEGIN IMMEDIATE` 串行化写事务，并发请求不会突破限额
- **用量查询**：随时查询桶当前窗口用量，`direct_used`（仅直接点名本桶）与
  `total_used`（含后代沿父链的汇总；`used` 为其历史别名），另以
  `reserved_used` 单列其中生效预留的占用
- **哈希审计链**：首次判定（放行/拒绝）、首次成功释放、首次成功调整与预留的
  **首次创建成功及最终状态变更**（确认/取消/过期）均在各自原事务内追加一条
  SHA-256 哈希链记录，幂等重放不重复记录；提供按 `seq` 翻页查询与全链重算校验
- **历史回放模拟**：`POST /v1/quota/simulations` 在隔离内存中回放历史消费，
  评估候选桶配置（limit/window）会带来的判定翻转（祖先限额同样参与判定），
  全程只读、不写数据库也不追加审计链

## 快速启动

```bash
docker compose up --build
```

单条命令完成构建、依赖安装与初始化（建表 + 从 `buckets.json` 播种桶配置），
服务监听 **8000** 端口，数据持久化在 `quota-data` 卷。

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 桶配置

启动时从 `buckets.json`（可用环境变量 `QUOTA_BUCKETS_CONFIG` 替换）播种，同名桶按文件内容更新。
文件中的 `parent_key` 可按**任意顺序**声明（父桶在文件中后出现亦可）；
缺失父桶、自引用或任何直接/间接环都会导致整批播种回滚：

```json
{
  "buckets": [
    {"key": "api-requests", "limit": 100, "window_seconds": 60},
    {"key": "llm-tokens",   "limit": 10000, "window_seconds": 60},
    {"key": "heavy-jobs",   "limit": 5,   "window_seconds": 300}
  ]
}
```

### 多级配额树（`parent_key`）

每个桶都可以通过可选 `parent_key` 挂到另一个桶上，形成任意深度的配额树
（省略或显式 `null` 表示根桶）：

```json
{
  "buckets": [
    {"key": "org-quota",   "limit": 100000, "window_seconds": 60},
    {"key": "team-quota",  "limit": 10000,  "window_seconds": 60,
     "parent_key": "org-quota"},
    {"key": "api-requests", "limit": 100,   "window_seconds": 60,
     "parent_key": "team-quota"}
  ]
}
```

层级语义：

- **消费即上卷**：对 `api-requests` 的每一笔消费，除桶自身外还会沿父链
  向 `team-quota`、`org-quota` 各计入一份**汇总贡献**，并在同一事务内
  对三层限额逐一校验——任一层超限，整笔请求（含其他桶）一律不扣减；
- **共享祖先不重复**：一次请求同时消费 `api-requests` 与同属 `team-quota`
  的另一个叶桶时，`team-quota` 与 `org-quota` 各只累计两个后代金额**之和**，
  不会为同一祖先生成两条贡献；
- **每层独立窗口**：各桶保留各自的 `window_seconds`，贡献按所在桶的窗口
  独立过期；
- **内部桶也可直接消费**：直接点名非叶桶时，该桶既有 `direct` 用量又继续
  向其祖先上卷（用量不会向下传播给子桶）；
- **调整/释放同规则**：`adjustments` 按同一展开规则**替换整条贡献链**
  （先撤旧链再按当前树校验并写新链），`releases` **撤销整条贡献链**；
- **改父级**：`PUT /v1/buckets/{key}` 更新 `parent_key` 时在单个事务内
  拒绝自引用（`422`）、缺失父桶（`422`）与任何直接/间接环（`422`），
  失败时旧父级与限额窗口均不变。

运行时也可动态创建/更新：`PUT /v1/buckets/{key}`，请求体
`{"limit": 100, "window_seconds": 60, "parent_key": "team-quota"}`
（`parent_key` 省略或为 `null` 即根桶，PUT 为整体替换语义）。

## 示例请求

### 1. 配额判定（检查并扣减）

```bash
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'
```

放行（`200`）：

```json
{"request_id": "req-001", "allowed": true,
 "granted": [{"key": "api-requests", "amount": 1},
             {"key": "llm-tokens", "amount": 500}],
 "decided_at": 1789657657.47}
```

超限（`429`，含 `Retry-After` 响应头；所有桶均未扣减）：

```json
{"request_id": "req-002", "allowed": false,
 "limited_buckets": [{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
                      "used": 9950, "requested": 500,
                      "retry_at": 1789657660.12, "retry_after_seconds": 2.65}],
 "retry_at": 1789657660.12, "retry_after_seconds": 2.65,
 "decided_at": 1789657657.47}
```

> `retry_at` / `retry_after_seconds` 为 `null` 表示请求量本身超过限额，永远不可能放行。

### 2. 幂等语义

```bash
# 重复提交相同内容 → 原样返回首次判定，不重复扣减
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'

# 相同 request_id 但内容变化 → 409 Conflict
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 2}]}'
```

### 3. 释放已放行的消费（恢复用量）

```bash
curl -X POST http://localhost:8000/v1/quota/releases \
  -H 'Content-Type: application/json' \
  -d '{"release_id": "release-001", "request_id": "req-001"}'
```

成功（`200`）：在**单个事务**内删除 `req-001` 整条贡献链（直接桶 +
全部祖先）的用量事件，各层用量即刻恢复；不会出现部分桶已恢复、部分未恢复：

```json
{"release_id": "release-001", "request_id": "req-001",
 "released": true,
 "released_buckets": [{"key": "api-requests", "amount": 1},
                      {"key": "llm-tokens", "amount": 500}],
 "events_reverted": 2,
 "released_at": 1789657690.12}
```

> `released_buckets` 汇总请求**直接点名**的桶；`events_reverted` 为实际
> 删除的事件数（含每个受影响祖先各一条汇总事件）。例如三级树中的叶桶
> 消费会产生 3 条事件，释放时 `events_reverted` 为 3。

释放语义：

| 场景 | 结果 |
| --- | --- |
| 相同 `release_id` + 相同 `request_id` 重放 | `200`，原样返回首次释放结果，响应头 `X-Idempotent-Replay: true`，不重复恢复 |
| 相同 `release_id` 改指其他 `request_id` | `409` |
| 同一消费请求用另一个 `release_id` 再次释放（含并发竞争落败方） | `409` |
| 原 `request_id` 不存在 | `404` |
| 原消费判定为拒绝（`429`，从未扣减） | `409` |

释放后重放原 `consume` 仍返回其已固化的判定（带 `X-Idempotent-Replay: true`），
**不会再次扣减**；恢复出的额度可供新的 `request_id` 使用。

### 4. 调整已放行消费的桶贡献（原子替换）

```bash
curl -X POST http://localhost:8000/v1/quota/adjustments \
  -H 'Content-Type: application/json' \
  -d '{"adjustment_id": "adj-001", "request_id": "req-001",
       "buckets": [{"key": "heavy-jobs", "amount": 1}]}'
```

在**单个 `BEGIN IMMEDIATE` 事务**内完成：

1. 校验 `request_id` 存在、原判定为放行且尚未释放；
2. **先从窗口用量中排除该请求的整条原贡献链**（删除其全部用量事件，
   含各祖先的汇总事件）；
3. 再按**当前桶配置树**展开目标桶列表，对目标桶及其全部祖先逐桶做容量校验；
4. 任一目标桶未知或任一桶（在排除原贡献后）仍超限 → **整体回滚，原贡献链原样保留**；
5. 全部通过才按同一展开规则写入替换事件（沿用原消费时间戳）、固化调整结果，
   并在**同一事务**追加一条 `adjust/adjusted` 审计记录。

成功（`200`）：

```json
{"adjustment_id": "adj-001", "request_id": "req-001",
 "adjusted": true,
 "previous_buckets": [{"key": "api-requests", "amount": 1},
                      {"key": "llm-tokens", "amount": 500}],
 "buckets": [{"key": "heavy-jobs", "amount": 1}],
 "events_replaced": 1,
 "adjusted_at": 1789657750.12}
```

超限（`429`，含 `Retry-After`；原贡献**不发生任何变化**）：

```json
{"adjustment_id": "adj-002", "request_id": "req-001",
 "adjusted": false,
 "previous_buckets": [{"key": "api-requests", "amount": 1}],
 "target_buckets": [{"key": "heavy-jobs", "amount": 2}],
 "limited_buckets": [{"key": "heavy-jobs", "limit": 5, "window_seconds": 300,
                      "used": 4, "requested": 2,
                      "retry_at": 1789657900.0, "retry_after_seconds": 150.0}],
 "retry_at": 1789657900.0, "retry_after_seconds": 150.0,
 "decided_at": 1789657750.0}
```

调整语义：

| 场景 | 结果 |
| --- | --- |
| 相同 `adjustment_id` + 相同载荷重放 | `200`，原样返回首次调整结果，响应头 `X-Idempotent-Replay: true`，不重复替换、不重复写审计 |
| 相同 `adjustment_id` 改换载荷（含改指其他 `request_id`） | `409` |
| 目标列表含未配置桶 | `404`，原贡献保留 |
| 排除原贡献后目标桶仍超限 | `429`，原贡献保留（事务回滚） |
| `request_id` 不存在 / 原判定为拒绝 / 已释放 | `404` / `409` / `409` |
| 同一消费用**不同** `adjustment_id` 连续调整 | 允许；每次基于上一次调整后的现存贡献再次替换 |

> 校验目标桶时该请求的原贡献已被排除，因此把同一桶的贡献从 5 下调到 2 不会
> 被自己挡住；`429` 不固化任何结果，额度释放后同一 `adjustment_id` 可重试成功。

调整之后再调用释放接口，撤销的是**调整后的贡献**（以现存用量事件为准），
返回的 `released_buckets` 也是调整后的桶列表；调整后重放原 `consume` 仍只返回
首次判定、不重新扣减原桶。

### 5. 预留生命周期（先占容量，后确认/取消）

预留用于"先占住整条配额链容量，稍后再决定是否真正消费"：创建时在**单个
写事务**内按当前配额树展开整条贡献链并**立即占用**直接桶与全部祖先的容量，
任一层不足则整体失败、零占用。预留有 TTL（`expires_in` 秒），可被确认转正、
显式取消，或在过期后由后续相关操作惰性释放。

#### 5.1 创建预留（原子占用整条贡献链）

```bash
curl -X POST http://localhost:8000/v1/quota/reservations \
  -H 'Content-Type: application/json' \
  -d '{"reservation_id": "resv-001",
       "buckets": [{"key": "api-requests", "amount": 10},
                   {"key": "llm-tokens",  "amount": 2000}],
       "expires_in": 120}'
```

成功（`201`，容量已立即占用，占用持续到确认/取消/过期）：

```json
{"reservation_id": "resv-001", "reserved": true,
 "buckets": [{"key": "api-requests", "amount": 10},
             {"key": "llm-tokens", "amount": 2000}],
 "events_reserved": 2,
 "expires_at": 1789657800.12, "reserved_at": 1789657680.12}
```

> `events_reserved` 为整条贡献链的事件数：多级配额树中叶桶预留会同时占用
> 每个祖先各一条汇总事件（例如三级树中叶桶为 3）。配额树中任一层容量不足时
> 与 `consume` 同形返回 `429`（含 `limited_buckets`、`Retry-After`），
> **所有层均零占用**；`429` 不固化，额度释放后同一 `reservation_id` 可重试。
> 桶未配置返回 `404`；`expires_in` 为正数（上限 365 天）。

创建语义：

| 场景 | 结果 |
| --- | --- |
| 相同 `reservation_id` + 相同载荷（含相同 `expires_in`）重放 | `201`，原样返回首次创建结果，响应头 `X-Idempotent-Replay: true`，不重复占用（即使预留此后已确认/取消/过期，仍返回首次创建结果） |
| 相同 `reservation_id` 改换桶/金额/`expires_in` | `409` |
| `reservation_id` 与已存在的消费 `request_id` 撞键（或反向撞键） | `409` |
| 贡献链上任一桶容量不足 | `429`，整体零占用，不固化、可同 ID 重试 |
| 存在未配置桶（含祖先引用中断） | `404` |

#### 5.2 确认预留（转正式消费，不二次扣量）

```bash
curl -X POST http://localhost:8000/v1/quota/reservations/resv-001/confirm \
  -H 'Content-Type: application/json' \
  -d '{"confirm_id": "confirm-001"}'
```

成功（`200`）：把预留占用事件**原地转正**为正式消费（金额与时间戳不变，
故不二次扣量），此后该预留与普通放行消费无异——可被 `releases`/`adjustments`
处理，以同 ID 重放 `consume` 命中固化判定、不重复扣减：

```json
{"reservation_id": "resv-001", "confirmed": true,
 "buckets": [{"key": "api-requests", "amount": 10},
             {"key": "llm-tokens", "amount": 2000}],
 "events_committed": 2,
 "confirmed_at": 1789657690.12}
```

#### 5.3 取消预留（释放整条贡献链）

```bash
curl -X POST http://localhost:8000/v1/quota/reservations/resv-001/cancel \
  -H 'Content-Type: application/json' \
  -d '{"cancel_id": "cancel-001"}'
```

成功（`200`）：在单事务内把预留置为 `cancelled` 并删除整条占用贡献链
（直接桶 + 全部祖先），各层用量即刻恢复：

```json
{"reservation_id": "resv-001", "cancelled": true,
 "released_buckets": [{"key": "api-requests", "amount": 10},
                      {"key": "llm-tokens", "amount": 2000}],
 "events_reverted": 2,
 "cancelled_at": 1789657690.12}
```

确认与取消语义：

| 场景 | 结果 |
| --- | --- |
| 相同 `confirm_id`/`cancel_id` + 相同预留重放 | `200`，原样返回首次结果，响应头 `X-Idempotent-Replay: true`，不重复执行 |
| 相同 `confirm_id`/`cancel_id` 改指其他预留 | `409` |
| 预留不存在 | `404` |
| 确认已取消/已确认/已过期的预留，或取消已确认/已取消/已过期的预留 | `409`（确认、取消与过期清理只能有一个终态） |
| 确认/取消并发竞争落败（含恰在过期时刻） | `409`；终态唯一，胜出方为 `confirmed`/`cancelled`/`expired` 之一 |

#### 5.4 过期与惰性释放

预留到达 `expires_at` 后**不会**立即删除，而是在后续**相关操作**
（`consume`、`releases`、`adjustments`、创建/确认/取消预留）进入写事务前被
**惰性释放**：在独立短事务内条件抢占 `expired` 终态、删除整条占用贡献链并
追加一条 `reserve/expired` 审计。对已过期预留调用确认/取消一律返回 `409`
（终态只能是 `expired`）。终态转移由 `BEGIN IMMEDIATE` 串行化与条件
`UPDATE ... WHERE status='reserved' AND expires_at > now` 共同保证：
确认、取消与过期清理并发时只有一个终态，容量要么仍被正式消费占用、要么已完整
释放，不会出现二次扣量或容量泄漏。

### 6. 查询桶当前窗口用量

```bash
curl http://localhost:8000/v1/buckets/llm-tokens/usage
```

```json
{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
 "parent_key": null,
 "used": 500, "direct_used": 500, "total_used": 500,
 "reserved_used": 0,
 "remaining": 9500,
 "window_start": 1789657597.55, "as_of": 1789657657.55,
 "oldest_event_expires_at": 1789657717.47}
```

字段说明：

- `direct_used`：当前窗口内由**直接点名本桶**的请求贡献的用量；
- `total_used`：本桶全部用量 = 直接贡献 + 后代桶沿父链上卷的汇总贡献；
  内部（祖先）桶上 `direct_used` 可能为 0 而 `total_used` 非 0；
- `used`：`total_used` 的历史别名（保持兼容）；
- `reserved_used`：`total_used` 中由**生效预留**占用的部分（含本桶直接预留
  与后代预留上卷）；确认转正后归零（计入正式消费）、取消/过期后随链释放；
- `remaining`：按 `total_used` 相对 `limit` 计算的剩余额度；
- `parent_key`：该桶当前父桶（根桶为 `null`）。

### 7. 哈希审计链

每次**首次消费判定**（放行 `200` 或拒绝 `429`）、**首次成功释放**、**首次成功
调整**，以及预留的**首次创建成功**（`201`）与**最终状态变更**
（确认/取消/惰性过期），都在各自的原写事务内（同提交、同回滚）向仅追加的
`audit_log` 追加一条记录；幂等重放直接返回固化结果，**不重复记录**；失败的
释放/调整/预留操作（`404`/`409`/`429`）事务回滚，也不会留痕。

链规则：

- 记录使用全局递增 `seq`（从 1 开始）；
- 首条记录的 `prev_hash` 为 64 个 `0`，其后每条引用上一条的 `hash`；
- `hash = SHA-256(prev_hash 的原始 32 字节 + UTF-8 canonical JSON 事件)`；
- 事件 canonical JSON 键排序、无多余空白，固定包含：

| 字段 | 含义 |
| --- | --- |
| `op` | 操作类型：`consume`（消费判定）/ `release`（成功释放）/ `adjust`（成功调整）/ `reserve`（预留创建与终态变更） |
| `business_id` | 业务 ID：`consume` 取 `request_id`，`release` 取 `release_id`，`adjust` 取 `adjustment_id`，`reserve` 取 `reservation_id`（创建与终态两条共用同一 ID） |
| `result` | 结果：`allowed` / `denied` / `released` / `adjusted` / `reserved` / `confirmed` / `cancelled` / `expired` |
| `occurred_at` | 发生时间（epoch 秒，与判定/释放/调整/预留创建或终态变更时刻一致） |

按 `seq` 翻页查询（游标为上次最后一条的 `seq`，响应同时给出当前链头）：

```bash
curl "http://localhost:8000/v1/audit?after_seq=0&limit=100"
```

```json
{
  "records": [
    {"seq": 1,
     "prev_hash": "0000000000000000000000000000000000000000000000000000000000000000",
     "hash": "c979be39953f1ec4c07b26cbb9247815e7cb7584089d4ab5d33b401068203a4d",
     "event": {"business_id": "req-001", "occurred_at": 1789657657.47,
               "op": "consume", "result": "allowed"}},
    {"seq": 2,
     "prev_hash": "c979be39953f1ec4c07b26cbb9247815e7cb7584089d4ab5d33b401068203a4d",
     "hash": "5e8de1e930da96b784b8bb76a8a1bd9d4c5d1e18ecae2977401cbc5b60fce505",
     "event": {"business_id": "release-001", "occurred_at": 1789657690.12,
               "op": "release", "result": "released"}}
  ],
  "head": {"seq": 2, "hash": "5e8de1e930da96b784b8bb76a8a1bd9d4c5d1e18ecae2977401cbc5b60fce505"},
  "next_after_seq": 2
}
```

- `after_seq`：仅返回 `seq` 大于该值的记录，默认 `0`，必须 `>= 0`；
- `limit`：每页条数，默认 `100`，范围 `1..1000`；
- 空链的链头为 `{"seq": 0, "hash": "0000…0000"}`（64 个 0）。

> 预留产生成对记录：创建成功为 `{"op": "reserve", "result": "reserved"}`，
> 最终状态变更为同一 `business_id`（`reservation_id`）下的
> `confirmed` / `cancelled` / `expired` 之一；惰性过期的记录发生在后续相关
> 操作触发清理的那个事务中，`occurred_at` 为实际清理时刻。

全链重算校验（逐条核对 `seq` 连续性、`prev_hash` 链接与 SHA-256 重算值）：

```bash
curl http://localhost:8000/v1/audit/verify
```

```json
{"valid": true, "checked_count": 2,
 "head_hash": "5e8de1e930da96b784b8bb76a8a1bd9d4c5d1e18ecae2977401cbc5b60fce505",
 "first_invalid_seq": null}
```

发现篡改/断链时 `valid` 为 `false`，`first_invalid_seq` 给出最早失配记录的 `seq`。

### 8. 历史回放模拟（候选配置试算）

评估"如果桶当时用的是另一套 limit/window 配置，历史消费的判定会怎样变化"。
接口在**隔离内存**中重算，**只读**数据库（桶配置 + 首次消费固化记录），
不写任何业务数据、也不追加审计链：

- `from` / `to`：epoch 秒模拟区间，左闭右开 `[from, to)`；
- `buckets`：若干桶的候选 `limit/window_seconds`（必须成对给出），
  **未覆盖的桶沿用当前配置**；
- 预热：从 `from` 向前取各桶**生效窗口的最大值**作为预热期，
  预热区间内的历史消费只建立滚动窗口状态，**不计统计**；
- 回放：按 `(created_at, request_id)` 稳定顺序读取 `requests` 表中
  **首次消费**固化的载荷，重算滚动过期与多桶"全放行 / 全拒绝"；
  只回放消费（放行才占额度、拒绝不占），**不处理 release**；
- 非法范围、跨度超过 **7 天**、候选引用未知桶或同一桶重复 → **422**。

```bash
curl -X POST http://localhost:8000/v1/quota/simulations \
  -H 'Content-Type: application/json' \
  -d '{"from": 1789650000, "to": 1789650600,
       "buckets": [{"key": "llm-tokens", "limit": 5000,
                    "window_seconds": 60}]}'
```

```json
{
  "from": 1789650000,
  "to": 1789650600,
  "warmup_seconds": 300,
  "effective_buckets": [
    {"key": "api-requests", "limit": 100, "window_seconds": 60},
    {"key": "heavy-jobs", "limit": 5, "window_seconds": 300},
    {"key": "llm-tokens", "limit": 5000, "window_seconds": 60}
  ],
  "transitions": {
    "allowed_to_allowed": 42,
    "allowed_to_denied": 3,
    "denied_to_allowed": 1,
    "denied_to_denied": 4
  },
  "bucket_peaks": [
    {"key": "api-requests", "simulated_peak": 18},
    {"key": "heavy-jobs", "simulated_peak": 5},
    {"key": "llm-tokens", "simulated_peak": 4900}
  ],
  "flip_details": [
    {"created_at": 1789650123.45, "request_id": "req-104",
     "buckets": [{"key": "llm-tokens", "amount": 500}],
     "actual_decision": "allowed", "simulated_decision": "denied"},
    {"created_at": 1789650200.10, "request_id": "req-131",
     "buckets": [{"key": "llm-tokens", "amount": 200}],
     "actual_decision": "denied", "simulated_decision": "allowed"}
  ],
  "flip_details_truncated": false,
  "warmup_records": 27,
  "records_replayed": 50
}
```

返回字段：

| 字段 | 含义 |
| --- | --- |
| `transitions.allowed_to_allowed` / `allowed_to_denied` | 实际放行的消费中，模拟仍放行 / 变为拒绝的条数 |
| `transitions.denied_to_allowed` / `denied_to_denied` | 实际拒绝的消费中，模拟变为放行 / 仍拒绝的条数 |
| `bucket_peaks` | 模拟区间内每个被回放桶达到的滚动窗口用量峰值（含预热残留占用） |
| `flip_details` | 按 `(created_at, request_id)` 稳定顺序截取的**前 100 条**判定翻转明细 |
| `flip_details_truncated` | 翻转是否超过 100 条而被截断 |
| `warmup_seconds` / `warmup_records` | 预热时长（各生效窗口最大值）/ 仅用于建立状态的预热记录数 |
| `records_replayed` | 统计区间 `[from, to)` 内参与计数的消费条数 |
| `effective_buckets` | 本次模拟实际使用的桶配置（候选覆盖 + 其余沿用当前） |

> 说明：模拟忽略 release——即使一笔消费事后被释放，回放时它仍按首次消费
> 时刻占用模拟额度，以呈现"原始消费流"在候选配置下的判定结果。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/quota/consume` | 多桶检查 + 扣减；`200` 放行 / `429` 超限 / `409` 幂等冲突 / `404` 桶未配置 |
| `POST` | `/v1/quota/releases` | 释放一次已放行消费；`200` 成功或重放 / `409` 冲突 / `404` 原请求不存在 |
| `POST` | `/v1/quota/adjustments` | 原子替换一次已放行且未释放消费的桶贡献；`200` 成功或重放 / `429` 目标超限（原贡献保留） / `409` 幂等冲突或请求不可调整 / `404` 原请求或目标桶不存在 |
| `POST` | `/v1/quota/reservations` | 创建预留：提交桶列表与 `expires_in`，单事务展开贡献链立即占用；`201` 成功或重放 / `429` 任一层不足（零占用，可重试） / `409` 改换载荷或键撞车 / `404` 桶未配置 |
| `POST` | `/v1/quota/reservations/{reservation_id}/confirm` | 确认生效预留为正式消费（不二次扣量）；`200` 成功或重放 / `409` 终态冲突或改指 / `404` 预留不存在 |
| `POST` | `/v1/quota/reservations/{reservation_id}/cancel` | 取消生效预留，释放整条贡献链；`200` 成功或重放 / `409` 终态冲突或改指 / `404` 预留不存在 |
| `POST` | `/v1/quota/simulations` | 隔离内存回放历史消费评估候选配置；`200` 试算结果 / `422` 未知桶、非法范围或跨度超 7 天 |
| `GET` | `/v1/buckets/{key}/usage` | 查询桶当前滚动窗口用量（`direct_used`/`total_used`/`used`） |
| `PUT` | `/v1/buckets/{key}` | 创建或更新桶（`limit`、`window_seconds`、可选 `parent_key`）；自引用/缺失父桶/成环返回 `422` |
| `GET` | `/v1/buckets` | 列出全部桶配置（含 `parent_key`） |
| `GET` | `/v1/audit?after_seq=&limit=` | 按 `seq` 升序返回审计记录与链头 |
| `GET` | `/v1/audit/verify` | 全链重算，返回 `valid`/`checked_count`/`head_hash`/`first_invalid_seq` |
| `GET` | `/healthz` | 健康检查 |

交互式文档：`http://localhost:8000/docs`

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `QUOTA_DB_PATH` | `quota.db`（镜像内 `/data/quota.db`） | SQLite 数据库文件路径 |
| `QUOTA_BUCKETS_CONFIG` | `buckets.json`（镜像内 `/app/buckets.json`） | 启动时播种的桶配置文件，置空跳过 |
| `QUOTA_BUSY_TIMEOUT_MS` | `30000` | SQLite 写事务排队等待毫秒数 |

## 实现说明

- **配额树与贡献链**：`buckets.parent_key` 自引用形成配额树（根桶为
  `NULL`）。消费判定时由 `app/quota.py` 的纯函数 `expand_contributions`
  把规范化后的直接桶列表展开为贡献链：每个直接桶计一份 `direct=1` 事件，
  其祖先链（`ancestor_chain`，父→祖父→…）上每个桶各计一份 `direct=0`
  的汇总事件；当请求的多个桶共享同一祖先时，该祖先的各条后代链在同一
  聚合槽内相加（`rollup = Σ 后代金额`），因此每桶至多一条汇总事件，
  天然不重复。容量校验对贡献链上**每个桶**执行 `used + total > limit`，
  各桶按自身 `window_seconds` 取窗口内事件（直接 + 汇总）求和。直接点名
  内部桶时该桶可能同时存在 direct 与 rollup 两种事件；用量查询以
  `direct = 1` 区分 `direct_used` 与 `total_used`。
- **父级更新的事务内校验**：`PUT /v1/buckets/{key}` 在单个
  `BEGIN IMMEDIATE` 事务内先拦截自引用、再在写入前检查父桶存在性
  （SQLite 外键 `REFERENCES buckets(key)` 在写入时立即生效），写入后按
  **最终图**沿父链复查任何直接/间接环；任一不通过返回 `422` 并整体
  `ROLLBACK`，旧配置不发生部分更新。播种文件支持乱序 `parent_key`：
  先在"文件覆盖后的最终图"上统一校验，再按父子依赖迭代插入，缺父/自引
  /成环时整批回滚。
- **原子性**：`consume` 在单个 `BEGIN IMMEDIATE` 事务内完成"展开贡献链 →
  逐层读窗口用量 → 判定 → 写整条链事件 → 固化判定"，任一环节失败整体
  回滚。`release` 同理：在单个写事务内完成"校验原请求 → 删除其全部
  `usage_events`（整条链）→ 标记 `requests.release_id` → 写入 `releases`
  释放记录"，保证所有层同时恢复；`released_buckets` 仅汇总 `direct=1`
  事件，`events_reverted` 为实际删除的全部链事件数。`adjust` 在单个写
  事务内完成"校验原请求（放行且未释放）→ 删除原贡献链 → 按当前配置树
  展开并校验目标列表 → 写入替换链 → 写入 `adjustments` 记录"；校验发生
  在原贡献删除**之后**（从而原贡献不会挡住目标桶或其祖先），任一未知桶
  /超限即 `ROLLBACK`，已删除的原贡献链随回滚完整恢复，成功时替换事件
  沿用原消费 `created_at` 使窗口时刻保持一致，`events_replaced` 为新链
  实际事件数（每桶 direct/rollup 至多各一条）。
- **并发**：`BEGIN IMMEDIATE` 使写事务在数据库级串行化；WAL 模式保证读（用量查询）不阻塞。
  多个不同 `release_id` 并发释放同一消费时，事务串行化 + 占用标记保证仅一个成功，
  其余返回 `409`。多个不同 `adjustment_id` 并发调整同一消费时按顺序串行替换，
  每笔都基于上一笔提交后的现存贡献做排除与校验，贡献不会叠加、限额不会被突破。
- **幂等**：`requests` 表以 `request_id` 为主键，固化载荷 SHA-256 与判定 JSON；
  重放命中时直接返回，哈希不一致返回 409。`releases` 表以 `release_id` 为主键固化
  目标 `request_id` 与释放结果：同目标重放原样返回并带 `X-Idempotent-Replay: true`，
  改指其他目标返回 409。`adjustments` 表以 `adjustment_id` 为主键固化完整载荷
  （含 `request_id` 与规范化目标桶列表）的哈希与调整结果，同载荷重放原样返回、
  任何载荷变化（含改指 `request_id`）返回 409；`429` 超限不写幂等表，可在额度
  释放后以同一 `adjustment_id` 重试。
- **释放与调整的关系**：调整只替换 `usage_events`（整条贡献链）、不改写
  `requests` 中固化的首次判定，故释放以现存事件为准撤销**当前（可能已被
  调整）的贡献链**，`released_buckets` 同样仅由现存 `direct=1` 事件汇总；
  调整后重放原 `consume` 仍命中固化判定，不会把已搬走的贡献重新扣回原桶。
- **预留的存储与状态机**：`reservations` 表以 `reservation_id` 为主键，
  `status ∈ {reserved, confirmed, cancelled, expired}`（后三者互斥终态），
  固化规范化载荷（桶列表 + `expires_in`）的 SHA-256 与首次创建结果。创建时
  写入的占用事件与正式消费事件同在 `usage_events` 中（参与同一套窗口求和，
  故不会超卖），仅以非空 `reservation_id` 列标记归属。确认在单个写事务内
  条件 `UPDATE ... SET status='confirmed' WHERE status='reserved'
  AND expires_at > now` 抢占终态，随后 `UPDATE usage_events SET
  reservation_id=NULL` 原地转正——金额与 `created_at`（创建时刻）不变，
  **不二次扣量**；同时写入与 `consume` 同形的 `requests` 固化判定，使确认后
  的预留可被 `releases`/`adjustments` 处理、同 ID 的 `consume` 重放不重复扣。
  取消先条件抢占 `cancelled` 终态、再按 `reservation_id` 删除整条贡献链
  （`events_reverted` 含直接与全部祖先事件）。
- **预留终态的并发互斥与惰性过期**：确认、取消与过期清理的终态转移全部由
  `BEGIN IMMEDIATE` 串行化 + 带 `status='reserved' AND expires_at > now`
  条件的 UPDATE 保证仅一个 `rowcount=1`。过期无后台任务：
  `consume`/`releases`/`adjustments`/创建预留进入写事务前执行全局惰性清理，
  确认/取消先对目标预留做定向清理；每条过期预留在**独立短事务**内完成
  "条件抢占 `expired` → 删除整条占用链 → 追加 `reserve/expired` 审计"，
  与紧随其后的主事务互不污染。主事务内对目标预留再带条件复查一次，关闭
  清理与主事务之间的竞争窗口。
- **预留幂等与命名空间**：创建以 `reservation_id`、确认以 `confirm_id`、
  取消以 `cancel_id` 分别固化（`reservation_confirmations` /
  `reservation_cancellations` 表），同键同目标重放原样返回并带
  `X-Idempotent-Replay: true`，改指其他预留返回 409；创建载荷哈希覆盖
  规范化桶列表**与 `expires_in`**，任一变化返回 409；创建 `429` 不固化、
  可同 ID 重试。`reservation_id` 与消费 `request_id` 共享同一幂等键空间
  （预留事件以 `reservation_id` 作为 `request_id` 落盘），双向撞键均 409。
- **审计链**：`audit_log` 为仅追加表，`seq` 由 SQLite AUTOINCREMENT 全局递增。
  首次判定在固化 `requests` 之后、首次成功释放在写入 `releases` 之后、首次成功
  调整在写入 `adjustments` 之后、预留首次创建成功在写入 `reservations` 之后、
  预留终态变更（确认/取消在各自幂等表写入之后，过期在清理短事务内），于
  **同一写事务**内追加审计记录——业务回滚则审计不生效，二者不可能分离；
  幂等重放走不写入审计的独立分支。
  追加时先读链尾 `hash` 作为 `prev_hash`，按
  `SHA-256(bytes.fromhex(prev_hash) + canonical_json(event))` 计算当前 `hash`，
  写事务串行化保证链头读取与追加之间无并发分叉。`GET /v1/audit/verify` 从 64 个 0
  的创世前序哈希起逐条重算，同时核对 `seq` 连续性，报告首个失配位置。
- **最早重试时间**：按窗口内事件过期时刻累计释放量，找到最早可容纳本次请求的过期点；
  多桶受限时取各桶的最大值。
- **回放模拟**：`simulations` 全程只读、不开写事务，从 `requests` 表取出
  `[from - max_window, to)` 内首次消费固化的 `(created_at, request_id, payload,
  allowed)`，按 `(created_at, request_id)` 升序送入 `app/simulation.py` 的纯函数
  重算。滚动窗口用每桶一个 `deque` 内存事件队列实现，过期/占用规则与线上一致
  （严格 `created_at > ts - window`、贡献链上任一桶超限则全拒且不占用）；
  展开规则复用线上的 `expand_contributions`，祖先限额同样参与判定、峰值统计
  覆盖贡献链上的全部桶，共享祖先只计金额之和；`created_at < from` 的记录只
  推进队列、不计统计与峰值。候选配置仅在内存中覆盖当前配置（不覆盖父子树），
  release 不参与回放，数据库与审计链均不受影响。

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/ -q
```

覆盖：原子扣减、幂等重放（含拒绝判定重放）、409 冲突、滚动窗口过期恢复、
50 并发不超限额、同 `request_id` 并发只扣减一次，以及释放接口的多桶原子恢复、
幂等重放、改指冲突、重复释放冲突、404/409 分支、释放后 consume 重放不再次扣减、
20 并发释放仅一个成功；调整接口覆盖跨桶/多桶/同桶原子替换、原贡献排除校验、
未知桶 404 与超限 429 均保持原贡献（含 `Retry-After`/`retry_at=null`）、
同载荷重放原样返回不重复写审计、换载荷/改指请求 409、原单不存在/被拒/已释放
分支、不同 ID 连续调整基于现存贡献、429 后额度释放可重试、调整后释放撤销的是
调整后贡献、调整后 consume 重放不重新扣减、20 并发同 ID 只应用一次与不同 ID
串行替换不叠加、调整与消费并发不超限、成功调整仅记一条审计且全部失败分支不留痕；
此外覆盖哈希审计链的哈希规则纯函数、首条锚定 64 个 0、
放行/拒绝/成功释放/成功调整仅记一次且重放不重复、失败释放与 409/404 不留痕、
分页与链头、全链校验有效与篡改检出（事件内容/前序指针），以及回放模拟的四类转移
计数、候选覆盖与沿用当前配置、窗口收缩导致的 deny→allow、多桶全拒不占用、
预热边界与峰值、release 不回放、稳定顺序与 100 条截断、只读不留痕、
422 校验分支与已删除桶陈旧载荷；多级配额树覆盖贡献链展开纯函数
（叶→全部祖先、共享祖先只计金额之和、内部桶 direct 与 rollup 并存）、
PUT 的 parent_key 增删改查、自引用/缺失父桶/直接环与间接环事务内 422
回滚且旧配置不变、叶桶消费逐层计入祖先、祖先限额原子拦截且各层零扣减、
直接消费内部桶、`direct_used`/`total_used`/`used` 用量语义、释放撤销
整条链（含共享祖先仅删一次汇总）、调整按同规则替换/搬移整条链并校验祖先、
未知目标 404 保链、树消费幂等重放不重复累计、30 并发不突破祖先限额、
树场景审计语义不变、回放模拟的祖先拒绝/峰值与共享祖先不重复，以及播种
乱序父子与坏配置（缺父）整批回滚；预留生命周期覆盖整链立即占用与
`reserved_used` 用量、祖先层不足 all-or-nothing 零占用、占用挡住消费的
429（含 Retry-After）与 429 不固化可同 ID 重试、未知桶 404、同载荷重放
201 不重复占用、改桶/改金额/改 `expires_in` 409、与 consume 的 ID 双向撞键
409、确认原地转正不二次扣量（用量不变、占用标记归零）、确认后可被
releases 撤销且同 ID consume 重放不重复扣、确认/取消的幂等重放与改指
409、重复确认/取消与终态冲突 409、404 分支、取消释放整条链（含祖先）、
过期在 consume/新建预留中惰性释放（事件删除、状态 expired、容量恢复）、
过期后确认/取消 409、过期边界竞争终态唯一、20 确认 + 20 取消并发恰好一个
终态、同 confirm_id 并发只确认一次、同 reservation_id 并发只占用一次、
审计链中创建与终态各一条（reserved/confirmed/cancelled/expired）且失败不留痕、
422 校验分支，以及旧库增量迁移（`usage_events.reservation_id` 列与预留表、
旧事件视为正式消费），共 112 个用例。
