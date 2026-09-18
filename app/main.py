"""FastAPI 入口：多桶滚动配额判定 API（支持多级配额树）。

核心保证：
- 同一请求的所有桶在单个 BEGIN IMMEDIATE 事务内"检查 + 扣减"，
  任一超限则整体不扣减（原子性）；
- 桶可通过可选 parent_key 形成多级配额树：消费叶桶时用量沿父链
  同时计入全部祖先，并对各层限额逐一原子校验；一次请求包含共享
  祖先的多个后代时，祖先只累计各后代金额之和（不重复生成贡献）；
- BEGIN IMMEDIATE 使写事务串行化，并发请求不会突破限额；
- request_id 首次出现时固化载荷与判定，重复提交原样返回，
  载荷变化返回 409；
- 首次判定（放行/拒绝）与首次成功释放在各自原事务内追加一条
  可校验哈希审计链记录（见 app/audit.py），幂等重放不重复记录；
- POST /v1/quota/adjustments 在单个 BEGIN IMMEDIATE 事务内先排除
  已放行且未释放请求的原贡献链、再按当前桶配置树校验目标列表，
  任一未知桶或超限则原贡献不变，全部通过才原子替换整条贡献链；
  首次成功调整在原事务追加一条审计记录，同 adjustment_id 同载荷
  重放原样返回、改换载荷返回 409；
- 预留生命周期：POST /v1/quota/reservations 在单个写事务内按当前
  配额树展开整条贡献链并立即占用容量（任一层不足整体失败）；
  POST .../{id}/confirm 把生效预留转为正式消费且不二次扣量，
  POST .../{id}/cancel 释放整条贡献链；过期预留在相关操作中惰性
  释放（同事务原子转移到 expired 终态）；确认、取消与过期清理并发
  时只能有一个终态，重复请求幂等返回，改换载荷返回 409；
- 释放撤销的是该请求当前（可能已被调整或由预留确认转正）的整条
  贡献链。
"""
from __future__ import annotations

import json
import hashlib
import logging
import math
import os
import sqlite3
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import audit, config, simulation
from .db import _rollback_quietly, connect, init_db, seed_buckets
from .quota import (
    ParentError,
    earliest_retry_at,
    expand_contributions,
    merge_items,
    payload_hash,
    validate_parent_assignment,
)
from .schemas import (
    AdjustmentRequest,
    BucketConfig,
    ConsumeRequest,
    ReleaseRequest,
    ReservationCancelRequest,
    ReservationConfirmRequest,
    ReservationRequest,
    SimulationRequest,
)

logger = logging.getLogger("quota")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if config.BUCKETS_CONFIG and os.path.exists(config.BUCKETS_CONFIG):
        seeded = seed_buckets(config.BUCKETS_CONFIG)
        logger.info("seeded buckets from %s: %s", config.BUCKETS_CONFIG, seeded)
    yield


app = FastAPI(title="Rolling Quota API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------- 配额树工具

def _bucket_configs(
    conn: sqlite3.Connection,
) -> tuple[dict[str, sqlite3.Row], dict[str, str | None]]:
    """读取全部桶配置行与父子映射 ``{key: parent_key（根桶为 None）}``。"""
    rows = conn.execute(
        "SELECT key, limit_amount, window_seconds, parent_key FROM buckets"
    ).fetchall()
    configs = {r["key"]: r for r in rows}
    return configs, {r["key"]: r["parent_key"] for r in rows}


def _window_events(
    conn: sqlite3.Connection, key: str, window: float, now: float
) -> list[tuple[float, int]]:
    """桶当前滚动窗口（严格 created_at > now-window）内的全部贡献事件。"""
    return [
        (r["created_at"], r["amount"])
        for r in conn.execute(
            "SELECT created_at, amount FROM usage_events "
            "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
            (key, now - window),
        )
    ]


def _limited_bucket(
    key: str,
    bucket: sqlite3.Row,
    events: list[tuple[float, int]],
    requested: int,
    now: float,
) -> dict:
    """构造一个受限桶（429）明细，含各层限额与最早重试时间。"""
    limit = bucket["limit_amount"]
    window = bucket["window_seconds"]
    retry_at = earliest_retry_at(events, limit, window, requested, now)
    return {
        "key": key,
        "limit": limit,
        "window_seconds": window,
        "used": sum(a for _, a in events),
        "requested": requested,
        "retry_at": retry_at,
        "retry_after_seconds": (
            None if retry_at is None else max(0.0, retry_at - now)
        ),
    }


def _check_contributions(
    conn: sqlite3.Connection,
    items: list[dict],
    configs: dict[str, sqlite3.Row],
    parents: dict[str, str | None],
    now: float,
) -> tuple[dict[str, dict[str, int]], list[dict]]:
    """展开贡献链并对直接桶及全部祖先逐层做容量校验。

    返回 (contributions, limited)：contributions 为
    ``{key: {"direct","rollup","total"}}``；limited 为受限桶明细。
    调用方须先保证 items 中的直接桶全部存在（否则 404）；祖先桶由
    配额树保证存在，缺失（陈旧图）同样按 404 抛出。
    """
    contributions = expand_contributions(items, parents)
    limited: list[dict] = []
    for key, parts in contributions.items():
        bucket = configs.get(key)
        if bucket is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"祖先桶 '{key}' 未配置，配额树引用中断；"
                    f"请先通过 PUT /v1/buckets/{key} 创建"
                ),
            )
        events = _window_events(conn, key, bucket["window_seconds"], now)
        if sum(a for _, a in events) + parts["total"] > bucket["limit_amount"]:
            limited.append(
                _limited_bucket(key, bucket, events, parts["total"], now)
            )
    return contributions, limited


def _insert_contribution_events(
    conn: sqlite3.Connection,
    request_id: str,
    contributions: dict[str, dict[str, int]],
    created_at: float,
    reservation_id: str | None = None,
) -> None:
    """按展开结果写入整条贡献链事件：每桶至多 direct/rollup 各一条。

    ``reservation_id`` 非空时事件标记为生效预留的占用（确认时在原事务
    内置空转正，取消/过期时随整条链删除）；为空即正式消费。
    """
    for key, parts in contributions.items():
        if parts["direct"]:
            conn.execute(
                "INSERT INTO usage_events "
                "(bucket_key, request_id, amount, created_at, direct, reservation_id) "
                "VALUES (?, ?, ?, ?, 1, ?)",
                (key, request_id, parts["direct"], created_at, reservation_id),
            )
        if parts["rollup"]:
            conn.execute(
                "INSERT INTO usage_events "
                "(bucket_key, request_id, amount, created_at, direct, reservation_id) "
                "VALUES (?, ?, ?, ?, 0, ?)",
                (key, request_id, parts["rollup"], created_at, reservation_id),
            )


def _direct_items_for_request(
    conn: sqlite3.Connection, request_id: str
) -> list[dict]:
    """汇总某请求当前现存的**直接**贡献（direct=1），规范化后返回。"""
    rows = conn.execute(
        "SELECT bucket_key, amount FROM usage_events "
        "WHERE request_id = ? AND direct = 1",
        (request_id,),
    ).fetchall()
    return merge_items(
        [{"key": r["bucket_key"], "amount": r["amount"]} for r in rows]
    )


# ---------------------------------------------------------------- 预留惰性过期

def _expire_reservations(
    conn: sqlite3.Connection, now: float, reservation_id: str | None = None
) -> int:
    """惰性释放已过期但仍处于 ``reserved`` 的预留，返回转入 expired 的条数。

    在调用方开启其写事务**之前**、以本连接上的独立短事务逐条完成
    （连接为 autocommit，``BEGIN IMMEDIATE/COMMIT`` 后自动回到 autocommit）：

    1. 条件 ``UPDATE ... SET status='expired' WHERE status='reserved'
       AND expires_at <= now`` 与确认/取消的条件 UPDATE 互斥——并发竞争
       落败方 ``rowcount=0``，跳过提交，终态仍唯一；
    2. 删除该预留在 ``usage_events`` 上的整条占用贡献链；
    3. 同一事务追加一条 ``reserve/expired`` 审计记录。

    ``reservation_id`` 非空时只清理指定预留（确认/取消目标），否则清理
    全部过期预留（容量判定前的相关操作触发）。
    """
    if reservation_id is not None:
        rows = conn.execute(
            "SELECT reservation_id FROM reservations "
            "WHERE reservation_id = ? AND status = 'reserved' AND expires_at <= ?",
            (reservation_id, now),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT reservation_id FROM reservations "
            "WHERE status = 'reserved' AND expires_at <= ? "
            "ORDER BY expires_at, reservation_id",
            (now,),
        ).fetchall()

    expired = 0
    for r in rows:
        rid = r["reservation_id"]
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE reservations SET status = 'expired', ended_at = ? "
                "WHERE reservation_id = ? AND status = 'reserved' AND expires_at <= ?",
                (now, rid, now),
            )
            if cur.rowcount == 1:
                # 条件 UPDATE 已在数据库级抢占终态，随后删除整条贡献链
                conn.execute(
                    "DELETE FROM usage_events WHERE reservation_id = ?", (rid,)
                )
                audit.append_event(
                    conn,
                    op=audit.OP_RESERVE,
                    business_id=rid,
                    result=audit.RESULT_EXPIRED,
                    occurred_at=now,
                )
                expired += 1
            conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    return expired


# ---------------------------------------------------------------- 配额判定

@app.post("/v1/quota/consume")
def consume(req: ConsumeRequest):
    """检查并扣减一组桶；任一超限则全部不扣减。

    - 200：全部放行，已扣减；
    - 429：至少一个桶超限，返回受限桶与最早重试时间，未做任何扣减；
    - 409：request_id 已存在但载荷不同；
    - 404：存在未配置的桶。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    phash = payload_hash(items)
    payload_json = json.dumps(
        {"request_id": req.request_id, "buckets": items}, sort_keys=True
    )

    replayed = False
    conn = connect()
    try:
        # 容量判定属于"相关操作"：先惰性释放全部过期预留
        _expire_reservations(conn, now)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT payload_hash, decision FROM requests WHERE request_id = ?",
                (req.request_id,),
            ).fetchone()

            if row is not None:
                # 幂等：同 request_id 直接返回已固化的判定，不再扣减
                if row["payload_hash"] != phash:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"request_id '{req.request_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 request_id"
                        ),
                    )
                decision = json.loads(row["decision"])
                replayed = True
                conn.execute("COMMIT")
            else:
                decision = _evaluate_and_consume(conn, req.request_id, items, now)
                conn.execute(
                    "INSERT INTO requests "
                    "(request_id, payload_hash, payload, decision, allowed, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        req.request_id,
                        phash,
                        payload_json,
                        json.dumps(decision),
                        1 if decision["allowed"] else 0,
                        now,
                    ),
                )
                # 首次判定（放行或拒绝）在同一事务内追加审计记录；
                # 幂等重放走上面的已存在分支，不会重复记录
                audit.append_event(
                    conn,
                    op=audit.OP_CONSUME,
                    business_id=req.request_id,
                    result=(
                        audit.RESULT_ALLOWED
                        if decision["allowed"]
                        else audit.RESULT_DENIED
                    ),
                    occurred_at=now,
                )
                conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return _decision_response(decision, replayed)


def _evaluate_and_consume(
    conn: sqlite3.Connection, request_id: str, items: list[dict], now: float
) -> dict:
    """在事务内检查直接桶及其全部祖先；全部通过才写入整条贡献链。"""
    configs, parents = _bucket_configs(conn)

    # request_id 与 reservation_id 共享同一业务幂等键空间：预留事件以
    # reservation_id 作为 request_id 落盘，故禁止用生效预留的 ID 直接消费
    collision = conn.execute(
        "SELECT 1 FROM reservations WHERE reservation_id = ?", (request_id,)
    ).fetchone()
    if collision is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"request_id '{request_id}' 已被预留占用；请通过预留确认接口"
                "转正，或更换 request_id"
            ),
        )

    # 先确认请求直接点名的桶全部存在（保持原有 404 语义）
    for item in items:
        if item["key"] not in configs:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"桶 '{item['key']}' 未配置，请先通过 "
                    f"PUT /v1/buckets/{item['key']} 创建"
                ),
            )

    contributions, limited = _check_contributions(
        conn, items, configs, parents, now
    )

    if limited:
        # 任一层超限：全部不扣减；整体最早重试时间取各受限桶的最大值
        retry_times = [b["retry_at"] for b in limited]
        overall_retry_at = (
            max(retry_times) if all(t is not None for t in retry_times) else None
        )
        return {
            "request_id": request_id,
            "allowed": False,
            "limited_buckets": limited,
            "retry_at": overall_retry_at,
            "retry_after_seconds": (
                None if overall_retry_at is None
                else max(0.0, overall_retry_at - now)
            ),
            "decided_at": now,
        }

    # 全部通过：同事务写入直接 + 祖先汇总的全部贡献事件
    _insert_contribution_events(conn, request_id, contributions, now)
    return {
        "request_id": request_id,
        "allowed": True,
        "granted": items,
        "decided_at": now,
    }


def _decision_response(decision: dict, replayed: bool) -> JSONResponse:
    headers = {}
    if replayed:
        headers["X-Idempotent-Replay"] = "true"
    if decision["allowed"]:
        return JSONResponse(status_code=200, content=decision, headers=headers)
    if decision.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(decision["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=decision, headers=headers)


# ---------------------------------------------------------------- 配额释放

@app.post("/v1/quota/releases")
def release(req: ReleaseRequest):
    """撤销一次已放行消费对所有桶的用量贡献。

    首次释放在单个 BEGIN IMMEDIATE 事务内完成"校验 → 删除该请求全部用量
    事件 → 标记消费已释放 → 固化释放结果"，任一环节失败整体回滚，
    不会出现部分桶已恢复。

    - 200：释放成功（或同 release_id + 同 request_id 重放，原样返回并带
      X-Idempotent-Replay: true）；
    - 404：原消费 request_id 不存在；
    - 409：原判定为拒绝 / 已被其他 release_id 释放 / release_id 已指向
      其他 request_id / 并发竞争落败。
    """
    now = time.time()

    conn = connect()
    try:
        _expire_reservations(conn, now)
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT request_id, result FROM releases WHERE release_id = ?",
                (req.release_id,),
            ).fetchone()
            if existing is not None:
                # 幂等：同 release_id 必须指向同一目标，原样返回固化结果
                if existing["request_id"] != req.request_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"release_id '{req.release_id}' 已用于释放 "
                            f"request_id '{existing['request_id']}'，不可改指其他消费请求"
                        ),
                    )
                conn.execute("COMMIT")
                result = json.loads(existing["result"])
                replayed = True
            else:
                result = _release_consumption(conn, req.release_id, req.request_id, now)
                conn.execute(
                    "INSERT INTO releases (release_id, request_id, result, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        req.release_id,
                        req.request_id,
                        json.dumps(result),
                        now,
                    ),
                )
                # 仅首次成功释放在同一事务内追加审计记录；
                # 失败（404/409）整体回滚、重放走已存在分支，均不记录
                audit.append_event(
                    conn,
                    op=audit.OP_RELEASE,
                    business_id=req.release_id,
                    result=audit.RESULT_RELEASED,
                    occurred_at=now,
                )
                conn.execute("COMMIT")
                replayed = False
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _release_consumption(
    conn: sqlite3.Connection, release_id: str, request_id: str, now: float
) -> dict:
    """在事务内撤销一个消费请求对所有桶的用量贡献并记录占用关系。"""
    row = conn.execute(
        "SELECT allowed, release_id FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"消费请求 '{request_id}' 不存在，无法释放",
        )
    if not row["allowed"]:
        # 原判定为拒绝：从未产生任何扣减，无可释放内容
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 原判定为拒绝（429），未发生扣减，不可释放",
        )
    if row["release_id"] is not None:
        # 同一消费只能由一个 release_id 成功释放；并发竞争中的其余请求亦落到此
        raise HTTPException(
            status_code=409,
            detail=(
                f"消费请求 '{request_id}' 已被 release_id '{row['release_id']}' 释放；"
                "同一消费请求只能释放一次"
            ),
        )

    # 撤销该请求当前对配额树的整条贡献链：经调整后贡献来自替换写入的
    # 事件，故以现存事件为准（调整发生在释放之前，事件与当前贡献一一
    # 对应）。released_buckets 只汇总直接点名的桶；祖先汇总事件随整条
    # 链一并删除（events_reverted 含直接与全部祖先贡献）。
    current = _direct_items_for_request(conn, request_id)
    deleted = conn.execute(
        "DELETE FROM usage_events WHERE request_id = ?",
        (request_id,),
    ).rowcount
    # 条件 UPDATE 兜底并发竞争：仅当尚未被其他释放单占用时成功
    cur = conn.execute(
        "UPDATE requests SET release_id = ? WHERE request_id = ? AND release_id IS NULL",
        (release_id, request_id),
    )
    if cur.rowcount != 1:  # pragma: no cover - 理论上前置检查已覆盖，防御性保留
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 已被其他释放单抢先释放",
        )

    return {
        "release_id": release_id,
        "request_id": request_id,
        "released": True,
        "released_buckets": merge_items(current),
        "events_reverted": deleted,
        "released_at": now,
    }


# ---------------------------------------------------------------- 配额调整

@app.post("/v1/quota/adjustments")
def adjust(req: AdjustmentRequest):
    """把一次已放行且未释放消费的整条贡献链**原子替换**为目标桶列表。

    在单个 ``BEGIN IMMEDIATE`` 事务内：先从窗口用量中排除该请求的整条
    原贡献链（删除其全部直接/祖先汇总事件），再按**当前桶配置树**展开
    目标列表并逐桶（含全部祖先）做容量校验；任一目标桶未知或任一桶
    （在排除原贡献后）仍超限，则整体回滚、原贡献链原样保留；全部通过
    才按同一展开规则写入替换链、固化调整结果，并在同一事务追加一条
    审计记录。

    - 200：调整成功（或同 adjustment_id + 同载荷重放，原样返回并带
      X-Idempotent-Replay: true，不重复写审计）；
    - 404：request_id 不存在 / 存在未配置的目标桶；
    - 409：request_id 原判定为拒绝 / 已释放 / adjustment_id 已用于
      不同载荷（含改指其他 request_id）；
    - 429：目标列表（含祖先）在排除原贡献后仍超限（原贡献链不变），
      含 Retry-After。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    phash = payload_hash(items)
    payload_json = json.dumps(
        {"adjustment_id": req.adjustment_id,
         "request_id": req.request_id, "buckets": items},
        sort_keys=True,
    )

    conn = connect()
    replayed = False
    try:
        _expire_reservations(conn, now)
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT request_id, payload_hash, result FROM adjustments "
                "WHERE adjustment_id = ?",
                (req.adjustment_id,),
            ).fetchone()
            if existing is not None:
                # 幂等：同 adjustment_id 必须同 request_id 且同目标载荷，
                # 原样返回固化结果，不再写库；任一变化返回 409
                if (
                    existing["request_id"] != req.request_id
                    or existing["payload_hash"] != phash
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"adjustment_id '{req.adjustment_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 adjustment_id"
                        ),
                    )
                result = json.loads(existing["result"])
                replayed = True
                conn.execute("COMMIT")
            else:
                result = _evaluate_and_adjust(
                    conn, req.adjustment_id, req.request_id, items, now
                )
                if not result["adjusted"]:
                    # 目标桶超限：不写入替换事件，回滚以保证原贡献不变
                    conn.execute("ROLLBACK")
                else:
                    conn.execute(
                        "INSERT INTO adjustments "
                        "(adjustment_id, request_id, payload_hash, payload, "
                        " result, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            req.adjustment_id,
                            req.request_id,
                            phash,
                            payload_json,
                            json.dumps(result),
                            now,
                        ),
                    )
                    # 首次成功调整在同一事务内追加审计；失败（404/409/429）整体
                    # 回滚、重放走已存在分支，均不记录
                    audit.append_event(
                        conn,
                        op=audit.OP_ADJUST,
                        business_id=req.adjustment_id,
                        result=audit.RESULT_ADJUSTED,
                        occurred_at=now,
                    )
                    conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    if not result["adjusted"]:
        # 目标桶超限：原贡献因回滚保持不变，返回与 consume 同形的 429
        return _adjustment_limited_response(result)

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _evaluate_and_adjust(
    conn: sqlite3.Connection,
    adjustment_id: str,
    request_id: str,
    items: list[dict],
    now: float,
) -> dict:
    """事务内排除原贡献链后校验目标列表；全部通过才原子替换整条链。"""
    row = conn.execute(
        "SELECT allowed, release_id, created_at FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"消费请求 '{request_id}' 不存在，无法调整",
        )
    if not row["allowed"]:
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 原判定为拒绝（429），未发生扣减，不可调整",
        )
    if row["release_id"] is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"消费请求 '{request_id}' 已被 release_id '{row['release_id']}' 释放，"
                "用量贡献已撤销，不可调整"
            ),
        )

    # 先从窗口用量中排除该请求的整条原贡献链（当前现存事件，含祖先汇总；
    # 调整为替换语义，同一 request_id 至多存在一套事件）
    previous = _direct_items_for_request(conn, request_id)
    conn.execute("DELETE FROM usage_events WHERE request_id = ?", (request_id,))

    # 再按当前配额树配置校验目标列表（此刻窗口用量已不含本请求的原贡献）。
    # 先确认直接目标桶全部存在（保持原有 404 语义），再展开祖先链逐层校验。
    configs, parents = _bucket_configs(conn)
    for item in items:
        if item["key"] not in configs:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"目标桶 '{item['key']}' 未配置，请先通过 "
                    f"PUT /v1/buckets/{item['key']} 创建"
                ),
            )

    contributions, limited = _check_contributions(
        conn, items, configs, parents, now
    )

    if limited:
        # 任一超限：不写入替换事件；事务回滚后已删除的原贡献链原样恢复
        retry_times = [b["retry_at"] for b in limited]
        overall_retry_at = (
            max(retry_times) if all(t is not None for t in retry_times) else None
        )
        return {
            "adjustment_id": adjustment_id,
            "request_id": request_id,
            "adjusted": False,
            "previous_buckets": previous,
            "target_buckets": items,
            "limited_buckets": limited,
            "retry_at": overall_retry_at,
            "retry_after_seconds": (
                None if overall_retry_at is None
                else max(0.0, overall_retry_at - now)
            ),
            "decided_at": now,
        }

    # 全部通过：原子替换为目标桶整条贡献链。沿用原消费的 created_at，使
    # 新贡献与原贡献在同一滚动窗口时刻生效（与首次扣减事件一一替换）
    created_at = row["created_at"]
    _insert_contribution_events(conn, request_id, contributions, created_at)
    return {
        "adjustment_id": adjustment_id,
        "request_id": request_id,
        "adjusted": True,
        "previous_buckets": previous,
        "buckets": items,
        "events_replaced": sum(
            1 for parts in contributions.values()
            for flag in (parts["direct"], parts["rollup"]) if flag
        ),
        "adjusted_at": now,
    }


def _adjustment_limited_response(result: dict) -> JSONResponse:
    """目标桶超限：返回与 consume 同形的 429（含 Retry-After）。"""
    headers = {}
    if result.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(result["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=result, headers=headers)


# ---------------------------------------------------------------- 预留生命周期

def _reservation_payload_hash(items: list[dict], expires_in: float) -> str:
    """预留载荷哈希：规范化桶列表与 expires_in 共同决定（任一变化即 409）。"""
    return hashlib.sha256(
        json.dumps(
            {"buckets": items, "expires_in": expires_in},
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _reservation_limited_response(result: dict) -> JSONResponse:
    """预留容量不足：与 consume 同形的 429（含 Retry-After），整体未占用。"""
    headers = {}
    if result.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(result["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=result, headers=headers)


@app.post("/v1/quota/reservations", status_code=201)
def create_reservation(req: ReservationRequest):
    """创建预留：提交桶列表与 ``expires_in``（秒），立即占用整条贡献链。

    在单个 ``BEGIN IMMEDIATE`` 事务内按**当前配额树**展开直接桶列表
    （含全部祖先的汇总贡献）并对各层限额逐一原子校验，任一层不足则
    整体失败、不占用任何容量（429，含 Retry-After）；全部通过才写入
    整条占用事件（``usage_events.reservation_id`` 标记）与预留记录，
    并在同一事务追加一条 ``reserve/reserved`` 审计记录。

    - 201：预留生效，容量已占用（或同 reservation_id + 同载荷重放，
      原样返回首次创建结果并带 X-Idempotent-Replay: true）；
    - 404：存在未配置的桶 / 祖先桶引用中断；
    - 409：reservation_id 已用于不同载荷（含改 expires_in）、与
      已存在的消费 request_id 撞键、或预留已进入终态时重放仍按首次
      创建结果幂等返回（改换载荷才 409）；
    - 429：贡献链上任一桶容量不足，整体未占用；429 不固化，额度
      释放后同一 reservation_id 可重试成功。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    expires_in = float(req.expires_in)
    expires_at = now + expires_in
    phash = _reservation_payload_hash(items, expires_in)
    payload_json = json.dumps(
        {"reservation_id": req.reservation_id,
         "buckets": items, "expires_in": expires_in},
        sort_keys=True,
    )

    conn = connect()
    replayed = False
    try:
        # 创建占用容量，属于相关操作：先惰性释放全部过期预留
        _expire_reservations(conn, now)
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT payload_hash, payload FROM reservations "
                "WHERE reservation_id = ?",
                (req.reservation_id,),
            ).fetchone()
            if existing is not None:
                # 幂等：同 reservation_id 直接返回首次创建时固化的结果，
                # 无论预留此后进入哪个终态；载荷（含 expires_in）变化返回 409
                if existing["payload_hash"] != phash:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"reservation_id '{req.reservation_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 reservation_id"
                        ),
                    )
                result = json.loads(existing["payload"])
                replayed = True
                conn.execute("COMMIT")
            else:
                # 与消费的 request_id 共享幂等键空间：预留事件以
                # reservation_id 作为 request_id 落盘，撞键须拒绝
                clash = conn.execute(
                    "SELECT 1 FROM requests WHERE request_id = ?",
                    (req.reservation_id,),
                ).fetchone()
                if clash is not None:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"reservation_id '{req.reservation_id}' 已被消费请求"
                            "占用为 request_id；请更换 reservation_id"
                        ),
                    )
                result = _evaluate_and_reserve(
                    conn, req.reservation_id, items, expires_at, now
                )
                if not result["reserved"]:
                    # 容量不足：不写入占用事件，回滚以保证零占用
                    conn.execute("ROLLBACK")
                else:
                    conn.execute(
                        "INSERT INTO reservations "
                        "(reservation_id, payload_hash, payload, status, "
                        " expires_at, created_at) "
                        "VALUES (?, ?, ?, 'reserved', ?, ?)",
                        (
                            req.reservation_id,
                            phash,
                            json.dumps(result),
                            expires_at,
                            now,
                        ),
                    )
                    # 首次创建成功在同一事务追加审计；失败（404/409/429）
                    # 整体回滚、重放走已存在分支，均不记录
                    audit.append_event(
                        conn,
                        op=audit.OP_RESERVE,
                        business_id=req.reservation_id,
                        result=audit.RESULT_RESERVED,
                        occurred_at=now,
                    )
                    conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    if not result["reserved"]:
        return _reservation_limited_response(result)

    return JSONResponse(
        status_code=201,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _evaluate_and_reserve(
    conn: sqlite3.Connection,
    reservation_id: str,
    items: list[dict],
    expires_at: float,
    now: float,
) -> dict:
    """事务内按当前配额树校验并写入整条占用贡献链（任一层不足则不占用）。"""
    configs, parents = _bucket_configs(conn)

    # 先确认请求直接点名的桶全部存在（与 consume/adjust 一致的 404 语义）
    for item in items:
        if item["key"] not in configs:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"桶 '{item['key']}' 未配置，请先通过 "
                    f"PUT /v1/buckets/{item['key']} 创建"
                ),
            )

    # 容量校验复用贡献链展开：预留占用事件与正式消费事件同在
    # usage_events 中参与窗口求和，故确认前不会超卖
    contributions, limited = _check_contributions(
        conn, items, configs, parents, now
    )

    if limited:
        # 任一层不足：整体失败，尚未写入任何占用事件
        retry_times = [b["retry_at"] for b in limited]
        overall_retry_at = (
            max(retry_times) if all(t is not None for t in retry_times) else None
        )
        return {
            "reservation_id": reservation_id,
            "reserved": False,
            "target_buckets": items,
            "limited_buckets": limited,
            "retry_at": overall_retry_at,
            "retry_after_seconds": (
                None if overall_retry_at is None
                else max(0.0, overall_retry_at - now)
            ),
            "decided_at": now,
        }

    # 全部通过：同事务写入带 reservation_id 标记的整条占用贡献链
    _insert_contribution_events(
        conn, reservation_id, contributions, now, reservation_id
    )
    return {
        "reservation_id": reservation_id,
        "reserved": True,
        "buckets": items,
        "events_reserved": sum(
            1 for parts in contributions.values()
            for flag in (parts["direct"], parts["rollup"]) if flag
        ),
        "expires_at": expires_at,
        "reserved_at": now,
    }


@app.post("/v1/quota/reservations/{reservation_id}/confirm")
def confirm_reservation(reservation_id: str, req: ReservationConfirmRequest):
    """确认生效预留：占用转为正式消费，**不二次扣量**。

    在单个 ``BEGIN IMMEDIATE`` 事务内把预留状态从 ``reserved`` 条件更新为
    ``confirmed``、把其整条占用事件的 ``reservation_id`` 置空（转为正式
    消费事件，时间戳沿用创建时刻）、写入与 consume 同形的固化判定
    （此后释放/调整/幂等重放均按普通消费处理），并在同一事务追加一条
    ``reserve/confirmed`` 审计记录。

    - 200：确认成功（或同 confirm_id + 同目标重放，原样返回并带
      X-Idempotent-Replay: true）；
    - 404：预留不存在；
    - 409：预留已确认/已取消/已过期、confirm_id 已用于其他预留、或并发
      竞争落败（终态唯一，幂等键可原样重放自己的结果）。
    """
    now = time.time()

    conn = connect()
    replayed = False
    try:
        # 先对目标预留做一次定向惰性释放；主事务内还会带条件复查，
        # 关闭两次操作之间的竞争窗口
        _expire_reservations(conn, now, reservation_id)
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT reservation_id, result FROM reservation_confirmations "
                "WHERE confirm_id = ?",
                (req.confirm_id,),
            ).fetchone()
            if existing is not None:
                if existing["reservation_id"] != reservation_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"confirm_id '{req.confirm_id}' 已用于确认预留 "
                            f"'{existing['reservation_id']}'，不可改指其他预留"
                        ),
                    )
                result = json.loads(existing["result"])
                replayed = True
                conn.execute("COMMIT")
            else:
                result = _confirm_reservation(conn, reservation_id, now)
                conn.execute(
                    "INSERT INTO reservation_confirmations "
                    "(confirm_id, reservation_id, result, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (req.confirm_id, reservation_id, json.dumps(result), now),
                )
                # 首次成功确认在同一事务追加审计；失败（404/409）整体
                # 回滚、重放走已存在分支，均不记录
                audit.append_event(
                    conn,
                    op=audit.OP_RESERVE,
                    business_id=reservation_id,
                    result=audit.RESULT_CONFIRMED,
                    occurred_at=now,
                )
                conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _confirm_reservation(
    conn: sqlite3.Connection, reservation_id: str, now: float
) -> dict:
    """事务内把生效预留条件转为 confirmed 并把占用事件转正（不二次扣量）。"""
    row = conn.execute(
        "SELECT status, expires_at, created_at FROM reservations "
        "WHERE reservation_id = ?",
        (reservation_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"预留 '{reservation_id}' 不存在，无法确认",
        )

    if row["status"] != "reserved":
        raise HTTPException(
            status_code=409,
            detail=(
                f"预留 '{reservation_id}' 已处于终态 '{row['status']}'，"
                "确认、取消与过期清理只能有一个终态"
            ),
        )

    # 与惰性过期清理的竞争：在本事务内带条件复查，过期则回滚本次确认
    # （过期释放已由后续相关操作/重试完成），调用方收到 409
    if row["expires_at"] <= now:
        raise HTTPException(
            status_code=409,
            detail=(
                f"预留 '{reservation_id}' 已过期（expires_at={row['expires_at']}），"
                "不可确认"
            ),
        )

    # 条件 UPDATE 抢占唯一终态：并发确认/取消/过期清理仅一个 rowcount=1
    cur = conn.execute(
        "UPDATE reservations SET status = 'confirmed', confirmed_at = ?, "
        "ended_at = ? WHERE reservation_id = ? AND status = 'reserved' "
        "AND expires_at > ?",
        (now, now, reservation_id, now),
    )
    if cur.rowcount != 1:  # pragma: no cover - 前置检查已覆盖，防御性保留
        raise HTTPException(
            status_code=409,
            detail=f"预留 '{reservation_id}' 已被并发操作转入其他终态",
        )

    # 占用事件原地转正为正式消费：仅置空 reservation_id，金额与
    # created_at（创建时刻）不变——容量早已占用，确认不二次扣量
    events = conn.execute(
        "SELECT bucket_key, amount, direct FROM usage_events "
        "WHERE reservation_id = ?",
        (reservation_id,),
    ).fetchall()
    conn.execute(
        "UPDATE usage_events SET reservation_id = NULL WHERE reservation_id = ?",
        (reservation_id,),
    )
    events_committed = len(events)
    items = merge_items(
        [
            {"key": e["bucket_key"], "amount": e["amount"]}
            for e in events
            if e["direct"] == 1
        ]
    )
    created_at = row["created_at"]

    # 写入与 consume 同形的固化判定：确认后的预留作为普通消费可被
    # releases/adjustments 处理；同 reservation_id 的 consume 重放命中
    # 该固化判定，不会重复扣减
    decision = {
        "request_id": reservation_id,
        "allowed": True,
        "granted": items,
        "decided_at": created_at,
    }
    payload_json = json.dumps(
        {"request_id": reservation_id, "buckets": items}, sort_keys=True
    )
    conn.execute(
        "INSERT INTO requests "
        "(request_id, payload_hash, payload, decision, allowed, created_at) "
        "VALUES (?, ?, ?, ?, 1, ?)",
        (
            reservation_id,
            payload_hash(items),
            payload_json,
            json.dumps(decision),
            created_at,
        ),
    )

    return {
        "reservation_id": reservation_id,
        "confirmed": True,
        "buckets": items,
        "events_committed": events_committed,
        "confirmed_at": now,
    }


@app.post("/v1/quota/reservations/{reservation_id}/cancel")
def cancel_reservation(reservation_id: str, req: ReservationCancelRequest):
    """取消生效预留：在单事务内释放整条贡献链。

    - 200：取消成功（或同 cancel_id + 同目标重放，原样返回并带
      X-Idempotent-Replay: true）；
    - 404：预留不存在；
    - 409：预留已确认/已取消/已过期、cancel_id 已用于其他预留、或并发
      竞争落败（确认、取消与过期清理只能有一个终态）。
    """
    now = time.time()

    conn = connect()
    replayed = False
    try:
        _expire_reservations(conn, now, reservation_id)
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT reservation_id, result FROM reservation_cancellations "
                "WHERE cancel_id = ?",
                (req.cancel_id,),
            ).fetchone()
            if existing is not None:
                if existing["reservation_id"] != reservation_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"cancel_id '{req.cancel_id}' 已用于取消预留 "
                            f"'{existing['reservation_id']}'，不可改指其他预留"
                        ),
                    )
                result = json.loads(existing["result"])
                replayed = True
                conn.execute("COMMIT")
            else:
                result = _cancel_reservation(conn, reservation_id, now)
                conn.execute(
                    "INSERT INTO reservation_cancellations "
                    "(cancel_id, reservation_id, result, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (req.cancel_id, reservation_id, json.dumps(result), now),
                )
                audit.append_event(
                    conn,
                    op=audit.OP_RESERVE,
                    business_id=reservation_id,
                    result=audit.RESULT_CANCELLED,
                    occurred_at=now,
                )
                conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _cancel_reservation(
    conn: sqlite3.Connection, reservation_id: str, now: float
) -> dict:
    """事务内条件转为 cancelled 并删除整条占用贡献链。"""
    row = conn.execute(
        "SELECT status, expires_at FROM reservations WHERE reservation_id = ?",
        (reservation_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"预留 '{reservation_id}' 不存在，无法取消",
        )

    if row["status"] != "reserved":
        raise HTTPException(
            status_code=409,
            detail=(
                f"预留 '{reservation_id}' 已处于终态 '{row['status']}'，"
                "确认、取消与过期清理只能有一个终态"
            ),
        )

    if row["expires_at"] <= now:
        raise HTTPException(
            status_code=409,
            detail=(
                f"预留 '{reservation_id}' 已过期（expires_at={row['expires_at']}），"
                "终态由过期清理决定"
            ),
        )

    # 先抢占唯一终态，再删除整条贡献链：与确认/过期清理互斥
    cur = conn.execute(
        "UPDATE reservations SET status = 'cancelled', ended_at = ? "
        "WHERE reservation_id = ? AND status = 'reserved' AND expires_at > ?",
        (now, reservation_id, now),
    )
    if cur.rowcount != 1:  # pragma: no cover - 前置检查已覆盖，防御性保留
        raise HTTPException(
            status_code=409,
            detail=f"预留 '{reservation_id}' 已被并发操作转入其他终态",
        )

    # 汇总被释放的直接桶（direct=1）用于响应；祖先汇总事件随整条链删除
    direct_rows = conn.execute(
        "SELECT bucket_key, amount FROM usage_events "
        "WHERE reservation_id = ? AND direct = 1",
        (reservation_id,),
    ).fetchall()
    released_buckets = merge_items(
        [{"key": r["bucket_key"], "amount": r["amount"]} for r in direct_rows]
    )
    deleted = conn.execute(
        "DELETE FROM usage_events WHERE reservation_id = ?",
        (reservation_id,),
    ).rowcount

    return {
        "reservation_id": reservation_id,
        "cancelled": True,
        "released_buckets": released_buckets,
        "events_reverted": deleted,
        "cancelled_at": now,
    }


# ---------------------------------------------------------------- 回放模拟

@app.post("/v1/quota/simulations")
def simulate_quota(req: SimulationRequest):
    """在隔离内存中回放历史消费，评估候选桶配置的判定变化。

    给定 epoch 秒区间 ``[from, to)`` 与若干桶的候选
    ``limit/window_seconds``（未覆盖桶沿用当前配置），从 ``from`` 向前
    取各桶生效窗口最大值作为预热期，按 ``(created_at, request_id)``
    稳定顺序读取**首次消费**固化载荷重放滚动过期与多桶全放行/全拒绝。
    仅回放消费、不处理 release；全程只读，不写数据库也不追加审计链。

    - 200：返回实际判定 → 模拟判定的转移计数、每桶模拟峰值与前 100 条
      判定翻转明细；
    - 422：未知桶、候选桶重复、``from >= to`` 或跨度超过 7 天。
    """
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, limit_amount, window_seconds, parent_key FROM buckets"
        ).fetchall()
        db_configs = {
            r["key"]: (r["limit_amount"], float(r["window_seconds"])) for r in rows
        }
        parents = {r["key"]: r["parent_key"] for r in rows}
        overrides = [o.model_dump() for o in req.buckets]
        try:
            override_cfg = simulation.validate_request(
                req.from_, req.to, overrides, set(db_configs)
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        effective = simulation.resolve_configs(db_configs, override_cfg)
        warmup = simulation.warmup_seconds(effective)

        # 预热起点（含）：[from - max_window, to)；只读、不开写事务
        history = conn.execute(
            """
            SELECT created_at, request_id, payload, allowed
              FROM requests
             WHERE created_at >= ? AND created_at < ?
             ORDER BY created_at ASC, request_id ASC
            """,
            (req.from_ - warmup, float(req.to)),
        ).fetchall()
    finally:
        conn.close()

    records = []
    for r in history:
        items = merge_items(json.loads(r["payload"])["buckets"])
        # 历史载荷引用的桶必须仍有生效配置（防御已删除桶的陈旧固化记录）
        missing = [it["key"] for it in items if it["key"] not in effective]
        if missing:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"历史消费 {r['request_id']} 引用了未知/已删除桶 "
                    f"{sorted(missing)}，无法回放"
                ),
            )
        records.append((r["created_at"], r["request_id"], items,
                        bool(r["allowed"])))
    result = simulation.run_simulation(
        records, effective, float(req.from_), parents
    )

    return {
        "from": req.from_,
        "to": req.to,
        "warmup_seconds": warmup,
        "effective_buckets": [
            {"key": k, "limit": lim, "window_seconds": win}
            for k, (lim, win) in sorted(effective.items())
        ],
        "transitions": result["transitions"],
        "bucket_peaks": [
            {"key": k, "simulated_peak": peak}
            for k, peak in result["bucket_peaks"].items()
        ],
        "flip_details": result["flips"],
        "flip_details_truncated": result["flip_truncated"],
        "warmup_records": result["warmup_records"],
        "records_replayed": result["records_replayed"],
    }


# ---------------------------------------------------------------- 哈希审计链

@app.get("/v1/audit")
def get_audit(
    after_seq: int = Query(0, ge=0, description="仅返回 seq 大于该值的记录"),
    limit: int = Query(100, ge=1, le=1000, description="单次返回的最大记录数"),
):
    """按 seq 升序返回审计记录及当前链头。"""
    conn = connect()
    try:
        records, head_seq, head_hash = audit.list_events(conn, after_seq, limit)
    finally:
        conn.close()
    return {
        "records": records,
        "head": {"seq": head_seq, "hash": head_hash},
        "next_after_seq": records[-1]["seq"] if records else after_seq,
    }


@app.get("/v1/audit/verify")
def verify_audit():
    """全链重算并返回校验结果。

    - valid：链是否完整无误；
    - checked_count：校验的记录条数；
    - head_hash：当前链头哈希（空链为 64 个 0）；
    - first_invalid_seq：首个失配记录的 seq，无失配时为 null。
    """
    conn = connect()
    try:
        return audit.verify_chain(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------- 桶管理

@app.put("/v1/buckets/{key}", status_code=200)
def upsert_bucket(key: str, cfg: BucketConfig):
    """创建或更新桶的限额、窗口与可选父桶（parent_key）。

    父级校验在单个 ``BEGIN IMMEDIATE`` 事务、按**改动生效后**的配额树
    完成：parent_key 缺失父桶 / 指向自身 / 形成任何直接或间接环时整体
    回滚并返回 422，已有配置（含旧 parent_key）不发生变化。省略或显式
    传 null 都表示根桶（整体替换语义）。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 自引用先于外键检查给出明确错误
            if cfg.parent_key == key:
                raise HTTPException(
                    status_code=422,
                    detail=f"桶 '{key}' 的 parent_key 不能指向自身",
                )
            # 外键约束在 INSERT/UPDATE 时立即生效：缺失父桶须在写入前
            # 拒绝（BEGIN IMMEDIATE 已串行化，检查与写入之间无并发分叉）
            if cfg.parent_key is not None:
                exists = conn.execute(
                    "SELECT 1 FROM buckets WHERE key = ?", (cfg.parent_key,)
                ).fetchone()
                if exists is None:
                    raise HTTPException(
                        status_code=422,
                        detail=(
                            f"父桶 '{cfg.parent_key}' 不存在，请先创建该桶后"
                            "再配置 parent_key"
                        ),
                    )
            conn.execute(
                """
                INSERT INTO buckets (key, limit_amount, window_seconds, parent_key)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (key) DO UPDATE SET
                    limit_amount   = excluded.limit_amount,
                    window_seconds = excluded.window_seconds,
                    parent_key     = excluded.parent_key
                """,
                (key, cfg.limit, cfg.window_seconds, cfg.parent_key),
            )
            # 在最终图上复核：拒绝任何直接或间接环（此时 self/缺失父桶
            # 已在写入前拦截）
            rows = conn.execute(
                "SELECT key, parent_key FROM buckets"
            ).fetchall()
            parents = {r["key"]: r["parent_key"] for r in rows}
            try:
                validate_parent_assignment(key, cfg.parent_key, parents)
            except ParentError as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()
    return {
        "key": key,
        "limit": cfg.limit,
        "window_seconds": cfg.window_seconds,
        "parent_key": cfg.parent_key,
    }


@app.get("/v1/buckets")
def list_buckets():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, limit_amount, window_seconds, parent_key "
            "FROM buckets ORDER BY key"
        ).fetchall()
    finally:
        conn.close()
    return {
        "buckets": [
            {
                "key": r["key"],
                "limit": r["limit_amount"],
                "window_seconds": r["window_seconds"],
                "parent_key": r["parent_key"],
            }
            for r in rows
        ]
    }


@app.get("/v1/buckets/{key}/usage")
def bucket_usage(key: str):
    """查询桶当前滚动窗口用量。

    - direct_used：仅由直接点名本桶的请求贡献（direct=1 事件）；
    - total_used：本桶全部用量（直接 + 后代沿父链的汇总）；
    - used：total_used 的历史别名，保持向后兼容；
    - remaining：按 total_used 相对限额计算的剩余额度。
    """
    now = time.time()
    conn = connect()
    try:
        bucket = conn.execute(
            "SELECT limit_amount, window_seconds, parent_key "
            "FROM buckets WHERE key = ?",
            (key,),
        ).fetchone()
        if bucket is None:
            raise HTTPException(status_code=404, detail=f"桶 '{key}' 未配置")
        window = bucket["window_seconds"]
        rows = conn.execute(
            "SELECT created_at, amount, direct, reservation_id FROM usage_events "
            "WHERE bucket_key = ? AND created_at > ? "
            "ORDER BY created_at",
            (key, now - window),
        ).fetchall()
    finally:
        conn.close()

    direct_used = sum(r["amount"] for r in rows if r["direct"] == 1)
    total_used = sum(r["amount"] for r in rows)
    # 生效预留的占用（含本桶直接预留与后代预留上卷）：已计入 total_used，
    # 单列以便区分正式消费与预留占用；过期预留在相关操作中惰性释放
    reserved_used = sum(
        r["amount"] for r in rows if r["reservation_id"] is not None
    )
    oldest = rows[0]["created_at"] if rows else None
    return {
        "key": key,
        "limit": bucket["limit_amount"],
        "window_seconds": window,
        "parent_key": bucket["parent_key"],
        "used": total_used,
        "direct_used": direct_used,
        "total_used": total_used,
        "reserved_used": reserved_used,
        "remaining": max(0, bucket["limit_amount"] - total_used),
        "window_start": now - window,
        "as_of": now,
        "oldest_event_expires_at": None if oldest is None else oldest + window,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
