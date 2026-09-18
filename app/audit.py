"""可校验哈希审计链（append-only）。

链规则：
- 记录使用全局递增 ``seq``（SQLite AUTOINCREMENT）；
- 首条记录 ``prev_hash`` 为 64 个 ``'0'``，其后每条引用上一条的 ``hash``；
- ``hash = SHA-256(prev_hash 的原始 32 字节 + UTF-8 canonical JSON 事件)``；
- 事件 canonical JSON 使用键排序、无多余空白（``sort_keys`` + 紧凑分隔符），
  字段固定为 ``op``（操作类型）/ ``business_id``（业务 ID）/
  ``result``（结果）/ ``occurred_at``（发生时间, epoch 秒）。

``append_event`` 必须由调用方在已开启的写事务（BEGIN IMMEDIATE）内调用：
审计记录与业务变更同事务提交、同失败回滚；写事务在数据库级串行化，
因此链头读取与追加之间不存在并发分叉。幂等重放走各自的独立分支、
不调用本函数，故重放不会重复记录。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

# 创世前序哈希：首条记录的 prev_hash
GENESIS_HASH = "0" * 64

OP_CONSUME = "consume"
OP_RELEASE = "release"
OP_ADJUST = "adjust"

RESULT_ALLOWED = "allowed"
RESULT_DENIED = "denied"
RESULT_RELEASED = "released"
RESULT_ADJUSTED = "adjusted"


def canonical_event_json(event: dict[str, Any]) -> str:
    """事件的 canonical JSON：键排序、无多余空白。"""
    return json.dumps(
        event, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def compute_hash(prev_hash: str, event_json: str) -> str:
    """SHA-256(prev_hash 的原始 32 字节 + UTF-8 canonical JSON 事件)。"""
    return hashlib.sha256(
        bytes.fromhex(prev_hash) + event_json.encode("utf-8")
    ).hexdigest()


def append_event(
    conn: sqlite3.Connection,
    *,
    op: str,
    business_id: str,
    result: str,
    occurred_at: float,
) -> dict[str, Any]:
    """在当前写事务内追加一条审计记录，返回刚写入的记录（含 seq/hash）。

    调用方负责 BEGIN/COMMIT；本函数不自行开启事务。
    """
    head = conn.execute(
        "SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    prev_hash = head["hash"] if head is not None else GENESIS_HASH

    event = {
        "op": op,
        "business_id": business_id,
        "result": result,
        "occurred_at": occurred_at,
    }
    event_json = canonical_event_json(event)
    digest = compute_hash(prev_hash, event_json)

    cur = conn.execute(
        """
        INSERT INTO audit_log
            (prev_hash, hash, op, business_id, result, event_json, occurred_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (prev_hash, digest, op, business_id, result, event_json, occurred_at),
    )
    return {
        "seq": cur.lastrowid,
        "prev_hash": prev_hash,
        "hash": digest,
        "event": event,
    }


def list_events(
    conn: sqlite3.Connection, after_seq: int = 0, limit: int = 100
) -> tuple[list[dict[str, Any]], int, str]:
    """按 seq 升序返回 seq > after_seq 的记录，并给出当前链头。

    返回 (records, head_seq, head_hash)；空链链头为 (0, GENESIS_HASH)。
    """
    rows = conn.execute(
        """
        SELECT seq, prev_hash, hash, event_json
          FROM audit_log
         WHERE seq > ?
         ORDER BY seq ASC
         LIMIT ?
        """,
        (after_seq, limit),
    ).fetchall()
    records = [
        {
            "seq": r["seq"],
            "prev_hash": r["prev_hash"],
            "hash": r["hash"],
            "event": json.loads(r["event_json"]),
        }
        for r in rows
    ]

    head = conn.execute(
        "SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    if head is None:
        return records, 0, GENESIS_HASH
    return records, head["seq"], head["hash"]


def verify_chain(conn: sqlite3.Connection) -> dict[str, Any]:
    """全链重算，返回 valid / checked_count / head_hash / first_invalid_seq。

    逐条校验：seq 从 1 起连续递增、prev_hash 指向前一条 hash（首条指向
    64 个 0）、按规则重算出的 hash 与存储一致。任一不符即标记该 seq，
    first_invalid_seq 仅报告最早的一条。
    """
    rows = conn.execute(
        "SELECT seq, prev_hash, hash, event_json FROM audit_log ORDER BY seq"
    ).fetchall()

    expected_prev = GENESIS_HASH
    expected_seq = 1
    first_invalid_seq: int | None = None

    for row in rows:
        recomputed = compute_hash(row["prev_hash"], row["event_json"])
        if (
            first_invalid_seq is None
            and (
                row["seq"] != expected_seq
                or row["prev_hash"] != expected_prev
                or recomputed != row["hash"]
            )
        ):
            first_invalid_seq = row["seq"]
        expected_prev = row["hash"]
        expected_seq += 1

    return {
        "valid": first_invalid_seq is None,
        "checked_count": len(rows),
        "head_hash": rows[-1]["hash"] if rows else GENESIS_HASH,
        "first_invalid_seq": first_invalid_seq,
    }
