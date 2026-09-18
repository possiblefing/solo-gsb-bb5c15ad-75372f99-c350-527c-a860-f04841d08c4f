"""SQLite 连接与 schema 初始化。

设计要点：
- WAL 模式：读不阻塞写，写之间通过 BEGIN IMMEDIATE 串行化；
- busy_timeout：并发写事务排队等待，避免 "database is locked" 直接失败；
- isolation_level=None（自动提交），事务边界由代码显式控制。
"""
from __future__ import annotations

import json
import os
import sqlite3

from . import config, quota

SCHEMA = """
-- 配额树：parent_key 指向父桶（可空 = 根桶）。数据库层允许自引用/环，
-- 自引用、缺失父桶与直接/间接环一律由应用层在写事务内拒绝。
CREATE TABLE IF NOT EXISTS buckets (
    key             TEXT PRIMARY KEY,
    limit_amount    INTEGER NOT NULL CHECK (limit_amount > 0),
    window_seconds  REAL    NOT NULL CHECK (window_seconds > 0),
    parent_key      TEXT REFERENCES buckets (key)
);

-- 滚动窗口用量事件：每次放行（含预留占用与确认转正）按"贡献链"写入，
-- 窗口内求和即为当前用量。
-- direct=1：请求直接点名本桶的贡献；direct=0：后代桶消费沿父链向上
-- 汇总到本桶的贡献（同一请求对同一祖先至多一条汇总，不重复生成）。
-- reservation_id 非空表示该事件由某条**生效中**（status='reserved'）的
-- 预留占用；确认时在原事务内置空转为正式消费，取消/惰性过期时随整条
-- 贡献链删除。正式消费（含确认转正后的事件）该列为 NULL。
CREATE TABLE IF NOT EXISTS usage_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_key      TEXT    NOT NULL REFERENCES buckets (key),
    request_id      TEXT    NOT NULL,
    amount          INTEGER NOT NULL CHECK (amount > 0),
    created_at      REAL    NOT NULL,
    direct          INTEGER NOT NULL DEFAULT 1 CHECK (direct IN (0, 1)),
    reservation_id  TEXT
);
CREATE INDEX IF NOT EXISTS idx_usage_events_bucket_ts
    ON usage_events (bucket_key, created_at);
-- idx_usage_events_reservation（reservation_id 列）在迁移后创建，
-- 以便从更早版本（无该列）增量升级的旧库也能建索引：见 init_db 末尾
-- _POST_MIGRATION_INDEXES。

-- 幂等表：request_id 首次出现时固化载荷哈希与判定结果
-- release_id 非空表示该消费已被对应释放单成功释放
CREATE TABLE IF NOT EXISTS requests (
    request_id   TEXT PRIMARY KEY,
    payload_hash TEXT    NOT NULL,
    payload      TEXT    NOT NULL,
    decision     TEXT    NOT NULL,
    allowed      INTEGER NOT NULL,
    release_id   TEXT,
    created_at   REAL    NOT NULL
);

-- 释放幂等表：release_id 首次成功释放时固化目标 request_id 与释放结果
CREATE TABLE IF NOT EXISTS releases (
    release_id  TEXT PRIMARY KEY,
    request_id  TEXT    NOT NULL,
    result      TEXT    NOT NULL,
    created_at  REAL    NOT NULL
);

-- 调整幂等表：adjustment_id 首次成功调整时固化完整载荷哈希与调整结果。
-- 调整语义为"替换"：成功后该请求对各桶的用量贡献以 payload 中的
-- 目标桶列表为准（原贡献事件已在同事务中被原子删除替换）。
CREATE TABLE IF NOT EXISTS adjustments (
    adjustment_id TEXT PRIMARY KEY,
    request_id    TEXT    NOT NULL,
    payload_hash  TEXT    NOT NULL,
    payload       TEXT    NOT NULL,
    result        TEXT    NOT NULL,
    created_at    REAL    NOT NULL
);

-- 预留生命周期表：创建预留即在单个写事务内按当前配额树展开整条贡献链
-- 并立即以 usage_events.reservation_id = reservation_id 占用容量。
-- status：reserved（生效中）/ confirmed（已确认，占用转为正式消费）/
--         cancelled（已取消，整条贡献链已删除）/
--         expired（已过期惰性释放，整条贡献链已删除）。
-- 后三者为互斥终态：由 BEGIN IMMEDIATE 串行化 + 条件 UPDATE
-- （WHERE status='reserved' AND expires_at > now）保证确认、取消与
-- 惰性过期清理并发时只有一个终态。
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    payload_hash   TEXT    NOT NULL,
    payload        TEXT    NOT NULL,
    status         TEXT    NOT NULL
                   CHECK (status IN ('reserved', 'confirmed',
                                     'cancelled', 'expired')),
    expires_at     REAL    NOT NULL,
    created_at     REAL    NOT NULL,
    confirmed_at   REAL,
    ended_at       REAL
);
CREATE INDEX IF NOT EXISTS idx_reservations_expiry
    ON reservations (status, expires_at);

-- 确认幂等表：confirm_id 首次成功确认时固化目标预留与确认结果。
-- 同 confirm_id 同目标重放原样返回，改指其他预留返回 409。
CREATE TABLE IF NOT EXISTS reservation_confirmations (
    confirm_id     TEXT PRIMARY KEY,
    reservation_id TEXT    NOT NULL,
    result         TEXT    NOT NULL,
    created_at     REAL    NOT NULL
);

-- 取消幂等表：cancel_id 首次成功取消时固化目标预留与取消结果。
-- 同 cancel_id 同目标重放原样返回，改指其他预留返回 409。
CREATE TABLE IF NOT EXISTS reservation_cancellations (
    cancel_id      TEXT PRIMARY KEY,
    reservation_id TEXT    NOT NULL,
    result         TEXT    NOT NULL,
    created_at     REAL    NOT NULL
);

-- 哈希审计链：仅追加。seq 全局递增，首条 prev_hash 为 64 个 0，
-- hash = SHA-256(prev_hash 的原始32字节 + canonical JSON 事件)
CREATE TABLE IF NOT EXISTS audit_log (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    prev_hash    TEXT    NOT NULL,
    hash         TEXT    NOT NULL,
    op           TEXT    NOT NULL,
    business_id  TEXT    NOT NULL,
    result       TEXT    NOT NULL,
    event_json   TEXT    NOT NULL,
    occurred_at  REAL    NOT NULL
);
"""

# 旧库增量迁移：为已存在的表补充新列（SQLite 无 ADD COLUMN IF NOT EXISTS）
_MIGRATIONS = (
    (
        "requests",
        "release_id",
        "ALTER TABLE requests ADD COLUMN release_id TEXT",
    ),
    (
        "buckets",
        "parent_key",
        "ALTER TABLE buckets ADD COLUMN parent_key TEXT REFERENCES buckets (key)",
    ),
    (
        "usage_events",
        "direct",
        "ALTER TABLE usage_events ADD COLUMN direct INTEGER NOT NULL DEFAULT 1",
    ),
    (
        "usage_events",
        "reservation_id",
        "ALTER TABLE usage_events ADD COLUMN reservation_id TEXT",
    ),
)


# 依赖迁移后新列的索引（CREATE INDEX IF NOT EXISTS 在旧库升级时也安全）：
# 取消/过期按预留删除整条贡献链；确认按预留把事件置为正式消费
_POST_MIGRATION_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_usage_events_reservation "
    "ON usage_events (reservation_id)",
)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or config.DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={config.BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str | None = None) -> None:
    path = db_path or config.DB_PATH
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        _apply_migrations(conn)
        for ddl in _POST_MIGRATION_INDEXES:
            conn.execute(ddl)
    finally:
        conn.close()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """对旧版本数据库执行增量列迁移（SQLite 无 ADD COLUMN IF NOT EXISTS）。"""
    for table, column, ddl in _MIGRATIONS:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(ddl)


def seed_buckets(config_path: str, db_path: str | None = None) -> list[str]:
    """从 JSON 文件播种桶配置；同名桶按文件内容更新（配置文件即事实源）。

    文件内桶可按任意顺序声明 ``parent_key``（父桶在文件中后出现亦可）。
    全部写入在单个 ``BEGIN IMMEDIATE`` 事务内完成：缺失父桶、自引用或
    任何直接/间接环都会导致整批播种回滚（含 parent_key 的更新），
    已存在桶的旧配置不会被部分修改。
    """
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    entries = raw["buckets"] if isinstance(raw, dict) else raw

    conn = connect(db_path)
    seeded: list[str] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = {
                r["key"]: r["parent_key"]
                for r in conn.execute("SELECT key, parent_key FROM buckets")
            }
            file_parents = {e["key"]: e.get("parent_key") for e in entries}

            # 写入前在"文件覆盖后的最终图"上统一校验：缺失父桶 /
            # 自引用 / 直接或间接环（文件内可乱序引用，故不可在逐条
            # 插入时判断）
            final_parents = {**existing, **file_parents}
            try:
                quota.validate_parent_graph(final_parents)
            except quota.ParentError as exc:
                raise ValueError(str(exc)) from exc

            # 外键约束逐条生效：按最终图的父子依赖做拓扑排序后插入，
            # 文件内父桶后出现、或把已存在桶改挂到本文件新父桶下时都能
            # 在同一事务内完成（成环已在上面的校验中被拒绝，排序必收敛）
            ordered = _topo_order(entries, file_parents, existing)
            for e in ordered:
                _upsert_seed_bucket(conn, e)
                seeded.append(e["key"])
            conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()
    return seeded


def _topo_order(
    entries: list[dict],
    file_parents: dict[str, str | None],
    existing: dict[str, str | None],
) -> list[dict]:
    """父桶先于子桶排列文件条目：父在库中已存在则不约束顺序。

    成环（含文件内自引用）已由 ``validate_parent_graph`` 预先拒绝，
    因此循环必然收敛。
    """
    by_key = {e["key"]: e for e in entries}
    result: list[dict] = []
    placed: set[str] = set()

    def place(key: str, stack: set[str]) -> None:
        if key in placed or key not in by_key:
            return
        if key in stack:  # pragma: no cover - 已被环校验拦截
            raise ValueError(f"桶配置父子依赖无法解析（可能成环）：{key}")
        stack.add(key)
        parent = file_parents.get(key)
        # 父桶同样来自本文件时须先落库；父桶已在库中则无此约束
        if parent is not None and parent in by_key:
            place(parent, stack)
        stack.discard(key)
        placed.add(key)
        result.append(by_key[key])

    for e in entries:
        place(e["key"], set())
    return result


def _upsert_seed_bucket(conn: sqlite3.Connection, e: dict) -> None:
    conn.execute(
        """
        INSERT INTO buckets (key, limit_amount, window_seconds, parent_key)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (key) DO UPDATE SET
            limit_amount   = excluded.limit_amount,
            window_seconds = excluded.window_seconds,
            parent_key     = excluded.parent_key
        """,
        (e["key"], int(e["limit"]), float(e["window_seconds"]), e.get("parent_key")),
    )


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
