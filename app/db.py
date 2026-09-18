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

-- 滚动窗口用量事件：每次放行（含预留占用）按"贡献链"写入，窗口内求和即
-- 为当前用量。
-- direct=1：直接点名本桶的贡献；direct=0：后代桶消费沿父链向上
-- 汇总到本桶的贡献（同一请求/预留对同一祖先至多一条汇总，不重复生成）。
-- request_id 非空 = 已固化的正式消费（含已确认预留转正式）；
-- reservation_id 非空 = 预留持有中的容量占用（确认时转挂 request_id 并
-- 清空本列；取消/过期时整行删除），二者互斥。
CREATE TABLE IF NOT EXISTS usage_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_key      TEXT    NOT NULL REFERENCES buckets (key),
    request_id      TEXT,
    reservation_id  TEXT,
    amount          INTEGER NOT NULL CHECK (amount > 0),
    created_at      REAL    NOT NULL,
    direct          INTEGER NOT NULL DEFAULT 1 CHECK (direct IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_usage_events_bucket_ts
    ON usage_events (bucket_key, created_at);
-- reservation_id 列由迁移补齐旧库；其索引在迁移完成后再创建
-- （见 _apply_migrations），避免在缺列的旧表上直接建索引失败

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

-- 预留幂等/生命周期表：reservation_id 首次成功创建时固化完整载荷
-- （桶列表 + expires_in）与创建结果。状态机：
--   held（持有容量）→ confirmed（转为正式消费）/ cancelled（取消释放）
--                    / expired（惰性过期释放，后续操作内清扫）
-- 终态（confirmed/cancelled/expired）不可再迁移；创建重放原样返回。
-- request_id 为确认后转成的正式消费业务 ID（默认取 reservation_id），
-- 仅 confirmed 行非空，供 releases/adjustments 操作。
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    payload_hash   TEXT    NOT NULL,
    payload        TEXT    NOT NULL,
    status         TEXT    NOT NULL CHECK (
                        status IN ('held', 'confirmed', 'cancelled', 'expired')
                   ),
    request_id     TEXT,
    expires_in     REAL    NOT NULL,
    expires_at     REAL    NOT NULL,
    result         TEXT    NOT NULL,
    created_at     REAL    NOT NULL,
    finalized_at   REAL,
    -- 终态操作（确认/取消/过期）首次成功时固化的响应，供幂等重放原样返回
    terminal_result TEXT
);
CREATE INDEX IF NOT EXISTS idx_reservations_held_expiry
    ON reservations (status, expires_at);

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

# 旧库增量迁移：为已存在的表补充新列（SQLite 无 ADD COLUMN IF NOT EXISTS）。
# usage_events 需把 request_id 放宽为可空并新增 reservation_id：SQLite 无法
# 直接改列约束，故检测到旧结构后在单事务内按 12 步表重建流程整体迁移。
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
    finally:
        conn.close()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """对旧版本数据库执行增量列迁移（SQLite 无 ADD COLUMN IF NOT EXISTS）。"""
    for table, column, ddl in _MIGRATIONS:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(ddl)
    _migrate_usage_events(conn)


def _migrate_usage_events(conn: sqlite3.Connection) -> None:
    """旧库 usage_events 迁移：request_id 放宽为可空并新增 reservation_id。

    新列可由 ADD COLUMN 直接补入，但 ``request_id NOT NULL`` 约束无法就地
    修改；预留持有中的事件 request_id 须为 NULL，故检测到旧约束后按 SQLite
    推荐的表重建流程原子替换（外键临时关闭、单事务提交）。
    """
    cols = {r["name"]: r for r in conn.execute("PRAGMA table_info(usage_events)")}
    if not cols:
        return
    request_notnull = (
        bool(cols["request_id"]["notnull"]) if "request_id" in cols else False
    )
    # 旧结构：request_id 为 NOT NULL（无法就地放宽）或缺 direct 列——
    # 统一走表重建归一化到最新结构
    need_rebuild = request_notnull or "direct" not in cols
    if "reservation_id" not in cols and not need_rebuild:
        conn.execute(
            "ALTER TABLE usage_events ADD COLUMN reservation_id TEXT"
        )
    if not need_rebuild:
        # 新列已补且 request_id 本就可空（或刚由 SCHEMA 建成新表）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_usage_events_reservation "
            "ON usage_events (reservation_id)"
        )
        return

    # 极旧库可能同时缺 direct 列：拷贝时以默认值 1（直接消费）补齐
    has_direct = "direct" in cols
    direct_select = "direct" if has_direct else "1 AS direct"

    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN")
        conn.execute(
            """
            CREATE TABLE usage_events_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                bucket_key      TEXT    NOT NULL REFERENCES buckets (key),
                request_id      TEXT,
                reservation_id  TEXT,
                amount          INTEGER NOT NULL CHECK (amount > 0),
                created_at      REAL    NOT NULL,
                direct          INTEGER NOT NULL DEFAULT 1 CHECK (direct IN (0, 1))
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO usage_events_new
                (id, bucket_key, request_id, reservation_id, amount,
                 created_at, direct)
            SELECT id, bucket_key, request_id, NULL, amount, created_at,
                   {direct_select}
              FROM usage_events
            """
        )
        conn.execute("DROP TABLE usage_events")
        conn.execute("ALTER TABLE usage_events_new RENAME TO usage_events")
        conn.execute(
            "CREATE INDEX idx_usage_events_bucket_ts "
            "ON usage_events (bucket_key, created_at)"
        )
        conn.execute(
            "CREATE INDEX idx_usage_events_reservation "
            "ON usage_events (reservation_id)"
        )
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        # 重新开启外键（连接默认即 ON；PRAGMA 无事务生效）
        conn.execute("PRAGMA foreign_keys=ON")
        # 迁移后立即核对外键完整性（rebuild 期间外键被临时关闭）
        if conn.execute("PRAGMA foreign_key_check").fetchall():  # pragma: no cover
            raise sqlite3.IntegrityError("usage_events 迁移后外键校验失败")


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
