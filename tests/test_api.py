"""端到端测试：原子扣减、幂等、409、并发不超限、滚动窗口恢复、配置回放模拟。"""
import json
import os
import tempfile
import threading
import time
import uuid

# 必须在导入 app 之前设置环境（config 在导入时读取）
_TMP = tempfile.mkdtemp(prefix="quota-test-")
os.environ["QUOTA_DB_PATH"] = os.path.join(_TMP, "test.db")
os.environ["QUOTA_BUCKETS_CONFIG"] = os.path.join(_TMP, "buckets.json")

with open(os.environ["QUOTA_BUCKETS_CONFIG"], "w") as fh:
    json.dump(
        {"buckets": [{"key": "seeded", "limit": 100, "window_seconds": 60}]},
        fh,
    )

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

# 以上下文管理器方式启动，确保 lifespan（建表 + 播种桶配置）执行
_client_ctx = TestClient(app)
client = _client_ctx.__enter__()


def _rid() -> str:
    return uuid.uuid4().hex


def _bucket(limit: int, window: float) -> str:
    """为单个测试创建独立桶，避免测试间窗口互相干扰。"""
    key = f"b-{_rid()[:12]}"
    r = client.put(f"/v1/buckets/{key}", json={"limit": limit, "window_seconds": window})
    assert r.status_code == 200, r.text
    return key


def _consume(rid: str, key: str, amount: int):
    return client.post("/v1/quota/consume", json={
        "request_id": rid, "buckets": [{"key": key, "amount": amount}],
    })


def _release(rel_id: str, rid: str):
    return client.post("/v1/quota/releases", json={
        "release_id": rel_id, "request_id": rid,
    })


def test_seeded_bucket_and_consume():
    # 启动时从 QUOTA_BUCKETS_CONFIG 播种
    keys = [b["key"] for b in client.get("/v1/buckets").json()["buckets"]]
    assert "seeded" in keys

    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": "seeded", "amount": 30}],
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["allowed"] is True
    assert body["request_id"] == rid

    usage = client.get("/v1/buckets/seeded/usage").json()
    assert usage["used"] == 30
    assert usage["remaining"] == 70
    assert usage["limit"] == 100
    assert usage["window_seconds"] == 60


def test_atomic_all_or_nothing():
    """多桶请求中任一超限 → 全部不扣减。"""
    k1, k2 = _bucket(10, 60), _bucket(5, 60)
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(),
        "buckets": [{"key": k1, "amount": 1}, {"key": k2, "amount": 100}],
    })
    assert r.status_code == 429
    body = r.json()
    assert body["allowed"] is False
    assert [b["key"] for b in body["limited_buckets"]] == [k2]
    # 请求量本身超过限额 → 永远不可重试
    assert body["retry_at"] is None
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_denied_returns_retry_time():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    r = _consume(_rid(), key, 1)
    assert r.status_code == 429
    body = r.json()
    lb = body["limited_buckets"][0]
    assert lb["key"] == key
    assert lb["used"] == 2 and lb["requested"] == 1 and lb["limit"] == 2
    assert 0 < body["retry_after_seconds"] <= 60
    assert body["retry_at"] is not None
    assert "Retry-After" in r.headers


def test_idempotent_replay():
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 3}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.json() == r1.json()  # 原样返回固化判定
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    # 不得重复扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_idempotent_replay_of_denied_decision():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 429
    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 429
    assert r2.json() == r1.json()
    assert r2.headers.get("X-Idempotent-Replay") == "true"


def test_conflict_on_changed_payload():
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    r = _consume(rid, key, 2)
    assert r.status_code == 409


def test_same_amount_different_bucket_key_is_conflict():
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 1).status_code == 200
    assert _consume(rid, k2, 1).status_code == 409


def test_unknown_bucket_404():
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [{"key": "nope", "amount": 1}],
    })
    assert r.status_code == 404
    assert client.get("/v1/buckets/nope/usage").status_code == 404


def test_rolling_window_recovers():
    key = _bucket(2, 0.4)
    assert _consume(_rid(), key, 2).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.5)  # 窗口 0.4s，等待滚动过期
    assert _consume(_rid(), key, 1).status_code == 200


def test_rolling_window_partial_expiry():
    """窗口滚动时按事件过期逐步释放额度。"""
    key = _bucket(3, 0.6)
    assert _consume(_rid(), key, 2).status_code == 200
    time.sleep(0.35)
    assert _consume(_rid(), key, 1).status_code == 200  # 打满 3/3
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.35)  # 第一笔(2)过期，释放 2 额度
    assert _consume(_rid(), key, 2).status_code == 200


def test_concurrent_requests_never_exceed_limit():
    """50 个并发请求争抢限额 10 的桶：放行数必须恰好为 10。"""
    key = _bucket(10, 60)
    results = []

    def hit():
        results.append(_consume(_rid(), key, 1).status_code)

    threads = [threading.Thread(target=hit) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 10, f"放行 {results.count(200)} 次，超出限额 10"
    assert results.count(429) == 40
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 10


def test_same_request_id_concurrent_single_deduction():
    """同一 request_id 并发提交 10 次：只应扣减一次。"""
    key = _bucket(100, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    codes = []

    def hit():
        codes.append(client.post("/v1/quota/consume", json=payload).status_code)

    threads = [threading.Thread(target=hit) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_bucket_upsert_updates_config():
    key = _bucket(5, 60)
    r = client.put(f"/v1/buckets/{key}", json={"limit": 7, "window_seconds": 30})
    assert r.status_code == 200
    usage = client.get(f"/v1/buckets/{key}/usage").json()
    assert usage["limit"] == 7 and usage["window_seconds"] == 30


def test_validation_errors():
    key = _bucket(5, 60)
    # amount 必须为正
    assert _consume(_rid(), key, 0).status_code == 422
    # buckets 不能为空
    assert client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [],
    }).status_code == 422


# ---------------------------------------------------------------- 释放接口

def test_release_restores_all_buckets():
    """首次释放撤销对所有桶的用量贡献，额度恢复。"""
    k1, k2 = _bucket(10, 60), _bucket(20, 60)
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
    })
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 4
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 7

    rel = _rid()
    rr = _release(rel, rid)
    assert rr.status_code == 200, rr.text
    body = rr.json()
    assert body["released"] is True
    assert body["release_id"] == rel and body["request_id"] == rid
    assert body["events_reverted"] == 2
    # 载荷经 merge_items 规范化后按 key 排序
    assert sorted(body["released_buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
        key=lambda b: b["key"],
    )
    assert "X-Idempotent-Replay" not in rr.headers

    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_release_frees_capacity_for_subsequent_consume():
    """释放后新的消费可以使用恢复出的额度。"""
    key = _bucket(3, 60)
    rid1 = _rid()
    assert _consume(rid1, key, 3).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429

    assert _release(_rid(), rid1).status_code == 200
    r = _consume(_rid(), key, 3)
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_idempotent_replay():
    """相同 release_id + 相同 request_id 重放：原样返回，不再改动用量。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200
    rel = _rid()
    r1 = _release(rel, rid)
    assert r1.status_code == 200

    # 再制造一笔用量，重放不应影响它
    assert _consume(_rid(), key, 3).status_code == 200
    r2 = _release(rel, rid)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_retarget_conflict():
    """相同 release_id 改指其他 request_id → 409，且不影响任一目标。"""
    key = _bucket(10, 60)
    rid1, rid2 = _rid(), _rid()
    assert _consume(rid1, key, 2).status_code == 200
    assert _consume(rid2, key, 3).status_code == 200
    rel = _rid()
    assert _release(rel, rid1).status_code == 200

    r = _release(rel, rid2)
    assert r.status_code == 409
    # rid1 已释放（-2），rid2 未被释放，用量只剩 3
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_double_release_same_consume_conflict():
    """同一消费请求只能由一个 release_id 成功释放，第二个返回 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    assert _release(_rid(), rid).status_code == 200

    r = _release(_rid(), rid)
    assert r.status_code == 409
    # 用量不会被重复"撤销"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_unknown_request_404():
    r = _release(_rid(), f"missing-{_rid()}")
    assert r.status_code == 404


def test_release_denied_request_conflict():
    """原判定为拒绝（429）的消费不可释放 → 409。"""
    key = _bucket(1, 60)
    assert _consume(_rid(), key, 1).status_code == 200
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 429
    assert _release(_rid(), rid).status_code == 409
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_consume_replay_after_release_returns_frozen_result():
    """释放后重放原 consume：仍返回已固化的放行结果，且不再次扣减。"""
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 6}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    # 重放不得再次扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_atomic_restores_every_bucket():
    """释放是原子的：多桶用量全部恢复，不存在部分恢复。"""
    keys = [_bucket(5, 60) for _ in range(5)]
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k, "amount": 3} for k in keys],
    })
    assert r.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    for k in keys:
        assert client.get(f"/v1/buckets/{k}/usage").json()["used"] == 0


def test_concurrent_releases_only_one_wins():
    """不同 release_id 并发释放同一消费：仅一个 200，其余 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    outcomes: list[tuple[str, int]] = []
    barrier = threading.Barrier(20)

    def hit(rel: str):
        barrier.wait()
        outcomes.append((rel, _release(rel, rid).status_code))

    threads = [threading.Thread(target=hit, args=(_rid(),)) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert [c for _, c in outcomes].count(200) == 1
    assert [c for _, c in outcomes].count(409) == 19
    # 只删除了一次事件，用量为 0 而非负数
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_concurrent_same_release_id_single_result():
    """同一 release_id 并发释放同一消费：全部 200，只执行一次撤销。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    rel = _rid()
    codes = []
    barrier = threading.Barrier(15)

    def hit():
        barrier.wait()
        codes.append(_release(rel, rid).status_code)

    threads = [threading.Thread(target=hit) for _ in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_then_new_consume_replay_interaction():
    """释放后：旧 consume 重放不扣减，新 request_id 可正常占用恢复额度。"""
    key = _bucket(5, 60)
    rid_old = _rid()
    assert _consume(rid_old, key, 5).status_code == 200
    assert _release(_rid(), rid_old).status_code == 200

    # 旧单重放不占额度
    assert _consume(rid_old, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0
    # 新单可以打满恢复出的额度
    rid_new = _rid()
    assert _consume(rid_new, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5
    assert _consume(_rid(), key, 1).status_code == 429


def test_release_validation_errors():
    r = client.post("/v1/quota/releases", json={
        "release_id": "", "request_id": "x",
    })
    assert r.status_code == 422
    r = client.post("/v1/quota/releases", json={"release_id": "r1"})
    assert r.status_code == 422


# ---------------------------------------------------------------- 调整接口

def _adjust(aid: str, rid: str, buckets):
    return client.post("/v1/quota/adjustments", json={
        "adjustment_id": aid, "request_id": rid,
        "buckets": [{"key": k, "amount": a} for k, a in buckets],
    })


def test_adjust_moves_contribution_between_buckets():
    """调整把用量贡献从原桶移动到目标桶，原桶额度即刻恢复。"""
    k_src, k_dst = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k_src, 6).status_code == 200
    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 6
    assert client.get(f"/v1/buckets/{k_dst}/usage").json()["used"] == 0

    aid = _rid()
    r = _adjust(aid, rid, [(k_dst, 4)])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["adjusted"] is True
    assert body["adjustment_id"] == aid and body["request_id"] == rid
    assert body["previous_buckets"] == [{"key": k_src, "amount": 6}]
    assert body["buckets"] == [{"key": k_dst, "amount": 4}]
    assert body["events_replaced"] == 1
    assert "X-Idempotent-Replay" not in r.headers

    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k_dst}/usage").json()["used"] == 4


def test_adjust_multi_bucket_atomic_replace():
    """多桶贡献被目标多桶列表整体替换。"""
    k1, k2, k3, k4 = [_bucket(10, 60) for _ in range(4)]
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k1, "amount": 2}, {"key": k2, "amount": 3}],
    })
    assert r.status_code == 200

    rr = _adjust(_rid(), rid, [(k3, 1), (k4, 5), (k3, 1)])  # 同桶重复条目合并
    assert rr.status_code == 200, rr.text
    body = rr.json()
    assert sorted(body["previous_buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k1, "amount": 2}, {"key": k2, "amount": 3}], key=lambda b: b["key"]
    )
    assert sorted(body["buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k3, "amount": 2}, {"key": k4, "amount": 5}], key=lambda b: b["key"]
    )
    assert body["events_replaced"] == 2
    for k in (k1, k2):
        assert client.get(f"/v1/buckets/{k}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k3}/usage").json()["used"] == 2
    assert client.get(f"/v1/buckets/{k4}/usage").json()["used"] == 5


def test_adjust_same_bucket_amount_change():
    """目标桶即原桶时，仅替换贡献量。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 6).status_code == 200
    r = _adjust(_rid(), rid, [(key, 3)])
    assert r.status_code == 200
    assert r.json()["previous_buckets"] == [{"key": key, "amount": 6}]
    assert r.json()["buckets"] == [{"key": key, "amount": 3}]
    usage = client.get(f"/v1/buckets/{key}/usage").json()
    assert usage["used"] == 3 and usage["remaining"] == 7


def test_adjust_unknown_target_bucket_keeps_original():
    """目标桶未配置 → 404，原贡献完整保留。"""
    k1 = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 5).status_code == 200
    ghost = f"ghost-{_rid()[:10]}"
    r = _adjust(_rid(), rid, [(ghost, 1)])
    assert r.status_code == 404
    assert ghost in r.json()["detail"]
    # 原事件未被删除：用量仍为 5
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 5


def test_adjust_over_limit_429_keeps_original():
    """排除原贡献后目标桶仍超限 → 429，原贡献不变，返回受限桶与重试时间。"""
    k_src, k_dst = _bucket(10, 60), _bucket(5, 60)
    rid = _rid()
    assert _consume(rid, k_src, 4).status_code == 200
    # 其他请求在目标桶占用 4
    assert _consume(_rid(), k_dst, 4).status_code == 200

    r = _adjust(_rid(), rid, [(k_dst, 2)])
    assert r.status_code == 429
    body = r.json()
    assert body["adjusted"] is False
    assert [b["key"] for b in body["limited_buckets"]] == [k_dst]
    lb = body["limited_buckets"][0]
    assert lb["used"] == 4 and lb["requested"] == 2 and lb["limit"] == 5
    assert 0 < body["retry_after_seconds"] <= 60
    assert "Retry-After" in r.headers
    # 回滚：原桶贡献恢复，目标桶没有任何来自 rid 的事件
    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 4
    assert client.get(f"/v1/buckets/{k_dst}/usage").json()["used"] == 4


def test_adjust_request_amount_over_limit_retry_null():
    """目标请求量本身超过桶限额 → retry_at 为 null（与 consume 语义一致）。"""
    k_src, k_dst = _bucket(10, 60), _bucket(5, 60)
    rid = _rid()
    assert _consume(rid, k_src, 4).status_code == 200
    r = _adjust(_rid(), rid, [(k_dst, 99)])
    assert r.status_code == 429
    assert r.json()["retry_at"] is None
    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 4
    assert client.get(f"/v1/buckets/{k_dst}/usage").json()["used"] == 0


def test_adjust_original_contribution_excluded_from_check():
    """校验目标桶时原贡献已被排除：在同桶调小用量不应被自己挡住。"""
    key = _bucket(5, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200
    # 桶已满；若不排除原贡献，调小到 2 也会误判超限
    r = _adjust(_rid(), rid, [(key, 2)])
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 2


def test_adjust_frees_capacity_for_subsequent_consume():
    """调整移出原桶后，新的消费可使用恢复出的额度。"""
    k_src, k_dst = _bucket(5, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k_src, 5).status_code == 200
    assert _consume(_rid(), k_src, 1).status_code == 429
    assert _adjust(_rid(), rid, [(k_dst, 5)]).status_code == 200
    assert _consume(_rid(), k_src, 5).status_code == 200
    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 5
    assert client.get(f"/v1/buckets/{k_dst}/usage").json()["used"] == 5


def test_adjust_idempotent_replay():
    """同 adjustment_id + 同载荷重放：原样返回，不再改动用量。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 5).status_code == 200
    aid = _rid()
    payload = [(k2, 5)]
    r1 = _adjust(aid, rid, payload)
    assert r1.status_code == 200

    # 再制造一笔用量：重放不应影响它，也不应重复写事件
    assert _consume(_rid(), k2, 3).status_code == 200
    r2 = _adjust(aid, rid, payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 8


def test_adjust_changed_payload_conflict():
    """同 adjustment_id 改换载荷 → 409，且不落任何变更。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 2).status_code == 200
    aid = _rid()
    assert _adjust(aid, rid, [(k2, 2)]).status_code == 200

    r = _adjust(aid, rid, [(k2, 3)])
    assert r.status_code == 409
    # 首次调整结果保持不变
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 2


def test_adjust_same_id_retarget_other_request_conflict():
    """同 adjustment_id 改指其他 request_id → 409（即使目标桶列表完全相同）。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid1, rid2 = _rid(), _rid()
    assert _consume(rid1, k1, 2).status_code == 200
    assert _consume(rid2, k1, 3).status_code == 200
    aid = _rid()
    # 两笔消费都调整到完全相同的目标列表
    assert _adjust(aid, rid1, [(k2, 2)]).status_code == 200

    r = _adjust(aid, rid2, [(k2, 2)])
    assert r.status_code == 409
    # rid2 的贡献未被改动
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 3
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 2


def test_adjust_nonexistent_request_404():
    r = _adjust(_rid(), f"missing-{_rid()}", [("seeded", 1)])
    assert r.status_code == 404


def test_adjust_denied_request_conflict():
    """原判定为拒绝（429）的消费不可调整 → 409。"""
    key = _bucket(1, 60)
    assert _consume(_rid(), key, 1).status_code == 200
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 429
    other = _bucket(10, 60)
    r = _adjust(_rid(), rid, [(other, 1)])
    assert r.status_code == 409
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1
    assert client.get(f"/v1/buckets/{other}/usage").json()["used"] == 0


def test_adjust_released_request_conflict():
    """已释放的消费不可调整 → 409。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 4).status_code == 200
    assert _release(_rid(), rid).status_code == 200
    r = _adjust(_rid(), rid, [(k2, 4)])
    assert r.status_code == 409
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_adjust_can_be_repeated_with_new_id():
    """同一消费可被不同 adjustment_id 连续调整；校验基于当前（最新）贡献。"""
    k1, k2, k3 = _bucket(10, 60), _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 5).status_code == 200

    r1 = _adjust(_rid(), rid, [(k2, 5)])
    assert r1.status_code == 200
    r2 = _adjust(_rid(), rid, [(k3, 5)])
    assert r2.status_code == 200
    # 第二次调整排除的是现存的 k2 贡献，而非最初的 k1 载荷
    assert r2.json()["previous_buckets"] == [{"key": k2, "amount": 5}]
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k3}/usage").json()["used"] == 5


def test_adjust_retry_after_capacity_frees_then_succeeds():
    """429 不固化调整；额度释放后同 adjustment_id 可成功。"""
    k_src, k_dst = _bucket(10, 60), _bucket(5, 60)
    rid = _rid()
    assert _consume(rid, k_src, 4).status_code == 200
    blocker = _rid()
    assert _consume(blocker, k_dst, 4).status_code == 200
    aid = _rid()
    assert _adjust(aid, rid, [(k_dst, 2)]).status_code == 429
    # 原贡献仍在
    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 4

    assert _release(_rid(), blocker).status_code == 200
    r = _adjust(aid, rid, [(k_dst, 2)])
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{k_src}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k_dst}/usage").json()["used"] == 2


def test_adjust_then_release_reverts_adjusted_contribution():
    """调整之后释放：撤销的是调整后的贡献，返回的也是调整后的桶列表。"""
    k1, k2, k3 = _bucket(10, 60), _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
    }).status_code == 200
    assert _adjust(_rid(), rid, [(k3, 2), (k1, 1)]).status_code == 200
    # 现存贡献：k1=1, k3=2（k2 已归零）
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0

    rr = _release(_rid(), rid)
    assert rr.status_code == 200, rr.text
    body = rr.json()
    assert sorted(body["released_buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k1, "amount": 1}, {"key": k3, "amount": 2}], key=lambda b: b["key"]
    )
    assert body["events_reverted"] == 2
    for k in (k1, k2, k3):
        assert client.get(f"/v1/buckets/{k}/usage").json()["used"] == 0


def test_consume_replay_after_adjust_returns_frozen_without_rededuction():
    """调整后重放原 consume：仍返回首次判定，且不重新扣减原桶。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": k1, "amount": 5}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200
    assert _adjust(_rid(), rid, [(k2, 5)]).status_code == 200
    # 原桶被新请求占满：若重放重新扣减必然超限
    assert _consume(_rid(), k1, 10).status_code == 200

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 10
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 5


def test_adjust_validation_errors():
    key = _bucket(5, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    # adjustment_id 为空
    assert client.post("/v1/quota/adjustments", json={
        "adjustment_id": "", "request_id": rid,
        "buckets": [{"key": key, "amount": 1}],
    }).status_code == 422
    # 目标桶列表为空
    assert client.post("/v1/quota/adjustments", json={
        "adjustment_id": "a1", "request_id": rid, "buckets": [],
    }).status_code == 422
    # amount 非正
    assert _adjust("a2", rid, [(key, 0)]).status_code == 422
    # 缺少 request_id
    assert client.post("/v1/quota/adjustments", json={
        "adjustment_id": "a3",
        "buckets": [{"key": key, "amount": 1}],
    }).status_code == 422


def test_concurrent_adjustments_same_id_single_application():
    """同一 adjustment_id + 同载荷并发：全部 200，只应用一次、只写一条审计。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 5).status_code == 200
    aid = _rid()
    before = client.get("/v1/audit/verify").json()["checked_count"]

    codes = []
    barrier = threading.Barrier(20)

    def hit():
        barrier.wait()
        codes.append(_adjust(aid, rid, [(k2, 5)]).status_code)

    threads = [threading.Thread(target=hit) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 5
    after = client.get("/v1/audit/verify").json()["checked_count"]
    assert after - before == 1  # 仅一次成功调整落审计


def test_concurrent_adjustments_different_ids_serialized():
    """不同 adjustment_id 并发调整同一消费：串行化后均可成功，贡献不翻倍。

    每笔调整都把同一笔贡献在两桶间搬移；校验时必须排除该请求**当前**
    贡献，否则第二笔起会把目标桶里该请求自身的贡献误算为占用而误拒。
    """
    k_src, k_dst = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k_src, 10).status_code == 200

    outcomes: list[tuple[str, int]] = []
    barrier = threading.Barrier(20)

    def hit(aid: str, target):
        barrier.wait()
        outcomes.append((aid, _adjust(aid, rid, [(target, 10)]).status_code))

    aids = [_rid() for _ in range(20)]
    threads = []
    for i, aid in enumerate(aids):
        target = k_dst if i % 2 == 0 else k_src
        threads.append(threading.Thread(target=hit, args=(aid, target)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 串行替换：每笔调整都基于上一笔的结果排除当前贡献，故全部成功
    assert all(c == 200 for _, c in outcomes), outcomes
    # 任意时刻只有一套 10 的贡献存在：两桶用量之和恒为 10
    used_src = client.get(f"/v1/buckets/{k_src}/usage").json()["used"]
    used_dst = client.get(f"/v1/buckets/{k_dst}/usage").json()["used"]
    assert used_src + used_dst == 10
    # 每笔成功调整各写一条审计
    recs = _audit(0, 1000).json()["records"]
    assert sum(
        1 for r in recs
        if r["event"]["op"] == "adjust" and r["event"]["business_id"] in set(aids)
    ) == 20


def test_concurrent_adjust_and_consume_never_exceed_limit():
    """调整与普通消费并发争抢目标桶：任何串行顺序下都不会突破限额。"""
    k_src, k_dst = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k_src, 10).status_code == 200

    consume_codes = []
    barrier = threading.Barrier(30)

    # 单独收集调整结果
    adj_holder = {}

    def adjust2():
        barrier.wait()
        adj_holder["code"] = _adjust(_rid(), rid, [(k_dst, 10)]).status_code

    def consume():
        barrier.wait()
        consume_codes.append(_consume(_rid(), k_dst, 1).status_code)

    threads = [threading.Thread(target=adjust2)]
    for _ in range(29):
        threads.append(threading.Thread(target=consume))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    adj_code = adj_holder["code"]
    used_src = client.get(f"/v1/buckets/{k_src}/usage").json()["used"]
    used_dst = client.get(f"/v1/buckets/{k_dst}/usage").json()["used"]
    assert used_dst <= 10  # 核心不变量：绝不超限

    if adj_code == 200:
        # 调整先落地：目标桶被调整贡献占满，所有普通消费被拒
        assert used_src == 0 and used_dst == 10
        assert consume_codes.count(200) == 0
        assert consume_codes.count(429) == 29
    else:
        # 某笔消费先占 1 导致调整 429、原贡献保留；之后还能放 9 笔
        assert adj_code == 429
        assert used_src == 10 and used_dst == 10
        assert consume_codes.count(200) == 10
        assert consume_codes.count(429) == 19


def test_audit_adjust_appended_only_on_first_success():
    """仅首次成功调整记一条；重放与失败调整（404/409/429）均不记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    k1, k2, k3 = _bucket(10, 60), _bucket(10, 60), _bucket(1, 60)
    rid = _rid()
    assert _consume(rid, k1, 2).status_code == 200
    aid = _rid()
    assert _adjust(aid, rid, [(k2, 2)]).status_code == 200
    assert _adjust(aid, rid, [(k2, 2)]).status_code == 200  # 重放不重复记录

    # 失败调整：同 ID 换载荷(409)、原单不存在(404)、被拒消费(409)、
    # 已释放(409)、目标超限(429)、未知桶(404)
    assert _adjust(aid, rid, [(k2, 3)]).status_code == 409
    assert _adjust(_rid(), f"missing-{_rid()}", [(k2, 1)]).status_code == 404
    rid_denied = _rid()
    assert _consume(_rid(), k3, 1).status_code == 200
    assert _consume(rid_denied, k3, 1).status_code == 429
    assert _adjust(_rid(), rid_denied, [(k1, 1)]).status_code == 409
    rid_rel = _rid()
    assert _consume(rid_rel, k1, 1).status_code == 200
    assert _release(_rid(), rid_rel).status_code == 200
    assert _adjust(_rid(), rid_rel, [(k1, 1)]).status_code == 409
    # k2 已有 rid 的 2 占用：再把另一笔消费整桶调整到 k2 必超限
    rid2 = _rid()
    assert _consume(rid2, k1, 2).status_code == 200
    assert _adjust(_rid(), rid2, [(k2, 9)]).status_code == 429
    assert _adjust(_rid(), rid2, [(f"nope-{_rid()[:8]}", 1)]).status_code == 404

    after = client.get("/v1/audit/verify").json()["checked_count"]
    # consume: rid / k3 占位 / rid_denied(拒绝) / rid_rel / rid2 = 5 条，
    # release rid_rel = 1 条，adjust aid = 1 条 → 共 7 条
    assert after - before == 7

    recs = [
        r for r in _audit(before, 20).json()["records"]
        if r["event"]["business_id"] == aid
    ]
    assert len(recs) == 1
    event = recs[0]["event"]
    assert event["op"] == "adjust"
    assert event["result"] == "adjusted"
    assert set(event) == {"op", "business_id", "result", "occurred_at"}
    # 失败调整没有改动任何贡献
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 2


# ---------------------------------------------------------------- 哈希审计链

def _audit(after_seq: int = 0, limit: int = 1000):
    return client.get(
        "/v1/audit", params={"after_seq": after_seq, "limit": limit}
    )


def test_audit_hash_rule_pure():
    """链哈希规则：64 个 0 起链；SHA-256(prev 原始32字节 + canonical JSON)。"""
    import hashlib

    from app import audit

    assert audit.GENESIS_HASH == "0" * 64

    event = {
        "op": "consume",
        "business_id": "rid-1",
        "result": "allowed",
        "occurred_at": 1789657657.47,
    }
    event_json = audit.canonical_event_json(event)
    # 键排序且无多余空白
    assert event_json == (
        '{"business_id":"rid-1","occurred_at":1789657657.47,'
        '"op":"consume","result":"allowed"}'
    )
    digest = audit.compute_hash(audit.GENESIS_HASH, event_json)
    assert digest == hashlib.sha256(
        bytes(32) + event_json.encode("utf-8")
    ).hexdigest()

    # 第二条引用第一条 hash
    event2 = dict(event, business_id="rid-2", result="denied")
    digest2 = audit.compute_hash(digest, audit.canonical_event_json(event2))
    assert digest2 == hashlib.sha256(
        bytes.fromhex(digest)
        + audit.canonical_event_json(event2).encode("utf-8")
    ).hexdigest()
    assert digest2 != digest


def test_audit_first_record_anchors_at_genesis():
    """链首记录 seq=1、prev_hash 为 64 个 0。"""
    r = _audit(0, 1)
    assert r.status_code == 200
    first = r.json()["records"][0]
    assert first["seq"] == 1
    assert first["prev_hash"] == "0" * 64
    assert len(first["hash"]) == 64
    assert set(first["hash"]) <= set("0123456789abcdef")


def test_audit_consume_appended_once_allow_and_deny():
    """首次放行与首次拒绝各记一条；幂等重放不重复记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    # 放行 + 重放
    k_allow = _bucket(10, 60)
    rid_allow = _rid()
    assert _consume(rid_allow, k_allow, 3).status_code == 200
    assert _consume(rid_allow, k_allow, 3).status_code == 200

    # 先用一笔放行打满桶，再触发拒绝 + 拒绝重放
    k_deny = _bucket(1, 60)
    assert _consume(_rid(), k_deny, 1).status_code == 200
    rid_denied = _rid()
    assert _consume(rid_denied, k_deny, 1).status_code == 429
    assert _consume(rid_denied, k_deny, 1).status_code == 429

    after = client.get("/v1/audit/verify").json()["checked_count"]
    # 放行 2 笔 + 拒绝 1 笔 = 3 条；两次重放不记录
    assert after - before == 3

    records = _audit(before, 10).json()["records"]
    by_biz = {rec["event"]["business_id"]: rec for rec in records}
    allow_rec = by_biz[rid_allow]
    assert allow_rec["event"]["op"] == "consume"
    assert allow_rec["event"]["result"] == "allowed"
    assert isinstance(allow_rec["event"]["occurred_at"], (int, float))
    deny_rec = by_biz[rid_denied]
    assert deny_rec["event"]["op"] == "consume"
    assert deny_rec["event"]["result"] == "denied"

    # 链关系：逐条引用上一条 hash
    prev = records[0]["prev_hash"]
    for rec in records:
        assert rec["prev_hash"] == prev
        prev = rec["hash"]


def test_audit_release_appended_only_on_first_success():
    """仅首次成功释放记一条；重放与失败释放（404/409）均不记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    rel = _rid()
    assert _release(rel, rid).status_code == 200
    assert _release(rel, rid).status_code == 200  # 成功重放，不重复记录

    # 失败释放：原单不存在(404)、重复释放(409)、释放被拒消费(409)
    assert _release(_rid(), f"missing-{_rid()}").status_code == 404
    assert _release(_rid(), rid).status_code == 409
    rid_denied = _rid()
    k2 = _bucket(1, 60)
    assert _consume(rid_denied, k2, 1).status_code == 200
    rid_blocked = _rid()
    assert _consume(rid_blocked, k2, 1).status_code == 429
    assert _release(_rid(), rid_blocked).status_code == 409

    after = client.get("/v1/audit/verify").json()["checked_count"]
    # consume 放行 rid、consume 放行 rid_denied、consume 拒绝 rid_blocked、
    # release rel —— 共 4 条
    assert after - before == 4

    rec = [
        r for r in _audit(before, 10).json()["records"]
        if r["event"]["business_id"] == rel
    ]
    assert len(rec) == 1
    event = rec[0]["event"]
    assert event["op"] == "release"
    assert event["result"] == "released"
    assert event["business_id"] == rel
    assert isinstance(event["occurred_at"], (int, float))
    assert set(event) == {"op", "business_id", "result", "occurred_at"}


def test_audit_conflict_and_unknown_bucket_not_recorded():
    """409 幂等冲突与 404 桶未配置不产生审计记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    assert _consume(rid, key, 2).status_code == 409       # 载荷冲突
    assert client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [{"key": "nope-bucket", "amount": 1}],
    }).status_code == 404

    assert client.get("/v1/audit/verify").json()["checked_count"] - before == 1


def test_audit_pagination_and_head():
    """GET /v1/audit 按 after_seq/limit 升序分页，并返回链头。"""
    head_before = _audit(0, 1).json()["head"]["seq"]
    key = _bucket(100, 60)
    rids = []
    for _ in range(3):
        rid = _rid()
        rids.append(rid)
        assert _consume(rid, key, 1).status_code == 200

    page1 = _audit(head_before, 2).json()
    assert [r["seq"] for r in page1["records"]] == [
        head_before + 1, head_before + 2,
    ]
    assert page1["next_after_seq"] == head_before + 2
    assert page1["head"]["seq"] == head_before + 3

    page2 = _audit(head_before + 2, 2).json()
    assert [r["event"]["business_id"] for r in page2["records"]] == [rids[2]]
    assert page2["records"][0]["prev_hash"] == page1["records"][1]["hash"]

    # 追上链头后再翻页为空，链头保持不变
    empty = _audit(head_before + 3, 2).json()
    assert empty["records"] == []
    assert empty["next_after_seq"] == head_before + 3
    assert empty["head"] == page1["head"]

    verify = client.get("/v1/audit/verify").json()
    assert verify["head_hash"] == page1["head"]["hash"]


def test_audit_query_validation():
    r = client.get("/v1/audit", params={"after_seq": -1})
    assert r.status_code == 422
    r = client.get("/v1/audit", params={"limit": 0})
    assert r.status_code == 422
    r = client.get("/v1/audit", params={"limit": 1001})
    assert r.status_code == 422


def test_audit_verify_valid_full_chain():
    """所有业务测试产生的记录构成一条完整有效链。"""
    body = client.get("/v1/audit/verify").json()
    assert set(body) == {
        "valid", "checked_count", "head_hash", "first_invalid_seq",
    }
    assert body["valid"] is True
    assert body["first_invalid_seq"] is None
    assert len(body["head_hash"]) == 64
    assert body["checked_count"] >= 1

    listing = _audit(0, 1000).json()
    assert len(listing["records"]) == body["checked_count"]
    assert listing["head"]["hash"] == body["head_hash"]


def test_audit_verify_detects_tampering():
    """篡改事件内容或前序指针：verify 报告首个失配 seq；恢复后重新有效。"""
    from app.db import connect

    listing = _audit(0, 1000).json()
    records = listing["records"]
    target = records[-1]
    original = None

    conn = connect()
    try:
        original = conn.execute(
            "SELECT event_json, prev_hash FROM audit_log WHERE seq = ?",
            (target["seq"],),
        ).fetchone()

        # 1) 篡改事件内容：存储 hash 与重算值不符
        conn.execute(
            "UPDATE audit_log SET event_json = ? WHERE seq = ?",
            (
                '{"business_id":"forged","occurred_at":0,'
                '"op":"consume","result":"allowed"}',
                target["seq"],
            ),
        )
        body = client.get("/v1/audit/verify").json()
        assert body["valid"] is False
        assert body["first_invalid_seq"] == target["seq"]
        assert body["checked_count"] == len(records)

        # 恢复后再次有效
        conn.execute(
            "UPDATE audit_log SET event_json = ? WHERE seq = ?",
            (original["event_json"], target["seq"]),
        )
        assert client.get("/v1/audit/verify").json()["valid"] is True

        # 2) 篡改前序指针：prev_hash 与上一条不一致
        conn.execute(
            "UPDATE audit_log SET prev_hash = ? WHERE seq = ?",
            ("f" * 64, target["seq"]),
        )
        body = client.get("/v1/audit/verify").json()
        assert body["valid"] is False
        assert body["first_invalid_seq"] == target["seq"]
    finally:
        # 务必恢复，保证全链对后续检查保持有效
        if original is not None:
            conn.execute(
                "UPDATE audit_log SET event_json = ?, prev_hash = ? WHERE seq = ?",
                (original["event_json"], original["prev_hash"], target["seq"]),
            )
        conn.close()

    assert client.get("/v1/audit/verify").json()["valid"] is True


# ---------------------------------------------------------------- 回放模拟

def _insert_request(rid: str, key: str, amount: int, created_at: float,
                    allowed: bool):
    """直接写入一条历史消费固化记录（绕过 created_at=now）。

    模拟只读取 requests 表中首次消费固化的载荷与判定，不依赖
    usage_events，因此这里只构造 requests 记录。
    """
    from app.db import connect

    payload = {"request_id": rid,
               "buckets": [{"key": key, "amount": amount}]}
    decision = {"request_id": rid, "allowed": allowed,
                "decided_at": created_at}
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO requests (request_id, payload_hash, payload, "
            "decision, allowed, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (rid, "0" * 64, json.dumps(payload), json.dumps(decision),
             1 if allowed else 0, created_at),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def _insert_request_multi(rid: str, items: list[dict], created_at: float,
                          allowed: bool):
    from app.db import connect

    payload = {"request_id": rid, "buckets": items}
    decision = {"request_id": rid, "allowed": allowed,
                "decided_at": created_at}
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO requests (request_id, payload_hash, payload, "
            "decision, allowed, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (rid, "0" * 64, json.dumps(payload), json.dumps(decision),
             1 if allowed else 0, created_at),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def _simulate(payload: dict):
    return client.post("/v1/quota/simulations", json=payload)


def test_simulation_transitions_and_effective_config():
    """allow→allow / allow→deny / deny→deny 转移计数、生效配置与峰值。"""
    key = _bucket(10, 100.0)
    base = 1_000_000
    # 预热：t=950 放行 8；窗口 100，在 t=1000 仍占用
    _insert_request("sim-w", key, 8, base - 50, True)
    # 区间内：放行 1（9/10）、再放行 1（10/10）、
    # 实际拒绝且模拟仍拒绝 1 笔、实际放行但模拟拒绝 1 笔
    _insert_request("sim-a", key, 1, base + 10, True)
    _insert_request("sim-b", key, 1, base + 20, True)
    _insert_request("sim-c", key, 1, base + 30, False)
    _insert_request("sim-d", key, 1, base + 40, True)

    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 10,
                                "window_seconds": 100}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["from"] == base and body["to"] == base + 100
    assert body["warmup_seconds"] == max(
        b["window_seconds"] for b in body["effective_buckets"]
    )
    eff = {b["key"]: b for b in body["effective_buckets"]}
    assert eff[key] == {"key": key, "limit": 10, "window_seconds": 100.0}
    assert body["transitions"] == {
        "allowed_to_allowed": 2,
        "allowed_to_denied": 1,
        "denied_to_allowed": 0,
        "denied_to_denied": 1,
    }
    # 模拟峰值：8（预热残留）+1+1 = 10；被拒请求不占用
    peaks = {p["key"]: p["simulated_peak"] for p in body["bucket_peaks"]}
    assert peaks == {key: 10}
    assert body["records_replayed"] == 4
    assert body["warmup_records"] == 1
    assert body["flip_details_truncated"] is False

    flip = body["flip_details"][0]
    assert flip["request_id"] == "sim-d"
    assert flip["created_at"] == base + 40
    assert flip["actual_decision"] == "allowed"
    assert flip["simulated_decision"] == "denied"
    assert flip["buckets"] == [{"key": key, "amount": 1}]


def test_simulation_denied_to_allowed_on_window_shrink():
    """候选窗口变小：实际满窗拒绝的请求在模拟中窗口已滚动放行。"""
    key = _bucket(1, 100.0)
    base = 2_000_000
    # 预热事件早于候选窗口(50s)、但在实际窗口(100s)内：
    # 实际 t=base 时它仍占用 → simx-deny 被拒；模拟窗口 50s 已过期 → 放行
    _insert_request("simx-old", key, 1, base - 70, True)
    _insert_request("simx-deny", key, 1, base, False)
    _insert_request("simx-ok", key, 1, base + 60, True)

    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 1,
                                "window_seconds": 50}]})
    assert r.status_code == 200, r.text
    body = r.json()
    # 预热期取全部生效桶窗口的最大值（保守覆盖），不短于候选窗口
    assert body["warmup_seconds"] >= 50
    assert body["warmup_seconds"] == max(
        b["window_seconds"] for b in body["effective_buckets"]
    )
    assert body["warmup_records"] == 1
    assert body["transitions"] == {
        "allowed_to_allowed": 1,
        "allowed_to_denied": 0,
        "denied_to_allowed": 1,
        "denied_to_denied": 0,
    }
    flips = body["flip_details"]
    assert len(flips) == 1
    assert flips[0]["request_id"] == "simx-deny"
    assert flips[0]["actual_decision"] == "denied"
    assert flips[0]["simulated_decision"] == "allowed"


def test_simulation_uncovered_bucket_keeps_current_config():
    """未给候选的桶沿用当前配置。"""
    key = _bucket(3, 100.0)
    base = 3_000_000
    _insert_request("keep-a", key, 3, base + 1, True)
    _insert_request("keep-b", key, 1, base + 2, False)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    body = r.json()
    eff = {b["key"]: b for b in body["effective_buckets"]}
    assert eff[key] == {"key": key, "limit": 3, "window_seconds": 100.0}
    assert body["transitions"] == {
        "allowed_to_allowed": 1,
        "allowed_to_denied": 0,
        "denied_to_allowed": 0,
        "denied_to_denied": 1,
    }
    assert body["flip_details"] == []
    assert {p["key"]: p["simulated_peak"]
            for p in body["bucket_peaks"]} == {key: 3}


def test_simulation_releases_are_not_replayed():
    """模拟只回放消费：已释放的预热消费仍占用模拟额度（release 被忽略）。"""
    key = _bucket(1, 100.0)
    base = 4_000_000
    _insert_request("rel-old", key, 1, base - 10, True)
    # 线上随后释放 rel-old，使 rel-new 实际得以放行
    assert _release("rel-1", "rel-old").status_code == 200
    _insert_request("rel-new", key, 1, base + 5, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["warmup_records"] == 1
    assert body["transitions"]["allowed_to_denied"] == 1
    assert body["transitions"]["allowed_to_allowed"] == 0
    flip = body["flip_details"][0]
    assert flip["request_id"] == "rel-new"
    assert flip["actual_decision"] == "allowed"
    assert flip["simulated_decision"] == "denied"


def test_simulation_multi_bucket_all_or_nothing():
    """多桶请求：任一桶超限则模拟整体拒绝、所有桶均不占用。"""
    k1, k2 = _bucket(5, 100.0), _bucket(5, 100.0)
    base = 5_000_000
    _insert_request_multi("mb-1", [{"key": k1, "amount": 5},
                                   {"key": k2, "amount": 5}], base + 1, True)
    _insert_request_multi("mb-2", [{"key": k1, "amount": 1},
                                   {"key": k2, "amount": 1}], base + 2, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    t = r.json()["transitions"]
    assert t["allowed_to_allowed"] == 1
    assert t["allowed_to_denied"] == 1
    peaks = {p["key"]: p["simulated_peak"]
             for p in r.json()["bucket_peaks"]}
    # mb-2 被整体拒绝，两桶峰值都停留在 5
    assert peaks == {k1: 5, k2: 5}
    flip = r.json()["flip_details"][0]
    assert flip["request_id"] == "mb-2"
    assert sorted(b["key"] for b in flip["buckets"]) == sorted([k1, k2])


def test_simulation_warmup_boundary():
    """恰好落在预热起点的事件在区间开始时已过期。"""
    key = _bucket(100, 100.0)
    base = 6_000_000
    _insert_request("wb-edge", key, 5, base - 100, True)   # t=from-window
    _insert_request("wb-in", key, 3, base + 1, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    body = r.json()
    assert body["warmup_records"] == 1
    peaks = {p["key"]: p["simulated_peak"] for p in body["bucket_peaks"]}
    assert peaks[key] == 3


def test_simulation_flip_details_stable_order_and_limit():
    """翻转明细按 (created_at, request_id) 稳定顺序，最多 100 条。"""
    key = _bucket(1, 1000.0)
    base = 7_000_000
    # 同秒内 request_id 决定顺序：a 放行，b、c 及之后全部在模拟中翻转
    for i, rid in enumerate(["sa", "sb", "sc"] + [f"s{i:03d}" for i in range(120)]):
        _insert_request(rid, key, 1, float(base + i), True)

    r = _simulate({"from": base, "to": base + 1000, "buckets": []})
    body = r.json()
    # 首条 allowed→allowed，其余 122 条全部 allowed→denied
    assert body["transitions"]["allowed_to_allowed"] == 1
    assert body["transitions"]["allowed_to_denied"] == 122
    assert len(body["flip_details"]) == 100
    assert body["flip_details_truncated"] is True
    # 稳定顺序：先按时间（首条翻转是同秒的 sb，而非更晚的记录）
    assert body["flip_details"][0]["request_id"] == "sb"
    assert body["flip_details"][1]["request_id"] == "sc"
    ordered = [(f["created_at"], f["request_id"]) for f in body["flip_details"]]
    assert ordered == sorted(ordered)


def test_simulation_is_read_only():
    """模拟不写业务数据也不追加审计链。"""
    before_req = client.get("/v1/audit/verify").json()["checked_count"]
    key = _bucket(2, 100.0)
    base = 8_000_000
    _insert_request("ro-1", key, 1, base + 1, True)

    usage_before = client.get(f"/v1/buckets/{key}/usage").json()["used"]
    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 1,
                                "window_seconds": 10}]})
    assert r.status_code == 200
    # 桶配置未被候选覆盖
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == usage_before
    cfg = client.get("/v1/buckets").json()["buckets"]
    assert {b["key"]: (b["limit"], b["window_seconds"]) for b in cfg
            if b["key"] == key} == {key: (2, 100.0)}
    assert client.get("/v1/audit/verify").json()["checked_count"] == before_req


def test_simulation_validation_errors():
    """未知桶/非法范围/跨度超 7 天/候选重复 → 422。"""
    key = _bucket(1, 60.0)

    assert _simulate({"from": 100, "to": 90, "buckets": []}).status_code == 422
    assert _simulate({"from": 100, "to": 100, "buckets": []}).status_code == 422
    assert _simulate({
        "from": 0, "to": 7 * 24 * 3600 + 1, "buckets": [],
    }).status_code == 422
    # 恰好 7 天合法
    assert _simulate({
        "from": 0, "to": 7 * 24 * 3600, "buckets": [],
    }).status_code == 200
    # 未知桶
    assert _simulate({"from": 0, "to": 100, "buckets": [
        {"key": "not-a-bucket", "limit": 1, "window_seconds": 60},
    ]}).status_code == 422
    # 候选桶重复
    assert _simulate({"from": 0, "to": 100, "buckets": [
        {"key": key, "limit": 1, "window_seconds": 60},
        {"key": key, "limit": 2, "window_seconds": 60},
    ]}).status_code == 422
    # 候选值非法（pydantic）
    assert _simulate({"from": 0, "to": 100, "buckets": [
        {"key": key, "limit": 0, "window_seconds": 60},
    ]}).status_code == 422
    # 缺少必填字段
    assert _simulate({"to": 100, "buckets": []}).status_code == 422


def test_simulation_empty_range():
    """区间内无任何记录：转移全 0、峰值为空，预热仍正常执行。"""
    key = _bucket(1, 60.0)
    _insert_request("far-away", key, 1, 50.0, True)
    r = _simulate({"from": 9_000_000, "to": 9_000_100, "buckets": []})
    assert r.status_code == 200
    body = r.json()
    assert set(body["transitions"].values()) == {0}
    assert body["bucket_peaks"] == []
    assert body["flip_details"] == []
    assert body["warmup_records"] == 0
    assert body["records_replayed"] == 0


def test_simulation_peak_includes_warmup_residual_on_denials():
    """区间内请求全部被模拟拒绝时，峰值仍反映预热残留占用。"""
    key = _bucket(10, 100.0)
    base = 10_000_000
    _insert_request("res-w", key, 7, base - 10, True)   # 区间起点仍残留 7
    _insert_request("res-1", key, 4, base + 1, True)    # 模拟拒绝
    _insert_request("res-2", key, 4, base + 2, False)   # 模拟仍拒绝

    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 10,
                                "window_seconds": 100}]})
    assert r.status_code == 200, r.text
    peaks = {p["key"]: p["simulated_peak"]
             for p in r.json()["bucket_peaks"]}
    # 两次判定时刻占用均为 7（预热残留），峰值 7
    assert peaks == {key: 7}


def test_simulation_stale_payload_unknown_bucket_422():
    """历史固化载荷引用已不存在的桶：无法回放，422 且不留痕。"""
    from app.db import connect

    ghost = f"ghost-{_rid()[:10]}"
    assert client.put(f"/v1/buckets/{ghost}", json={
        "limit": 1, "window_seconds": 60,
    }).status_code == 200
    base = 11_000_000
    _insert_request("ghost-1", ghost, 1, base + 1, True)
    # 直接删除桶（无删除接口），模拟时历史载荷引用即失效
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM buckets WHERE key = ?", (ghost,))
        conn.execute("COMMIT")
    finally:
        conn.close()

    before = client.get("/v1/audit/verify").json()["checked_count"]
    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 422
    assert ghost in r.json()["detail"]
    assert client.get("/v1/audit/verify").json()["checked_count"] == before


# ---------------------------------------------------------------- 多级配额树

def _put_bucket(key: str, limit: int, window: float = 60.0,
                parent_key: str | None = None):
    body = {"limit": limit, "window_seconds": window}
    if parent_key is not None:
        body["parent_key"] = parent_key
    r = client.put(f"/v1/buckets/{key}", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _tree3(root_limit=1000, mid_limit=1000, leaf_limit=1000):
    """创建 root -> mid -> leaf 三级配额树，返回三个桶键。"""
    root = f"t-root-{_rid()[:10]}"
    mid = f"t-mid-{_rid()[:10]}"
    leaf = f"t-leaf-{_rid()[:10]}"
    _put_bucket(root, root_limit)
    _put_bucket(mid, mid_limit, parent_key=root)
    _put_bucket(leaf, leaf_limit, parent_key=mid)
    return root, mid, leaf


def _consume_multi(rid: str, items: list[tuple[str, int]]):
    return client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k, "amount": a} for k, a in items],
    })


def test_quota_tree_pure_expand_contributions():
    """expand_contributions：叶桶消费展开到全部祖先；共享祖先只汇总一次。"""
    from app.quota import ancestor_chain, expand_contributions

    parents = {"r": None, "m": "r", "a": "m", "b": "m"}
    assert ancestor_chain("a", parents) == ["m", "r"]
    assert ancestor_chain("r", parents) == []

    # 单叶：自身 direct，m/r 各 rollup
    c = expand_contributions([{"key": "a", "amount": 3}], parents)
    assert c == {
        "a": {"direct": 3, "rollup": 0, "total": 3},
        "m": {"direct": 0, "rollup": 3, "total": 3},
        "r": {"direct": 0, "rollup": 3, "total": 3},
    }

    # 共享祖先的两个后代：祖先只累计金额之和（3+2=5），不重复
    c = expand_contributions(
        [{"key": "a", "amount": 3}, {"key": "b", "amount": 2}], parents
    )
    assert c["r"] == {"direct": 0, "rollup": 5, "total": 5}
    assert c["m"] == {"direct": 0, "rollup": 5, "total": 5}
    assert c["a"] == {"direct": 3, "rollup": 0, "total": 3}
    assert c["b"] == {"direct": 2, "rollup": 0, "total": 2}

    # 同桶重复条目先经 merge_items 合并；直接点名内部桶时 direct 与
    # rollup 并存
    c = expand_contributions(
        [{"key": "a", "amount": 3}, {"key": "m", "amount": 4}], parents
    )
    assert c["m"] == {"direct": 4, "rollup": 3, "total": 7}
    assert c["r"] == {"direct": 0, "rollup": 7, "total": 7}


def test_bucket_put_accepts_and_returns_parent_key():
    """PUT 可创建带 parent_key 的桶；GET 列表与响应都带回 parent_key。"""
    root = f"p-root-{_rid()[:10]}"
    child = f"p-child-{_rid()[:10]}"
    body = _put_bucket(root, 10)
    assert body["parent_key"] is None
    body = _put_bucket(child, 5, parent_key=root)
    assert body["parent_key"] == root

    listing = {b["key"]: b for b in client.get("/v1/buckets").json()["buckets"]}
    assert listing[root]["parent_key"] is None
    assert listing[child]["parent_key"] == root

    # 省略 parent_key（或显式 null）= 根桶
    r = client.put(f"/v1/buckets/{child}", json={"limit": 5, "window_seconds": 60})
    assert r.status_code == 200 and r.json()["parent_key"] is None
    r = client.put(f"/v1/buckets/{child}",
                   json={"limit": 5, "window_seconds": 60, "parent_key": None})
    assert r.status_code == 200 and r.json()["parent_key"] is None
    # 重新挂回
    assert _put_bucket(child, 5, parent_key=root)["parent_key"] == root


def test_bucket_parent_self_reference_rejected_in_transaction():
    """parent_key 指向自身 → 422，配置不发生任何变化。"""
    key = f"self-{_rid()[:10]}"
    _put_bucket(key, 10)
    r = client.put(f"/v1/buckets/{key}",
                   json={"limit": 99, "window_seconds": 60, "parent_key": key})
    assert r.status_code == 422
    assert key in r.json()["detail"]
    # 限额/窗口/父级均未被部分更新（事务回滚）
    u = client.get(f"/v1/buckets/{key}/usage").json()
    assert u["limit"] == 10
    listing = {b["key"]: b for b in client.get("/v1/buckets").json()["buckets"]}
    assert listing[key]["parent_key"] is None


def test_bucket_parent_missing_rejected():
    """parent_key 指向不存在的桶 → 422（创建与更新均拒绝）。"""
    key = f"miss-{_rid()[:10]}"
    ghost = f"ghost-{_rid()[:10]}"
    r = client.put(f"/v1/buckets/{key}",
                   json={"limit": 10, "window_seconds": 60, "parent_key": ghost})
    assert r.status_code == 422
    assert ghost in r.json()["detail"]
    # 桶未被创建
    assert client.get(f"/v1/buckets/{key}/usage").status_code == 404

    _put_bucket(key, 10)
    r = client.put(f"/v1/buckets/{key}",
                   json={"limit": 10, "window_seconds": 60, "parent_key": ghost})
    assert r.status_code == 422


def test_bucket_parent_indirect_cycle_rejected():
    """任何直接或间接环都在事务内拒绝，整棵树保持原状。"""
    root, mid, leaf = _tree3()
    # root 挂到 leaf -> root-mid-leaf-root 间接环
    r = client.put(f"/v1/buckets/{root}",
                   json={"limit": 1000, "window_seconds": 60, "parent_key": leaf})
    assert r.status_code == 422
    # mid 挂到自己的后代 leaf
    r = client.put(f"/v1/buckets/{mid}",
                   json={"limit": 1000, "window_seconds": 60, "parent_key": leaf})
    assert r.status_code == 422
    listing = {b["key"]: b["parent_key"]
               for b in client.get("/v1/buckets").json()["buckets"]}
    assert listing[root] is None
    assert listing[mid] == root
    assert listing[leaf] == mid

    # 合法的重新挂接（leaf 改挂 root）成功
    assert _put_bucket(leaf, 1000, parent_key=root)["parent_key"] == root


def test_leaf_consume_counts_into_all_ancestors():
    """消费叶桶：自身 direct，全部祖先按同一窗口汇总 total。"""
    root, mid, leaf = _tree3()
    rid = _rid()
    r = _consume(rid, leaf, 3)
    assert r.status_code == 200, r.text
    leaf_u = client.get(f"/v1/buckets/{leaf}/usage").json()
    mid_u = client.get(f"/v1/buckets/{mid}/usage").json()
    root_u = client.get(f"/v1/buckets/{root}/usage").json()
    assert (leaf_u["direct_used"], leaf_u["total_used"]) == (3, 3)
    assert (mid_u["direct_used"], mid_u["total_used"]) == (0, 3)
    assert (root_u["direct_used"], root_u["total_used"]) == (0, 3)
    # used 作为 total_used 的历史别名保留；remaining 按总量计算
    assert leaf_u["used"] == leaf_u["total_used"]
    assert mid_u["remaining"] == mid_u["limit"] - 3
    # 响应带回 parent_key
    assert leaf_u["parent_key"] == mid
    assert root_u["parent_key"] is None


def test_shared_ancestor_accumulates_sum_not_duplicate():
    """一次请求含共享祖先的两个后代：祖先只累计金额之和，事件不重复。"""
    from app.db import connect

    root, mid, leaf = _tree3()
    sibling = f"t-sib-{_rid()[:10]}"
    _put_bucket(sibling, 1000, parent_key=mid)

    rid = _rid()
    r = _consume_multi(rid, [(leaf, 3), (sibling, 2)])
    assert r.status_code == 200, r.text

    mid_u = client.get(f"/v1/buckets/{mid}/usage").json()
    root_u = client.get(f"/v1/buckets/{root}/usage").json()
    assert mid_u["direct_used"] == 0 and mid_u["total_used"] == 5
    assert root_u["total_used"] == 5

    # 每个祖先仅一条汇总事件（3+2=5），而非两条 3/2
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT bucket_key, amount, direct FROM usage_events "
            "WHERE request_id = ? ORDER BY bucket_key, direct",
            (rid,),
        ).fetchall()
    finally:
        conn.close()
    by_bucket: dict[str, list[tuple[int, int]]] = {}
    for row in rows:
        by_bucket.setdefault(row["bucket_key"], []).append(
            (row["amount"], row["direct"])
        )
    assert by_bucket[mid] == [(5, 0)]
    assert by_bucket[root] == [(5, 0)]
    assert by_bucket[leaf] == [(3, 1)]
    assert by_bucket[sibling] == [(2, 1)]


def test_ancestor_limit_blocks_leaf_consume_atomically():
    """祖先限额不足 → 429 且任何一层（含叶桶与其他祖先）都不扣减。"""
    root, mid, leaf = _tree3(root_limit=10, mid_limit=1000, leaf_limit=1000)
    rid = _rid()
    r = _consume(rid, leaf, 11)  # 叶桶/中桶充足，但超过祖先 root 的 10
    assert r.status_code == 429
    body = r.json()
    assert [b["key"] for b in body["limited_buckets"]] == [root]
    lb = body["limited_buckets"][0]
    assert lb["used"] == 0 and lb["requested"] == 11 and lb["limit"] == 10
    # 请求量本身超过祖先限额 → 永远不可重试
    assert body["retry_at"] is None
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0

    # 放 4 后叶桶再消费 7：叶桶自身充足，但 root 4+7>10 被整体拒绝
    assert _consume(_rid(), leaf, 4).status_code == 200
    r = _consume(_rid(), leaf, 7)
    assert r.status_code == 429
    assert "Retry-After" in r.headers
    assert [b["key"] for b in r.json()["limited_buckets"]] == [root]
    assert client.get(f"/v1/buckets/{leaf}/usage").json()["total_used"] == 4

    # 多层同时受限时全部列出，整体重试时间取最大值
    root2, mid2, leaf2 = _tree3(root_limit=10, mid_limit=3, leaf_limit=1000)
    assert _consume(_rid(), leaf2, 2).status_code == 200  # mid=2,root=2
    r = _consume(_rid(), leaf2, 2)  # mid 4>3、root 4<=10
    assert r.status_code == 429
    assert [b["key"] for b in r.json()["limited_buckets"]] == [mid2]


def test_internal_bucket_direct_consume_rolls_up():
    """直接消费内部桶：本桶记 direct，继续向其祖先汇总。"""
    root, mid, leaf = _tree3()
    assert _consume(_rid(), mid, 7).status_code == 200
    mid_u = client.get(f"/v1/buckets/{mid}/usage").json()
    root_u = client.get(f"/v1/buckets/{root}/usage").json()
    leaf_u = client.get(f"/v1/buckets/{leaf}/usage").json()
    assert (mid_u["direct_used"], mid_u["total_used"]) == (7, 7)
    assert root_u["total_used"] == 7
    assert leaf_u["total_used"] == 0  # 消费不向子树传播


def test_tree_consume_unknown_bucket_404():
    """直接点名未配置桶仍是 404，且不写任何贡献。"""
    root, mid, leaf = _tree3()
    ghost = f"nope-{_rid()[:10]}"
    r = _consume_multi(_rid(), [(ghost, 1), (leaf, 1)])
    assert r.status_code == 404
    assert client.get(f"/v1/buckets/{root}/usage").json()["total_used"] == 0


def test_release_reverts_full_contribution_chain():
    """释放撤销整条贡献链（直接 + 全部祖先），各层同步恢复。"""
    root, mid, leaf = _tree3()
    rid = _rid()
    assert _consume(rid, leaf, 5).status_code == 200
    rel = _rid()
    r = _release(rel, rid)
    assert r.status_code == 200
    body = r.json()
    # released_buckets 只汇总直接点名桶；events_reverted 为整条链事件数
    assert body["released_buckets"] == [{"key": leaf, "amount": 5}]
    assert body["events_reverted"] == 3  # leaf direct + mid/root rollup
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0

    # 重放释放不重复撤销；容量恢复后新消费可打满祖先
    assert _release(rel, rid).status_code == 200
    assert _consume(_rid(), leaf, 1000).status_code == 200


def test_release_shared_ancestor_request_reverts_sum_once():
    """释放含共享祖先的多桶请求：祖先的汇总事件（金额之和）被删一次。"""
    root, mid, leaf = _tree3()
    sibling = f"t-sib-{_rid()[:10]}"
    _put_bucket(sibling, 1000, parent_key=mid)
    rid = _rid()
    assert _consume_multi(rid, [(leaf, 3), (sibling, 2)]).status_code == 200
    r = _release(_rid(), rid)
    assert r.status_code == 200
    assert r.json()["events_reverted"] == 4  # 2 direct + mid/root rollup
    for k in (root, mid, leaf, sibling):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0


def test_adjust_replaces_full_chain_and_checks_ancestors():
    """调整按同一展开规则替换整条贡献链，并对目标的各层祖先校验限额。"""
    root, mid, leaf = _tree3(root_limit=10)
    rid = _rid()
    assert _consume(rid, leaf, 4).status_code == 200

    # 目标贡献链 root 需要 11（现存 4 + 新增 7）> 10 → 429 原链保留
    r = _adjust(_rid(), rid, [(leaf, 11)])
    assert r.status_code == 429
    assert [b["key"] for b in r.json()["limited_buckets"]] == [root]
    for k, used in ((root, 4), (mid, 4), (leaf, 4)):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == used

    # 合法调小：整条链（leaf direct + mid/root rollup）被原子替换
    r = _adjust(_rid(), rid, [(leaf, 2)])
    assert r.status_code == 200
    body = r.json()
    assert body["previous_buckets"] == [{"key": leaf, "amount": 4}]
    assert body["buckets"] == [{"key": leaf, "amount": 2}]
    assert body["events_replaced"] == 3
    for k in (root, mid):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 2
    leaf_u = client.get(f"/v1/buckets/{leaf}/usage").json()
    assert (leaf_u["direct_used"], leaf_u["total_used"]) == (2, 2)


def test_adjust_moves_chain_between_subtrees():
    """调整把贡献链从一棵子树搬到另一棵：旧祖先恢复，新祖先计入。"""
    root = f"mv-root-{_rid()[:10]}"
    m1 = f"mv-m1-{_rid()[:10]}"
    m2 = f"mv-m2-{_rid()[:10]}"
    a = f"mv-a-{_rid()[:10]}"
    b = f"mv-b-{_rid()[:10]}"
    _put_bucket(root, 1000)
    _put_bucket(m1, 1000, parent_key=root)
    _put_bucket(m2, 1000, parent_key=root)
    _put_bucket(a, 1000, parent_key=m1)
    _put_bucket(b, 1000, parent_key=m2)

    rid = _rid()
    assert _consume(rid, a, 5).status_code == 200
    assert _adjust(_rid(), rid, [(b, 3)]).status_code == 200
    assert client.get(f"/v1/buckets/{a}/usage").json()["total_used"] == 0
    assert client.get(f"/v1/buckets/{m1}/usage").json()["total_used"] == 0
    assert client.get(f"/v1/buckets/{b}/usage").json()["total_used"] == 3
    assert client.get(f"/v1/buckets/{m2}/usage").json()["total_used"] == 3
    assert client.get(f"/v1/buckets/{root}/usage").json()["total_used"] == 3

    # 再释放撤销的是调整后的整条链
    r = _release(_rid(), rid)
    assert r.status_code == 200
    assert r.json()["released_buckets"] == [{"key": b, "amount": 3}]
    assert r.json()["events_reverted"] == 3
    for k in (root, m1, m2, a, b):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0


def test_adjust_unknown_target_keeps_chain():
    """调整到未知桶 → 404，整条原贡献链完整保留。"""
    root, mid, leaf = _tree3()
    rid = _rid()
    assert _consume(rid, leaf, 4).status_code == 200
    ghost = f"ghost-{_rid()[:10]}"
    r = _adjust(_rid(), rid, [(ghost, 1)])
    assert r.status_code == 404
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 4


def test_tree_consume_idempotent_replay_does_not_duplicate_chain():
    """树消费的幂等重放返回固化判定，祖先汇总不重复累计。"""
    root, mid, leaf = _tree3()
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": leaf, "amount": 3}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200
    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 3


def test_concurrent_tree_consumes_never_exceed_ancestor_limit():
    """30 个并发叶桶消费争抢 root=10：恰好放行 10，祖先绝不超限。"""
    root, mid, leaf = _tree3(root_limit=10, mid_limit=1000, leaf_limit=1000)
    codes = []

    def hit():
        codes.append(_consume(_rid(), leaf, 1).status_code)

    threads = [threading.Thread(target=hit) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert codes.count(200) == 10
    assert codes.count(429) == 20
    assert client.get(f"/v1/buckets/{root}/usage").json()["total_used"] == 10
    assert client.get(f"/v1/buckets/{mid}/usage").json()["total_used"] == 10
    assert client.get(f"/v1/buckets/{leaf}/usage").json()["total_used"] == 10


def test_audit_semantics_unchanged_with_tree_events():
    """树消费/释放/调整的审计语义与扁平模式一致：首次各一条、重放不重复。"""
    root, mid, leaf = _tree3()
    before = client.get("/v1/audit/verify").json()["checked_count"]
    rid = _rid()
    assert _consume(rid, leaf, 2).status_code == 200
    assert _consume(rid, leaf, 2).status_code == 200  # 重放
    rel = _rid()
    assert _release(rel, rid).status_code == 200
    assert _release(rel, rid).status_code == 200      # 重放
    rid2 = _rid()
    assert _consume(rid2, leaf, 2).status_code == 200
    assert _adjust(_rid(), rid2, [(leaf, 1)]).status_code == 200
    after = client.get("/v1/audit/verify").json()["checked_count"]
    assert after - before == 4  # 2 consume + 1 release + 1 adjust
    # 审计链始终完整
    assert client.get("/v1/audit/verify").json()["valid"] is True


def test_simulation_tree_aware_ancestor_denial_and_peaks():
    """回放模拟同样沿配额树校验祖先限额，峰值覆盖贡献链上的全部桶。"""
    root = f"sim-root-{_rid()[:8]}"
    leaf = f"sim-leaf-{_rid()[:8]}"
    _put_bucket(root, 5, 100.0)
    _put_bucket(leaf, 1000, 100.0, parent_key=root)
    base = 12_000_000
    # root 限额 5：叶桶放 5 之后，下一笔叶桶消费在模拟中被祖先拒绝
    _insert_request("tree-a", leaf, 5, base + 1, True)
    _insert_request("tree-b", leaf, 1, base + 2, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["transitions"] == {
        "allowed_to_allowed": 1,
        "allowed_to_denied": 1,
        "denied_to_allowed": 0,
        "denied_to_denied": 0,
    }
    peaks = {p["key"]: p["simulated_peak"] for p in body["bucket_peaks"]}
    # 峰值覆盖叶桶与祖先；被拒请求不占用，峰值停在 5
    assert peaks == {leaf: 5, root: 5}
    flip = body["flip_details"][0]
    assert flip["request_id"] == "tree-b"
    assert flip["simulated_decision"] == "denied"


def test_simulation_tree_shared_ancestor_no_duplicate():
    """模拟中共享祖先的两个后代只累计金额之和，不因展开而重复计数。"""
    root = f"sim-sh-r-{_rid()[:8]}"
    mid = f"sim-sh-m-{_rid()[:8]}"
    a = f"sim-sh-a-{_rid()[:8]}"
    b = f"sim-sh-b-{_rid()[:8]}"
    _put_bucket(root, 100, 100.0)
    _put_bucket(mid, 100, 100.0, parent_key=root)
    _put_bucket(a, 100, 100.0, parent_key=mid)
    _put_bucket(b, 100, 100.0, parent_key=mid)
    base = 13_000_000
    _insert_request_multi(
        "sh-1",
        [{"key": a, "amount": 3}, {"key": b, "amount": 2}],
        base + 1,
        True,
    )
    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    peaks = {p["key"]: p["simulated_peak"]
             for p in r.json()["bucket_peaks"]}
    assert peaks[mid] == 5 and peaks[root] == 5 and peaks[a] == 3 and peaks[b] == 2
    assert r.json()["transitions"]["allowed_to_allowed"] == 1


def test_seed_tree_out_of_order_and_bad_config_rejected():
    """播种支持乱序 parent_key；缺失父桶/环的配置文件整体不生效。"""
    import tempfile

    from app.db import connect as _connect, init_db, seed_buckets

    d = tempfile.mkdtemp(prefix="quota-seed-")
    dbp = os.path.join(d, "seed.db")
    init_db(dbp)  # 与 lifespan 一致：先建表再播种

    good = os.path.join(d, "good.json")
    with open(good, "w") as fh:
        json.dump({"buckets": [
            {"key": "s-leaf", "limit": 1, "window_seconds": 60,
             "parent_key": "s-mid"},
            {"key": "s-root", "limit": 1, "window_seconds": 60},
            {"key": "s-mid", "limit": 1, "window_seconds": 60,
             "parent_key": "s-root"},
        ]}, fh)
    assert sorted(seed_buckets(good, dbp)) == ["s-leaf", "s-mid", "s-root"]
    conn = _connect(dbp)
    try:
        parents = dict(conn.execute(
            "SELECT key, parent_key FROM buckets ORDER BY key"
        ).fetchall())
    finally:
        conn.close()
    assert parents == {"s-leaf": "s-mid", "s-mid": "s-root", "s-root": None}

    bad = os.path.join(d, "bad.json")
    with open(bad, "w") as fh:
        json.dump({"buckets": [
            {"key": "s-leaf", "limit": 9, "window_seconds": 60,
             "parent_key": "missing-parent"},
        ]}, fh)
    import pytest
    with pytest.raises(Exception):
        seed_buckets(bad, dbp)
    conn = _connect(dbp)
    try:
        # 失败播种整体回滚：s-leaf 仍为旧配置且树关系不变
        row = conn.execute(
            "SELECT limit_amount, parent_key FROM buckets WHERE key = 's-leaf'"
        ).fetchone()
    finally:
        conn.close()
    assert row[0] == 1 and row[1] == "s-mid"


# ---------------------------------------------------------------- 预留生命周期

def _reserve(rv: str, items, expires_in: float = 300.0):
    return client.post("/v1/quota/reservations", json={
        "reservation_id": rv,
        "buckets": [{"key": k, "amount": a} for k, a in items],
        "expires_in": expires_in,
    })


def _reserve_one(rv: str, key: str, amount: int, expires_in: float = 300.0):
    return _reserve(rv, [(key, amount)], expires_in)


def _confirm(rv: str, request_id: str | None = "sentinel-omit", body: object = ...):
    """确认预留；request_id 为 None 显式传 null，'sentinel-omit' 表示空 body。"""
    if body is not ...:
        return client.post(f"/v1/quota/reservations/{rv}/confirm", json=body)
    if request_id == "sentinel-omit":
        return client.post(f"/v1/quota/reservations/{rv}/confirm")
    return client.post(f"/v1/quota/reservations/{rv}/confirm",
                       json={"request_id": request_id})


def _cancel(rv: str):
    return client.post(f"/v1/quota/reservations/{rv}/cancel")


def _get_reservation(rv: str):
    return client.get(f"/v1/quota/reservations/{rv}")


def test_reservation_create_holds_chain_capacity():
    """创建预留：返回 201，整条贡献链（叶+祖先）立即占用。"""
    root, mid, leaf = _tree3()
    rv = _rid()
    r = _reserve_one(rv, leaf, 4, expires_in=60)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["reserved"] is True
    assert body["reservation_id"] == rv
    assert body["buckets"] == [{"key": leaf, "amount": 4}]
    assert body["expires_in"] == 60.0
    assert body["expires_at"] > body["reserved_at"]
    assert "X-Idempotent-Replay" not in r.headers
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 4

    g = _get_reservation(rv).json()
    assert g["status"] == "held"
    assert g["request_id"] is None
    assert g["buckets"] == [{"key": leaf, "amount": 4}]
    assert g["expires_in"] == 60.0
    assert g["finalized_at"] is None


def test_reservation_blocks_subsequent_consume_at_every_layer():
    """持有中的预留计入窗口用量：后续消费在任一层不足即 429。"""
    root, mid, leaf = _tree3(root_limit=10, mid_limit=1000, leaf_limit=1000)
    assert _reserve_one(_rid(), leaf, 8).status_code == 201
    # leaf/mid 充足，root 现存 8 + 3 > 10 → 被祖先拦截
    r = _consume(_rid(), leaf, 3)
    assert r.status_code == 429
    assert [b["key"] for b in r.json()["limited_buckets"]] == [root]
    # 仍在限额内的消费放行
    assert _consume(_rid(), leaf, 2).status_code == 200
    assert client.get(f"/v1/buckets/{root}/usage").json()["total_used"] == 10


def test_reservation_create_over_limit_429_nothing_held():
    """创建时任一层不足 → 429，整体不占用且不固化（可稍后重试）。"""
    root, mid, leaf = _tree3(root_limit=10)
    rv = _rid()
    r = _reserve_one(rv, leaf, 11)
    assert r.status_code == 429
    body = r.json()
    assert body["reserved"] is False
    assert [b["key"] for b in body["limited_buckets"]] == [root]
    assert body["retry_at"] is None
    assert "Retry-After" not in r.headers
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0
    # 未固化：同 ID 可重新提交（非幂等冲突）
    assert _reserve_one(rv, leaf, 5).status_code == 201


def test_reservation_create_unknown_bucket_404():
    ghost = f"ghost-{_rid()[:10]}"
    r = _reserve_one(_rid(), ghost, 1)
    assert r.status_code == 404
    assert ghost in r.json()["detail"]


def test_reservation_create_replay_same_payload():
    """同 reservation_id 同载荷重放：原样返回、不重复占用。"""
    key = _bucket(10, 60)
    rv = _rid()
    r1 = _reserve_one(rv, key, 3, expires_in=90)
    assert r1.status_code == 201
    r2 = _reserve_one(rv, key, 3, expires_in=90)
    assert r2.status_code == 201
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_reservation_create_changed_payload_409():
    """同 reservation_id 改换桶列表或 expires_in → 409，原占用不变。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, k1, 3, expires_in=60).status_code == 201
    # 改桶金额
    assert _reserve_one(rv, k1, 4, expires_in=60).status_code == 409
    # 改桶
    assert _reserve_one(rv, k2, 3, expires_in=60).status_code == 409
    # 仅改 expires_in 也算改换载荷
    assert _reserve_one(rv, k1, 3, expires_in=61).status_code == 409
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 3
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_reservation_create_replay_returns_frozen_result_after_terminal():
    """终态后重放创建请求：仍返回首次 201 固化结果，不重复占用（同 consume）。"""
    key = _bucket(10, 60)
    rv = _rid()
    r1 = _reserve_one(rv, key, 3, expires_in=60)
    assert r1.status_code == 201
    assert _confirm(rv).status_code == 200
    r2 = _reserve_one(rv, key, 3, expires_in=60)
    assert r2.status_code == 201
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3
    # consume 重放同名正式消费同样幂等
    assert _consume(rv, key, 3).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_reservation_confirm_converts_to_consume_without_double_charge():
    """确认：事件转挂为正式消费，容量零增量，并生成可释放的 request_id。"""
    root, mid, leaf = _tree3()
    rv = _rid()
    assert _reserve_one(rv, leaf, 5, expires_in=60).status_code == 201

    r = _confirm(rv)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["confirmed"] is True
    # 默认正式消费 ID 取 reservation_id
    assert body["request_id"] == rv
    assert body["buckets"] == [{"key": leaf, "amount": 5}]
    assert body["events_consumed"] == 3  # leaf direct + mid/root rollup
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 5

    g = _get_reservation(rv).json()
    assert g["status"] == "confirmed"
    assert g["request_id"] == rv
    assert g["finalized_at"] is not None

    # 转为正式消费后可被 releases 释放；释放后整条链归零
    assert _release(_rid(), rv).status_code == 200
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0


def test_reservation_confirm_with_explicit_request_id():
    """确认时显式指定 request_id；释放/调整按该 ID 作用于正式消费。"""
    key = _bucket(10, 60)
    rv, rid = _rid(), _rid()
    assert _reserve_one(rv, key, 4).status_code == 201
    r = _confirm(rv, request_id=rid)
    assert r.status_code == 200
    assert r.json()["request_id"] == rid
    # 默认 ID（rv）不是正式消费
    assert _release(_rid(), rv).status_code == 404
    assert _release(_rid(), rid).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_reservation_confirm_empty_body_and_null_body_default_id():
    """空 body 与显式 {"request_id": null} 都默认取 reservation_id。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 2).status_code == 201
    assert _confirm(rv, body=None).status_code == 200
    assert _get_reservation(rv).json()["request_id"] == rv


def test_reservation_confirm_replay_idempotent():
    """确认重放：原样返回、不二次扣量、不重复审计。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 3).status_code == 201
    r1 = _confirm(rv)
    assert r1.status_code == 200
    # 再制造一笔占用，重放确认不得影响它
    assert _consume(_rid(), key, 2).status_code == 200
    r2 = _confirm(rv)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5


def test_reservation_confirm_retarget_request_id_409():
    """已确认预留改换 request_id → 409。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 2).status_code == 201
    assert _confirm(rv, request_id="formal-1").status_code == 200
    r = _confirm(rv, request_id="formal-2")
    assert r.status_code == 409
    # 首次确认保持有效
    assert _get_reservation(rv).json()["request_id"] == "formal-1"


def test_reservation_confirm_request_id_collision_409():
    """指定的正式 request_id 已被其他消费占用 → 409，预留仍为 held。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    rv = _rid()
    assert _reserve_one(rv, key, 1).status_code == 201
    assert _confirm(rv, request_id=rid).status_code == 409
    assert _get_reservation(rv).json()["status"] == "held"
    # 换一个未占用 ID 即可成功
    assert _confirm(rv, request_id=_rid()).status_code == 200


def test_reservation_confirm_unknown_404():
    assert _confirm(f"missing-{_rid()}").status_code == 404


def test_reservation_cancel_releases_full_chain():
    """取消：删除整条贡献链事件，各层用量同步恢复。"""
    root, mid, leaf = _tree3()
    rv = _rid()
    assert _reserve_one(rv, leaf, 6, expires_in=60).status_code == 201
    r = _cancel(rv)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cancelled"] is True
    assert body["released_buckets"] == [{"key": leaf, "amount": 6}]
    assert body["events_reverted"] == 3
    for k in (root, mid, leaf):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0
    assert _get_reservation(rv).json()["status"] == "cancelled"


def test_reservation_cancel_frees_capacity_for_consume():
    key = _bucket(3, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 3).status_code == 201
    assert _consume(_rid(), key, 1).status_code == 429
    assert _cancel(rv).status_code == 200
    assert _consume(_rid(), key, 3).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_reservation_cancel_replay_idempotent():
    """取消重放：原样返回，不再改动用量。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 4).status_code == 201
    r1 = _cancel(rv)
    assert r1.status_code == 200
    assert _consume(_rid(), key, 2).status_code == 200
    r2 = _cancel(rv)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 2


def test_reservation_cancel_unknown_404():
    assert _cancel(f"missing-{_rid()}").status_code == 404


def test_reservation_confirm_after_cancel_409():
    """终态唯一：取消后确认 → 409。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 2).status_code == 201
    assert _cancel(rv).status_code == 200
    assert _confirm(rv).status_code == 409


def test_reservation_cancel_after_confirm_409():
    """终态唯一：确认后取消 → 409（应走 releases）。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 2).status_code == 201
    assert _confirm(rv).status_code == 200
    assert _cancel(rv).status_code == 409
    # 容量仍被正式消费持有
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 2


def test_reservation_expiry_lazily_released_on_consume():
    """过期预留在后续消费中惰性释放：容量恢复、状态变 expired、审计留痕。"""
    root, mid, leaf = _tree3(root_limit=10)
    rv = _rid()
    assert _reserve_one(rv, leaf, 10, expires_in=0.2).status_code == 201
    assert _consume(_rid(), leaf, 10).status_code == 429  # 仍被预留占用
    time.sleep(0.3)
    # 相关操作（consume）触发惰性过期
    assert _consume(_rid(), leaf, 10).status_code == 200
    g = _get_reservation(rv).json()
    assert g["status"] == "expired"
    assert g["finalized_at"] is not None
    assert client.get(f"/v1/buckets/{root}/usage").json()["total_used"] == 10


def test_reservation_expiry_on_confirm_returns_409():
    """确认已过期预留 → 409，且容量已被惰性释放。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 10, expires_in=0.2).status_code == 201
    time.sleep(0.3)
    assert _confirm(rv).status_code == 409
    assert _get_reservation(rv).json()["status"] == "expired"
    # 释放出的容量立即可用
    assert _consume(_rid(), key, 10).status_code == 200


def test_reservation_expiry_on_cancel_returns_409_and_idempotent():
    """取消已过期预留 → 409；重复取消仍 409（终态 expired 唯一）。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 10, expires_in=0.2).status_code == 201
    time.sleep(0.3)
    assert _cancel(rv).status_code == 409
    assert _cancel(rv).status_code == 409
    assert _get_reservation(rv).json()["status"] == "expired"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_reservation_expiry_on_get():
    """查询预留本身也会触发惰性过期。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 10, expires_in=0.2).status_code == 201
    time.sleep(0.3)
    g = _get_reservation(rv)
    assert g.status_code == 200
    assert g.json()["status"] == "expired"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_reservation_expiry_on_create_frees_capacity():
    """创建新预留时惰性释放到期预留，腾退容量供本次占用。"""
    key = _bucket(5, 60)
    rv_old = _rid()
    assert _reserve_one(rv_old, key, 5, expires_in=0.2).status_code == 201
    assert _reserve_one(_rid(), key, 5).status_code == 429  # 旧预留仍有效
    time.sleep(0.3)
    rv_new = _rid()
    r = _reserve_one(rv_new, key, 5)
    assert r.status_code == 201, r.text
    assert _get_reservation(rv_old).json()["status"] == "expired"
    assert _get_reservation(rv_new).json()["status"] == "held"


def test_reservation_expired_capacity_freed_even_on_failed_operation():
    """过期清扫独立提交：后续操作失败（404）也不回滚已完成的过期释放。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 10, expires_in=0.2).status_code == 201
    time.sleep(0.3)
    # 一笔必失败的操作触发清扫
    assert _consume(_rid(), f"nope-{_rid()[:8]}", 1).status_code == 404
    assert _get_reservation(rv).json()["status"] == "expired"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_reservation_multi_bucket_all_or_nothing():
    """多桶预留任一不足：全部不占用。"""
    k1, k2 = _bucket(10, 60), _bucket(5, 60)
    rv = _rid()
    r = _reserve(rv, [(k1, 1), (k2, 6)])
    assert r.status_code == 429
    assert [b["key"] for b in r.json()["limited_buckets"]] == [k2]
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_reservation_shared_ancestor_holds_sum_once():
    """多桶预留共享祖先：祖先只占用金额之和；确认/取消同样整条处理。"""
    root, mid, leaf = _tree3()
    sibling = f"t-sib-{_rid()[:10]}"
    _put_bucket(sibling, 1000, parent_key=mid)
    rv = _rid()
    assert _reserve(rv, [(leaf, 3), (sibling, 2)]).status_code == 201
    assert client.get(f"/v1/buckets/{mid}/usage").json()["total_used"] == 5
    assert client.get(f"/v1/buckets/{root}/usage").json()["total_used"] == 5

    r = _cancel(rv)
    assert r.status_code == 200
    # 2 个直接桶 + mid/root 各一条汇总 = 4 条事件
    assert r.json()["events_reverted"] == 4
    for k in (root, mid, leaf, sibling):
        assert client.get(f"/v1/buckets/{k}/usage").json()["total_used"] == 0


def test_concurrent_reservations_never_exceed_limit():
    """30 个并发预留争抢 limit=10：恰好创建成功 10 个，用量绝不超限。"""
    key = _bucket(10, 60)
    codes = []
    barrier = threading.Barrier(30)

    def hit():
        barrier.wait()
        codes.append(_reserve_one(_rid(), key, 1).status_code)

    threads = [threading.Thread(target=hit) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert codes.count(201) == 10
    assert codes.count(429) == 20
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 10


def test_concurrent_confirm_and_cancel_only_one_terminal_state():
    """同一预留并发确认/取消：只有一个终态，容量不丢不重。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 5).status_code == 201
    outcomes: list[tuple[str, int]] = []
    barrier = threading.Barrier(20)

    def confirm_hit():
        barrier.wait()
        outcomes.append(("confirm", _confirm(rv, request_id=_rid()).status_code))

    def cancel_hit():
        barrier.wait()
        outcomes.append(("cancel", _cancel(rv).status_code))

    threads = [threading.Thread(target=confirm_hit) for _ in range(10)]
    threads += [threading.Thread(target=cancel_hit) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    status = _get_reservation(rv).json()["status"]
    assert status in ("confirmed", "cancelled")
    confirms = [c for op, c in outcomes if op == "confirm"]
    cancels = [c for op, c in outcomes if op == "cancel"]
    if status == "confirmed":
        assert confirms.count(200) == 1
        assert set(cancels) == {409}
        assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5
    else:
        # 取消无请求载荷：首个取消完成状态迁移，其余同预留的重复取消按
        # 幂等原样返回 200（与 releases 的同 ID 并发语义一致），但只释放一次
        assert set(confirms) == {409}
        assert all(c == 200 for c in cancels)
        assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_concurrent_same_reservation_confirm_single_success():
    """同预留并发确认（各自不同 request_id）：只有一个 request_id 胜出。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 5).status_code == 201
    outcomes: list[int] = []
    barrier = threading.Barrier(15)

    def hit():
        barrier.wait()
        outcomes.append(_confirm(rv, request_id=_rid()).status_code)

    threads = [threading.Thread(target=hit) for _ in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert outcomes.count(200) == 1
    assert outcomes.count(409) == 14
    # 只转挂一次：用量仍为 5
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5
    assert _get_reservation(rv).json()["status"] == "confirmed"


def test_concurrent_same_reservation_cancel_idempotent():
    """同一预留并发重复取消：全部 200，只释放一次。"""
    key = _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 5).status_code == 201
    codes = []
    barrier = threading.Barrier(15)

    def hit():
        barrier.wait()
        codes.append(_cancel(rv).status_code)

    threads = [threading.Thread(target=hit) for _ in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_reservation_audit_create_and_terminal_states():
    """审计记录首次创建成功 + 最终状态变更，各一条；重放不重复。"""
    # 三个预留分别走向 confirmed / cancelled / expired
    k_conf, k_canc, k_exp = _bucket(10, 60), _bucket(10, 60), _bucket(10, 60)
    rv_conf, rv_canc, rv_exp = _rid(), _rid(), _rid()
    before = client.get("/v1/audit/verify").json()["checked_count"]

    assert _reserve_one(rv_conf, k_conf, 1).status_code == 201
    assert _reserve_one(rv_conf, k_conf, 1).status_code == 201  # 重放
    assert _reserve_one(rv_canc, k_canc, 1).status_code == 201
    assert _reserve_one(rv_exp, k_exp, 1, expires_in=0.2).status_code == 201

    assert _confirm(rv_conf).status_code == 200
    assert _confirm(rv_conf).status_code == 200  # 重放不重复
    assert _cancel(rv_canc).status_code == 200
    assert _cancel(rv_canc).status_code == 200   # 重放不重复

    time.sleep(0.3)
    _expire_due = client.get(f"/v1/quota/reservations/{rv_exp}")
    assert _expire_due.json()["status"] == "expired"

    recs = _audit(before, 100).json()["records"]
    by_biz = {}
    for rec in recs:
        ev = rec["event"]
        assert ev["op"] == "reserve"
        assert set(ev) == {"op", "business_id", "result", "occurred_at"}
        by_biz.setdefault(ev["business_id"], []).append(ev["result"])
    assert by_biz[rv_conf] == ["reserved", "confirmed"]
    assert by_biz[rv_canc] == ["reserved", "cancelled"]
    assert by_biz[rv_exp] == ["reserved", "expired"]

    # 终态冲突操作不留痕
    n = client.get("/v1/audit/verify").json()["checked_count"]
    assert _confirm(rv_canc).status_code == 409
    assert _cancel(rv_conf).status_code == 409
    assert client.get("/v1/audit/verify").json()["checked_count"] == n
    # 链始终完整
    assert client.get("/v1/audit/verify").json()["valid"] is True


def test_reservation_create_failure_not_recorded():
    """创建失败（429/404/409）不产生审计记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]
    key = _bucket(1, 60)
    rv = _rid()
    assert _reserve_one(rv, key, 1).status_code == 201
    assert _reserve_one(_rid(), key, 1).status_code == 429  # 容量不足
    assert _reserve_one(_rid(), f"nope-{_rid()[:8]}", 1).status_code == 404
    assert _reserve_one(rv, key, 1, expires_in=120).status_code == 409  # 改 expires_in
    after = client.get("/v1/audit/verify").json()["checked_count"]
    assert after - before == 1


def test_reservation_429_retry_after_reflects_window_expiry():
    """预留创建 429 的 Retry-After 反映窗口内最早过期时刻。"""
    key = _bucket(2, 0.5)
    assert _consume(_rid(), key, 2).status_code == 200
    r = _reserve_one(_rid(), key, 1)
    assert r.status_code == 429
    body = r.json()
    assert 0 < body["retry_after_seconds"] <= 0.5
    assert "Retry-After" in r.headers


def test_reservation_amount_over_limit_retry_null():
    key = _bucket(5, 60)
    r = _reserve_one(_rid(), key, 6)
    assert r.status_code == 429
    assert r.json()["retry_at"] is None


def test_reservation_then_adjust_confirmed_consumption():
    """确认后的正式消费可被 adjustments 调整（按新贡献链替换）。"""
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rv = _rid()
    assert _reserve_one(rv, k1, 6).status_code == 201
    assert _confirm(rv).status_code == 200
    assert _adjust(_rid(), rv, [(k2, 4)]).status_code == 200
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 4
    # 预留行仍为 confirmed，不随调整改变
    assert _get_reservation(rv).json()["status"] == "confirmed"


def test_reservation_validation_errors():
    """参数校验：expires_in 必须为正；桶列表非空。"""
    key = _bucket(5, 60)
    assert client.post("/v1/quota/reservations", json={
        "reservation_id": _rid(),
        "buckets": [{"key": key, "amount": 1}],
        "expires_in": 0,
    }).status_code == 422
    assert client.post("/v1/quota/reservations", json={
        "reservation_id": _rid(), "buckets": [], "expires_in": 60,
    }).status_code == 422
    assert client.post("/v1/quota/reservations", json={
        "reservation_id": "",
        "buckets": [{"key": key, "amount": 1}], "expires_in": 60,
    }).status_code == 422
    assert client.post("/v1/quota/reservations", json={
        "reservation_id": _rid(),
        "buckets": [{"key": key, "amount": 1}],
    }).status_code == 422


def test_legacy_usage_events_schema_migration():
    """旧版 usage_events（request_id NOT NULL、无 reservation_id/direct）可迁移。"""
    import tempfile

    from app.db import connect as _connect, init_db

    d = tempfile.mkdtemp(prefix="quota-legacy-")
    dbp = os.path.join(d, "legacy.db")

    # 手工构造一个最旧形态的库：buckets + 仅含基础列的 usage_events
    conn = _connect(dbp)
    try:
        conn.execute(
            "CREATE TABLE buckets (key TEXT PRIMARY KEY, limit_amount INTEGER "
            "NOT NULL CHECK (limit_amount > 0), window_seconds REAL NOT NULL "
            "CHECK (window_seconds > 0), parent_key TEXT)"
        )
        conn.execute(
            "CREATE TABLE usage_events (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "bucket_key TEXT NOT NULL, request_id TEXT NOT NULL, "
            "amount INTEGER NOT NULL, created_at REAL NOT NULL)"
        )
        conn.execute(
            "INSERT INTO buckets (key, limit_amount, window_seconds, parent_key) "
            "VALUES ('legacy-b', 10, 60.0, NULL)"
        )
        conn.execute(
            "INSERT INTO usage_events (bucket_key, request_id, amount, created_at) "
            "VALUES ('legacy-b', 'old-req', 3, 1.0)"
        )
    finally:
        conn.close()

    init_db(dbp)  # 触发建表 + 迁移

    conn = _connect(dbp)
    try:
        cols = {r["name"]: r for r in conn.execute("PRAGMA table_info(usage_events)")}
        assert "reservation_id" in cols
        assert cols["request_id"]["notnull"] == 0
        # direct 列已按默认 1 补齐，结构与新建表一致（NOT NULL DEFAULT 1）
        assert cols["direct"]["dflt_value"] == "1"
        row = conn.execute(
            "SELECT request_id, reservation_id, amount, direct "
            "FROM usage_events"
        ).fetchone()
        assert row["request_id"] == "old-req"
        assert row["reservation_id"] is None
        assert row["amount"] == 3
        assert row["direct"] == 1  # 旧数据按直接消费补齐
        # 迁移后新写入允许 request_id 为 NULL 的预留事件
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO usage_events (bucket_key, request_id, reservation_id, "
            "amount, created_at, direct) VALUES ('legacy-b', NULL, 'rv-x', 2, 2.0, 1)"
        )
        conn.execute("COMMIT")
        total = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS s FROM usage_events"
        ).fetchone()["s"]
        assert total == 5
    finally:
        conn.close()
