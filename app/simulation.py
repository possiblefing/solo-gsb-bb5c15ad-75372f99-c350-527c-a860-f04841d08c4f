"""配额配置回放模拟（纯内存、纯函数，不触碰数据库与审计链）。

给定一段历史时间区间 ``[start, end)``（epoch 秒）与候选桶配置，把
``requests`` 表中**首次消费**固化的载荷按 ``(created_at, request_id)``
稳定顺序回放：

- 滚动窗口过期与"多桶全放行/全拒绝"规则与线上 ``consume`` 完全一致，
  但所有状态只保存在本模块的内存结构中；
- 配额树规则与线上一致：消费叶桶时沿 ``parents`` 父链向全部祖先汇总
  用量并逐层校验限额，共享祖先的多个后代只累计金额之和（见
  ``app.quota.expand_contributions``）；
- 只回放消费（放行才占额度，拒绝不占），不处理 release——即回放的是
  "首次消费判定形成的原始用量流"，释放在模拟中忽略；
- ``start`` 之前再取各桶生效窗口的最大值作为预热期，预热记录只建立
  滚动窗口状态、不计任何统计；
- 区间内同时给出"实际固化判定 → 模拟判定"的转移计数、每桶模拟峰值
  用量，以及按稳定顺序截取的前 100 条判定翻转明细。
"""
from __future__ import annotations

from collections import deque
from typing import Any, Deque

from .quota import expand_contributions

# 判定翻转明细最多返回条数
FLIP_DETAIL_LIMIT = 100

# 允许模拟的最大区间跨度（秒）：7 天
MAX_RANGE_SECONDS = 7 * 24 * 3600


class _BucketState:
    """单个桶在模拟中的滚动窗口状态。

    events 保持 created_at 升序，仅保留严格晚于 ``ts - window`` 的事件
    （与线上 ``created_at > now - window`` 的判定一致）。
    """

    __slots__ = ("limit", "window", "events", "used", "peak")

    def __init__(self, limit: int, window: float):
        self.limit = limit
        self.window = window
        self.events: Deque[tuple[float, int]] = deque()
        self.used = 0
        self.peak = 0

    def expire(self, ts: float) -> None:
        cutoff = ts - self.window
        while self.events and self.events[0][0] <= cutoff:
            self.used -= self.events.popleft()[1]


def validate_request(
    from_ts: int,
    to_ts: int,
    overrides: list[dict[str, Any]],
    known_keys: set[str],
) -> dict[str, tuple[int, float]]:
    """校验模拟请求并合并候选配置，失败时由调用方转 422。

    返回候选桶覆盖映射 ``{key: (limit, window)}``。
    """
    if from_ts < 0 or to_ts < 0:
        raise ValueError("from/to 必须为非负 epoch 秒")
    if from_ts >= to_ts:
        raise ValueError(f"非法区间：from ({from_ts}) 必须小于 to ({to_ts})")
    if to_ts - from_ts > MAX_RANGE_SECONDS:
        raise ValueError(
            f"跨度 {to_ts - from_ts} 秒超过上限 {MAX_RANGE_SECONDS} 秒（7 天）"
        )

    effective: dict[str, tuple[int, float]] = {}
    for o in overrides:
        if o["key"] not in known_keys:
            raise ValueError(
                f"未知桶 '{o['key']}'：候选配置只能覆盖已存在的桶，"
                "请先通过 PUT /v1/buckets/{key} 创建"
            )
        if o["key"] in effective:
            raise ValueError(f"候选配置中桶 '{o['key']}' 重复，每个桶至多给出一条")
        effective[o["key"]] = (o["limit"], float(o["window_seconds"]))
    return effective


def resolve_configs(
    db_configs: dict[str, tuple[int, float]],
    overrides: dict[str, tuple[int, float]],
) -> dict[str, tuple[int, float]]:
    """候选配置覆盖当前配置；未覆盖桶沿用当前（数据库）配置。"""
    return {k: overrides.get(k, cfg) for k, cfg in db_configs.items()}


def warmup_seconds(configs: dict[str, tuple[int, float]]) -> float:
    """预热期 = 各桶生效窗口的最大值。"""
    return max((window for _, window in configs.values()), default=0.0)


def run_simulation(
    records: list[tuple[float, str, list[dict[str, Any]], bool]],
    configs: dict[str, tuple[int, float]],
    start: float,
    parents: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """在隔离内存中回放首次消费载荷并重算判定。

    :param records: ``(created_at, request_id, items, actual_allowed)``，
        items 为规范化后的 ``[{"key", "amount"}]``；调用方须保证已按
        ``(created_at, request_id)`` 升序排列，且时间覆盖预热起点。
    :param configs: 全部已配置桶的生效 ``{key: (limit, window)}``。
    :param start: 统计区间起点（epoch 秒）；``created_at < start`` 仅预热。
    :param parents: 配额树父子映射（根桶值为 None）；None 视为全根桶，
        退化为无祖先汇总的扁平行为。
    """
    parents = parents or {}
    states = {
        key: _BucketState(limit, window) for key, (limit, window) in configs.items()
    }

    transitions = {
        "allowed_to_allowed": 0,
        "allowed_to_denied": 0,
        "denied_to_allowed": 0,
        "denied_to_denied": 0,
    }
    flips: list[dict[str, Any]] = []
    warmed = 0
    counted = 0

    def evaluate(ts: float, items: list[dict[str, Any]], measure: bool) -> bool:
        """与线上一致：直接桶 + 全部祖先任一层超限则整体拒绝，全部通过
        才按贡献链整体占用（直接事件 + 每祖先一条汇总事件）。

        measure=True 时在过期后、放行占用后各度量一次占用峰值，使区间
        起点的预热残留与被拒时刻的占用都计入模拟峰值。
        """
        contributions = expand_contributions(items, parents)
        affected = list(contributions)
        for bk in affected:
            st = states[bk]
            st.expire(ts)
            if measure and st.used > st.peak:
                st.peak = st.used
        denied = any(
            states[bk].used + parts["total"] > states[bk].limit
            for bk, parts in contributions.items()
        )
        if denied:
            return False
        for bk, parts in contributions.items():
            st = states[bk]
            st.events.append((ts, parts["total"]))
            st.used += parts["total"]
            if measure and st.used > st.peak:
                st.peak = st.used
        return True

    in_range_keys: set[str] = set()

    for ts, rid, items, actual_allowed in records:
        in_range = ts >= start
        if in_range:
            # 峰值统计覆盖该请求贡献链上的全部桶（直接桶 + 祖先）
            in_range_keys.update(expand_contributions(items, parents))
        sim_allowed = evaluate(ts, items, in_range)
        if not in_range:
            # 预热：只建立滚动窗口状态，不计统计、不计峰值、不收翻转明细
            warmed += 1
            continue

        counted += 1

        if actual_allowed:
            key = "allowed_to_allowed" if sim_allowed else "allowed_to_denied"
        else:
            key = "denied_to_allowed" if sim_allowed else "denied_to_denied"
        transitions[key] += 1

        if actual_allowed != sim_allowed and len(flips) < FLIP_DETAIL_LIMIT:
            flips.append(
                {
                    "created_at": ts,
                    "request_id": rid,
                    "buckets": items,
                    "actual_decision": "allowed" if actual_allowed else "denied",
                    "simulated_decision": "allowed" if sim_allowed else "denied",
                }
            )

    # 仅回放在统计区间回放载荷贡献链上出现过的桶，按 key 稳定输出
    peaks = {
        key: states[key].peak
        for key in sorted(in_range_keys)
    }

    total_flips = (
        transitions["allowed_to_denied"] + transitions["denied_to_allowed"]
    )
    return {
        "transitions": transitions,
        "bucket_peaks": peaks,
        "flips": flips,
        "flip_truncated": total_flips > len(flips),
        "warmup_records": warmed,
        "records_replayed": counted,
    }
