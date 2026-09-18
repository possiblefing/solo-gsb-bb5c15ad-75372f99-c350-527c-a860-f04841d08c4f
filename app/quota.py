"""滚动窗口配额核心逻辑（纯函数，便于单测）。"""
from __future__ import annotations

import hashlib
import json


class ParentError(ValueError):
    """桶父子关系非法：缺失父桶 / 自引用 / 直接或间接成环。"""


def merge_items(items: list[dict]) -> list[dict]:
    """合并同一桶的重复条目，并按 key 排序，得到规范化载荷。"""
    merged: dict[str, int] = {}
    for it in items:
        merged[it["key"]] = merged.get(it["key"], 0) + it["amount"]
    return [{"key": k, "amount": v} for k, v in sorted(merged.items())]


def payload_hash(items: list[dict]) -> str:
    """对规范化后的桶列表取哈希，用于幂等比对。"""
    canonical = json.dumps(items, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def ancestor_chain(
    key: str, parents: dict[str, str | None]
) -> list[str]:
    """返回 key 的全部祖先（父 → 祖父 → …），不含自身。

    调用方须保证 ``parents`` 描述的图无环（见 :func:`validate_parent_graph`）；
    为防御陈旧数据，遇到环（再次见到已访问节点）即停止，不重复返回。
    """
    chain: list[str] = []
    seen = {key}
    cur = parents.get(key)
    while cur is not None and cur not in seen:
        chain.append(cur)
        seen.add(cur)
        cur = parents.get(cur)
    return chain


def validate_parent_assignment(
    key: str,
    parent_key: str | None,
    parents: dict[str, str | None],
) -> None:
    """校验把 key 的父桶改为 parent_key 是否合法（在最终图上）。

    ``parents`` 为**改动生效后**的完整父子映射：所有桶都须在键集合中，
    根桶值为 None（如 PUT/种子已在同一事务内落库，直接查全表即可）。

    - parent_key == key：自引用 → ParentError；
    - parent_key 不是任何现存桶的 key：缺失父桶 → ParentError；
    - key 出现在 parent_key 的祖先链上（直接/间接环）→ ParentError。
    """
    if parent_key is None:
        return
    if parent_key == key:
        raise ParentError(f"桶 '{key}' 的 parent_key 不能指向自身")
    if parent_key not in parents:
        raise ParentError(
            f"父桶 '{parent_key}' 不存在，请先创建该桶后再配置 parent_key"
        )
    # 间接环：沿父链向上若再次经过 key，则 key 成为自己的后代
    cur = parent_key
    seen = {key}
    while cur is not None:
        if cur in seen:
            raise ParentError(
                f"配置 parent_key='{parent_key}' 会使桶 '{key}' 形成配额环"
                f"（桶 '{cur}' 已在链上），禁止直接或间接引用自己的后代"
            )
        seen.add(cur)
        cur = parents.get(cur)


def validate_parent_graph(parents: dict[str, str | None]) -> None:
    """对全量父子映射做合法性校验：缺失父桶、自引用与任何环。

    ``parents`` 须包含**全部**桶（根桶值为 None）；父键不在键集合中即
    判定为缺失父桶。
    """
    for key, parent_key in parents.items():
        validate_parent_assignment(key, parent_key, parents)


def expand_contributions(
    items: list[dict], parents: dict[str, str | None]
) -> dict[str, dict[str, int]]:
    """把请求的直接桶列表展开为"直接 + 祖先汇总"的完整贡献链。

    :param items: 已规范化（merge_items 后）的 ``[{"key", "amount"}]``。
    :param parents: 当前配额树父子映射。
    :returns: ``{bucket_key: {"direct": 直接点名金额, "rollup": 后代汇总
        金额, "total": 二者之和}}``，包含全部直接桶与受影响祖先，
        按 key 升序。一次请求包含共享祖先的多个（直接或间接）后代时，
        祖先只累计各后代金额之和——每条祖先链各自贡献一次，天然不重复。
    """
    agg: dict[str, dict[str, int]] = {}

    def add(bucket_key: str, amount: int, *, direct: bool) -> None:
        slot = agg.setdefault(bucket_key, {"direct": 0, "rollup": 0})
        slot["direct" if direct else "rollup"] += amount

    for item in items:
        key = item["key"]
        amount = item["amount"]
        add(key, amount, direct=True)
        for anc in ancestor_chain(key, parents):
            add(anc, amount, direct=False)

    return {
        key: {**slot, "total": slot["direct"] + slot["rollup"]}
        for key, slot in sorted(agg.items())
    }


def earliest_retry_at(
    events: list[tuple[float, int]],
    limit: int,
    window: float,
    amount: int,
    now: float,
) -> float | None:
    """计算该桶最早可重试的 epoch 秒。

    events 为窗口内 (created_at, amount) 列表。窗口随事件过期而滚动释放，
    找到累计释放量足以容纳本次请求的最早过期时刻即可。
    返回 None 表示请求量本身超限，永远不可能成功。
    """
    if amount > limit:
        return None
    used = sum(a for _, a in events)
    if used + amount <= limit:
        return now
    need = used + amount - limit
    freed = 0
    for ts, a in sorted(events):
        freed += a
        if freed >= need:
            return ts + window
    return None  # pragma: no cover - 逻辑上不可达（need <= used）
