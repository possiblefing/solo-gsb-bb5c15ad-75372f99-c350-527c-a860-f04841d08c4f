"""请求/响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class BucketItem(BaseModel):
    key: str = Field(min_length=1, max_length=128)
    amount: int = Field(gt=0, le=2**31 - 1)


class ConsumeRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    buckets: list[BucketItem] = Field(min_length=1, max_length=100)


class ReleaseRequest(BaseModel):
    release_id: str = Field(min_length=1, max_length=128)
    request_id: str = Field(min_length=1, max_length=128)


class AdjustmentRequest(BaseModel):
    adjustment_id: str = Field(min_length=1, max_length=128)
    request_id: str = Field(min_length=1, max_length=128)
    buckets: list[BucketItem] = Field(min_length=1, max_length=100)


class BucketConfig(BaseModel):
    limit: int = Field(gt=0, le=2**31 - 1)
    window_seconds: float = Field(gt=0, le=365 * 24 * 3600)
    # 可选父桶：省略或显式 null 都表示根桶（PUT 为整体替换语义，
    # 未携带即清空父级）；非空时在事务内校验存在性与无环
    parent_key: str | None = Field(
        default=None, min_length=1, max_length=128
    )


class SimulationBucketOverride(BaseModel):
    """模拟时覆盖某个桶的候选 limit/window_seconds（必须成对给出）。"""

    key: str = Field(min_length=1, max_length=128)
    limit: int = Field(gt=0, le=2**31 - 1)
    window_seconds: float = Field(gt=0, le=365 * 24 * 3600)


class SimulationRequest(BaseModel):
    """配额回放模拟请求：epoch 秒区间 [from, to) 与若干桶的候选配置。"""

    # from 是 Python 关键字，字段名取 from_ 并以 "from" 作为对外别名
    from_: int = Field(alias="from", ge=0)
    to: int = Field(ge=0)
    buckets: list[SimulationBucketOverride] = Field(default_factory=list, max_length=100)

    model_config = {"populate_by_name": True}
