"""运行时配置（全部可通过环境变量覆盖）。"""
from __future__ import annotations

import os

# SQLite 数据库文件路径；Docker 中默认指向挂载卷 /data
DB_PATH = os.environ.get("QUOTA_DB_PATH", "quota.db")

# 启动时用于播种/更新桶配置的 JSON 文件；置空字符串则跳过
BUCKETS_CONFIG = os.environ.get("QUOTA_BUCKETS_CONFIG", "buckets.json")

# SQLite 忙等待超时（秒）：并发写事务在此时间内排队而非直接报错
BUSY_TIMEOUT_MS = int(os.environ.get("QUOTA_BUSY_TIMEOUT_MS", "30000"))
