FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY buckets.json ./buckets.json

# 数据库文件落在挂载卷 /data；桶配置随镜像内置，也可用环境变量替换
ENV QUOTA_DB_PATH=/data/quota.db \
    QUOTA_BUCKETS_CONFIG=/app/buckets.json

RUN useradd --create-home appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
