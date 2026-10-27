"""Cliente MinIO (S3) para la capa de almacenamiento del lakehouse.

Capas:
  bronze/  → JSON tal como llega de la fuente, particionado por día
  silver/  → parquet limpio y tipado (lo escribe Spark)
  gold/    → parquet de agregados listos para consumo (lo escribe Spark)
"""
from __future__ import annotations

import io
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config as BotoConfig

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def get_s3_client() -> Any:
    """Cliente S3 apuntando a MinIO (credenciales solo desde entorno)."""
    return boto3.client(
        "s3",
        endpoint_url=_env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=_env("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=_env("MINIO_SECRET_KEY", "minioadmin"),
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def bucket_list() -> list[str]:
    buckets = _env("MINIO_BUCKETS", "bronze,silver,gold")
    return [b.strip() for b in buckets.split(",") if b.strip()]


def ensure_buckets() -> None:
    """Crea los buckets si no existen (idempotente)."""
    client = get_s3_client()
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for name in bucket_list():
        if name not in existing:
            client.create_bucket(Bucket=name)
            logger.info("Bucket creado: %s", name)


def write_bronze_batch(source_id: str, events: list[dict[str, Any]],
                       fetched_at: datetime | None = None) -> str:
    """Escribe un lote de eventos crudos en bronze/<source>/dt=YYYY-MM-DD/.

    Devuelve la clave S3 del objeto. Cada ejecución escribe un objeto
    nuevo (append-only); la partición por día facilita el reproceso.
    """
    ensure_buckets()
    client = get_s3_client()
    ts = fetched_at or datetime.now(timezone.utc)
    key = (
        f"{source_id}/dt={ts:%Y-%m-%d}/"
        f"{ts:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.json"
    )
    payload = json.dumps(events, default=str, ensure_ascii=False)
    client.put_object(
        Bucket="bronze",
        Key=key,
        Body=io.BytesIO(payload.encode("utf-8")),
        ContentType="application/json",
    )
    logger.info("bronze: %d eventos → s3://bronze/%s", len(events), key)
    return key


def list_bronze_keys(source_id: str | None = None,
                     prefix: str = "") -> list[str]:
    client = get_s3_client()
    prefix_full = f"{source_id or ''}" if source_id else ""
    if prefix:
        prefix_full = f"{prefix_full.rstrip('/')}/{prefix}"
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket="bronze", Prefix=prefix_full):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys
