"""DAG de ingesta: extrae el feed de USGS cada 5 minutos.

Escritura dual:
  - raw.events (PostgreSQL): registro operativo (frescura, idempotencia).
  - bronze/ (MinIO): archivo JSON particionado por día → lakehouse que
    procesa Spark.

Al terminar emite el Dataset `sismowatch/raw/events`, que dispara el DAG
de transformación (Spark). Las fuentes se leen de ops.sources en tiempo
de ejecución (dynamic task mapping): añadir una fuente = insertar una fila.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

from callbacks import on_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "sismowatch",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="ingesta_usgs",
    description="Extrae las fuentes activas → raw (PG) + bronze (MinIO)",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "ingesta", "minio"],
    doc_md="""
### Ingesta (medallón: bronze)
Para cada fuente activa en `ops.sources` (dynamic task mapping):
1. El conector descarga con reintentos (lógica en `include/connectors`).
2. Upsert idempotente en `raw.events` (PostgreSQL).
3. Escritura append-only del lote JSON en `s3://bronze/<source>/dt=YYYY-MM-DD/`.
4. Al final se emite el Dataset `sismowatch/raw/events` → transformación.

Si la API no responde tras los reintentos, la tarea falla explícitamente
y `on_failure_callback` dispara la alerta.
""",
)
def ingesta_usgs():

    @task
    def list_active_sources() -> list[dict]:
        """Fuentes activas leídas en runtime (no en el parseo del DAG)."""
        from db import fetch_all
        return fetch_all(
            "transform",
            "SELECT source_id, connector_type, config FROM ops.sources "
            "WHERE is_active = true ORDER BY source_id",
        )

    @task(
        task_group=TaskGroup("extraccion_y_carga"),
        retries=3,
        max_active_tis_per_dag=2,
    )
    def ingest_source(source: dict) -> int:
        """Extrae y carga en raw (PG) + bronze (MinIO) sin XCom de payload."""
        from connectors import build_connector
        from connectors.base import ConnectorError, payload_hash
        from db import get_conn
        from minio_client import write_bronze_batch

        connector = build_connector(
            source["source_id"], source["connector_type"], source["config"])
        try:
            result = connector.extract()
        except ConnectorError as exc:
            raise AirflowException(
                f"Fuente '{source['source_id']}': {exc}") from exc

        events = [
            {**ev, "source_id": source["source_id"]}
            for ev in result.events
            if ev.get("event_uid") is not None
        ]
        if not events:
            logger.warning("[%s] feed sin eventos", source["source_id"])
            return 0

        # 1) PostgreSQL (raw): upsert idempotente
        rows = [
            (source["source_id"], str(ev.get("event_uid")),
             json.dumps(ev, default=str, ensure_ascii=False),
             payload_hash(ev))
            for ev in events
        ]
        with get_conn("ingest") as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.events (source_id, event_uid, payload, payload_hash)
                VALUES (%s, %s, %s::jsonb, %s)
                ON CONFLICT (source_id, event_uid) DO UPDATE
                   SET payload = EXCLUDED.payload,
                       payload_hash = EXCLUDED.payload_hash,
                       ingested_at = now()
                 WHERE raw.events.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash
                """,
                rows,
            )

        # 2) MinIO (bronze): archivo JSON particionado por día
        write_bronze_batch(source["source_id"], events)

        logger.info("[%s] %d eventos → raw + bronze", source["source_id"],
                    len(rows))
        return len(rows)

    @task(outlets=[Dataset("sismowatch/raw/events")])
    def emit_raw_dataset(counts: list[int]) -> int:
        """Emite el Dataset que dispara la transformación."""
        total = sum(c or 0 for c in counts)
        logger.info("ingesta completa: %d eventos", total)
        return total

    sources = list_active_sources()
    counts = ingest_source.expand(source=sources)
    emit_raw_dataset(counts)


ingesta_usgs()
