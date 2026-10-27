"""Backfill histórico: carga los últimos N días desde el endpoint FDSN.

Ejecución parametrizada (desde la UI de Airflow con "Trigger with conf"
o desde la API de la plataforma):
    {"days": 30, "min_magnitude": 2.5, "source_id": "usgs_feed"}

Idempotencia: el mismo upsert por (source_id, event_uid) que usa la
ingesta normal; ejecutar dos veces no duplica registros.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from callbacks import on_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "sismowatch",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=60),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="backfill_historico",
    description="Carga histórica parametrizada sin duplicar registros",
    default_args=DEFAULT_ARGS,
    schedule=None,                    # solo se lanza manualmente
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "backfill", "historico"],
    doc_md="""
### Backfill histórico
Parámetros (`Trigger with conf`):
```json
{"days": 30, "min_magnitude": 2.5, "source_id": "usgs_feed"}
```
Usa el conector de la fuente (`fetch_last_days`) y el mismo upsert
idempotente de la ingesta: re-ejecutarlo no duplica.
""",
)
def backfill_historico():

    @task
    def fetch_params() -> dict:
        ctx_params = {}
        try:
            from airflow.operators.python import get_current_context
            ctx = get_current_context()
            ctx_params = ctx.get("params") or (ctx["dag_run"].conf or {})
        except Exception:  # noqa: BLE001 - lanzamiento manual sin contexto
            ctx_params = {}
        return {
            "days": int(ctx_params.get("days", 30)),
            "min_magnitude": float(ctx_params.get("min_magnitude", 2.5)),
            "source_id": ctx_params.get("source_id", "usgs_feed"),
        }

    @task
    def load_history(params: dict) -> int:
        from connectors import build_connector
        from connectors.base import ConnectorError, payload_hash
        from db import fetch_one, get_conn

        source = fetch_one(
            "transform",
            "SELECT source_id, connector_type, config FROM ops.sources "
            "WHERE source_id = %s AND is_active",
            (params["source_id"],),
        )
        if not source:
            raise AirflowException(
                f"Fuente '{params['source_id']}' no encontrada o inactiva")

        connector = build_connector(source["source_id"],
                                    source["connector_type"], source["config"])
        try:
            raw = connector.fetch_last_days(
                days=params["days"],
                min_magnitude=params["min_magnitude"],
            )
            events = connector.normalize(raw)
        except (ConnectorError, AttributeError) as exc:
            raise AirflowException(f"Backfill falló: {exc}") from exc

        rows = [
            (source["source_id"], str(ev.get("event_uid")),
             json.dumps(ev, default=str, ensure_ascii=False), payload_hash(ev))
            for ev in events if ev.get("event_uid") is not None
        ]
        # para MinIO: cada evento lleva su source_id (mismo formato de ingesta)
        events_minio = [
            {**ev, "source_id": source["source_id"]}
            for ev in events if ev.get("event_uid") is not None
        ]
        if not rows:
            logger.info("Sin eventos en el rango solicitado")
            return 0

        # troceado para no pasar listas enormes por un solo INSERT
        with get_conn("ingest") as conn, conn.cursor() as cur:
            for start in range(0, len(rows), 1000):
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
                    rows[start:start + 1000],
                )

        # bronze en MinIO: archivo JSON particionado por día (mismo formato
        # que la ingesta normal, para que Spark lo procese igual)
        from minio_client import write_bronze_batch
        write_bronze_batch(source["source_id"], events_minio)

        logger.info("Backfill: %d eventos leídos, %d upserts",
                    len(events), len(rows))
        return len(rows)

    @task
    def run_transform(count: int) -> dict:
        """Dispara el DAG de transformación sobre los datos históricos recién cargados.

        Si no hay eventos nuevos, omite el trigger para no generar ejecuciones vacías.
        Si Airflow no está disponible, registra la advertencia pero no falla el backfill
        (los datos ya están en raw; el scheduler lanzará la transformación en el
        siguiente ciclo normal).
        """
        import os

        import httpx

        if count == 0:
            logger.info("Sin eventos cargados; se omite el trigger de transformación")
            return {"triggered": False, "reason": "no_events"}

        base = os.environ.get("AIRFLOW__API__BASE_URL",
                              "http://localhost:8080")
        user = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "admin")
        password = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "")
        try:
            with httpx.Client(timeout=15, auth=(user, password)) as client:
                resp = client.post(
                    f"{base}/api/v1/dags/transformacion_calidad/dagRuns",
                    json={"conf": {"triggered_by": "backfill_historico"}},
                )
            if resp.status_code in (200, 201):
                run_id = resp.json().get("dag_run_id", "unknown")
                logger.info("transformacion_calidad disparado: run_id=%s", run_id)
                return {"triggered": True, "run_id": run_id}
            if resp.status_code == 409:
                logger.info("transformacion_calidad ya está en ejecución")
                return {"triggered": False, "reason": "already_running"}
            logger.warning(
                "No se pudo disparar transformacion_calidad: HTTP %s — %s",
                resp.status_code, resp.text[:200],
            )
            return {"triggered": False, "reason": f"http_{resp.status_code}"}
        except httpx.HTTPError as exc:
            # No fallamos el backfill: los datos están en raw y el scheduler
            # procesará el Dataset sismowatch/raw/events en el siguiente ciclo.
            logger.warning("Airflow API no disponible para trigger: %s", exc)
            return {"triggered": False, "reason": str(exc)}

    params = fetch_params()
    count = load_history(params)
    run_transform(count)


backfill_historico()
