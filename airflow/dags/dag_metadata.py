"""DAG de publicación de metadatos en OpenMetadata.

Cataloga la BD (servicio → base → esquemas → tablas clave), los DAGs como
pipelines, MinIO como servicio de storage con sus contenedores
(bronze/silver/gold) y el linaje entre pipeline y tablas.

Es independiente del flujo de datos: si OpenMetadata aún está
arrancando, la tarea reintenta con espera creciente; el pipeline de datos
no depende de este DAG.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from callbacks import on_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "sismowatch",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=25),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="publicar_metadatos",
    description="Publica catálogo, tablas, pipelines y linaje en OpenMetadata",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",          # diario 01:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "openmetadata", "catalogo"],
    doc_md="""
### Publicación de metadatos (OpenMetadata)
1. Espera a que OpenMetadata (:8585) responda.
2. Login con el usuario admin de OM.
3. Crea/actualiza servicios: Postgres, Airflow y MinIO (S3).
4. Cataloga base → esquemas (raw/staging/curated/ops) → tablas clave.
5. Publica los DAGs como pipelines del servicio Airflow.
6. Publica contenedores bronze/silver/gold del storage MinIO.
7. Registra el linaje ingesta → raw → staging → curated.
8. Intenta deployar y disparar la ingestión automática (best-effort).

Cada paso queda en `ops.metadata_runs` con su resultado.
""",
)
def publicar_metadatos():

    @task
    def publish() -> dict:
        from db import execute
        from om_client import OMError, publish_catalog
        from airflow.operators.python import get_current_context

        try:
            steps = publish_catalog()
            status = "ok"
        except OMError as exc:
            steps = {}
            status = "error"
            logger.error("Publicación de metadatos falló: %s", exc)

        ctx = get_current_context()
        execute(
            "transform",
            """
            INSERT INTO ops.metadata_runs (dag_run_id, status, steps)
            VALUES (%s, %s, %s::jsonb)
            """,
            (ctx["ti"].run_id, status, json.dumps(steps, ensure_ascii=False)),
        )
        if status != "ok":
            from airflow.exceptions import AirflowException
            raise AirflowException("OpenMetadata no publicó el catálogo; "
                                   "se reintentará")
        return steps


    publish()


publicar_metadatos()
