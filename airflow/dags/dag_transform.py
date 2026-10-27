"""DAG de transformación y calidad con Apache Spark (bronze → silver → gold).

Se dispara por Dataset al terminar la ingesta.
  1. Spark `bronze_to_silver`: JSON de MinIO → parquet limpio y tipado.
  2. Calidad (Python, bloqueante): lee silver, valida; inválidos →
     cuarentena con motivo; métricas → ops.quality_results.
  3. Publicación: staging (todos los tipados) y curated (solo si PASS).
  4. Spark `silver_to_gold`: agregados horarios y de región → gold.
  5. Merge de gold → curated.hourly_activity (consumo del dashboard).

Plan alterno: si el clúster Spark no responde, una tarea Python hace el
mismo camino (raw → staging → curated) para no perder la actualización.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from callbacks import on_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "sismowatch",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=25),
    "on_failure_callback": on_failure_alert,
}

SPARK_JARS = (
    "/opt/airflow/spark/jars/hadoop-aws-3.3.4.jar,"
    "/opt/airflow/spark/jars/aws-java-sdk-bundle-1.12.367.jar,"
    "/opt/airflow/spark/jars/postgresql-42.7.3.jar"
)

SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.memory": "512m",
    "spark.executor.memory": "700m",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.session.timeZone": "UTC",
}


@dag(
    dag_id="transformacion_calidad",
    description="Spark: bronze → silver → calidad bloqueante → curated/gold",
    default_args=DEFAULT_ARGS,
    schedule=[Dataset("sismowatch/raw/events")],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "spark", "calidad", "medallon"],
    doc_md="""
### Transformación con Spark (medallón: silver → gold)
- `spark_bronze_to_silver`: JSON de `s3://bronze` → parquet tipado y
  saneado en `s3://silver/events` (mismas reglas que `event_to_staging`).
- `quality_and_publish`: lee silver (últimas 6 h), aplica las
  validaciones bloqueantes; cuarentena con motivo y métricas.
- `publish_curated`: staging para todos los tipados; curated solo si
  `status='pass'` (si falla, lanza excepción).
- `spark_silver_to_gold`: agregados por hora y región → `s3://gold`.
- `merge_gold`: upsert de agregados en `curated.hourly_activity`.
- Si Spark falla → `fallback_python_pipeline` (trigger ALL_FAILED) hace
  el mismo camino con Python puro sobre raw (el flujo nunca se detiene).
""",
)
def transformacion_calidad():

    tg_spark = TaskGroup("spark")
    tg_calidad = TaskGroup("calidad")
    tg_gold = TaskGroup("gold")
    tg_fallback = TaskGroup("fallback")

    spark_bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        task_group=tg_spark,
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/bronze_to_silver.py",
        application_args=["--bronze", "s3a://bronze", "--silver", "s3a://silver"],
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        name="sismowatch_bronze_to_silver",
        verbose=True,
    )

    @task(task_group=tg_calidad)
    def quality_and_publish() -> dict:
        """Lee silver desde MinIO, valida y registra calidad/cuarentena."""
        from db import execute, fetch_one
        from quality import evaluate_batch

        rows = _read_silver_recent()
        if not rows:
            logger.warning("silver vacío; nada que validar")
            return {"status": "pass", "score": 100, "valid_rows": []}

        latest = fetch_one(
            "transform", "SELECT MAX(ingested_at) AS ts FROM raw.events")
        report = evaluate_batch(
            rows,
            latest_ingested_at=latest["ts"] if latest else None,
            freshness_max_minutes=30,
            volume_min=1,
            volume_max=10000,
        )

        # 1) staging: todos los tipados (staging = capa limpia y tipada)
        _upsert_staging(rows)

        # 2) cuarentena: los inválidos no se pierden
        by_uid = {str(r["event_uid"]): r for r in rows}
        for rej in report["rejected_rows"]:
            payload = by_uid.get(str(rej.event_uid), {})
            execute(
                "transform",
                """
                INSERT INTO ops.quarantine (source_id, event_uid, payload, reasons)
                VALUES (%s, %s, %s::jsonb, %s::jsonb)
                ON CONFLICT DO NOTHING
                """,
                (payload.get("source_id", "unknown"), rej.event_uid,
                 json.dumps(payload, default=str),
                 json.dumps(rej.reasons)),
            )

        # 3) métricas de la ejecución
        ctx = get_current_context()
        execute(
            "transform",
            """
            INSERT INTO ops.quality_results
                (dag_run_id, source_id, rules_evaluated, rules_passed,
                 rules_rejected, records_total, records_valid, records_rejected,
                 score, status, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                ctx["ti"].run_id, "usgs_feed",
                report["rules_evaluated"], report["rules_passed"],
                report["rules_rejected"], report["records_total"],
                report["records_valid"], report["records_rejected"],
                report["score"], report["status"],
                json.dumps({"rules": report["rules"]}, default=str),
            ),
        )

        return {
            "status": report["status"],
            "score": report["score"],
            "records_valid": report["records_valid"],
            "records_rejected": report["records_rejected"],
            "valid_rows": report["valid_rows"],
        }

    @task(task_group=tg_calidad, trigger_rule=TriggerRule.ALL_SUCCESS)
    def publish_curated(quality: dict) -> int:
        """curated solo si el lote pasó; si falla, lanza excepción."""
        if quality["status"] != "pass":
            raise AirflowException(
                "Lote RECHAZADO por calidad "
                f"(score={quality['score']}); no se publica en curated. "
                f"Registros en cuarentena: {quality['records_rejected']}"
            )

        from db import get_conn
        rows = quality.get("valid_rows") or []
        if not rows:
            return 0
        params = [(
            r["source_id"], r["event_uid"], r["mag"], r["place"], r["region"],
            r["event_time"], r["updated_time"], r["tsunami"], r["sig"],
            r["status"], r["mag_type"], r["event_type"], r["url"],
            r["longitude"], r["latitude"], r["depth_km"],
        ) for r in rows]

        with get_conn("transform") as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO curated.events
                    (source_id, event_uid, mag, place, region, event_time,
                     updated_time, tsunami, sig, status, mag_type, event_type,
                     url, longitude, latitude, depth_km)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (source_id, event_uid) DO UPDATE
                   SET mag = EXCLUDED.mag, place = EXCLUDED.place,
                       region = EXCLUDED.region,
                       event_time = EXCLUDED.event_time,
                       updated_time = EXCLUDED.updated_time,
                       tsunami = EXCLUDED.tsunami, sig = EXCLUDED.sig,
                       status = EXCLUDED.status, mag_type = EXCLUDED.mag_type,
                       event_type = EXCLUDED.event_type, url = EXCLUDED.url,
                       longitude = EXCLUDED.longitude,
                       latitude = EXCLUDED.latitude,
                       depth_km = EXCLUDED.depth_km,
                       transformed_at = now()
                """,
                params,
            )
        logger.info("curated: %d eventos publicados", len(params))
        return len(params)

    spark_silver_to_gold = SparkSubmitOperator(
        task_id="spark_silver_to_gold",
        task_group=tg_spark,
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/silver_to_gold.py",
        application_args=["--silver", "s3a://silver", "--gold", "s3a://gold",
                          "--days", "7"],
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        name="sismowatch_silver_to_gold",
        verbose=True,
    )

    @task(task_group=tg_gold)
    def merge_gold() -> int:
        """gold parquet → curated.hourly_activity (ON CONFLICT)."""
        from db import get_conn

        hourly = _read_gold_parquet("hourly_activity")
        if not hourly:
            return 0
        with get_conn("transform") as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO curated.hourly_activity
                    (bucket_hour, event_count, max_mag, avg_mag,
                     tsunami_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (bucket_hour) DO UPDATE
                   SET event_count = EXCLUDED.event_count,
                       max_mag = EXCLUDED.max_mag,
                       avg_mag = EXCLUDED.avg_mag,
                       tsunami_count = EXCLUDED.tsunami_count,
                       updated_at = now()
                """,
                [
                    (r["bucket_hour"], r["event_count"], r["max_mag"],
                     r["avg_mag"], r["tsunami_count"])
                    for r in hourly
                ],
            )
        return len(hourly)

    @task(task_group=tg_fallback, trigger_rule=TriggerRule.ALL_FAILED)
    def fallback_python_pipeline() -> str:
        """Plan alterno: mismo camino en Python puro si Spark no responde."""
        from db import execute, fetch_all, fetch_one, get_conn
        from quality import evaluate_batch
        from transform import event_to_staging

        raw_rows = fetch_all(
            "transform",
            """
            SELECT source_id, event_uid, payload, ingested_at
            FROM raw.events
            WHERE ingested_at >= now() - interval '6 hours'
            ORDER BY ingested_at DESC LIMIT 5000
            """,
        )
        rows = []
        for raw in raw_rows:
            row = event_to_staging(raw["payload"], raw["source_id"])
            row["event_uid"] = raw["event_uid"]
            row["ingested_at"] = raw["ingested_at"]
            rows.append(row)
        if not rows:
            return "sin filas"

        latest = fetch_one(
            "transform", "SELECT MAX(ingested_at) AS ts FROM raw.events")
        report = evaluate_batch(rows, latest_ingested_at=latest["ts"] if latest else None,
                                freshness_max_minutes=30, volume_min=1,
                                volume_max=10000)

        _upsert_staging(rows)
        by_uid = {str(r["event_uid"]): r for r in rows}
        for rej in report["rejected_rows"]:
            payload = by_uid.get(str(rej.event_uid), {})
            execute(
                "transform",
                """
                INSERT INTO ops.quarantine (source_id, event_uid, payload, reasons)
                VALUES (%s, %s, %s::jsonb, %s::jsonb)
                ON CONFLICT DO NOTHING
                """,
                (payload.get("source_id", "unknown"), rej.event_uid,
                 json.dumps(payload, default=str), json.dumps(rej.reasons)),
            )
        ctx = get_current_context()
        execute(
            "transform",
            """
            INSERT INTO ops.quality_results
                (dag_run_id, source_id, rules_evaluated, rules_passed,
                 rules_rejected, records_total, records_valid, records_rejected,
                 score, status, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (ctx["ti"].run_id, "usgs_feed", report["rules_evaluated"],
             report["rules_passed"], report["rules_rejected"],
             report["records_total"], report["records_valid"],
             report["records_rejected"], report["score"], report["status"],
             json.dumps({"rules": report["rules"], "fallback": "python"}, default=str)),
        )
        if report["status"] != "pass":
            raise AirflowException(
                f"Fallback Python: lote RECHAZADO (score={report['score']})")
        _publish_valid(report["valid_rows"])
        return f"fallback python OK ({report['records_valid']} válidos)"

    quality = quality_and_publish()
    spark_bronze_to_silver >> quality >> publish_curated(quality)
    spark_bronze_to_silver >> spark_silver_to_gold >> merge_gold()
    spark_bronze_to_silver >> fallback_python_pipeline()


# ---------------------------------------------------------------------------
# Helpers de lectura de parquet (MinIO) — módulos pesados cargados aquí para
# no ralentizar el parseo del DAG.
# ---------------------------------------------------------------------------

def _s3fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        client_kwargs={
            "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        },
        use_ssl=os.environ.get("MINIO_SECURE", "0") == "1",
    )


def _read_silver_recent(hours: int = 6) -> list[dict]:
    """Lee silver/events (últimas N horas) desde MinIO con pyarrow."""
    import pyarrow.dataset as ds

    fs = _s3fs()
    dataset = ds.dataset("silver/events", filesystem=fs, format="parquet")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    table = dataset.to_table(
        filter=ds.field("event_time") >= cutoff.replace(tzinfo=None))
    if table.num_rows == 0:
        return []
    df = table.to_pandas()
    rows = []
    for _, series in df.iterrows():
        row = series.to_dict()
        row["event_time"] = _tz_aware(series["event_time"])
        row["updated_time"] = _tz_aware(series["updated_time"])
        row["mag"] = _opt_float(series.get("mag"))
        row["longitude"] = _opt_float(series.get("longitude"))
        row["latitude"] = _opt_float(series.get("latitude"))
        row["depth_km"] = _opt_float(series.get("depth_km"))
        row["sig"] = _opt_int(series.get("sig"))
        row["tsunami"] = bool(row.get("tsunami")) if _notna(row.get("tsunami")) else False
        row["place"] = _opt_str(row.get("place"))
        rows.append(row)
    return rows


def _notna(value) -> bool:
    import pandas as pd
    return value is not None and not pd.isna(value)


def _opt_float(value):
    import pandas as pd
    if value is None or pd.isna(value):
        return None
    return float(value)


def _opt_int(value):
    import pandas as pd
    if value is None or pd.isna(value):
        return None
    return int(value)


def _opt_str(value):
    import pandas as pd
    if value is None or pd.isna(value):
        return None
    return str(value)


def _tz_aware(value) -> datetime | None:
    import pandas as pd
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


def _read_gold_parquet(name: str) -> list[dict]:
    import pyarrow.dataset as ds

    fs = _s3fs()
    try:
        dataset = ds.dataset(f"gold/{name}", filesystem=fs, format="parquet")
    except Exception as exc:  # gold aún no existe
        logger.warning("gold/%s no disponible: %s", name, exc)
        return []
    table = dataset.to_table()
    return table.to_pylist()


def _upsert_staging(rows: list[dict]) -> None:
    from db import get_conn
    params = [(
        r["source_id"], str(r["event_uid"]), r["mag"], r["place"], r["region"],
        r["event_time"], r["updated_time"], r["tsunami"], r["sig"],
        r["status"], r["mag_type"], r["event_type"], r["url"],
        r["longitude"], r["latitude"], r["depth_km"],
    ) for r in rows if r.get("event_uid") is not None]
    if not params:
        return
    with get_conn("transform") as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO staging.events
                (source_id, event_uid, mag, place, region, event_time,
                 updated_time, tsunami, sig, status, mag_type, event_type,
                 url, longitude, latitude, depth_km)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_id, event_uid) DO UPDATE
               SET mag = EXCLUDED.mag, place = EXCLUDED.place,
                   region = EXCLUDED.region, event_time = EXCLUDED.event_time,
                   updated_time = EXCLUDED.updated_time,
                   tsunami = EXCLUDED.tsunami, sig = EXCLUDED.sig,
                   status = EXCLUDED.status, mag_type = EXCLUDED.mag_type,
                   event_type = EXCLUDED.event_type, url = EXCLUDED.url,
                   longitude = EXCLUDED.longitude,
                   latitude = EXCLUDED.latitude,
                   depth_km = EXCLUDED.depth_km, transformed_at = now()
            """,
            params,
        )


def _publish_valid(rows: list[dict]) -> None:
    """Publica en curated (mismo SQL de publish_curated, reutilizable)."""
    from db import get_conn
    params = [(
        r["source_id"], r["event_uid"], r["mag"], r["place"], r["region"],
        r["event_time"], r["updated_time"], r["tsunami"], r["sig"],
        r["status"], r["mag_type"], r["event_type"], r["url"],
        r["longitude"], r["latitude"], r["depth_km"],
    ) for r in rows]
    if not params:
        return
    with get_conn("transform") as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO curated.events
                (source_id, event_uid, mag, place, region, event_time,
                 updated_time, tsunami, sig, status, mag_type, event_type,
                 url, longitude, latitude, depth_km)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_id, event_uid) DO UPDATE
               SET mag = EXCLUDED.mag, place = EXCLUDED.place,
                   region = EXCLUDED.region, event_time = EXCLUDED.event_time,
                   updated_time = EXCLUDED.updated_time,
                   tsunami = EXCLUDED.tsunami, sig = EXCLUDED.sig,
                   status = EXCLUDED.status, mag_type = EXCLUDED.mag_type,
                   event_type = EXCLUDED.event_type, url = EXCLUDED.url,
                   longitude = EXCLUDED.longitude,
                   latitude = EXCLUDED.latitude,
                   depth_km = EXCLUDED.depth_km, transformed_at = now()
            """,
            params,
        )


transformacion_calidad()
