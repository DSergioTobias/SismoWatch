"""Cliente de OpenMetadata (REST) para publicar el catálogo.

Publica: servicio de BD (Postgres), base + esquemas + tablas clave,
servicio de pipelines (Airflow) + DAGs, servicio de storage (MinIO) +
contenedores (bronze/silver/gold), linaje entre pipeline y tablas.

Diseño best-effort: cada paso se registra; si OpenMetadata no responde
aún, el DAG falla y reintenta (el pipeline de datos no depende de esto).
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

API_URL = os.environ.get("OM_API_URL", "http://openmetadata:8585/api")
USERNAME = os.environ.get("OM_USERNAME", "admin@open-metadata.org")
PASSWORD = os.environ.get("OM_PASSWORD", "admin")


class OMError(Exception):
    pass


def _auth_headers(token: str | None) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def wait_ready(timeout_s: int = 300, poll_s: int = 10) -> bool:
    import time
    started = time.monotonic()
    while time.monotonic() - started < timeout_s:
        try:
            r = httpx.get(f"{API_URL}/v1/system/version", timeout=5)
            if r.status_code == 200:
                logger.info("OpenMetadata listo: %s", r.json())
                return True
        except httpx.HTTPError:
            pass
        logger.info("esperando a OpenMetadata…")
        time.sleep(poll_s)
    return False


def login() -> str:
    r = httpx.post(
        f"{API_URL}/v1/users/login",
        json={"email": USERNAME, "password": PASSWORD},
        timeout=30,
    )
    if r.status_code != 200:
        raise OMError(f"login OM falló: {r.status_code} {r.text[:200]}")
    return r.json()["accessToken"]


def _get(token: str, path: str) -> dict | None:
    r = httpx.get(f"{API_URL}{path}", headers=_auth_headers(token), timeout=30)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise OMError(f"GET {path} → {r.status_code} {r.text[:200]}")
    return r.json()


def _post(token: str, path: str, body: dict) -> dict:
    r = httpx.post(f"{API_URL}{path}", headers=_auth_headers(token),
                   json=body, timeout=60)
    if r.status_code not in (200, 201):
        raise OMError(f"POST {path} → {r.status_code} {r.text[:300]}")
    return r.json()


def _put(token: str, path: str, body: dict) -> dict:
    r = httpx.put(f"{API_URL}{path}", headers=_auth_headers(token),
                  json=body, timeout=60)
    if r.status_code not in (200, 201):
        raise OMError(f"PUT {path} → {r.status_code} {r.text[:300]}")
    return r.json()


def _upsert_entity(token: str, collection: str, name: str,
                   create_body: dict, update_body: dict) -> dict:
    """GET by name → POST si no existe, PUT si existe."""
    existing = _get(token, f"/v1/services/{collection}/name/{name}")
    if existing is None:
        return _post(token, f"/v1/services/{collection}", create_body)
    return _put(token, f"/v1/services/{collection}",
                {**update_body, "id": existing["id"]})


# ---------------------------------------------------------------------------
# Servicios y entidades
# ---------------------------------------------------------------------------

POSTGRES_SERVICE = {
    "name": "sismowatch_postgres",
    "serviceType": "Postgres",
    "description": "PostgreSQL de SismoWatch (raw/staging/curated/ops)",
    "connection": {
        "config": {
            "type": "Postgres",
            "scheme": "postgresql+psycopg2",
            "username": os.environ.get("SISMO_PG_USER_TRANSFORM", "sismo_transform"),
            "password": os.environ.get("SISMO_PG_PASSWORD_TRANSFORM", ""),
            "hostPort": "postgres:5432",
            "database": os.environ.get("SISMO_PG_DB", "sismowatch"),
            "supportsMetadataExtraction": True,
            "supportsDBTExtraction": False,
        }
    },
}

AIRFLOW_SERVICE = {
    "name": "sismowatch_airflow",
    "serviceType": "Airflow",
    "description": "Apache Airflow de SismoWatch (orquestación)",
    "connection": {
        "config": {
            "type": "Airflow",
            "hostPort": "http://airflow-webserver:8080",
            "numberOfStatus": 5,
            "supportsMetadataExtraction": True,
        }
    },
}

MINIO_SERVICE = {
    "name": "sismowatch_minio",
    "serviceType": "S3",
    "description": "MinIO: lakehouse bronze/silver/gold",
    "connection": {
        "config": {
            "type": "S3",
            "awsAccessKeyId": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            "awsSecretAccessKey": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            "awsRegion": "us-east-1",
            "endPointURL": "http://minio:9000",
            "supportsMetadataExtraction": True,
        }
    },
}

SCHEMAS = {
    "raw": "Capa bronze (PostgreSQL): dato tal como llega",
    "staging": "Capa silver: limpio y tipado",
    "curated": "Capa gold: listo para consumo",
    "ops": "Operación: calidad, cuarentena, IA, reportes",
}

# tablas clave que se catalogan con sus columnas principales
TABLES = {
    "raw": {
        "events": [
            ("raw_id", "BIGINT"), ("source_id", "VARCHAR"),
            ("event_uid", "VARCHAR"), ("payload", "JSON"),
            ("payload_hash", "VARCHAR"), ("ingested_at", "TIMESTAMP"),
        ],
    },
    "staging": {
        "events": [
            ("source_id", "VARCHAR"), ("event_uid", "VARCHAR"),
            ("mag", "DECIMAL"), ("place", "VARCHAR"), ("region", "VARCHAR"),
            ("event_time", "TIMESTAMP"), ("tsunami", "BOOLEAN"),
            ("longitude", "DECIMAL"), ("latitude", "DECIMAL"),
            ("depth_km", "DECIMAL"),
        ],
    },
    "curated": {
        "events": [
            ("source_id", "VARCHAR"), ("event_uid", "VARCHAR"),
            ("mag", "DECIMAL"), ("place", "VARCHAR"), ("region", "VARCHAR"),
            ("event_time", "TIMESTAMP"), ("tsunami", "BOOLEAN"),
            ("longitude", "DECIMAL"), ("latitude", "DECIMAL"),
            ("depth_km", "DECIMAL"), ("published_date", "DATE"),
        ],
        "hourly_activity": [
            ("bucket_hour", "TIMESTAMP"), ("event_count", "INT"),
            ("max_mag", "DECIMAL"), ("avg_mag", "DECIMAL"),
            ("tsunami_count", "INT"),
        ],
    },
    "ops": {
        "quality_results": [
            ("id", "BIGINT"), ("dag_run_id", "VARCHAR"), ("score", "DECIMAL"),
            ("status", "VARCHAR"), ("evaluated_at", "TIMESTAMP"),
        ],
        "quarantine": [
            ("id", "BIGINT"), ("source_id", "VARCHAR"),
            ("event_uid", "VARCHAR"), ("reasons", "JSON"),
        ],
        "reports": [
            ("id", "BIGINT"), ("report_date", "DATE"), ("title", "VARCHAR"),
            ("model", "VARCHAR"), ("status", "VARCHAR"),
        ],
    },
}

PIPELINES = [
    "ingesta_usgs", "transformacion_calidad", "reporte_diario_ia",
    "deteccion_anomalias", "backfill_historico", "publicar_metadatos",
]

CONTAINERS = {
    "bronze": "Datos crudos de las fuentes (JSON por día)",
    "silver": "Datos limpios y tipados (parquet por hora)",
    "gold": "Agregados listos para consumo (parquet)",
}


def publish_catalog() -> dict[str, str]:
    """Publica el catálogo completo. Devuelve resumen por paso."""
    steps: dict[str, str] = {}
    if not wait_ready():
        raise OMError("OpenMetadata no respondió a tiempo")

    token = login()
    steps["login"] = "ok"

    try:
        _upsert_entity(token, "databaseServices", POSTGRES_SERVICE["name"],
                       POSTGRES_SERVICE, POSTGRES_SERVICE)
        steps["databaseService"] = "ok"
    except OMError as exc:
        steps["databaseService"] = f"error: {exc}"

    try:
        _upsert_entity(token, "pipelineServices", AIRFLOW_SERVICE["name"],
                       AIRFLOW_SERVICE, AIRFLOW_SERVICE)
        steps["pipelineService"] = "ok"
    except OMError as exc:
        steps["pipelineService"] = f"error: {exc}"

    try:
        _upsert_entity(token, "storageServices", MINIO_SERVICE["name"],
                       MINIO_SERVICE, MINIO_SERVICE)
        steps["storageService"] = "ok"
    except OMError as exc:
        steps["storageService"] = f"error: {exc}"

    steps["schemas"] = _publish_schemas_tables(token)
    steps["pipelines"] = _publish_pipelines(token)
    steps["containers"] = _publish_containers(token)
    steps["lineage"] = _publish_lineage(token)
    steps["ingestion"] = _publish_ingestion(token)

    return steps


def _svc_ref(service_type: str, name: str) -> dict:
    return {"type": service_type, "name": name}


def _publish_schemas_tables(token: str) -> str:
    try:
        db_name = os.environ.get("SISMO_PG_DB", "sismowatch")
        database = _get(token, f"/v1/databases/name/{db_name}")
        if database is None:
            database = _post(token, "/v1/databases", {
                "name": db_name,
                "service": _svc_ref("databaseService", POSTGRES_SERVICE["name"]),
            })
        db_fqn = database["fullyQualifiedName"]

        count = 0
        for schema_name, description in SCHEMAS.items():
            schema = _get(token, f"/v1/databaseSchemas/name/{db_fqn}.{schema_name}")
            if schema is None:
                schema = _post(token, "/v1/databaseSchemas", {
                    "name": schema_name,
                    "database": _svc_ref("database", db_fqn),
                    "description": description,
                })
            schema_fqn = schema["fullyQualifiedName"]
            for table_name, columns in TABLES.get(schema_name, {}).items():
                table = _get(token, f"/v1/tables/name/{schema_fqn}.{table_name}")
                cols = [
                    {"name": name, "dataType": dtype}
                    for name, dtype in columns
                ]
                if table is None:
                    _post(token, "/v1/tables", {
                        "name": table_name,
                        "columns": cols,
                        "databaseSchema": _svc_ref("databaseSchema", schema_fqn),
                    })
                else:
                    _put(token, "/v1/tables", {
                        "id": table["id"], "name": table_name,
                        "columns": cols,
                        "databaseSchema": _svc_ref("databaseSchema", schema_fqn),
                    })
                count += 1
        return f"ok ({count} tablas)"
    except OMError as exc:
        return f"error: {exc}"


def _publish_pipelines(token: str) -> str:
    try:
        count = 0
        for dag_name in PIPELINES:
            pipeline = _get(token, f"/v1/pipelines/name/"
                                  f"{AIRFLOW_SERVICE['name']}.{dag_name}")
            if pipeline is None:
                _post(token, "/v1/pipelines", {
                    "name": dag_name,
                    "displayName": f"DAG {dag_name}",
                    "service": _svc_ref("pipelineService", AIRFLOW_SERVICE["name"]),
                })
            count += 1
        return f"ok ({count} pipelines)"
    except OMError as exc:
        return f"error: {exc}"


def _publish_containers(token: str) -> str:
    try:
        count = 0
        for name, description in CONTAINERS.items():
            container = _get(token, f"/v1/containers/name/"
                                    f"{MINIO_SERVICE['name']}.{name}")
            if container is None:
                try:
                    _post(token, "/v1/containers", {
                        "name": name,
                        "description": description,
                        "service": _svc_ref("storageService", MINIO_SERVICE["name"]),
                    })
                except OMError as exc:
                    # en algunas versiones los contenedores se crean vía
                    # ingestion del storage service; no es crítico
                    logger.warning("contenedor %s: %s", name, exc)
            count += 1
        return f"ok ({count} contenedores)"
    except OMError as exc:
        return f"error: {exc}"


def _publish_lineage(token: str) -> str:
    """Linaje: ingesta → raw.events → staging.events → curated.events."""
    db_name = os.environ.get("SISMO_PG_DB", "sismowatch")
    svc = POSTGRES_SERVICE["name"]
    edges = [
        ("pipeline", f"{AIRFLOW_SERVICE['name']}.ingesta_usgs",
         "table", f"{svc}.{db_name}.raw.events",
         "Ingesta escribe los datos crudos"),
        ("pipeline", f"{AIRFLOW_SERVICE['name']}.transformacion_calidad",
         "table", f"{svc}.{db_name}.curated.events",
         "Spark + calidad publican eventos validados"),
        ("table", f"{svc}.{db_name}.raw.events",
         "table", f"{svc}.{db_name}.staging.events",
         "raw → staging (limpieza y tipado con Spark)"),
        ("table", f"{svc}.{db_name}.staging.events",
         "table", f"{svc}.{db_name}.curated.events",
         "staging → curated (solo registros que pasan calidad)"),
    ]
    ok = 0
    for from_type, from_fqn, to_type, to_fqn, desc in edges:
        try:
            _post(token, "/v1/lineage", {
                "description": desc,
                "edge": {
                    "fromEntity": {"type": from_type,
                                   "fullyQualifiedName": from_fqn},
                    "toEntity": {"type": to_type,
                                 "fullyQualifiedName": to_fqn},
                },
            })
            ok += 1
        except OMError as exc:
            logger.warning("linaje %s→%s: %s", from_fqn, to_fqn, exc)
    return f"ok ({ok}/{len(edges)} edges)"


def _publish_ingestion(token: str) -> str:
    """Intenta crear y disparar el pipeline de ingestión de metadatos del
    Postgres (best-effort: requiere que el deploy a Airflow esté habilitado)."""
    try:
        body = {
            "name": "sismowatch_postgres_metadata",
            "service": _svc_ref("databaseService", POSTGRES_SERVICE["name"]),
            "pipelineType": "metadata",
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "markDeletedTables": True,
                    "includeTables": True,
                    "includeViews": True,
                    "schemaFilterPattern": {
                        "includes": list(SCHEMAS.keys()),
                    },
                }
            },
            "airflowConfig": {
                "scheduleInterval": "0 12 * * *",
                "username": os.environ.get("AIRFLOW_ADMIN_USERNAME", "admin"),
                "password": os.environ.get("AIRFLOW_ADMIN_PASSWORD", ""),
                "hostPort": "http://airflow-webserver:8080",
                "apiVersion": "v1",
                "authProvider": "basic",
                "apiEndpoint": "/api/v1",
            },
        }
        pipeline = _post(token, "/v1/services/ingestionPipelines", body)
        try:
            _post(token, f"/v1/services/ingestionPipelines/deploy/{pipeline['id']}",
                  {})
            _post(token, f"/v1/services/ingestionPipelines/trigger/{pipeline['id']}",
                  {})
            return "ok (deployed+triggered)"
        except OMError as exc:
            return f"pipeline creado, trigger pendiente: {exc}"
    except OMError as exc:
        return f"error: {exc}"
