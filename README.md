# SismoWatch 🌋

Plataforma de monitoreo sísmico global en tiempo casi real construida como
**lakehouse**: Airflow orquesta, MinIO almacena (bronze/silver/gold),
Spark procesa, OpenMetadata cataloga, PostgreSQL sirve, FastAPI + React
exponen dashboard, reportes, tiempo real y un chatbot de consultas con IA
local (Ollama).

---

## Arquitectura (resumen)

```
USGS (API pública) → Airflow (ingesta */5min)
   ├─ raw.events (PostgreSQL, upsert idempotente)
   └─ s3://bronze (JSON por día)
        │  SparkSubmitOperator (clúster standalone)
        ▼
   s3://silver/events (parquet, limpio y tipado)
        │  calidad bloqueante (Python) → cuarentena + métricas
        ▼
   curated.events + curated.hourly_activity (PostgreSQL)
        │  Spark → s3://gold (agregados)
        ▼
   FastAPI (:8010, JWT + roles) ← React (:3000, dashboard + chatbot + SSE)
        │
        └─ OpenMetadata (:8585): catálogo, pipelines, linaje
```

El detalle completo está en [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md):
propósito, diagrama de componentes, modelo de datos, decisiones y trade-offs.

---

## Requisitos

- Docker 24+ y Docker Compose v2
- Ollama corriendo en el host (`http://localhost:11434`) con `llama3.2`
- Puertos libres: `8090, 8010, 3000, 5435, 9000, 9001, 8585, 9200, 3307, 7077, 8091, 8092`

---

## Puesta en marcha (un solo comando)

```bash
cp .env.example .env
# editar .env: passwords, JWT_SECRET y clave Fernet propios
docker compose up -d --build
```

Servicios:

| Servicio | URL |
|---|---|
| Plataforma web | http://localhost:3000 |
| API (documentación) | http://localhost:8010/docs |
| Airflow | http://localhost:8090 (credenciales del `.env`) |
| OpenMetadata | http://localhost:8585 (credenciales del `.env`) |
| MinIO console | http://localhost:9001 (credenciales del `.env`) |
| Spark master UI | http://localhost:8091 |

**Primer arranque**: los DAGs aparecen sin errores en Airflow.
`ingesta_usgs` corre cada 5 minutos; `transformacion_calidad` se dispara
solo (Dataset). Para datos históricos, lanza `backfill_historico` con
`{"days": 30}` desde la UI (Trigger with conf) o desde la plataforma
(rol admin).

**Usuarios de la plataforma** (sembrados desde `.env`):
`admin@sismowatch.local` (admin) y `viewer@sismowatch.local` (viewer).

---

## Calidad de datos

**Herramienta elegida: validaciones propias en Python**
(`airflow/include/quality/`), documentado y justificado en
[docs/ARQUITECTURA.md](docs/ARQUITECTURA.md#5-calidad-de-datos).

Reglas: esquema y tipos, campos obligatorios, rangos (lat/lon, magnitud,
profundidad), unicidad de `event_uid`, formato de fecha, frescura
(< 30 min) y volumen. Son **bloqueantes**: si fallan, nada llega a
`curated`. Los inválidos van a `ops.quarantine` con el motivo; cada
ejecución deja su puntaje en `ops.quality_results`, visible en la
pestaña **Pipeline** de la plataforma (cuarentena: solo admin).

## Seguridad

- **Secretos**: solo en `.env` (gitignored). Fernet y JWT generados propios.
- **Mínimo privilegio en BD**: roles `sismo_ingest` / `sismo_transform` /
  `sismo_reader` (la plataforma) / `sismo_admin` — grants explícitos en
  `db/init/04_grants.sql`.
- **Plataforma**: JWT + roles (401 sin token, 403 para acciones de admin),
  CORS restringido, validación de entradas con Pydantic.
- **Datos externos**: HTTPS + timeouts + reintentos, validación de payload
  y **sanitización de textos** antes de cualquier LLM (prompt injection).
- **Chatbot**: guardia SQL de solo lectura (allowlist, LIMIT, sin
  sentencias múltiples) + rol de BD read-only.

### Evidencia del escaneo de seguridad

```bash
# gitleaks (secretos en el repo y su historial)
docker run --rm -v "$PWD":/repo zricethezav/gitleaks:latest detect -s /repo -v --report-format json --report-path /repo/security_scan_report.txt
# bandit (análisis estático del código Python)
docker run --rm -v "$PWD":/src cgr.dev/chainguard/bandit:latest -r /src/airflow/include /src/api/app
# pip-audit (dependencias)
pip install pip-audit && pip-audit -r airflow/requirements.txt
```

Resultado documentado en este README y en el historial de commits.

## IA (Ollama local)

| Automatización | Técnica | Anti-alucinación |
|---|---|---|
| Reporte diario | IA generativa | Cifras por SQL; el modelo solo redacta; `validate_numbers()` verifica que todo número del texto exista en los KPIs; plan alterno por plantilla si Ollama falla |
| Anomalías | z-score (analítica) | Código calcula valor/esperado/z-score; el LLM solo redacta la explicación |
| Chatbot (dashboard) | NL → SQL | Guardia SQL + ejecución con rol read-only + respuesta sobre resultados reales |

Prompts versionados en `airflow/include/ai/prompts/`. Cada ejecución queda
en `ops.ai_runs` (modelo, tokens, latencia, costo).

## Tiempo real

**SSE** (`/api/stream`): indicador "en vivo", última actualización y
reconexión automática cada 5 s. Justificación completa en
[docs/ARQUITECTURA.md](docs/ARQUITECTURA.md#9-plataforma-de-consumo).

## Pruebas

```bash
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow && pytest tests -q"
docker compose exec api bash -lc "cd /app && pytest -q"
```

Cubren: transformación unitaria (GeoJSON → staging), integridad de DAGs
(importan sin errores), reglas de calidad (caso válido e inválido),
endpoints de la API (401 sin token, 403 para viewer en rutas admin) y la
guardia SQL del chatbot.

## Conectores genéricos

Añadir una fuente nueva **sin tocar el pipeline**: insertar una fila en
`ops.sources` con su `connector_type` (`usgs` o `generic_rest`) y el
`field_map` JSON. Ver `db/init/05_seed.sql` y
`airflow/include/connectors/`.

## Limitaciones conocidas y mejoras

- El driver de Spark corre en el contenedor del scheduler (modo cliente);
  en producción usaría `deploy-mode cluster` o `KubernetesExecutor`.
- Un solo worker Spark con 1 GB (acotado al hardware de la prueba).
- OpenMetadata se publica de forma asíncrona y tolerante a fallos; la
  ingestión automática del Postgres requiere que el deploy a Airflow esté
  habilitado (best-effort).
- Usuarios de la plataforma sembrados en memoria desde `.env`; en
  producción irían a una tabla con hash y refresh tokens.
- Ver sección "Qué haría diferente en producción" en
  [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md).
