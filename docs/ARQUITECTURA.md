# SismoWatch — Documento de Arquitectura

> Plataforma de monitoreo sísmico en tiempo casi real: ingesta automatizada,
> calidad controlada, **lakehouse sobre MinIO + Spark**, **catálogo de
> metadatos con OpenMetadata**, IA aplicada donde aporta valor y plataforma
> propia de consumo (dashboard, reportes, tiempo real y chatbot).

---

## 1. Propósito de la arquitectura

**Qué problema resuelve.** El feed público de USGS cambia cada minuto, no
guarda historial, no se valida y no es consultable cómodamente. Operaciones y
gestión de riesgo necesitan saber *qué está ocurriendo* (dónde, cuándo, con
qué magnitud) para priorizar decisiones.

**Para quién.** Equipos de operaciones y gestión de riesgo de SuperApp, con
dos perfiles de acceso: `viewer` (consulta) y `admin` (operación del
pipeline).

**Qué decisiones habilita.**

- **Historial confiable**: los eventos viven en una base con capas
  separadas; un evento actualizado por USGS se actualiza sin duplicarse.
- **Calidad visible**: cada ejecación deja métricas y los registros
  inválidos quedan en cuarentena con su motivo (nada se pierde, nada
  inválido llega al consumo).
- **Automatización total**: de la fuente al dashboard sin pasos manuales.
- **IA sin alucinaciones**: las cifras las calcula el código; el LLM solo
  redacta o genera SQL restringido y verificado.
- **Extensibilidad**: añadir una fuente nueva es insertar una fila en
  `ops.sources`, no escribir un DAG nuevo.

---

## 2. Diagrama de componentes y capas

```
                        ┌──────────────────────────────────────────────┐
   FUENTES              │              ORQUESTACIÓN (Airflow)          │
 ┌───────────────┐      │  ┌────────────┐  ┌───────────────────────┐  │
 │ USGS feed     │──────┼─▶│ dag        │  │ dag                   │  │
 │ (GeoJSON)     │      │  │ ingesta_   │─▶│ transformacion_       │  │
 ├───────────────┤      │  │ usgs       │  │ calidad (Dataset)     │  │
 │ Cualquier API │──────┼─▶│ (*/5 min)  │  │ SparkSubmitOperator   │  │
 │ REST (config) │      │  └────────────┘  └──────────┬────────────┘  │
 └───────────────┘      │  ┌────────────┐            │ bloqueante    │
   connectors/          │  │ backfill_  │────────────┤               │
   base · usgs ·        │  │ historico  │            ▼               │
   generic_rest ·       │  └────────────┘  ┌───────────────────────┐ │
   registry             │  ┌────────────┐  │ dag reporte_diario_ia │ │
                        │  │ publicar_  │  │ dag deteccion_anomalias│ │
                        │  │ metadatos  │  │ dag publicar_metadatos │ │
                        │  └────────────┘  └───────────────────────┘ │
                        └───────┬────────────────────────┬───────────┘
                                │ SparkSubmitOperator   │ roles: ingest / transform
                                ▼                       ▼
   ┌─────────── MINIO (lakehouse) ───────────┐   ┌── POSTGRESQL (serving) ──────┐
   │ bronze/ (JSON por día)                  │   │ raw.* · staging.* · curated.*│
   │ silver/ (parquet limpio, Spark)         │   │ ops.* (calidad, cuarentena,  │
   │ gold/   (agregados, Spark)              │   │ IA, metadatos)               │
   └────────────┬────────────────────────────┘   │ roles: ingest/transform/     │
                │ s3a                            │ reader/admin                │
                ▼                                └──────────────┬───────────────┘
   ┌── SPARK standalone (master+worker) ──┐                   │ solo lectura (reader)
   │ bronze_to_silver · silver_to_gold    │                   ▼
   └──────────────────────────────────────┘   ┌───────────── API FastAPI (:8010) ─┐
                                              │ JWT+roles · KPIs · SSE · chat    │
                                              └───┬────────────────────────────┬─┘
   ┌── OpenMetadata (:8585) ──┐                  │ REST + SSE                  │
   │ catálogo · linaje ·      │◀─ publicación ──┐│                             ▼
   │ pipelines · containers   │ (dag_metadata)  ▼                    ┌──────────────┐
   └──────────────────────────┘   ┌─────────────┐                    │ Web React+TS │
                                  │ Ollama local│◀───────▶───────────│ (:3010)      │
                                  │ (IA, :11434)│                    └──────────────┘
                                  └─────────────┘
```

---

## 3. Capas de datos (arquitectura medallón en dos planos)

**Plano 1 — Lakehouse (MinIO + Spark)**, procesamiento distribuido:

| Capa | Bucket | Formato | Quién escribe |
|---|---|---|---|
| **Bronze** | `bronze/` | JSON particionado por día (payload tal como llega) | Airflow (ingesta/backfill) |
| **Silver** | `silver/events` | Parquet particionado por hora, limpio y tipado | Spark (`bronze_to_silver`) |
| **Gold** | `gold/` | Parquet de agregados (actividad por hora, regiones) | Spark (`silver_to_gold`) |

**Plano 2 — Serving (PostgreSQL)** para la plataforma:

| Capa | Esquema | Qué guarda | Quién escribe |
|---|---|---|---|
| **Bronze / raw** | `raw` | Payload JSONB + `payload_hash` + `ingested_at` (registro operativo e idempotencia) | `sismo_ingest` |
| **Silver / staging** | `staging` | Filas tipadas y saneadas (lo que produce Spark, replicado para servir) | `sismo_transform` |
| **Gold / curated** | `curated` | Eventos validados + `hourly_activity` | `sismo_transform` |
| **Operación** | `ops` | `sources`, `quality_results`, `quarantine`, `ai_runs`, `reports`, `anomalies`, `chat_messages`, `metadata_runs` | `sismo_transform` |

**Idempotencia en todas las capas**: `PRIMARY KEY (source_id, event_uid)` +
`INSERT … ON CONFLICT DO UPDATE … WHERE payload_hash IS DISTINCT FROM
EXCLUDED.payload_hash`. Ejecutar dos veces el mismo intervalo **no duplica**
registros; si USGS actualiza un evento (magnitud/estado), solo cambia la fila.

### Modelo de datos (relaciones)

```
ops.sources 1 ──── * raw.events        (source_id FK)
raw.events  1 ──── 1 staging.events    (source_id + event_uid, misma llave)
staging.events 1 ── 1 curated.events   (solo los que pasan validación)
curated.events 1 ── * curated.hourly_activity (agregación por hora)
ops.quality_results   (snapshot por ejecución, sin FK: histórico append-only)
ops.quarantine        (eventos rechazados + motivos)
ops.reports / ops.anomalies / ops.ai_runs / ops.chat_messages
```

---

## 4. Ingesta y orquestación

| DAG | Schedule | Qué hace |
|---|---|---|
| `ingesta_usgs` | `*/5 * * * *` | Para cada fuente activa: conector → extracción con reintentos → upsert en `raw` + escritura de JSON en `bronze/` → emite Dataset |
| `transformacion_calidad` | Dataset `sismowatch/raw/events` | Spark `bronze→silver` → validaciones **bloqueantes** → staging/curated → Spark `silver→gold` → merge de agregados |
| `reporte_diario_ia` | `30 6 * * *` | KPIs por SQL → redacción con LLM → validación numérica → `ops.reports` |
| `deteccion_anomalias` | `15 * * * *` | Serie horaria → z-score → anomalías + explicación IA → `ops.anomalies` |
| `backfill_historico` | manual (`Trigger with conf`) | Últimos N días desde el endpoint FDSN → raw + bronze, sin duplicar |
| `publicar_metadatos` | `0 1 * * *` | Catálogo, tablas, pipelines, contenedores y linaje en OpenMetadata |

**Spark integrado al orquestador**: las transformaciones pesadas se lanzan
con `SparkSubmitOperator` contra el clúster standalone (`spark-master:7077`,
worker con 2 cores/1 GB). El driver corre en el contenedor del scheduler
(modo cliente) con los jars de `hadoop-aws` + `aws-java-sdk` + JDBC de
PostgreSQL ya incluidos en la imagen de Airflow, y MinIO como filesystem
S3A (`s3a://bronze|silver|gold`).

**Plan alterno ante fallo de Spark**: si el clúster no responde, la tarea
`fallback_python_pipeline` (trigger `ALL_FAILED`) ejecuta el mismo camino
con Python puro sobre `raw.events` — la actualización no se pierde.

**Buenas prácticas aplicadas**: TaskFlow API, TaskGroups, `tags`, `doc_md`
en cada DAG, toda la lógica pesada en `include/` (no en los archivos de DAG),
payloads que **nunca viajan por XCom** (extracción y carga en la misma
tarea; solo se publican conteos), `retries` con `retry_exponential_backoff`,
`execution_timeout`, `max_active_runs=1` y `on_failure_callback` con webhook
(configurable por `AIRFLOW_ALERT_WEBHOOK_URL`).

**Caso API no responde**: `BaseConnector.extract()` reintenta con espera
creciente (2ⁿ s) y luego lanza `ConnectorError`; la tarea falla de forma
explícita y salta la alerta. No hay estado silencioso.

---

## 5. Calidad de datos

**Herramienta elegida: validaciones propias en Python** (`include/quality/`).

*Justificación*: las reglas del enunciado son específicas de dominio
(rangos de lat/lon, magnitud plausible, frescura, volumen) y necesitan
alimentar dos consumidores distintos (bloqueo en Airflow + métricas en la
plataforma). Un módulo propio da control total del puntaje, de la
cuarentena y del desglose por regla, con cero fricción de configuración en
un entorno de 8 horas. Great Expectations añadiría una capa de
configuración (DataContext, suites, stores) sin aportar reglas que no
podamos expresar en 100 líneas; se documenta como opción válida en el
README.

**Reglas implementadas**

| Tipo | Reglas |
|---|---|
| Fila | `required_fields` (id, time, coords), `latitude_range` [−90,90], `longitude_range` [−180,180], `magnitude_plausible` [−1.5,10], `depth_plausible` [−100,1000], `event_time_format` (datetime, no futura) |
| Lote | `uniqueness` (id único), `volume` (rango esperado), `freshness` (última carga < 30 min) |

**Flujo bloqueante**: `run_quality` evalúa → inválidos a `ops.quarantine`
con `reasons` → métricas a `ops.quality_results` (reglas evaluadas,
aprobadas, rechazadas, puntaje 0-100, desglose) → `publish_curated` **lanza
excepción** si `status != 'pass'`, por lo que curated no recibe nada.

Los resultados se muestran en la plataforma (pestaña *Pipeline*: puntaje,
válidos/rechazados, cuarentena para admin).

---

## 6. Seguridad

- **Secretos**: solo en `.env` (gitignored; `.env.example` sin valores
  reales). Clave Fernet y `JWT_SECRET` generados propios; credenciales de
  Airflow cambiadas. Ninguna credencial en código, config ni logs.
- **Mínimo privilegio en BD**: 4 roles con grants explícitos
  (`db/init/04_grants.sql`). La plataforma usa `sismo_reader`, que **solo
  tiene SELECT sobre `curated` y `ops`**; `raw` y `staging` son inaccesibles
  desde la API.
- **Plataforma**: JWT (HS256, expiración), roles `viewer`/`admin` validados
  en cada endpoint (401 sin token, 403 para acciones de admin), CORS
  restringido a los orígenes del `.env`, validación de entradas con Pydantic.
- **Datos externos no confiables**: HTTPS + timeouts + reintentos en las
  llamadas, validación del payload (`features`, `field_map`), y
  **sanitización de textos** (`place`, `region`) que eliminan caracteres de
  control y HTML antes de cualquier uso posterior, incluidos los prompts
  (mitigación de prompt injection, ver OWASP Top 10 para LLM).
- **Chatbot**: SQL generada por el LLM pasa por una **guardia de solo
  lectura** (allowlist de tablas/columnas, solo `SELECT`/`WITH`, sin `;`,
  sin comentarios, sin `pg_*`, `LIMIT ≤ 200` obligatorio) **y** se ejecuta
  con el rol `sismo_reader` (defense in depth).
- **Escaneo de seguridad**: `gitleaks` (secretos) + `bandit` (código) +
  `pip-audit` (dependencias) con evidencia en el README.
- **Logs**: sin credenciales ni payloads sensibles (los secretos solo se
  leen de entorno).

---

## 7. IA aplicada

**Proveedor: Ollama local** (`llama3.2`), sin coste por token y sin sacar
datos de la organización fuera de la máquina.

| Automatización | Técnica | Control anti-alucinación |
|---|---|---|
| Reporte narrativo diario | IA generativa (redacción) | Cifras calculadas por SQL; el modelo solo redacta; `validate_numbers()` verifica que **todos** los números del texto existan en los KPIs; salida JSON validada; si falla → **plan alterno** por plantilla y el pipeline no se rompe |
| Detección de anomalías | Analítica: **z-score** (global y local en ventana) sobre la serie horaria + enjambres por magnitud | El código calcula valor, esperado y z-score; el LLM solo redacta la explicación con esos hechos; si falla, la alerta se guarda sin explicación |
| Chatbot de consultas | NL → SQL (solo lectura) | Guardia SQL + rol BD read-only + respuesta redactada **con los resultados reales** en el contexto |

**Registro**: cada ejecución queda en `ops.ai_runs` (propósito, modelo,
prompt_version, tokens de entrada/salida, latencia, coste — 0 USD por ser
local, estado `ok|fallback|error`). **Prompts versionados** en
`airflow/include/ai/prompts/`.

---

## 8. Catálogo de metadatos y linaje (OpenMetadata)

OpenMetadata (UI en `:8585`) cataloga todo el sistema y lo publica
automáticamente el DAG `publicar_metadatos`:

- **Servicios**: PostgreSQL (con sus 4 esquemas y tablas clave con
  columnas), Airflow (los 6 DAGs como pipelines) y MinIO (storage S3 con
  contenedores bronze/silver/gold).
- **Linaje**: `ingesta_usgs → raw.events → staging.events →
  curated.events` más el edge pipeline→tabla de la transformación.
- **Ingestión automática**: se crea un pipeline de metadatos sobre el
  Postgres y se intenta deployar + disparar en Airflow (best-effort).
- Cada ejecución queda registrada en `ops.metadata_runs` con el resultado
  por paso.

Trade-off: la publicación es asíncrona y tolerante a fallos (si OM aún
arranca, el DAG reintenta con backoff); el flujo de datos nunca depende
del catálogo.

---

## 9. Plataforma de consumo

- **Backend** FastAPI: lee **únicamente de `curated` + `ops`** con el rol
  `sismo_reader`. Endpoints de KPIs, eventos (filtros+paginación), series,
  mapa, reportes (+export CSV/HTML), pipeline, cuarentena (admin), backfill
  y regeneración (admin), chat y **SSE** en `/api/stream`.
- **Frontend** React + TypeScript + Vite: dashboard con KPIs, tres gráficos
  (serie horaria, distribución por magnitud, regiones), mapa Leaflet con
  filtros (rango de fechas, mag. mínima, región), pestaña de chatbot,
  reportes con búsqueda/paginación/export, estado del pipeline y login.
  Estados de **carga, vacío y error** en cada vista (nunca pantalla en
  blanco).
- **Tiempo real**: **SSE** (Server-Sent Events) con indicador "en vivo",
  hora de última actualización y **reconexión automática** con backoff
  (5 s) si se pierde la conexión. Elección justificada: el flujo es
  unidireccional (servidor → cliente), SSE se auto-reconecta de forma
  nativa y no requiere管理 de conexiones del lado del servidor como
  WebSocket; el servidor hace un chequeo interno cada 10 s (≤ 30 s).

---

## 10. Decisiones y trade-offs

| Decisión | Alternativa | Por qué |
|---|---|---|
| **Airflow** en vez de cron | cron + scripts | Orquestación con dependencias (Dataset), reintentos, backfills, historial, alertas, UI y control de concurrencia; reproducibilidad y auditoría |
| **MinIO + Spark para el lakehouse** | Todo en PostgreSQL | Los datos crudos y transformados viven en almacenamiento de objetos (escalable, barato, versionable) y Spark procesa de forma distribuida; PostgreSQL queda como capa de serving para la plataforma |
| **Spark standalone en compose** | Spark local / EMR gestionado | Clúster real (master+worker) para poder defender el modelo de ejecución distribuida sin depender de servicios cloud |
| **Dual-write bronze** (MinIO + raw.events) | Solo MinIO | `raw.events` da idempotencia operativa y frescura inmediata para las reglas de calidad; MinIO conserva el archivo histórico reprocesable |
| **Medallón en un solo Postgres** con schemas | Data lake + catálogo | Volumen del feed es pequeño; separar por esquema + roles da aislamiento y simplicidad operacional sin perder la separación lógica |
| **Conectores por registro** (`ops.sources`) | DAG por fuente | Escalabilidad: nueva API = fila + `field_map`; sin tocar el orquestador |
| **Validaciones propias** de calidad | Great Expectations / Pandera / Soda | Ver sección 5; control total del puntaje y la cuarentena con mínima fricción |
| **Ollama local** | OpenAI/Claude API | Sin coste, sin fuga de datos, sin API key; trade-off: modelo más débil → prompts estrictos + validación numérica obligatoria |
| **SSE** para tiempo real | WebSocket / polling | Unidirección, auto-reconexión nativa, más simple de implementar y probar; polling descartado por el requisito de ≤30 s |
| **`payload JSONB` en raw** | Filas aplanadas en raw | Raw debe ser fiel a lo que llegó (reprocesable) y detectar cambios por hash |
| **Chatbot NL→SQL** | Resumen precalculado | Consultas abiertas de alto nivel sin predefinir preguntas; acotado por guardia + rol read-only |

---

## 11. Qué haría diferente en producción

1. **Airflow separado de la BD de datos** (metastore propio, ideally
   managed) y backend `CeleryExecutor`/`KubernetesExecutor` con workers
   dedicados.
2. **Secrets backend real** (Vault/AWS Secrets Manager) en lugar de `.env`,
   con rotación automática.
3. **Particionado** de `curated.events` por `published_date` y retención
   política (raw 90 días, curated indefinido con particiones).
4. **CI/CD**: pipelines con lint (ruff), type-check (mypy), tests y escaneo
   obligatorio antes de merge; infraestructura como código.
5. **Observabilidad**: métricas Prometheus + alertas basadas en frescura y
   puntaje de calidad, tracing de las llamadas al LLM.
6. **Autenticación con IdP corporativo** (OIDC) en vez de usuarios
   sembrados, y tabla de usuarios con hash + refresh tokens.
7. **LLM gestionado** con fallback entre proveedores y presupuesto de
   tokens; evaluación automatizada de la calidad de las respuestas.
8. **Tests de contrato** del conector contra la API real (smoke tests
   programados) y data contracts con las fuentes.
9. **Chatbot con historial de conversación** y streaming de tokens al
   cliente; caché de consultas frecuentes.
