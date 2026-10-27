-- ============================================================
-- 03_tables.sql - Modelo de datos
-- Llaves: los eventos usan el id publicado por la fuente (event_uid)
-- junto con source_id, lo que garantiza unicidad global e idempotencia
-- (upsert ON CONFLICT en todas las capas).
-- ============================================================

-- ---------- Catálogo de fuentes (core del diseño genérico) ----------
CREATE TABLE IF NOT EXISTS ops.sources (
    source_id       text PRIMARY KEY,
    name            text NOT NULL,
    connector_type  text NOT NULL,           -- 'usgs' | 'generic_rest' | ...
    config          jsonb NOT NULL DEFAULT '{}',  -- URL, params, mapeo de campos
    schedule        text NOT NULL DEFAULT '*/5 * * * *',
    is_active       boolean NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------- RAW (bronze): payload tal como llega ----------
CREATE TABLE IF NOT EXISTS raw.events (
    raw_id          bigint GENERATED ALWAYS AS IDENTITY,
    source_id       text NOT NULL REFERENCES ops.sources(source_id),
    event_uid       text NOT NULL,           -- id original del evento en la fuente
    payload         jsonb NOT NULL,           -- JSON completo tal como llegó
    payload_hash    text NOT NULL,            -- detecta cambios en el evento
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (raw_id),
    UNIQUE (source_id, event_uid)
);
CREATE INDEX IF NOT EXISTS idx_raw_events_ingested_at ON raw.events (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_payload ON raw.events USING gin (payload);

-- ---------- STAGING (silver): limpio y tipado ----------
CREATE TABLE IF NOT EXISTS staging.events (
    source_id       text NOT NULL,
    event_uid       text NOT NULL,
    mag             numeric(4,2),
    place           text,                     -- ya sanitizado (sin HTML/control chars)
    region          text,                     -- derivado de place
    event_time      timestamptz NOT NULL,     -- properties.time (ms epoch → timestamptz)
    updated_time    timestamptz,
    tsunami         boolean NOT NULL DEFAULT false,
    sig             integer,
    status          text,
    mag_type        text,
    event_type      text,
    url             text,
    longitude       numeric(9,6),             -- geometry.coordinates[0]
    latitude        numeric(9,6),             -- geometry.coordinates[1]
    depth_km        numeric(8,3),             -- geometry.coordinates[2]
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    transformed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, event_uid)
);
CREATE INDEX IF NOT EXISTS idx_staging_event_time ON staging.events (event_time);
CREATE INDEX IF NOT EXISTS idx_staging_mag ON staging.events (mag);
CREATE INDEX IF NOT EXISTS idx_staging_region ON staging.events (region);

-- ---------- CURATED (gold): listo para consumo ----------
CREATE TABLE IF NOT EXISTS curated.events (
    source_id       text NOT NULL,
    event_uid       text NOT NULL,
    mag             numeric(4,2),             -- NULL permitido: algunos feeds no reportan magnitud
    place           text,
    region          text NOT NULL DEFAULT 'unknown',
    event_time      timestamptz NOT NULL,
    updated_time    timestamptz,
    tsunami         boolean NOT NULL DEFAULT false,
    sig             integer,
    status          text,
    mag_type        text,
    event_type      text,
    url             text,
    longitude       numeric(9,6) NOT NULL,
    latitude        numeric(9,6) NOT NULL,
    depth_km        numeric(8,3),
    published_date  date GENERATED ALWAYS AS ((event_time AT TIME ZONE 'UTC')::date) STORED,
    transformed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, event_uid)
);
CREATE INDEX IF NOT EXISTS idx_curated_event_time ON curated.events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_curated_mag ON curated.events (mag DESC);
CREATE INDEX IF NOT EXISTS idx_curated_region ON curated.events (region);
CREATE INDEX IF NOT EXISTS idx_curated_tsunami ON curated.events (tsunami) WHERE tsunami = true;
CREATE INDEX IF NOT EXISTS idx_curated_published ON curated.events (published_date);

-- Agregado por hora (alimenta la serie temporal del dashboard)
CREATE TABLE IF NOT EXISTS curated.hourly_activity (
    bucket_hour     timestamptz PRIMARY KEY,
    event_count     integer NOT NULL DEFAULT 0,
    max_mag         numeric(4,2),
    avg_mag         numeric(4,2),
    tsunami_count   integer NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------- OPERACIÓN: calidad, cuarentena, IA ----------
CREATE TABLE IF NOT EXISTS ops.quality_results (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_run_id      text NOT NULL,
    source_id       text,
    evaluated_at    timestamptz NOT NULL DEFAULT now(),
    rules_evaluated integer NOT NULL,
    rules_passed    integer NOT NULL,
    rules_rejected  integer NOT NULL,
    records_total   integer NOT NULL DEFAULT 0,
    records_valid   integer NOT NULL DEFAULT 0,
    records_rejected integer NOT NULL DEFAULT 0,
    score           numeric(5,2) NOT NULL,    -- 0..100
    status          text NOT NULL,            -- 'pass' | 'fail' (bloqueante)
    details         jsonb NOT NULL DEFAULT '{}'  -- desglose por regla
);
CREATE INDEX IF NOT EXISTS idx_quality_evaluated_at ON ops.quality_results (evaluated_at DESC);

CREATE TABLE IF NOT EXISTS ops.quarantine (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id       text NOT NULL,
    event_uid       text NOT NULL,
    payload         jsonb NOT NULL,
    reasons         jsonb NOT NULL,           -- [{"rule":"...", "detail":"..."}]
    detected_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, event_uid, detected_at)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_detected ON ops.quarantine (detected_at DESC);

-- Registro de cada ejecución de IA (modelo, tokens, latencia, costo)
CREATE TABLE IF NOT EXISTS ops.ai_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    purpose         text NOT NULL,            -- 'daily_report' | 'anomaly_explanation' | 'chat_sql'
    model           text NOT NULL,
    prompt_version  text,
    prompt_tokens   integer DEFAULT 0,
    completion_tokens integer DEFAULT 0,
    latency_ms      integer,
    estimated_cost_usd numeric(10,6) DEFAULT 0,
    status          text NOT NULL,            -- 'ok' | 'error' | 'fallback'
    error_detail    text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ai_runs_created ON ops.ai_runs (created_at DESC);

-- Reportes diarios generados por IA
CREATE TABLE IF NOT EXISTS ops.reports (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_date     date NOT NULL,
    title           text NOT NULL,
    content_md      text NOT NULL,
    kpis            jsonb NOT NULL,           -- cifras calculadas por el código
    model           text,
    status          text NOT NULL DEFAULT 'ok',  -- 'ok' | 'fallback'
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (report_date)
);

-- Anomalías detectadas (serie + explicación)
CREATE TABLE IF NOT EXISTS ops.anomalies (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    detected_at     timestamptz NOT NULL DEFAULT now(),
    window_start    timestamptz NOT NULL,
    window_end      timestamptz NOT NULL,
    metric          text NOT NULL,            -- 'event_count' | 'max_mag' ...
    value           numeric(12,3) NOT NULL,
    expected        numeric(12,3),
    z_score         numeric(8,3),
    severity        text NOT NULL,            -- 'low' | 'medium' | 'high'
    explanation     text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Ejecuciones de publicación de metadatos (OpenMetadata)
CREATE TABLE IF NOT EXISTS ops.metadata_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_run_id      text NOT NULL,
    status          text NOT NULL,            -- 'ok' | 'error'
    steps           jsonb NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Historial del chatbot (consulta NL → SQL → respuesta)
CREATE TABLE IF NOT EXISTS ops.chat_messages (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      text NOT NULL,
    user_role       text NOT NULL,
    question        text NOT NULL,
    generated_sql   text,
    answer          text,
    status          text NOT NULL,            -- 'ok' | 'error' | 'blocked'
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_chat_session ON ops.chat_messages (session_id, created_at);
