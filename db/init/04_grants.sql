-- ============================================================
-- 04_grants.sql - Mínimo privilegio por rol
--   sismo_ingest     : escribe únicamente en raw
--   sismo_transform  : lee raw, escribe staging/curated/ops
--   sismo_reader     : SOLO lectura en curated + ops (plataforma)
--   sismo_admin      : lectura ampliada (cuarentena, calidad, chat)
-- ============================================================

-- ---------- sismo_ingest: solo raw ----------
GRANT SELECT ON ops.sources TO sismo_ingest;
GRANT INSERT, UPDATE ON raw.events TO sismo_ingest;
GRANT USAGE, SELECT ON SEQUENCE raw.events_raw_id_seq TO sismo_ingest;

-- ---------- sismo_transform: raw → staging → curated ----------
GRANT SELECT, INSERT, UPDATE ON raw.events TO sismo_transform;
GRANT SELECT ON ops.sources TO sismo_transform;
GRANT SELECT, INSERT, UPDATE ON staging.events TO sismo_transform;
GRANT SELECT, INSERT, UPDATE ON curated.events TO sismo_transform;
GRANT SELECT, INSERT, UPDATE ON curated.hourly_activity TO sismo_transform;
GRANT INSERT, SELECT ON ops.quality_results TO sismo_transform;
GRANT INSERT, SELECT ON ops.quarantine TO sismo_transform;
GRANT INSERT, SELECT ON ops.ai_runs TO sismo_transform;
GRANT INSERT, SELECT ON ops.metadata_runs TO sismo_transform;
GRANT INSERT, UPDATE ON ops.reports TO sismo_transform;
GRANT INSERT, SELECT ON ops.anomalies TO sismo_transform;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO sismo_transform;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA curated TO sismo_transform;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO sismo_transform;

-- ---------- sismo_reader: solo lectura (la plataforma usa este rol) ----------
GRANT SELECT ON curated.events, curated.hourly_activity TO sismo_reader;
GRANT INSERT ON ops.chat_messages TO sismo_reader;   # historial del chatbot
GRANT SELECT ON ops.quality_results, ops.reports, ops.anomalies,
               ops.ai_runs, ops.sources TO sismo_reader;
-- Estado del pipeline: tablas de metadatos de Airflow (creadas después;
-- se autorizan por default privileges para tablas futuras del esquema public)
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT SELECT ON TABLES TO sismo_reader;

-- ---------- sismo_admin: además de lo anterior, cuarentena y chat ----------
GRANT SELECT ON ops.quarantine, ops.chat_messages TO sismo_admin;
GRANT SELECT ON staging.events, raw.events TO sismo_admin;
GRANT SELECT ON curated.events, curated.hourly_activity,
                ops.quality_results, ops.reports, ops.anomalies,
                ops.ai_runs, ops.sources TO sismo_admin;
