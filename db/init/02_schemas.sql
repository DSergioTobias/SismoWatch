-- ============================================================
-- 02_schemas.sql - Capas de la arquitectura medallón + operación
--   raw      : dato tal como llega de la fuente (bronze)
--   staging  : limpio y tipado (silver)
--   curated  : listo para consumo (gold)
--   ops      : control de calidad, cuarentena, IA, reportes
-- ============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS ops;

GRANT USAGE ON SCHEMA raw, staging, curated, ops TO sismo_ingest, sismo_transform, sismo_reader, sismo_admin;
