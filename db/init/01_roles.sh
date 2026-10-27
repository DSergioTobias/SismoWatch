#!/bin/bash
# ============================================================
# 01_roles.sh - Roles de PostgreSQL con mínimo privilegio
#   sismo_ingest     -> solo escribe en raw (ingesta)
#   sismo_transform  -> lee raw, escribe staging/curated/ops
#   sismo_reader     -> solo lee curated + métricas (plataforma)
#   sismo_admin      -> acciones de administración (cuarentena, etc.)
# Se ejecuta una sola vez, durante el primer arranque del contenedor.
# ============================================================
set -euo pipefail

INGEST_PW="${SISMO_PG_PASSWORD_INGEST:-ingest_secret_change_me}"
TRANSFORM_PW="${SISMO_PG_PASSWORD_TRANSFORM:-transform_secret_change_me}"
READER_PW="${SISMO_PG_PASSWORD_READER:-reader_secret_change_me}"
ADMIN_PW="${SISMO_PG_PASSWORD_ADMIN:-admin_secret_change_me}"

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<EOSQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sismo_ingest') THEN
        CREATE ROLE sismo_ingest WITH LOGIN PASSWORD '${INGEST_PW}';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sismo_transform') THEN
        CREATE ROLE sismo_transform WITH LOGIN PASSWORD '${TRANSFORM_PW}';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sismo_reader') THEN
        CREATE ROLE sismo_reader WITH LOGIN PASSWORD '${READER_PW}';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sismo_admin') THEN
        CREATE ROLE sismo_admin WITH LOGIN PASSWORD '${ADMIN_PW}';
    END IF;
END
\$\$;

REVOKE ALL ON DATABASE ${POSTGRES_DB} FROM PUBLIC;
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO sismo_ingest, sismo_transform, sismo_reader, sismo_admin;
EOSQL

echo "[db-init] Roles creados/verificados: sismo_ingest, sismo_transform, sismo_reader, sismo_admin"
