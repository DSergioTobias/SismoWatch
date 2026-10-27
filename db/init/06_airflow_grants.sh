#!/bin/bash
# ============================================================
# 06_airflow_grants.sh - Grant sobre tablas de Airflow
#
# Airflow crea sus tablas en el schema public DESPUÉS de que
# PostgreSQL ejecuta docker-entrypoint-initdb.d. Por eso los
# grants se ponen en 04_grants.sql con ALTER DEFAULT PRIVILEGES
# (cubre tablas futuras) pero las tablas ya existentes en
# reinicios necesitan un grant explícito.
#
# Este script registra una función que puede ejecutarse
# manualmente o por el operador al arrancar Airflow:
#   docker exec sismowatch-postgres psql -U postgres -d sismowatch \
#     -c "SELECT grant_airflow_to_reader();"
# ============================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Función idempotente: da SELECT en las tablas de Airflow si existen
    CREATE OR REPLACE FUNCTION grant_airflow_to_reader()
    RETURNS void LANGUAGE plpgsql AS \$\$
    DECLARE
        t text;
        airflow_tables text[] := ARRAY['dag','dag_run','task_instance',
                                       'dag_tag','import_error','log',
                                       'serialized_dag','slot_pool'];
    BEGIN
        FOREACH t IN ARRAY airflow_tables LOOP
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = t
            ) THEN
                EXECUTE format('GRANT SELECT ON public.%I TO sismo_reader', t);
            END IF;
        END LOOP;
    END;
    \$\$;
EOSQL

echo "Función grant_airflow_to_reader() creada en la BD."
