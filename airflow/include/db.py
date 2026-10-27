"""Acceso a PostgreSQL con roles de mínimo privilegio.

Cada tarea usa el rol que le corresponde:
  - ingesta      → sismo_ingest   (solo escribe en raw)
  - transform    → sismo_transform (raw → staging → curated)
  - plataforma   → sismo_reader    (solo lectura)
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection


def _conn_params(role: str) -> dict[str, str]:
    """role: ingest | transform | reader | admin"""
    suffix = role.upper()
    return {
        "host": os.environ.get("SISMO_PG_HOST", "postgres"),
        "port": os.environ.get("SISMO_PG_PORT", "5432"),
        "dbname": os.environ.get("SISMO_PG_DB", "sismowatch"),
        "user": os.environ.get(f"SISMO_PG_USER_{suffix}", f"sismo_{role}"),
        "password": os.environ.get(f"SISMO_PG_PASSWORD_{suffix}", ""),
        "connect_timeout": 10,
    }


@contextmanager
def get_conn(role: str = "transform") -> Iterator[PgConnection]:
    conn = psycopg2.connect(**_conn_params(role))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(role: str, sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    with get_conn(role) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def fetch_one(role: str, sql: str, params: tuple | dict | None = None) -> dict[str, Any] | None:
    rows = fetch_all(role, sql, params)
    return rows[0] if rows else None


def execute(role: str, sql: str, params: tuple | dict | None = None) -> int:
    """Ejecuta DML y devuelve filas afectadas (la conexión hace commit)."""
    with get_conn(role) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount
