"""Conexión a PostgreSQL.

La plataforma usa `sismo_reader` (solo lectura sobre curated/ops).
`sismo_admin` se usa exclusivamente para cuarentena y acciones admin.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from app.config import settings


def _params(role: str) -> dict[str, Any]:
    if role == "admin":
        user, password = (settings.sismo_pg_user_admin,
                          settings.sismo_pg_password_admin)
    else:
        user, password = (settings.sismo_pg_user_reader,
                          settings.sismo_pg_password_reader)
    return {
        "host": settings.sismo_pg_host,
        "port": settings.sismo_pg_port,
        "dbname": settings.sismo_pg_db,
        "user": user,
        "password": password,
        "connect_timeout": 10,
    }


@contextmanager
def get_conn(role: str = "reader") -> Iterator[Any]:
    conn = psycopg2.connect(**_params(role))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(role: str, sql: str, params: Any = None) -> list[dict[str, Any]]:
    with get_conn(role) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def fetch_one(role: str, sql: str, params: Any = None) -> dict[str, Any] | None:
    rows = fetch_all(role, sql, params)
    return rows[0] if rows else None
