"""Pruebas de la guardia SQL de solo lectura (chatbot).

Casos válidos e inválidos: cualquier intento de escritura o acceso fuera
del esquema curated debe bloquearse.
"""
from __future__ import annotations

import os
import sys

import pytest

# El origen único de la guardia es airflow/include/ai/sql_guard.py;
# el Dockerfile de la API la copia a app/sql_guard.py (mismo archivo).
_INCLUDE_AI = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "airflow", "include", "ai",
)
sys.path.insert(0, _INCLUDE_AI)

from sql_guard import SqlBlockedError, validate_readonly_sql  # noqa: E402

# ---------------- Válidas ----------------

def test_simple_select_is_allowed():
    sql = "SELECT mag, place FROM curated.events LIMIT 50"
    assert validate_readonly_sql(sql) == sql


def test_aggregate_without_limit_is_allowed():
    sql = "SELECT COUNT(*) AS total FROM curated.events"
    assert validate_readonly_sql(sql)


def test_where_and_order_allowed():
    sql = ("SELECT event_uid, mag FROM curated.events "
           "WHERE mag > 5 ORDER BY mag DESC LIMIT 100")
    assert validate_readonly_sql(sql)


def test_hourly_activity_allowed():
    sql = ("SELECT bucket_hour, event_count FROM curated.hourly_activity "
           "ORDER BY bucket_hour DESC LIMIT 24")
    assert validate_readonly_sql(sql)


def test_group_by_region_allowed():
    sql = ("SELECT region, COUNT(*) AS n FROM curated.events "
           "GROUP BY region ORDER BY n DESC LIMIT 10")
    assert validate_readonly_sql(sql)


# ---------------- Inválidas ----------------

def test_insert_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("INSERT INTO curated.events VALUES (1)")


def test_update_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("UPDATE curated.events SET mag = 99")


def test_drop_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("DROP TABLE curated.events")


def test_multiple_statements_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT 1; DROP TABLE curated.events")


def test_out_of_scope_schema_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT * FROM raw.events LIMIT 10")


def test_other_table_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT rolname FROM pg_roles LIMIT 10")


def test_system_catalog_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT * FROM pg_shadow LIMIT 5")


def test_missing_limit_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT mag, place FROM curated.events")


def test_limit_too_high_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT mag FROM curated.events LIMIT 100000")


def test_comment_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT 1 -- comentario FROM curated.events")


def test_unknown_column_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("SELECT password_hash FROM curated.events LIMIT 1")


def test_empty_blocked():
    with pytest.raises(SqlBlockedError):
        validate_readonly_sql("   ")
