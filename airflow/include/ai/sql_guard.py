"""Guardia de SQL de solo lectura para el chatbot.

El LLM propone una sentencia; esta capa la valida de forma estricta
antes de tocar la base de datos. Cualquier duda → se rechaza.
"""
from __future__ import annotations

import re
from datetime import datetime

ALLOWED_SCHEMAS = {"curated"}
ALLOWED_TABLES = {
    "curated.events",
    "curated.hourly_activity",
}
ALLOWED_COLUMNS = {
    "curated.events": {
        "event_uid", "mag", "place", "region", "event_time", "tsunami",
        "sig", "status", "mag_type", "event_type", "url", "longitude",
        "latitude", "depth_km", "published_date", "source_id",
    },
    "curated.hourly_activity": {
        "bucket_hour", "event_count", "max_mag", "avg_mag", "tsunami_count",
    },
}

MAX_LIMIT = 200

_FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter", "create", "truncate",
    "grant", "revoke", "copy", "call", "do ", "execute", "prepare",
    "vacuum", "analyze", "set ", "reset", "listen", "notify", "lock",
    "refresh", "comment", "security", "pg_read", "lo_import",
]

_FORBIDDEN_PATTERNS = [
    (r";", "múltiples sentencias"),
    (r"--", "comentarios SQL"),
    (r"/\*", "comentarios SQL"),
    (r"\bpg_", "acceso a catálogo del sistema"),
    (r"\bcurrent_setting\b", "acceso a configuración"),
    (r"\binto\b", "cláusula INTO"),
    (r"\bfor\s+update\b", "bloqueo de filas"),
    (r"\bover\s+ride\b", None),  # placeholder desactivado
]


class SqlBlockedError(Exception):
    """La sentencia fue rechazada por la guardia."""


def _strip_string_literals(sql: str) -> str:
    """Quita 'literales' para no confundir palabras dentro de comillas."""
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


def validate_readonly_sql(sql: str) -> str:
    """Valida y devuelve la SQL normalizada. Lanza SqlBlockedError si no pasa."""
    if not sql or not sql.strip():
        raise SqlBlockedError("Consulta vacía")

    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        raise SqlBlockedError("Consulta vacía")

    # 1) Una sola sentencia
    if ";" in cleaned:
        raise SqlBlockedError("Solo se permite una sentencia")

    # 2) Debe empezar por SELECT (o WITH ... SELECT)
    if not re.match(r"^(select|with)\b", cleaned, re.IGNORECASE):
        raise SqlBlockedError("Solo se permiten consultas SELECT")

    # 3) Palabras prohibidas (sobre el texto sin literales)
    no_literals = _strip_string_literals(cleaned).lower()
    for keyword in _FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword.strip())}\b", no_literals):
            raise SqlBlockedError(f"Palabra prohibida: {keyword.strip()}")

    for pattern, label in _FORBIDDEN_PATTERNS:
        if label and re.search(pattern, no_literals, re.IGNORECASE):
            raise SqlBlockedError(f"Patrón prohibido: {label}")

    # 4) Tablas y columnas referenciadas dentro del allowlist
    _check_tables_and_columns(cleaned)

    # 5) LIMIT obligatorio para consultas a nivel de detalle
    has_aggregate = bool(re.search(
        r"\b(count|sum|avg|min|max)\s*\(", no_literals))
    has_limit = bool(re.search(r"\blimit\s+\d+", no_literals))
    if not has_limit:
        if has_aggregate and "group by" in no_literals:
            pass  # agregados por grupo: el número de grupos es acotado
        elif has_aggregate:
            pass  # agregado global: devuelve 1 fila
        else:
            raise SqlBlockedError("Falta LIMIT (máximo 200)")
    else:
        limit_match = re.search(r"\blimit\s+(\d+)", no_literals)
        if limit_match and int(limit_match.group(1)) > MAX_LIMIT:
            raise SqlBlockedError(f"LIMIT máximo permitido: {MAX_LIMIT}")

    # 6) Sin funciones de escritura ni de red/sistema
    for fn in ("pg_sleep", "dblink", "pg_terminate", "pg_cancel",
               "lo_export", "pg_read_file", "pg_write_file"):
        if fn in no_literals:
            raise SqlBlockedError(f"Función prohibida: {fn}")

    return cleaned


def _check_tables_and_columns(sql: str) -> None:
    """Verifica que toda tabla referenciada esté permitida.

    Las columnas se validan contra el esquema permitido; si la SQL usa
    columnas que no existen en el allowlist se bloquea.
    """
    no_literals = _strip_string_literals(sql)

    # Tablas después de FROM / JOIN
    table_refs = re.findall(
        r"\b(?:from|join)\s+([a-zA-Z_][\w\.]*)", no_literals, re.IGNORECASE)
    if not table_refs:
        raise SqlBlockedError("Consulta sin tabla referenciada")

    allowed_bare: set[str] = set()
    for ref in table_refs:
        name = ref.lower().strip('"')
        if "." not in name:
            qualified = f"curated.{name}"   # sin esquema → curated
        else:
            qualified = name
        if qualified not in ALLOWED_TABLES:
            raise SqlBlockedError(f"Tabla no permitida: {ref}")
        allowed_bare.add(qualified.split(".", 1)[1])

    # Todo se valida en minúsculas
    lowered = no_literals.lower()

    # Identificadores referenciados. Recogemos los alias declarados (as <alias>
    # y alias implícitos tras FROM/JOIN) para no bloquearlos, y validamos el
    # resto contra las columnas permitidas.
    declared_aliases: set[str] = set()
    for match in re.finditer(r"\bas\s+([a-zA-Z_][\w]*)", lowered):
        declared_aliases.add(match.group(1))
    for match in re.finditer(
            r"\b(?:from|join)\s+[a-zA-Z_][\w\.]*\s+(?:as\s+)?"
            r"([a-zA-Z_][\w]*)", lowered):
        alias = match.group(1)
        if alias not in {"where", "group", "order", "limit", "having", "inner",
                         "left", "right", "full", "join", "on", "union",
                         "cross", "outer", "using", "is", "not", "null"}:
            declared_aliases.add(alias)

    # eliminamos alias declarados y referencias a tablas (schema.table)
    working = re.sub(r"\bas\s+([a-zA-Z_][\w]*)", " ", lowered)
    working = re.sub(r"\b[a-z_][\w]*\.[a-z_][\w]*\b", " ", working)
    identifiers = set(re.findall(r"\b([a-z_][\w]*)\b", working))
    keywords = {
        "select", "from", "where", "group", "by", "order", "limit", "offset",
        "as", "and", "or", "not", "in", "is", "null", "like", "ilike",
        "between", "case", "when", "then", "else", "end", "desc", "asc",
        "distinct", "having", "on", "join", "left", "right", "inner", "outer",
        "with", "union", "all", "count", "sum", "avg", "min", "max", "coalesce",
        "date_trunc", "extract", "round", "cast", "to_char", "to_timestamp",
        "now", "interval", "true", "false", "extract", "age", "length",
        "lower", "upper", "trim", "abs", "ceil", "floor", "round", "greatest",
        "least", "hour", "minute", "second", "day", "month", "year", "week",
        "quarter", "dow", "doy", "epoch", "date", "timestamp", "timestamptz",
        "numeric", "integer", "text", "boolean", "bigint", "varchar",
        "over", "partition", "rows", "range", "unbounded", "preceding",
        "following", "current", "row", "filter", "within", "extract",
        "to_date", "concat", "concat_ws", "nullif", "generate_series",
        "lateral", "json", "jsonb", "array", "exists", "any", "some",
        "values", "default", "only", "returning", "explain", "analyze",
        "public", "curated", "staging", "raw", "ops",
    }
    # Columnas válidas = unión de todas las permitidas
    valid_columns = set().union(*ALLOWED_COLUMNS.values())
    suspicious = (identifiers - keywords - valid_columns
                  - declared_aliases - allowed_bare)
    if suspicious:
        raise SqlBlockedError(
            "Identificadores no permitidos: " + ", ".join(sorted(suspicious))
        )


def explain_guard_error(exc: Exception) -> str:
    return f"Consulta bloqueada por seguridad: {exc}"
