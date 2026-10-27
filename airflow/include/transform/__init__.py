"""Transformación raw → staging → curated.

Reglas centrales:
  - `event_geojson_to_staging`: convierte un evento normalizado (con la
    forma plana que producen los conectores) a la fila tipada de staging.
  - `sanitize_text`: elimina caracteres de control y limita largo (mitiga
    prompt injection / datos sucios que viajan hacia el LLM).
  - Idempotencia: upsert por (source_id, event_uid) en todas las capas.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f<>]")
_MULTI_SPACE = re.compile(r"\s+")

MAX_TEXT_LEN = 300


def sanitize_text(value: Any, max_len: int = MAX_TEXT_LEN) -> str | None:
    """Normaliza un texto externo: sin HTML ni caracteres de control,
    espacios colapsados, longitud limitada."""
    if value is None:
        return None
    text = str(value)
    text = _CONTROL_CHARS.sub(" ", text)
    text = _MULTI_SPACE.sub(" ", text).strip()
    return text[:max_len] or None


def extract_region(place: str | None) -> str:
    """Deriva la región de 'place' (todo después de la primera coma).

    '12 km SW of Foo' → 'SW of Foo'; sin coma → el texto completo.
    """
    if not place:
        return "unknown"
    if "," in place:
        region = place.split(",", 1)[1].strip()
    else:
        region = place.strip()
    return sanitize_text(region) or "unknown"


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "t"}
    return False


def ms_epoch_to_ts(value: Any) -> datetime | None:
    """properties.time viene en milisegundos epoch (UTC)."""
    ms = _to_float(value)
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def event_to_staging(event: dict[str, Any], source_id: str) -> dict[str, Any]:
    """Convierte un evento normalizado por el conector en fila de staging.

    Esta es la función de transformación probada por unit test.
    """
    place = sanitize_text(event.get("place"))
    return {
        "source_id": source_id,
        "event_uid": str(event.get("event_uid")),
        "mag": _to_float(event.get("mag")),
        "place": place,
        "region": extract_region(place),
        "event_time": ms_epoch_to_ts(event.get("event_time")),
        "updated_time": ms_epoch_to_ts(event.get("updated_time")),
        "tsunami": _to_bool(event.get("tsunami")),
        "sig": _to_int(event.get("sig")),
        "status": sanitize_text(event.get("status"), 40),
        "mag_type": sanitize_text(event.get("mag_type"), 20),
        "event_type": sanitize_text(event.get("event_type"), 40),
        "url": sanitize_text(event.get("url"), 500),
        "longitude": _to_float(event.get("longitude")),
        "latitude": _to_float(event.get("latitude")),
        "depth_km": _to_float(event.get("depth_km")),
    }
