"""Reglas de calidad de datos (validaciones bloqueantes).

Se ejecutan sobre las filas de staging propuestas. Si alguna regla
obligatoria falla a nivel de lote (bloqueo), nada llega a curated.
Los registros individualmente inválidos van a cuarentena con su motivo.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Definición de reglas
# ---------------------------------------------------------------------------

@dataclass
class RuleResult:
    code: str
    description: str
    passed: bool
    rejected: int = 0            # filas que la incumplen
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "description": self.description,
            "passed": self.passed,
            "rejected": self.rejected,
            "detail": self.detail,
        }


@dataclass
class RowRejection:
    event_uid: str
    reasons: list[dict[str, str]] = field(default_factory=list)


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


# --- Reglas de fila (devuelven la lista de motivos de rechazo) ---

def rule_required_fields(row: dict[str, Any]) -> list[str]:
    missing = [f for f in ("event_uid", "event_time", "latitude", "longitude")
               if _is_missing(row.get(f))]
    return [f"campo obligatorio ausente: {m}" for m in missing]


def rule_latitude_range(row: dict[str, Any]) -> list[str]:
    lat = row.get("latitude")
    if lat is None:
        return []  # ya cubierto por required_fields
    return [] if -90 <= float(lat) <= 90 else [f"latitud fuera de rango: {lat}"]


def rule_longitude_range(row: dict[str, Any]) -> list[str]:
    lon = row.get("longitude")
    if lon is None:
        return []
    return [] if -180 <= float(lon) <= 180 else [f"longitud fuera de rango: {lon}"]


def rule_magnitude_plausible(row: dict[str, Any]) -> list[str]:
    mag = row.get("mag")
    if mag is None:
        return []  # la magnitud puede ser nula en feeds crudos; no bloquea
    mag = float(mag)
    if not -1.5 <= mag <= 10.0:
        return [f"magnitud implausible: {mag}"]
    return []


def rule_depth_plausible(row: dict[str, Any]) -> list[str]:
    depth = row.get("depth_km")
    if depth is None:
        return []
    depth = float(depth)
    if not -100 <= depth <= 1000:
        return [f"profundidad implausible: {depth} km"]
    return []


def rule_event_time_format(row: dict[str, Any]) -> list[str]:
    et = row.get("event_time")
    if et is None:
        return []
    if not isinstance(et, datetime):
        return [f"fecha inválida (no es datetime): {et!r}"]
    if et > datetime.now(timezone.utc) + timedelta(minutes=10):
        return [f"fecha en el futuro: {et.isoformat()}"]
    return []


# --- Reglas de lote (control de frescura y volumen) ---

def rule_freshness(latest_ingested_at: datetime | None,
                   max_age_minutes: int = 30) -> RuleResult:
    """La última carga exitosa no debe superar el umbral."""
    if latest_ingested_at is None:
        return RuleResult(
            code="freshness",
            description=f"última carga con menos de {max_age_minutes} min",
            passed=False, detail="sin cargas registradas",
        )
    age = datetime.now(timezone.utc) - latest_ingested_at
    ok = age <= timedelta(minutes=max_age_minutes)
    return RuleResult(
        code="freshness",
        description=f"última carga con menos de {max_age_minutes} min",
        passed=ok,
        detail=f"edad de la carga: {age.total_seconds() / 60:.1f} min",
    )


def rule_volume(count: int, expected_min: int = 1,
                expected_max: int = 5000) -> RuleResult:
    """Volumen anormalmente bajo o alto respecto al rango esperado."""
    ok = expected_min <= count <= expected_max
    return RuleResult(
        code="volume",
        description=f"volumen entre {expected_min} y {expected_max} registros",
        passed=ok,
        detail=f"registros procesados: {count}",
    )


def rule_uniqueness(rows: list[dict[str, Any]]) -> RuleResult:
    seen: set[str] = set()
    dupes = 0
    for row in rows:
        uid = str(row.get("event_uid"))
        if uid in seen:
            dupes += 1
        seen.add(uid)
    return RuleResult(
        code="uniqueness",
        description="event_uid único dentro del lote",
        passed=dupes == 0,
        rejected=dupes,
        detail=f"duplicados detectados: {dupes}",
    )


# Evaluador principal -------------------------------------------------------

ROW_RULES: list[tuple[str, Callable[[dict[str, Any]], list[str]]]] = [
    ("required_fields", rule_required_fields),
    ("latitude_range", rule_latitude_range),
    ("longitude_range", rule_longitude_range),
    ("magnitude_plausible", rule_magnitude_plausible),
    ("depth_plausible", rule_depth_plausible),
    ("event_time_format", rule_event_time_format),
]


def evaluate_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]],
                                                       list[RowRejection]]:
    """Aplica reglas de fila. Devuelve (válidos, rechazados con motivos)."""
    valid: list[dict[str, Any]] = []
    rejected: list[RowRejection] = []
    for row in rows:
        reasons: list[dict[str, str]] = []
        for code, fn in ROW_RULES:
            for message in fn(row):
                reasons.append({"rule": code, "detail": message})
        if reasons:
            rejected.append(RowRejection(event_uid=str(row.get("event_uid")),
                                         reasons=reasons))
        else:
            valid.append(row)
    return valid, rejected


def evaluate_batch(rows: list[dict[str, Any]],
                   latest_ingested_at: datetime | None = None,
                   freshness_max_minutes: int = 30,
                   volume_min: int = 1,
                   volume_max: int = 5000) -> dict[str, Any]:
    """Evalúa todas las reglas del lote y calcula el puntaje (0-100)."""
    results: list[RuleResult] = [
        rule_uniqueness(rows),
        rule_volume(len(rows), volume_min, volume_max),
        rule_freshness(latest_ingested_at, freshness_max_minutes),
    ]
    # reglas a nivel de registro (resumen)
    valid, rejected = evaluate_rows(rows)
    row_rules_failed = {r["rule"] for rej in rejected for r in rej.reasons}
    for code, _fn in ROW_RULES:
        results.append(RuleResult(
            code=code,
            description=f"registro cumple {code}",
            passed=code not in row_rules_failed,
            rejected=sum(1 for rej in rejected
                         for r in rej.reasons if r["rule"] == code),
        ))

    passed = sum(1 for r in results if r.passed)
    score = round(100.0 * passed / len(results), 2)
    status = "pass" if all(r.passed for r in results) else "fail"
    return {
        "rules": [r.as_dict() for r in results],
        "rules_evaluated": len(results),
        "rules_passed": passed,
        "rules_rejected": len(results) - passed,
        "records_total": len(rows),
        "records_valid": len(valid),
        "records_rejected": len(rejected),
        "score": score,
        "status": status,
        "rejected_rows": rejected,
        "valid_rows": valid,
    }
