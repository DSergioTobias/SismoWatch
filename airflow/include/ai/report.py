"""Generación del reporte diario con IA.

Flujo con controles anti-alucinación:
  1. El código calcula las cifras con SQL (KPIs reales).
  2. El LLM SOLO redacta a partir de esas cifras.
  3. Se valida que todos los números del texto existan en los KPIs.
  4. Si el proveedor falla → plan alterno (reporte generado por plantilla)
     y el pipeline NO se rompe.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from ai.llm import (LLMError, LLMResponse, OllamaClient, extract_json,
                    load_prompt, record_ai_run, sanitize_for_llm)
from db import fetch_all, fetch_one

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1) Cifras calculadas por el código (SQL puro)
# ---------------------------------------------------------------------------

def compute_kpis(days: int = 1) -> dict[str, Any]:
    """KPIs del período, calculados exclusivamente con SQL."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    totals = fetch_one(
        "reader",
        """
        SELECT COUNT(*)::int                          AS events,
               COALESCE(MAX(mag), 0)::numeric(4,2)    AS max_mag,
               COALESCE(AVG(mag), 0)::numeric(4,2)    AS avg_mag,
               COALESCE(SUM(CASE WHEN tsunami THEN 1 ELSE 0 END), 0)::int AS tsunami_events,
               COALESCE(MAX(event_time), %s)          AS last_event_time
        FROM curated.events
        WHERE event_time >= %s
        """,
        (since, since),
    ) or {}

    regions = fetch_all(
        "reader",
        """
        SELECT region, COUNT(*)::int AS events, COALESCE(MAX(mag),0)::numeric(4,2) AS max_mag
        FROM curated.events
        WHERE event_time >= %s
        GROUP BY region
        ORDER BY events DESC
        LIMIT 5
        """,
        (since,),
    )

    top_events = fetch_all(
        "reader",
        """
        SELECT event_uid, mag, place, event_time
        FROM curated.events
        WHERE event_time >= %s
        ORDER BY mag DESC NULLS LAST
        LIMIT 3
        """,
        (since,),
    )

    quality = fetch_one(
        "reader",
        """
        SELECT score, status, evaluated_at
        FROM ops.quality_results
        ORDER BY evaluated_at DESC
        LIMIT 1
        """,
    )

    return {
        "period_days": days,
        "events": int(totals.get("events") or 0),
        "max_mag": float(totals.get("max_mag") or 0),
        "avg_mag": float(totals.get("avg_mag") or 0),
        "tsunami_events": int(totals.get("tsunami_events") or 0),
        "top_regions": [dict(r) for r in regions],
        "top_events": [
            {
                "event_uid": e["event_uid"],
                "mag": float(e["mag"]) if e["mag"] is not None else None,
                "place": sanitize_for_llm(e.get("place"), 120),
                "event_time": e["event_time"].isoformat() if e.get("event_time") else None,
            }
            for e in top_events
        ],
        "quality_score": float(quality["score"]) if quality else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# 3) Validación numérica: todo número del texto debe existir en los KPIs
# ---------------------------------------------------------------------------

def extract_numbers(text: str) -> set[str]:
    """Extrae números normalizados de un texto (enteros y decimales)."""
    found: set[str] = set()
    for match in re.finditer(r"-?\d+(?:\.\d+)?", text):
        token = match.group(0)
        try:
            value = float(token)
        except ValueError:
            continue
        found.add(f"{value:.2f}")
        found.add(f"{value:.0f}")
    return found


def allowed_numbers(kpis: dict[str, Any]) -> set[str]:
    """Universo de números permitidos: los KPIs + rangos derivados de ellos."""
    allowed: set[str] = set()

    def add(value: Any) -> None:
        if value is None:
            return
        try:
            v = float(value)
        except (TypeError, ValueError):
            return
        allowed.add(f"{v:.2f}")
        allowed.add(f"{v:.0f}")

    for key in ("events", "max_mag", "avg_mag", "tsunami_events",
                "quality_score", "period_days"):
        add(kpis.get(key))
    for region in kpis.get("top_regions", []):
        add(region.get("events"))
        add(region.get("max_mag"))
    for event in kpis.get("top_events", []):
        add(event.get("mag"))
    return allowed


def validate_numbers(text: str, kpis: dict[str, Any]) -> tuple[bool, list[str]]:
    """Comprueba que cada número del texto exista en los KPIs calculados."""
    allowed = allowed_numbers(kpis)
    used = extract_numbers(text)
    unknown = sorted(n for n in used if n not in allowed)
    return (len(unknown) == 0), unknown


# ---------------------------------------------------------------------------
# 2) Redacción con el LLM + plan alterno
# ---------------------------------------------------------------------------

def _fallback_report(kpis: dict[str, Any]) -> dict[str, str]:
    """Plan alterno: reporte generado por plantilla si el LLM falla."""
    regions = kpis.get("top_regions", [])
    zones = ", ".join(r["region"] for r in regions[:3]) or "sin datos"
    return {
        "title": f"Reporte sísmico — {datetime.now(timezone.utc):%Y-%m-%d}",
        "summary": (
            f"En las últimas {kpis['period_days']}h se registraron "
            f"{kpis['events']} eventos. Magnitud máxima {kpis['max_mag']}, "
            f"promedio {kpis['avg_mag']}."
        ),
        "zones": [zones],
        "highlights": [
            f"{e['mag']} — {e['place']}"
            for e in kpis.get("top_events", []) if e.get("mag") is not None
        ],
        "quality_note": (
            f"Puntaje de calidad más reciente: {kpis.get('quality_score')}."
        ),
    }


def generate_daily_report(days: int = 1) -> dict[str, Any]:
    """Genera el reporte narrativo. Nunca lanza excepción al pipeline."""
    kpis = compute_kpis(days=days)
    prompt_tpl, version = load_prompt("daily_report.md")

    data_block = json.dumps(kpis, ensure_ascii=False, default=str, indent=1)
    prompt = prompt_tpl + "\n" + data_block

    response: LLMResponse | None = None
    try:
        client = OllamaClient()
        response = client.generate(prompt, json_format=True, num_predict=800)
        response.prompt_version = version
        report = extract_json(response.text)

        # normalización defensiva de campos
        clean = {
            "title": sanitize_for_llm(report.get("title"), 200)
                     or f"Reporte {datetime.now(timezone.utc):%Y-%m-%d}",
            "summary": sanitize_for_llm(report.get("summary"), 3000),
            "zones": [sanitize_for_llm(z, 200)
                      for z in (report.get("zones") or []) if z][:5],
            "highlights": [sanitize_for_llm(h, 300)
                           for h in (report.get("highlights") or []) if h][:5],
            "quality_note": sanitize_for_llm(report.get("quality_note"), 500),
        }

        # validación anti-alucinación: números del texto ⊆ KPIs
        full_text = " ".join([clean["summary"], *clean["zones"],
                              *clean["highlights"], clean["quality_note"]])
        ok, unknown = validate_numbers(full_text, kpis)
        if not ok:
            raise LLMError(
                f"El texto contiene números no calculados por el sistema: {unknown}"
            )
        if not clean["summary"]:
            raise LLMError("El modelo devolvió un reporte vacío")

        record_ai_run("daily_report", response, status="ok")
        return {**clean, "kpis": kpis, "status": "ok", "model": response.model}

    except LLMError as exc:
        logger.warning("IA no disponible para el reporte (%s); usando plan alterno", exc)
        record_ai_run("daily_report", response, status="fallback",
                      error_detail=str(exc))
        return {**_fallback_report(kpis), "kpis": kpis,
                "status": "fallback", "model": None, "error": str(exc)}
