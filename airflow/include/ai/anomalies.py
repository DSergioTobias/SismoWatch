"""Detección de anomalías sísmicas (técnica analítica, sin LLM).

Técnica elegida: z-score sobre la actividad horaria (ventana móvil) +
detección de enjambres (picos de conteo y de magnitud máxima).

El código calcula TODAS las cifras; el LLM solo redacta la explicación
y posteriormente se valida que los números del texto coincidan.
"""
from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

Z_THRESHOLD_HIGH = 3.0
Z_THRESHOLD_MEDIUM = 2.0
MIN_HISTORY_BUCKETS = 12   # mínimo de horas de historial para evaluar


def compute_hourly_series(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrega eventos crudos a buckets horarios.

    rows: [{event_time: datetime, mag: float|None, tsunami: bool}]
    """
    buckets: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        et = row.get("event_time")
        if et is None:
            continue
        if et.tzinfo is None:
            et = et.replace(tzinfo=timezone.utc)
        hour = et.replace(minute=0, second=0, microsecond=0)
        bucket = buckets.setdefault(hour, {
            "bucket_hour": hour, "event_count": 0,
            "max_mag": None, "_mags": [], "tsunami_count": 0,
        })
        bucket["event_count"] += 1
        mag = row.get("mag")
        if mag is not None:
            bucket["_mags"].append(float(mag))
            if bucket["max_mag"] is None or float(mag) > bucket["max_mag"]:
                bucket["max_mag"] = float(mag)
        if row.get("tsunami"):
            bucket["tsunami_count"] += 1

    series = []
    for hour in sorted(buckets):
        b = buckets[hour]
        mags = b.pop("_mags")
        b["avg_mag"] = round(sum(mags) / len(mags), 3) if mags else None
        series.append(b)
    return series


def zscore(value: float, population: list[float]) -> float | None:
    """Z-score con desviación estándar poblacional; None si no hay variación."""
    if len(population) < 3:
        return None
    stdev = statistics.pstdev(population)
    if stdev == 0:
        return 0.0 if value == statistics.mean(population) else None
    return round((value - statistics.mean(population)) / stdev, 3)


def detect_anomalies(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Detecta picos anómalos de actividad en la serie horaria.

    Para cada bucket se compara su conteo (y su magnitud máxima) contra
    el resto de la serie. Devuelve anomalías con el z-score calculado.
    """
    if len(series) < MIN_HISTORY_BUCKETS:
        logger.info("Serie insuficiente (%d buckets) para detectar anomalías",
                    len(series))
        return []

    counts = [float(b["event_count"]) for b in series]
    anomalies: list[dict[str, Any]] = []

    for idx, bucket in enumerate(series):
        count = float(bucket["event_count"])
        z_count = zscore(count, counts)

        # comparación local: media de las 6 horas vecinas (excluyendo el bucket)
        neighbors = counts[max(0, idx - 6):idx] + counts[idx + 1:idx + 7]
        z_local = zscore(count, neighbors) if len(neighbors) >= 3 else None

        trigger_z = max(
            [z for z in (z_count, z_local) if z is not None], default=None
        )
        if trigger_z is None or trigger_z < Z_THRESHOLD_MEDIUM:
            continue

        severity = "high" if trigger_z >= Z_THRESHOLD_HIGH else "medium"
        anomalies.append({
            "window_start": bucket["bucket_hour"],
            "window_end": bucket["bucket_hour"] + timedelta(hours=1),
            "metric": "event_count",
            "value": count,
            "expected": round(statistics.mean(
                [v for i, v in enumerate(counts) if i != idx]), 3),
            "z_score": trigger_z,
            "severity": severity,
            # cifras exactas para la explicación (la calcula el código)
            "facts": {
                "event_count": int(count),
                "avg_hourly_baseline": round(statistics.mean(
                    [v for i, v in enumerate(counts) if i != idx]), 2),
                "max_mag": bucket.get("max_mag"),
                "hour_utc": bucket["bucket_hour"].isoformat(),
            },
        })

    # enjambre por magnitud: hora con magnitud máxima muy por encima del resto
    mags = [float(b["max_mag"]) for b in series if b.get("max_mag") is not None]
    if len(mags) >= MIN_HISTORY_BUCKETS:
        for bucket in series:
            mag = bucket.get("max_mag")
            if mag is None:
                continue
            z_mag = zscore(float(mag), mags)
            if z_mag is not None and z_mag >= Z_THRESHOLD_HIGH:
                # evita duplicar la misma hora ya marcada por conteo
                if any(a["window_start"] == bucket["bucket_hour"]
                       for a in anomalies):
                    continue
                anomalies.append({
                    "window_start": bucket["bucket_hour"],
                    "window_end": bucket["bucket_hour"] + timedelta(hours=1),
                    "metric": "max_mag",
                    "value": float(mag),
                    "expected": round(statistics.mean(
                        [v for v in mags if v != float(mag)]), 3),
                    "z_score": z_mag,
                    "severity": "high",
                    "facts": {
                        "max_mag": float(mag),
                        "avg_max_mag": round(statistics.mean(mags), 2),
                        "hour_utc": bucket["bucket_hour"].isoformat(),
                    },
                })

    anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)
    return anomalies
