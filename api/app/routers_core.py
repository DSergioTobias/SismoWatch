"""Endpoints de autenticación y datos del dashboard."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.db import fetch_all, fetch_one
from app.security import authenticate, create_token, current_user, require_admin

router = APIRouter()


# ----------------------------- Auth -----------------------------

class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=200)
    password: str = Field(min_length=1, max_length=200)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


@router.post("/auth/login", response_model=TokenResponse)
def login(body: LoginRequest) -> TokenResponse:
    user = authenticate(body.email, body.password)
    if user is None:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    token = create_token(body.email, user["role"])
    return TokenResponse(access_token=token, role=user["role"],
                         username=user["username"])


# ----------------------------- KPIs -----------------------------

@router.get("/api/kpis")
def get_kpis(user: dict = Depends(current_user)) -> dict[str, Any]:
    totals = fetch_one(
        "reader",
        """
        SELECT COUNT(*)::int AS events_24h,
               COALESCE(MAX(mag), 0)::numeric(4,2) AS max_mag,
               COALESCE(AVG(mag), 0)::numeric(4,2) AS avg_mag,
               COALESCE(SUM(CASE WHEN tsunami THEN 1 ELSE 0 END), 0)::int AS tsunami_events,
               MAX(event_time) AS last_event_time
        FROM curated.events
        WHERE event_time >= now() - interval '24 hours'
        """,
    ) or {}
    quality = fetch_one(
        "reader",
        "SELECT score, status, evaluated_at FROM ops.quality_results "
        "ORDER BY evaluated_at DESC LIMIT 1",
    )
    last_update = fetch_one(
        "reader", "SELECT MAX(transformed_at) AS ts FROM curated.events")
    return {
        "events_24h": totals.get("events_24h", 0),
        "max_mag": float(totals.get("max_mag") or 0),
        "avg_mag": float(totals.get("avg_mag") or 0),
        "tsunami_events": totals.get("tsunami_events", 0),
        "quality_score": float(quality["score"]) if quality else None,
        "quality_status": quality["status"] if quality else None,
        "last_event_time": _iso(totals.get("last_event_time")),
        "last_update": _iso(last_update["ts"] if last_update else None),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


# ----------------------------- Eventos -----------------------------

@router.get("/api/events")
def get_events(
    user: dict = Depends(current_user),
    start: str | None = Query(None),
    end: str | None = Query(None),
    min_mag: float | None = Query(None, ge=-2, le=10),
    region: str | None = Query(None, max_length=120),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    where, params = _event_filters(start, end, min_mag, region)
    total = fetch_one("reader",
                      f"SELECT COUNT(*)::int AS n FROM curated.events {where}",
                      params) or {"n": 0}
    rows = fetch_all(
        "reader",
        f"""
        SELECT event_uid, mag, place, region, event_time, tsunami, status,
               mag_type, url, longitude, latitude, depth_km
        FROM curated.events
        {where}
        ORDER BY event_time DESC
        LIMIT %s OFFSET %s
        """,
        (*params, limit, offset),
    )
    return {"items": rows, "total": total["n"], "limit": limit,
            "offset": offset}


def _event_filters(start, end, min_mag, region) -> tuple[str, list]:
    clauses, params = [], []
    if start:
        clauses.append("event_time >= %s")
        params.append(start)
    if end:
        clauses.append("event_time <= %s")
        params.append(end)
    if min_mag is not None:
        clauses.append("mag >= %s")
        params.append(min_mag)
    if region:
        clauses.append("region ILIKE %s")
        params.append(f"%{region}%")
    return ((" WHERE " + " AND ".join(clauses)) if clauses else ""), params


# ----------------------------- Estadísticas -----------------------------

@router.get("/api/stats/timeseries")
def timeseries(
    user: dict = Depends(current_user),
    start: str | None = Query(None),
    end: str | None = Query(None),
    min_mag: float | None = Query(None, ge=-2, le=10),
    region: str | None = Query(None, max_length=120),
) -> dict[str, Any]:
    where, params = _event_filters(start, end, min_mag, region)
    hourly = fetch_all(
        "reader",
        f"""
        SELECT date_trunc('hour', event_time) AS bucket,
               COUNT(*)::int AS events,
               COALESCE(MAX(mag), 0)::numeric(4,2) AS max_mag
        FROM curated.events {where}
        GROUP BY 1 ORDER BY 1
        """,
        params,
    )
    by_mag = fetch_all(
        "reader",
        f"""
        SELECT CASE
                 WHEN mag IS NULL THEN 'sin dato'
                 WHEN mag < 2 THEN '<2'
                 WHEN mag < 4 THEN '2-4'
                 WHEN mag < 6 THEN '4-6'
                 WHEN mag < 8 THEN '6-8'
                 ELSE '8+' END AS bucket,
               COUNT(*)::int AS events
        FROM curated.events {where}
        GROUP BY 1 ORDER BY 1
        """,
        params,
    )
    regions = fetch_all(
        "reader",
        f"""
        SELECT region, COUNT(*)::int AS events,
               COALESCE(MAX(mag),0)::numeric(4,2) AS max_mag
        FROM curated.events {where}
        GROUP BY region ORDER BY events DESC LIMIT 10
        """,
        params,
    )
    return {"hourly": hourly, "by_magnitude": by_mag, "regions": regions}


@router.get("/api/stats/map")
def map_events(
    user: dict = Depends(current_user),
    start: str | None = Query(None),
    end: str | None = Query(None),
    min_mag: float | None = Query(None, ge=-2, le=10),
    region: str | None = Query(None, max_length=120),
    limit: int = Query(1000, ge=1, le=3000),
) -> dict[str, Any]:
    where, params = _event_filters(start, end, min_mag, region)
    rows = fetch_all(
        "reader",
        f"""
        SELECT event_uid, mag, place, region, event_time,
               longitude, latitude, depth_km, tsunami
        FROM curated.events {where}
        ORDER BY event_time DESC LIMIT %s
        """,
        (*params, limit),
    )
    return {"items": rows}
