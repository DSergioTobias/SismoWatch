"""Endpoints de reportes, pipeline, tiempo real (SSE), cuarentena, admin
y chatbot IA."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.db import fetch_all, fetch_one, get_conn
from app.security import current_user, require_admin

logger = logging.getLogger(__name__)
router = APIRouter()


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return value if value is None else str(value)


# ----------------------------- Reportes -----------------------------

@router.get("/api/reports")
def list_reports(
    user: dict = Depends(current_user),
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    search: str | None = Query(None, max_length=200),
) -> dict[str, Any]:
    where, params = "", []
    if search:
        where = "WHERE title ILIKE %s OR content_md ILIKE %s"
        params = [f"%{search}%", f"%{search}%"]
    total = fetch_one("reader",
                      f"SELECT COUNT(*)::int AS n FROM ops.reports {where}",
                      params) or {"n": 0}
    rows = fetch_all(
        "reader",
        f"""
        SELECT id, report_date, title, content_md, kpis, model, status, created_at
        FROM ops.reports {where}
        ORDER BY report_date DESC, created_at DESC
        LIMIT %s OFFSET %s
        """,
        (*params, limit, offset),
    )
    items = [{
        "id": r["id"],
        "report_date": _iso(r["report_date"]),
        "title": r["title"],
        "content": _parse_content(r["content_md"]),
        "kpis": r["kpis"],
        "model": r["model"],
        "status": r["status"],
        "created_at": _iso(r["created_at"]),
    } for r in rows]
    return {"items": items, "total": total["n"], "limit": limit,
            "offset": offset}


def _parse_content(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {"summary": str(raw or "")}


@router.get("/api/reports/export")
def export_reports(
    user: dict = Depends(current_user),
    format: str = Query("csv", pattern="^(csv|html)$"),
    search: str | None = Query(None, max_length=200),
) -> StreamingResponse:
    where, params = "", []
    if search:
        where = "WHERE title ILIKE %s"
        params = [f"%{search}%"]
    rows = fetch_all(
        "reader",
        f"SELECT report_date, title, content_md, model, status "
        f"FROM ops.reports {where} ORDER BY report_date DESC",
        params,
    )
    if format == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["fecha", "titulo", "contenido", "modelo", "estado"])
        for r in rows:
            content = _parse_content(r["content_md"])
            writer.writerow([
                str(r["report_date"]), r["title"],
                content.get("summary", ""), r["model"], r["status"],
            ])
        data = buf.getvalue()
        media = "text/csv; charset=utf-8"
        fname = "reportes_sismowatch.csv"
    else:
        blocks = "\n".join(
            f"<section><h3>{r['title']} ({r['report_date']})</h3>"
            f"<p>{_parse_content(r['content_md']).get('summary','')}</p></section>"
            for r in rows
        )
        data = ("<!doctype html><html><meta charset='utf-8'>"
                "<title>Reportes SismoWatch</title><h1>Reportes SismoWatch</h1>"
                f"{blocks}</html>")
        media = "text/html; charset=utf-8"
        fname = "reportes_sismowatch.html"

    return StreamingResponse(
        iter([data]), media_type=media,
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


# ----------------------------- Pipeline -----------------------------

@router.get("/api/pipeline/status")
def pipeline_status(user: dict = Depends(current_user)) -> dict[str, Any]:
    """Última ejecución de cada DAG + calidad + frescura.

    Usa el rol reader (select sobre metadatos de Airflow autorizado por
    default privileges) y las tablas de calidad de ops.
    """
    dags = fetch_all(
        "reader",
        """
        SELECT d.dag_id,
               (SELECT state FROM airflow.dag_run r
                 WHERE r.dag_id = d.dag_id
                 ORDER BY r.execution_date DESC LIMIT 1) AS last_state,
               (SELECT run_id FROM airflow.dag_run r
                 WHERE r.dag_id = d.dag_id
                 ORDER BY r.execution_date DESC LIMIT 1) AS last_run_id,
               (SELECT execution_date FROM airflow.dag_run r
                 WHERE r.dag_id = d.dag_id
                 ORDER BY r.execution_date DESC LIMIT 1) AS last_execution
        FROM airflow.dag d
        WHERE d.is_paused = false
        ORDER BY d.dag_id
        """,
    )
    quality = fetch_all(
        "reader",
        """
        SELECT dag_run_id, score, status, records_total, records_valid,
               records_rejected, evaluated_at
        FROM ops.quality_results
        WHERE status <> 'ingest_ok'
        ORDER BY evaluated_at DESC LIMIT 5
        """,
    )
    freshness = fetch_one(
        "reader",
        "SELECT MAX(ingested_at) AS last_ingest FROM raw.events",
    )
    quarantine_count = fetch_one(
        "reader",
        "SELECT COUNT(*)::int AS n FROM ops.quarantine",
    ) if user["role"] == "admin" else {"n": None}

    return {
        "dags": [{
            "dag_id": d["dag_id"],
            "last_state": d["last_state"],
            "last_run_id": d["last_run_id"],
            "last_execution": _iso(d["last_execution"]),
        } for d in dags],
        "quality": [{
            "dag_run_id": q["dag_run_id"],
            "score": float(q["score"]),
            "status": q["status"],
            "records_total": q["records_total"],
            "records_valid": q["records_valid"],
            "records_rejected": q["records_rejected"],
            "evaluated_at": _iso(q["evaluated_at"]),
        } for q in quality],
        "freshness": {
            "last_ingest": _iso(freshness["last_ingest"]) if freshness else None,
            "fresh": _is_fresh(freshness["last_ingest"] if freshness else None),
        },
        "quarantine_count": quarantine_count["n"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _is_fresh(last: Any, max_minutes: int = 30) -> bool | None:
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - last
    return age.total_seconds() <= max_minutes * 60


# ----------------------------- Cuarentena (admin) -----------------------------

@router.get("/api/quality/quarantine")
def quarantine(user: dict = Depends(require_admin),
               limit: int = Query(50, ge=1, le=500),
               offset: int = Query(0, ge=0)) -> dict[str, Any]:
    total = fetch_one("admin",
                      "SELECT COUNT(*)::int AS n FROM ops.quarantine") or {"n": 0}
    rows = fetch_all(
        "admin",
        """
        SELECT id, source_id, event_uid, payload, reasons, detected_at
        FROM ops.quarantine
        ORDER BY detected_at DESC LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )
    return {"items": rows, "total": total["n"], "limit": limit,
            "offset": offset}


# ----------------------------- Tiempo real (SSE) -----------------------------

@router.get("/api/stream")
async def stream_events(request: Request,
                        user: dict = Depends(current_user)) -> StreamingResponse:
    """Server-Sent Events: eventos nuevos + heartbeat (reconexión nativa)."""

    async def generator() -> AsyncIterator[str]:
        last_seen = fetch_one(
            "reader",
            "SELECT COALESCE(MAX(event_time), now()) AS ts FROM curated.events",
        )["ts"]
        yield _sse({"type": "connected",
                    "last_update": _iso(last_seen)})

        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(10)          # polling interno ≤30s
            rows = fetch_all(
                "reader",
                """
                SELECT event_uid, mag, place, region, event_time,
                       longitude, latitude, tsunami
                FROM curated.events
                WHERE event_time > %s
                ORDER BY event_time ASC LIMIT 50
                """,
                (last_seen,),
            )
            if rows:
                last_seen = rows[-1]["event_time"]
                yield _sse({"type": "events", "items": [
                    {**r, "event_time": _iso(r["event_time"])} for r in rows],
                    "last_update": _iso(last_seen)})
            else:
                yield _sse({"type": "heartbeat",
                            "last_update": _iso(last_seen)})

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, default=str)}\n\n"


# ----------------------------- Admin -----------------------------

class BackfillRequest(BaseModel):
    days: int = Field(default=30, ge=1, le=365)
    min_magnitude: float = Field(default=2.5, ge=0, le=10)
    source_id: str = Field(default="usgs_feed", max_length=100)


@router.post("/api/admin/backfill")
def launch_backfill(body: BackfillRequest,
                    user: dict = Depends(require_admin)) -> dict[str, Any]:
    """Lanza el DAG de backfill en Airflow (vía su API REST con basic auth)."""
    return _trigger_airflow_dag("backfill_historico", body.model_dump())


class RegenerateRequest(BaseModel):
    days: int = Field(default=1, ge=1, le=30)


@router.post("/api/admin/reports/regenerate")
def regenerate_report(body: RegenerateRequest,
                      user: dict = Depends(require_admin)) -> dict[str, Any]:
    return _trigger_airflow_dag("reporte_diario_ia", {"days": body.days})


def _trigger_airflow_dag(dag_id: str, conf: dict) -> dict[str, Any]:
    """Dispara un DAG vía la API REST de Airflow (auth básica del admin).

    Si Airflow no está disponible se responde 502 con detalle claro
    (nunca pantalla en blanco ni error silencioso).
    """
    import os
    base = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080")
    user = os.environ.get("AIRFLOW_ADMIN_USERNAME", "admin")
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "")
    try:
        with httpx.Client(timeout=15, auth=(user, password)) as client:
            resp = client.post(
                f"{base}/api/v1/dags/{dag_id}/dagRuns",
                json={"conf": conf},
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                return {"status": "launched", "dag_id": dag_id,
                        "run_id": data.get("dag_run_id")}
            if resp.status_code == 409:
                return {"status": "already_running", "dag_id": dag_id}
            raise HTTPException(
                status_code=502,
                detail=f"Airflow respondió {resp.status_code}: {resp.text[:300]}",
            )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"No se pudo contactar con Airflow: {exc}",
        ) from exc


# ----------------------------- Chatbot IA -----------------------------

class ChatRequest(BaseModel):
    question: str = Field(min_length=2, max_length=1000)
    session_id: str | None = Field(default=None, max_length=64)


class ChatSqlGuardError(Exception):
    pass


@router.post("/api/chat")
async def chat(body: ChatRequest,
               user: dict = Depends(current_user)) -> dict[str, Any]:
    """Consulta en lenguaje natural → SQL de solo lectura → respuesta.

    Capas de control:
      1. El LLM genera SQL restringido al esquema curated.
      2. sql_guard valida (SELECT únicamente, allowlist, LIMIT).
      3. Se ejecuta con el rol reader (solo lectura a nivel de BD).
      4. El LLM redacta la respuesta con los resultados reales.
    """
    try:
        from app.sql_guard import SqlBlockedError, validate_readonly_sql
    except ImportError:   # dentro del contenedor de Airflow (mismo archivo)
        from ai.sql_guard import SqlBlockedError, validate_readonly_sql

    session_id = body.session_id or uuid.uuid4().hex
    question = body.question.strip()

    try:
        sql = await _llm_generate_sql(question)
    except Exception as exc:  # proveedor de IA caído
        _save_chat(session_id, user["role"], question, None,
                   None, "error")
        raise HTTPException(
            status_code=503,
            detail={"message": "El asistente de IA no está disponible ahora "
                               "mismo. Inténtalo de nuevo en unos minutos.",
                    "error": str(exc)},
        ) from exc

    if not sql:
        _save_chat(session_id, user["role"], question, None, None, "no_answer")
        return {
            "session_id": session_id,
            "answer": "No puedo responder eso con los datos disponibles "
                      "(sísmicos de las últimas semanas). Pregunta, por "
                      "ejemplo: «¿cuántos eventos hubo hoy?» o «zona más "
                      "activa de las últimas 24 horas».",
            "sql": None, "status": "no_answer", "rows": None,
        }

    try:
        sql = validate_readonly_sql(sql)
    except SqlBlockedError as exc:
        _save_chat(session_id, user["role"], question, sql, None, "blocked")
        return {
            "session_id": session_id,
            "answer": f"Consulta bloqueada por seguridad: {exc}",
            "sql": None, "status": "blocked", "rows": None,
        }

    try:
        rows = _execute_readonly(sql)
    except Exception as exc:
        logger.warning("SQL del chatbot falló: %s — %s", sql, exc)
        _save_chat(session_id, user["role"], question, sql, None, "error")
        return {
            "session_id": session_id,
            "answer": "La consulta no se pudo ejecutar. Prueba reformularla.",
            "sql": sql, "status": "error", "rows": None,
        }

    answer = await _llm_answer(question, sql, rows)
    _save_chat(session_id, user["role"], question, sql, answer, "ok")
    return {
        "session_id": session_id,
        "answer": answer,
        "sql": sql,
        "status": "ok",
        "rows": rows[:50],
        "row_count": len(rows),
    }


async def _llm_generate_sql(question: str) -> str | None:
    """Pide al LLM la SQL; devuelve None si no aplica.

    Usa solo httpx + el prompt versionado del repo (copiado en la imagen).
    """
    from pathlib import Path

    prompt_file = Path(__file__).parent / "prompts" / "chat_sql.md"
    prompt_tpl = prompt_file.read_text(encoding="utf-8")
    prompt = (prompt_tpl
              .replace("{now}", datetime.now(timezone.utc).isoformat())
              + question)
    payload = {
        "model": settings.ollama_model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0, "num_predict": 500},
    }
    async with httpx.AsyncClient(timeout=settings.ollama_timeout_seconds) as client:
        resp = await client.post(
            f"{settings.ollama_base_url.rstrip('/')}/api/generate", json=payload)
        resp.raise_for_status()
        data = resp.json()
    parsed = _extract_json(data.get("response") or "{}")
    sql = (parsed.get("sql") or "").strip()
    return sql or None


def _extract_json(text: str) -> dict[str, Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return {}
        return {}


async def _llm_answer(question: str, sql: str, rows: list[dict]) -> str:
    """Redacta la respuesta con los resultados reales (sin inventar)."""
    import json as _json

    snippet = _json.dumps(rows[:30], default=str, ensure_ascii=False)
    prompt = (
        "Eres el asistente de datos de SismoWatch. Responde en español, "
        "máximo 4 frases, usando EXCLUSIVAMENTE los resultados de la consulta "
        "(no inventes cifras). Si no hay filas, dilo claramente.\n"
        f"Pregunta: {question}\n"
        f"Consulta SQL: {sql}\n"
        f"Resultados: {snippet}"
    )
    try:
        payload = {
            "model": settings.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2, "num_predict": 400},
        }
        async with httpx.AsyncClient(
                timeout=settings.ollama_timeout_seconds) as client:
            resp = await client.post(
                f"{settings.ollama_base_url.rstrip('/')}/api/generate",
                json=payload)
            resp.raise_for_status()
            text = (resp.json().get("response") or "").strip()
        return text or "Sin respuesta del modelo."
    except (httpx.HTTPError, ValueError) as exc:
        # Plan alterno: respuesta con los datos crudos (no depende de la IA)
        logger.warning("Fallback de respuesta: %s", exc)
        if not rows:
            return "La consulta no devolvió resultados."
        return (f"La consulta devolvió {len(rows)} fila(s). "
                f"Primeros resultados: {snippet[:600]}")


def _execute_readonly(sql: str) -> list[dict]:
    from app.db import fetch_all
    return fetch_all("reader", sql)


def _save_chat(session_id: str, role: str, question: str, sql: str | None,
               answer: str | None, status: str) -> None:
    try:
        with get_conn("reader") as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.chat_messages
                    (session_id, user_role, question, generated_sql, answer, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (session_id, role, question[:2000], sql, answer, status),
            )
    except Exception as exc:  # el registro nunca rompe la respuesta
        logger.warning("No se pudo guardar el mensaje de chat: %s", exc)
