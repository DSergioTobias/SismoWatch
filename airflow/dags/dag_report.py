"""DAG diario de reporte narrativo con IA (Ollama local).

Controles:
  - Las cifras las calcula el código (SQL en include/ai/report.py).
  - El LLM solo redacta; `validate_numbers` verifica que los números del
    texto coincidan con los calculados.
  - Salida validada (JSON) y plan alterno si Ollama falla: el pipeline
    NUNCA se rompe por la IA.
  - Cada ejecución queda registrada en ops.ai_runs.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from callbacks import on_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "sismowatch",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="reporte_diario_ia",
    description="Resumen ejecutivo diario generado con IA (cifras por código)",
    default_args=DEFAULT_ARGS,
    schedule="30 6 * * *",        # cada día 06:30 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "ia", "reporte"],
    doc_md="""
### Reporte diario con IA
1. `compute_kpis` — KPIs calculados con SQL puros (nunca por el LLM).
2. `generate_report` — Ollama redacta; si falla → plan alterno por plantilla.
3. `validate_report` — números del texto ⊆ KPIs calculados; si no, se marca
   `fallback` y se regenera por plantilla (el pipeline no falla).
4. `save_report` — upsert en `ops.reports` (histórico para la plataforma).
""",
)
def reporte_diario_ia():

    @task
    def compute_kpis() -> dict:
        from ai.report import compute_kpis as _compute
        return _compute(days=1)

    @task
    def generate_report(kpis: dict) -> dict:
        from ai.report import generate_daily_report
        return generate_daily_report(days=1)

    @task
    def validate_report(report: dict) -> dict:
        """Segunda barrera anti-alucinación antes de persistir."""
        from ai.report import validate_numbers
        text = " ".join([
            report.get("summary", ""),
            *report.get("zones", []),
            *report.get("highlights", []),
            report.get("quality_note", ""),
        ])
        ok, unknown = validate_numbers(text, report.get("kpis") or {})
        if not ok:
            logger.error("Números no permitidos en el reporte: %s", unknown)
            # se degrada a plan alterno en lugar de publicar texto inventado
            from ai.report import _fallback_report
            report = {**_fallback_report(report.get("kpis") or {}),
                      "kpis": report.get("kpis") or {},
                      "status": "fallback", "model": None,
                      "error": f"validación numérica fallida: {unknown}"}
        return report

    @task
    def save_report(report: dict) -> int:
        from db import execute
        kpis = report.get("kpis") or {}
        ctx = get_current_context()
        report_date = ctx["data_interval_end"].date() if ctx.get("data_interval_end") \
            else datetime.utcnow().date()

        content = {
            "summary": report.get("summary", ""),
            "zones": report.get("zones", []),
            "highlights": report.get("highlights", []),
            "quality_note": report.get("quality_note", ""),
        }
        return execute(
            "transform",
            """
            INSERT INTO ops.reports
                (report_date, title, content_md, kpis, model, status)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (report_date) DO UPDATE
               SET title = EXCLUDED.title,
                   content_md = EXCLUDED.content_md,
                   kpis = EXCLUDED.kpis,
                   model = EXCLUDED.model,
                   status = EXCLUDED.status,
                   created_at = now()
            """,
            (
                report_date,
                report.get("title") or f"Reporte {report_date}",
                json.dumps(content, ensure_ascii=False),
                json.dumps(kpis, ensure_ascii=False, default=str),
                report.get("model"),
                report.get("status", "ok"),
            ),
        )

    kpis = compute_kpis()
    report = generate_report(kpis)
    report = validate_report(report)
    save_report(report)


reporte_diario_ia()
