"""DAG de detección de anomalías (analítica/ML) + explicación con IA.

Técnica: z-score sobre la serie horaria de actividad + detección de
enjambres por magnitud (include/ai/anomalies.py). El código calcula las
cifras; Ollama redacta una explicación breve de cada alerta.
Si Ollama falla, la anomalía se guarda sin explicación (no se rompe nada).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from callbacks import on_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "sismowatch",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="deteccion_anomalias",
    description="Anomalías sísmicas (z-score) con explicación IA",
    default_args=DEFAULT_ARGS,
    schedule="15 * * * *",         # cada hora
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "ia", "anomalias"],
    doc_md="""
### Detección de anomalías
1. Serie horaria calculada por código (SQL sobre curated).
2. z-score (ventana global y local) → anomalías con valor, esperado y severidad.
3. Ollama redacta una explicación de cada alerta; si falla, la alerta
   se guarda igualmente sin explicación (plan alterno).
""",
)
def deteccion_anomalias():

    @task
    def load_series() -> list[dict]:
        from db import fetch_all
        rows = fetch_all(
            "reader",
            """
            SELECT event_time, mag, tsunami
            FROM curated.events
            WHERE event_time >= now() - interval '7 days'
            ORDER BY event_time
            """,
        )
        return rows

    @task
    def detect(series: list[dict]) -> list[dict]:
        from ai.anomalies import compute_hourly_series, detect_anomalies
        hourly = compute_hourly_series(series)
        return detect_anomalies(hourly)

    @task
    def explain(anomalies: list[dict]) -> list[dict]:
        """Explicación breve con IA (opcional: si falla, queda sin texto)."""
        from ai.llm import (LLMError, OllamaClient, extract_json, load_prompt,
                            record_ai_run, sanitize_for_llm)
        if not anomalies:
            return []

        try:
            prompt_tpl, version = load_prompt("anomaly_explanation.md")
            client = OllamaClient()
        except LLMError as exc:
            logger.warning("Prompts/IA no disponible: %s", exc)
            return [{**a, "explanation": None} for a in anomalies]

        explained = []
        for anomaly in anomalies:
            facts = anomaly.get("facts") or {}
            payload = json.dumps(
                {**facts, "z_score": anomaly["z_score"],
                 "severity": anomaly["severity"], "metric": anomaly["metric"]},
                ensure_ascii=False, default=str)
            try:
                response = client.generate(
                    prompt_tpl + "\n" + payload,
                    json_format=True, num_predict=300)
                response.prompt_version = version
                data = extract_json(response.text)
                text = sanitize_for_llm(data.get("explanation"), 600)
                record_ai_run("anomaly_explanation", response, status="ok")
                explained.append({**anomaly, "explanation": text or None})
            except LLMError as exc:
                logger.warning("IA no disponible para explicar anomalía: %s", exc)
                record_ai_run("anomaly_explanation", None, status="fallback",
                              error_detail=str(exc))
                explained.append({**anomaly, "explanation": None})
        return explained

    @task
    def persist(anomalies: list[dict]) -> int:
        from db import execute
        count = 0
        for a in anomalies:
            execute(
                "transform",
                """
                INSERT INTO ops.anomalies
                    (window_start, window_end, metric, value, expected,
                     z_score, severity, explanation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (a["window_start"], a["window_end"], a["metric"], a["value"],
                 a.get("expected"), a.get("z_score"), a["severity"],
                 a.get("explanation")),
            )
            count += 1
        logger.info("%d anomalías registradas", count)
        return count

    anomalies = detect(load_series())
    persist(explain(anomalies))


deteccion_anomalias()
