"""Callbacks de alerta para fallos de DAG.

`AIRFLOW_ALERT_WEBHOOK_URL` acepta cualquier webhook (Slack, Telegram,
Discord, o un endpoint propio). Si no está configurado, solo se loguea.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)


def on_failure_alert(context: dict) -> None:
    """on_failure_callback: notifica el fallo de una tarea/DAG."""
    dag_id = context.get("dag_run") and context["dag_run"].dag_id or context.get("dag")
    run_id = (context.get("dag_run").run_id if context.get("dag_run") else "-")
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "-"
    exception = context.get("exception")
    url = os.environ.get("AIRFLOW_ALERT_WEBHOOK_URL", "").strip()

    message = (
        f"[SismoWatch] FALLÓ {dag_id} · run={run_id} · task={task_id}\n"
        f"error: {exception}\n"
        f"hora (UTC): {datetime.now(timezone.utc).isoformat()}"
    )
    logger.error(message)

    if not url:
        logger.warning("AIRFLOW_ALERT_WEBHOOK_URL no configurado; alerta solo en logs")
        return

    try:
        # Formato genérico compatible con Slack/Telegram/Discord
        with httpx.Client(timeout=10) as client:
            if "slack" in url:
                payload = {"text": message}
            elif "api.telegram.org" in url:
                payload = {"text": message}
            elif "discord" in url:
                payload = {"content": message}
            else:
                payload = {"text": message, "message": message}
            client.post(url, json=payload)
        logger.info("Alerta enviada a %s", url)
    except httpx.HTTPError as exc:
        logger.warning("No se pudo enviar la alerta: %s", exc)
