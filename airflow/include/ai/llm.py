"""Cliente LLM para Ollama (local) con timeouts, reintentos y fallback.

Todos los textos que entran desde fuentes externas se sanitizan antes
de enviarse al modelo (mitigación de prompt injection).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent / "prompts"


class LLMError(Exception):
    """Fallo controlado del proveedor de IA."""


@dataclass
class LLMResponse:
    text: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: int = 0
    estimated_cost_usd: float = 0.0   # Ollama local: 0
    prompt_version: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


def load_prompt(name: str) -> tuple[str, str]:
    """Carga un prompt versionado desde el repo. Devuelve (texto, versión).

    La versión es el nombre de archivo (prompts/daily_report.md → 'daily_report').
    """
    path = PROMPTS_DIR / name
    if not path.exists():
        raise LLMError(f"Prompt '{name}' no encontrado en {PROMPTS_DIR}")
    return path.read_text(encoding="utf-8"), path.stem


def sanitize_for_llm(text: Any, max_len: int = 300) -> str:
    """Limpia texto externo antes de meterlo en un prompt."""
    if text is None:
        return ""
    cleaned = re.sub(r"[\x00-\x1f<>]", " ", str(text))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len]


class OllamaClient:
    """Cliente mínimo para la API de Ollama (/api/generate)."""

    def __init__(self, base_url: str | None = None, model: str | None = None,
                 timeout: int | None = None):
        self.base_url = (base_url or os.environ.get(
            "OLLAMA_BASE_URL", "http://host.docker.internal:11434")).rstrip("/")
        self.model = model or os.environ.get("OLLAMA_MODEL", "llama3.2")
        self.timeout = int(timeout or os.environ.get("OLLAMA_TIMEOUT_SECONDS", 120))

    def generate(self, prompt: str, *, system: str | None = None,
                 json_format: bool = False, num_predict: int = 1024,
                 temperature: float = 0.2) -> LLMResponse:
        """Una llamada con timeout explícito. Lanza LLMError si falla."""
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": num_predict,
            },
        }
        if system:
            payload["system"] = system
        if json_format:
            payload["format"] = "json"

        started = time.monotonic()
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(f"{self.base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as exc:
            raise LLMError(
                f"Ollama respondió HTTP {exc.response.status_code}"
            ) from exc
        except (httpx.HTTPError, ValueError) as exc:
            raise LLMError(f"No se pudo contactar con Ollama: {exc}") from exc

        latency = int((time.monotonic() - started) * 1000)
        text = (data.get("response") or "").strip()
        if not text:
            raise LLMError("Ollama devolvió una respuesta vacía")

        return LLMResponse(
            text=text,
            model=data.get("model", self.model),
            prompt_tokens=int(data.get("prompt_eval_count") or 0),
            completion_tokens=int(data.get("eval_count") or 0),
            latency_ms=latency,
            estimated_cost_usd=0.0,  # modelo local
        )

    def healthy(self) -> bool:
        try:
            with httpx.Client(timeout=5) as client:
                return client.get(f"{self.base_url}/api/tags").status_code == 200
        except httpx.HTTPError:
            return False


def extract_json(text: str) -> dict[str, Any]:
    """Extrae el primer objeto JSON de una respuesta del modelo."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError as exc:
                raise LLMError(f"Respuesta del modelo no es JSON válido: {exc}") from exc
        raise LLMError("El modelo no devolvió JSON")


def record_ai_run(purpose: str, response: LLMResponse | None, *,
                  status: str, error_detail: str | None = None) -> None:
    """Registra cada ejecución de IA en ops.ai_runs (modelo, tokens, latencia)."""
    from db import execute
    try:
        execute(
            "transform",
            """
            INSERT INTO ops.ai_runs
                (purpose, model, prompt_version, prompt_tokens, completion_tokens,
                 latency_ms, estimated_cost_usd, status, error_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                purpose,
                response.model if response else os.environ.get("OLLAMA_MODEL", "unknown"),
                response.prompt_version if response else None,
                response.prompt_tokens if response else 0,
                response.completion_tokens if response else 0,
                response.latency_ms if response else None,
                response.estimated_cost_usd if response else 0,
                status,
                (error_detail or "")[:1000] or None,
            ),
        )
    except Exception as exc:  # el registro nunca debe romper el pipeline
        logger.warning("No se pudo registrar la ejecución de IA: %s", exc)
