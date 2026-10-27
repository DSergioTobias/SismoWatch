"""Interfaz común de conectores de datos.

Toda fuente (USGS, cualquier API REST, ...) implementa esta interfaz.
El pipeline solo conoce `BaseConnector`, por lo que añadir una fuente
nueva no exige modificar los DAGs.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ConnectorError(Exception):
    """Error controlado de conectividad o formato de la fuente."""


@dataclass
class ExtractResult:
    """Resultado estandarizado de una extracción."""

    events: list[dict[str, Any]]           # eventos ya normalizados (clave plana)
    raw_payload: Any = None                # payload original completo (para raw)
    fetched_at: float = field(default_factory=time.time)
    source_id: str = ""

    @property
    def count(self) -> int:
        return len(self.events)


def payload_hash(payload: Any) -> str:
    """Hash estable del payload, para detectar actualizaciones de eventos."""
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def get_by_path(data: Any, path: str) -> Any:
    """Lee un valor de un JSON con notación de puntos e índices.

    Ejemplos: "properties.mag", "geometry.coordinates[0]".
    Devuelve None si la ruta no existe (campo opcional).
    """
    current = data
    for part in path.split("."):
        if current is None:
            return None
        while "[" in part:
            name, _, rest = part.partition("[")
            idx_str, _, part = rest.partition("]")
            if name:
                if not isinstance(current, dict) or name not in current:
                    return None
                current = current[name]
            try:
                idx = int(idx_str)
            except ValueError:
                return None
            if not isinstance(current, (list, tuple)) or idx >= len(current):
                return None
            current = current[idx]
        if part:
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
    return current


class BaseConnector(ABC):
    """Contrato mínimo de un conector de datos."""

    connector_type: str = "base"

    def __init__(self, source_id: str, config: dict[str, Any]):
        self.source_id = source_id
        self.config = config or {}
        self.timeout = float(self.config.get("timeout_seconds", 30))
        self.max_retries = int(self.config.get("max_retries", 3))

    @abstractmethod
    def fetch_raw(self) -> Any:
        """Descarga el payload original de la fuente (JSON serializable)."""

    @abstractmethod
    def normalize(self, raw: Any) -> list[dict[str, Any]]:
        """Convierte el payload en una lista de eventos con clave plana."""

    def extract(self) -> ExtractResult:
        """Descarga + normaliza con reintentos y manejo explícito de fallos."""
        last_error: Exception | None = None
        raw: Any = None
        for attempt in range(1, self.max_retries + 1):
            try:
                raw = self.fetch_raw()
                events = self.normalize(raw)
                logger.info(
                    "[%s] extracción OK (intento %d/%d): %d eventos",
                    self.source_id, attempt, self.max_retries, len(events),
                )
                return ExtractResult(
                    events=events,
                    raw_payload=raw,
                    source_id=self.source_id,
                )
            except (httpx.HTTPError, ConnectorError, ValueError, KeyError) as exc:
                last_error = exc
                wait = 2 ** attempt
                logger.warning(
                    "[%s] fallo en intento %d/%d: %s — reintento en %ss",
                    self.source_id, attempt, self.max_retries, exc, wait,
                )
                if attempt < self.max_retries:
                    time.sleep(wait)
        raise ConnectorError(
            f"La fuente '{self.source_id}' no respondió tras "
            f"{self.max_retries} intentos: {last_error}"
        )

    # ---------- utilidades compartidas ----------

    def _http_get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """GET en HTTPS con timeout; lanza ConnectorError ante cualquier fallo."""
        try:
            with httpx.Client(
                timeout=self.timeout,
                follow_redirects=True,
                headers={"User-Agent": "SismoWatch-DataPipeline/1.0"},
            ) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                f"HTTP {exc.response.status_code} en {url}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Error de red consultando {url}: {exc}") from exc
        except ValueError as exc:
            raise ConnectorError(f"Respuesta no JSON desde {url}: {exc}") from exc
