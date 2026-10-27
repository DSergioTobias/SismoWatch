"""Conector genérico para cualquier API REST que devuelva JSON.

Configuración (en la columna `config` de ops.sources):

{
  "url": "https://api.ejemplo.com/v1/datos",
  "method": "GET",
  "headers": {"X-Api-Key": "{{secret:API_KEY}}"},   # valores tomados de env
  "params": {"page": 1},
  "items_path": "data.records",      # dónde está la lista (vacío = raíz)
  "timeout_seconds": 30,
  "max_retries": 3,
  "field_map": {"event_uid": "id", "mag": "magnitude", ...}
}
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

from connectors.base import BaseConnector, ConnectorError, get_by_path

logger = logging.getLogger(__name__)

_SECRET_RE = re.compile(r"\{\{\s*secret:([A-Z0-9_]+)\s*\}\}")


def resolve_secrets(value: Any) -> Any:
    """Sustituye {{secret:VAR}} por el valor de la variable de entorno.

    Así ninguna credencial vive en la configuración de la fuente.
    """
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            var = match.group(1)
            secret = os.environ.get(var)
            if secret is None:
                raise ConnectorError(
                    f"Variable de entorno '{var}' requerida por la fuente no está definida"
                )
            return secret
        return _SECRET_RE.sub(_replace, value)
    if isinstance(value, dict):
        return {k: resolve_secrets(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_secrets(v) for v in value]
    return value


class GenericRestConnector(BaseConnector):
    """Cualquier API JSON REST, configurable por field_map."""

    connector_type = "generic_rest"

    def fetch_raw(self) -> Any:
        url = self.config.get("url")
        if not url:
            raise ConnectorError(f"source '{self.source_id}' sin 'url' en config")
        cfg = resolve_secrets(self.config)
        headers = cfg.get("headers") or {}
        params = cfg.get("params") or {}
        # _http_get siempre hace GET; para APIs con método distinto se documenta
        # en ARQUITECTURA.md que el conector soporta GET (el caso del enunciado).
        return self._http_get(url, params=params or None) if not headers else self._http_get_with_headers(
            url, params or None, headers
        )

    def _http_get_with_headers(self, url: str, params: dict[str, Any] | None,
                               headers: dict[str, str]) -> Any:
        import httpx
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True,
                              headers=headers) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(f"HTTP {exc.response.status_code} en {url}") from exc
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Error de red consultando {url}: {exc}") from exc
        except ValueError as exc:
            raise ConnectorError(f"Respuesta no JSON desde {url}: {exc}") from exc

    def normalize(self, raw: Any) -> list[dict[str, Any]]:
        items_path = (self.config.get("items_path") or "").strip()
        items = get_by_path(raw, items_path) if items_path else raw
        if items is None:
            raise ConnectorError(
                f"items_path '{items_path}' no encontrado en la respuesta"
            )
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            raise ConnectorError("La respuesta no contiene una lista de registros")

        field_map: dict[str, str] = self.config.get("field_map") or {}
        if not field_map:
            raise ConnectorError(f"source '{self.source_id}' sin field_map en config")

        events: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            row = {target: get_by_path(item, path) for target, path in field_map.items()}
            uid = row.get("event_uid")
            if uid is None:
                continue
            row["event_uid"] = str(uid)
            events.append(row)
        return events
