"""Conector USGS Earthquake Catalog (feed GeoJSON + endpoint histórico)."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from connectors.base import BaseConnector, ConnectorError, get_by_path

logger = logging.getLogger(__name__)

DEFAULT_REALTIME_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)
DEFAULT_HISTORIC_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


class UsgsConnector(BaseConnector):
    """Extrae eventos del feed público de USGS (no requiere API key)."""

    connector_type = "usgs"

    def fetch_raw(self) -> Any:
        url = self.config.get("realtime_url") or DEFAULT_REALTIME_URL
        return self._http_get(url)

    def fetch_historic(self, start: datetime, end: datetime,
                       min_magnitude: float = 2.5) -> Any:
        """Consulta el endpoint FDSN para backfill histórico."""
        url = self.config.get("historic_url") or DEFAULT_HISTORIC_URL
        params = {
            "format": "geojson",
            "starttime": start.strftime("%Y-%m-%d"),
            "endtime": end.strftime("%Y-%m-%d"),
            "minmagnitude": min_magnitude,
        }
        return self._http_get(url, params=params)

    def fetch_last_days(self, days: int = 30, min_magnitude: float = 2.5) -> Any:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        return self.fetch_historic(start, end, min_magnitude)

    def normalize(self, raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, dict) or "features" not in raw:
            raise ConnectorError("Payload USGS sin clave 'features'")
        field_map: dict[str, str] = self.config.get("field_map") or {}
        if not field_map:
            raise ConnectorError(f"source '{self.source_id}' sin field_map en config")

        events: list[dict[str, Any]] = []
        for feature in raw.get("features") or []:
            if not isinstance(feature, dict):
                continue
            row: dict[str, Any] = {}
            for target, path in field_map.items():
                row[target] = get_by_path(feature, path)
            event_uid = row.get("event_uid") or feature.get("id")
            if not event_uid:
                continue  # evento sin identificador: se descarta (regla de calidad)
            row["event_uid"] = str(event_uid)
            events.append(row)
        return events
