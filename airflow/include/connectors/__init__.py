"""Registro de conectores: mapea connector_type → clase.

Añadir una fuente nueva = insertar una fila en ops.sources con su
connector_type; ningún DAG necesita cambios.
"""
from __future__ import annotations

import logging
from typing import Any

from connectors.base import BaseConnector, ConnectorError
from connectors.generic_rest import GenericRestConnector
from connectors.usgs import UsgsConnector

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, type[BaseConnector]] = {
    UsgsConnector.connector_type: UsgsConnector,
    GenericRestConnector.connector_type: GenericRestConnector,
}


def register(connector_cls: type[BaseConnector]) -> type[BaseConnector]:
    _REGISTRY[connector_cls.connector_type] = connector_cls
    return connector_cls


def build_connector(source_id: str, connector_type: str,
                    config: dict[str, Any]) -> BaseConnector:
    cls = _REGISTRY.get(connector_type)
    if cls is None:
        raise ConnectorError(
            f"Tipo de conector desconocido: '{connector_type}'. "
            f"Disponibles: {sorted(_REGISTRY)}"
        )
    return cls(source_id=source_id, config=config)


def available_types() -> list[str]:
    return sorted(_REGISTRY)
