"""Integridad de DAGs: todos los DAGs se importan sin errores.

Se usa DagBag de Airflow para validar sintaxis, imports y que no haya
ciclos ni errores de parseo.
"""
from __future__ import annotations

import os

import pytest

DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags")


@pytest.fixture(scope="module")
def dag_bag():
    from airflow.models import DagBag

    # sin datos de conexión reales: solo validamos la importación
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}, (
        f"Errores al importar DAGs: {dag_bag.import_errors}"
    )


def test_expected_dags_present(dag_bag):
    expected = {
        "ingesta_usgs",
        "transformacion_calidad",
        "reporte_diario_ia",
        "backfill_historico",
        "deteccion_anomalias",
        "publicar_metadatos",
    }
    missing = expected - set(dag_bag.dag_ids)
    assert not missing, f"DAGs ausentes: {missing}"


def test_every_dag_has_doc_md(dag_bag):
    for dag in dag_bag.dags.values():
        assert dag.doc_md, f"El DAG {dag.dag_id} no tiene doc_md"
