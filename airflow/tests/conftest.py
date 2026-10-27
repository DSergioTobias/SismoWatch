"""Asegura que `include/` y `dags/` estén en sys.path durante los tests."""
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # airflow/
INCLUDE = os.path.join(BASE, "include")
DAGS = os.path.join(BASE, "dags")

for path in (INCLUDE, DAGS):
    if path not in sys.path:
        sys.path.insert(0, path)
