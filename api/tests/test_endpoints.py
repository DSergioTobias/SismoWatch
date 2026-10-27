"""Pruebas de endpoints de la API.

Cubre lo mínimo exigido:
  - un endpoint de datos sin token → 401
  - un rol sin permiso (viewer) en acción de admin → 403
  - login y acceso con token válido → 200
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("JWT_SECRET", "test_secret_suficientemente_largo_123456")
os.environ.setdefault("JWT_EXPIRE_MINUTES", "30")
os.environ.setdefault("ADMIN_PASSWORD", "admin_test_password")
os.environ.setdefault("VIEWER_PASSWORD", "viewer_test_password")
os.environ.setdefault("CORS_ORIGINS", "http://localhost:3000")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.security import create_token, seed_users  # noqa: E402


@pytest.fixture(scope="module")
def client():
    seed_users()
    # raise_server_exceptions=False: sin BD en los tests el endpoint
    # devuelve 500 controlado en vez de propagar la excepción
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def test_health(client):
    assert client.get("/health").status_code == 200


def test_kpis_without_token_returns_401(client):
    """Sin sesión, ninguna ruta de datos es accesible."""
    res = client.get("/api/kpis")
    assert res.status_code == 401


def test_events_without_token_returns_401(client):
    assert client.get("/api/events").status_code == 401


def test_invalid_token_returns_401(client):
    res = client.get("/api/kpis", headers={"Authorization": "Bearer token_falso"})
    assert res.status_code == 401


def test_login_wrong_password_returns_401(client):
    res = client.post("/auth/login",
                      json={"email": "admin@sismowatch.local", "password": "mala"})
    assert res.status_code == 401


def test_login_ok_returns_token(client):
    res = client.post("/auth/login",
                      json={"email": "viewer@sismowatch.local",
                            "password": "viewer_test_password"})
    assert res.status_code == 200
    body = res.json()
    assert body["role"] == "viewer"
    assert body["access_token"]


def test_viewer_cannot_access_admin_endpoint(client):
    """Un viewer que intenta una acción de admin recibe 403 claro."""
    token = create_token("viewer@sismowatch.local", "viewer")
    res = client.get("/api/quality/quarantine",
                     headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 403


def test_viewer_cannot_launch_backfill(client):
    token = create_token("viewer@sismowatch.local", "viewer")
    res = client.post("/api/admin/backfill",
                      json={"days": 7},
                      headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 403


def test_admin_token_passes_authorization(client):
    """El guardián de rol deja pasar al admin (la BD puede no estar en tests)."""
    token = create_token("admin@sismowatch.local", "admin")
    res = client.get("/api/quality/quarantine",
                     headers={"Authorization": f"Bearer {token}"})
    # 200 si hay BD; 500 si no la hay (error controlado) — nunca 401/403
    assert res.status_code not in (401, 403)


def test_chat_requires_auth(client):
    res = client.post("/api/chat", json={"question": "¿cuántos eventos hay?"})
    assert res.status_code == 401
