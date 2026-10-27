"""Autenticación JWT + control de acceso por rol.

Roles: viewer (lectura completa de la plataforma) y admin (además
cuarentena, backfill y regeneración de reportes). Sin token no hay
ningún endpoint de datos accesible.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=False)

# Usuarios en memoria (sembrados en el arranque desde .env); en producción
# vivirían en una tabla con hash. Nunca se comparten por código.
_USERS: dict[str, dict[str, str]] = {}


def seed_users() -> None:
    """Crea los usuarios admin/viewer a partir de variables de entorno."""
    _USERS.clear()
    if settings.admin_password:
        _USERS[settings.admin_email.lower()] = {
            "username": settings.admin_username,
            "password_hash": pwd_context.hash(settings.admin_password),
            "role": "admin",
        }
    if settings.viewer_password:
        _USERS[settings.viewer_email.lower()] = {
            "username": settings.viewer_username,
            "password_hash": pwd_context.hash(settings.viewer_password),
            "role": "viewer",
        }


def authenticate(email: str, password: str) -> dict[str, str] | None:
    user = _USERS.get(email.strip().lower())
    if not user or not pwd_context.verify(password, user["password_hash"]):
        return None
    return user


def create_token(email: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=settings.jwt_expire_minutes)
    payload = {"sub": email, "role": role, "exp": expire}
    return jwt.encode(payload, settings.jwt_secret,
                      algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, settings.jwt_secret,
                          algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


def current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> dict[str, Any]:
    """Extrae el usuario del JWT. Sin token → 401 (todas las rutas de datos)."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Autenticación requerida",
            headers={"WWW-Authenticate": "Bearer"},
        )
    payload = decode_token(credentials.credentials)
    email = payload.get("sub")
    role = payload.get("role")
    if not email or role not in {"viewer", "admin"}:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Token sin rol válido")
    return {"email": email, "role": role}


def require_admin(user: dict[str, Any] = Depends(current_user)) -> dict[str, Any]:
    """viewer intentando acción de admin → 403 claro."""
    if user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Requiere rol admin",
        )
    return user
