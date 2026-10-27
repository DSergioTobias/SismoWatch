"""Configuración de la API desde variables de entorno."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Base de datos (rol de SOLO LECTURA para la plataforma)
    sismo_pg_host: str = "postgres"
    sismo_pg_port: int = 5432
    sismo_pg_db: str = "sismowatch"
    sismo_pg_user_reader: str = "sismo_reader"
    sismo_pg_password_reader: str = ""
    sismo_pg_user_admin: str = "sismo_admin"
    sismo_pg_password_admin: str = ""

    # JWT
    jwt_secret: str = ""
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 480

    # CORS
    cors_origins: str = "http://localhost:3000"

    # Usuarios sembrados (se crean en el arranque)
    admin_email: str = "admin@sismowatch.local"
    admin_password: str = ""
    admin_username: str = "admin"
    viewer_email: str = "viewer@sismowatch.local"
    viewer_password: str = ""
    viewer_username: str = "viewer"

    # Ollama
    ollama_base_url: str = "http://host.docker.internal:11434"
    ollama_model: str = "llama3.2"
    ollama_timeout_seconds: int = 120


settings = Settings()
