"""API de la plataforma SismoWatch.

Lee EXCLUSIVAMENTE de curated + ops (rol sismo_reader).
Autenticación JWT con roles viewer/admin.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.routers_core import router as core_router
from app.routers_extra import router as extra_router
from app.security import seed_users

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("sismowatch.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    seed_users()
    logger.info("Usuarios sembrados desde .env: admin y viewer")
    yield


app = FastAPI(
    title="SismoWatch API",
    version="1.0.0",
    description="Plataforma de consumo de datos sísmicos (solo lectura sobre curated)",
    lifespan=lifespan,
)

origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,          # CORS restringido
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(core_router)
app.include_router(extra_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "sismowatch-api"}


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Ningún error deja al usuario sin respuesta JSON clara."""
    logger.exception("Error no controlado en %s", request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Error interno del servidor",
                 "path": request.url.path},
    )
