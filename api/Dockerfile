# ============================================================
# API SismoWatch (FastAPI) - sólo LEE de curated + ops
# ============================================================
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# La guardia de SQL se comparte con el pipeline (mismo código, un solo origen)
COPY api/app ./app
COPY airflow/include/ai/sql_guard.py ./app/sql_guard.py
COPY airflow/include/ai/prompts/chat_sql.md ./app/prompts/chat_sql.md

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=10s --retries=10 \
    CMD curl -sf http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
