FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# System deps (for wheels, curl for health/debug)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY src ./src

# Expose API
EXPOSE 8000

# Default envs (can be overridden at docker run)
ENV SMARTSQL_OFFLINE=1 \
    PROVIDER=gemini

CMD ["uvicorn", "smartsql.api:app", "--host", "0.0.0.0", "--port", "8000"]
