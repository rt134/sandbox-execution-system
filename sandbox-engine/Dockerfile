# syntax=docker/dockerfile:1
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

FROM python:3.11-slim AS runner

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir --no-deps -e .

RUN groupadd -g 1000 engine && \
    useradd -u 1000 -g engine -m -s /bin/bash engine && \
    mkdir -p /workspaces && \
    chown -R engine:engine /app /workspaces

USER engine

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --retries=3 --start-period=5s \
    CMD curl -f http://localhost:8080/livez || exit 1

ENTRYPOINT ["uvicorn", "sandbox_engine.main:app", "--host", "0.0.0.0", "--port", "8080"]
