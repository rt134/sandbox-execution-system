#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="sandbox-postgres"
PORT=5432
USER="sandbox"
PASSWORD="sandbox"
DB="sandbox"

if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "✅ Container '${CONTAINER_NAME}' is already running on port ${PORT}."
    exit 0
fi

if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "🔄 Starting existing '${CONTAINER_NAME}' container..."
    docker start "${CONTAINER_NAME}"
    echo "✅ Container '${CONTAINER_NAME}' started."
    exit 0
fi

echo "🚀 Starting new PostgreSQL container '${CONTAINER_NAME}' on port ${PORT}..."
docker run -d \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_USER="${USER}" \
    -e POSTGRES_PASSWORD="${PASSWORD}" \
    -e POSTGRES_DB="${DB}" \
    -p "${PORT}:5432" \
    postgres:16-alpine

echo "⏳ Waiting for PostgreSQL to be ready..."
until docker exec "${CONTAINER_NAME}" pg_isready -U "${USER}" -d "${DB}" > /dev/null 2>&1; do
    sleep 1
done

echo "✅ PostgreSQL is ready at postgresql://${USER}:${PASSWORD}@localhost:${PORT}/${DB}"
