# ==========================================
# Stage 1: Build the static Go daemon (shellctl)
# ==========================================
FROM golang:1.22-bookworm AS builder

WORKDIR /build

# Pre-fetch dependencies
COPY go.mod ./
RUN go mod download || true

COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/shellctl main.go

# ==========================================
# Stage 2: Base Sandbox Runtime Environment
# ==========================================
FROM python:3.11-slim-bookworm

# Avoid interactive prompts during apt
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV WORKSPACE_DIR=/workspace
ENV PYTHON_RUNNER_PATH=/opt/runner/python_runner.py
ENV SHELL=/bin/bash

# Install essential system tools & Node.js 20 LTS
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    bash \
    jq \
    tar \
    procps \
    ca-certificates \
    gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update && apt-get install -y nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir pytest

# Copy compiled shellctl daemon
COPY --from=builder /bin/shellctl /usr/local/bin/shellctl
RUN chmod +x /usr/local/bin/shellctl

# Copy stateless python runner script
RUN mkdir -p /opt/runner
COPY runner/python_runner.py /opt/runner/python_runner.py
RUN chmod +x /opt/runner/python_runner.py

# Create non-root sandbox user (UID 1000) and /workspace directory
RUN groupadd -g 1000 sandbox && \
    useradd -m -u 1000 -g sandbox -s /bin/bash sandbox && \
    mkdir -p /workspace && \
    chown -R sandbox:sandbox /workspace

# Set default working directory
WORKDIR /workspace

# Switch to non-root sandbox user
USER sandbox

# Expose internal daemon port
EXPOSE 5004

ENTRYPOINT ["/usr/local/bin/shellctl"]
