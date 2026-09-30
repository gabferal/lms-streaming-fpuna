# ── Build stage: resolve dependencies ─────────────────────────────────────────
FROM python:3.12-slim AS builder

# Java is required by Apache Beam's KafkaIO cross-language expansion service.
# The Python SDK auto-starts a local Java expansion service when Java is on PATH.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Install uv (fast Python package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Cache dependency resolution
COPY pyproject.toml ./
RUN uv sync --no-install-project

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=builder /app/.venv /app/.venv

WORKDIR /app

# Copy full project
COPY . .

# Make the virtual-env available
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Default: run the pipeline (overridden per service in docker-compose.yml)
CMD ["python", "-m", "src.pipeline"]
