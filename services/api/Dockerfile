# ==============================================================================
# Multi-stage Dockerfile for ColdChainIQ API Service
# Security hardened: Non-root user, slim base image, healthcheck
# ==============================================================================

# Stage 1: Build stage
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy common library and API service requirements
COPY libs/coldchain_common /build/libs/coldchain_common
COPY services/api /build/services/api

# Install wheels into temporary directory
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --prefix=/install \
        /build/libs/coldchain_common /build/services/api



# Stage 2: Runtime stage
FROM python:3.11-slim AS runtime

WORKDIR /app

# Install runtime dependencies (libpq for postgres, curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create dedicated non-root user and group
RUN groupadd -g 10001 coldchain && \
    useradd -u 10001 -g coldchain -s /bin/bash -m coldchain

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source code
COPY --chown=coldchain:coldchain libs/coldchain_common /app/libs/coldchain_common
COPY --chown=coldchain:coldchain services/api /app/services/api

# Set python path
ENV PYTHONPATH=/app/libs/coldchain_common/src:/app/services/api/src
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER coldchain

# Expose API port
EXPOSE 8002

# Healthcheck probe against /health
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Launch uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8002"]
