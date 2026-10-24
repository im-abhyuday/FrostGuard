FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="/app/src"

WORKDIR /app

# Install system dependencies for confluent-kafka and compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY README.md PHYSICS.md ./

# Default entrypoint runs live streaming mode; environment variables override flags
ENTRYPOINT ["python", "-m", "simulator.cli"]
CMD ["live"]
