FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy shared library and consumer service
COPY libs/coldchain_common /app/libs/coldchain_common
COPY services/consumer /app/services/consumer

# Install dependencies
RUN pip install --no-cache-dir -e /app/libs/coldchain_common
RUN pip install --no-cache-dir -r /app/services/consumer/requirements.txt
RUN pip install --no-cache-dir -e /app/services/consumer

WORKDIR /app/services/consumer

ENV PYTHONPATH="/app/libs/coldchain_common/src:/app/services/consumer/src:${PYTHONPATH}"

EXPOSE 8081

CMD ["python", "-m", "consumer.main"]
