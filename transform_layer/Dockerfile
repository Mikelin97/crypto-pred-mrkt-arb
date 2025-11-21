FROM python:3.11-slim

WORKDIR /app

# Install system deps for asyncpg
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

COPY transform_layer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY transform_layer transform_layer
COPY shared shared

ENV PYTHONUNBUFFERED=1 \
    SERVICE=kafka_bridge

# SERVICE can be "kafka_bridge" or "transform_writer"
CMD ["sh", "-c", "python -m transform_layer.${SERVICE}"]
