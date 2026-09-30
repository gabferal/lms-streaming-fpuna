FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir "apache-beam>=2.74.0" "confluent-kafka>=2.6.0"

COPY . .

ENV PYTHONPATH="/app"

CMD ["python", "-m", "src.pipeline"]
