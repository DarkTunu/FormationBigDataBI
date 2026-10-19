FROM python:3.12-slim
# Retry slow package-index downloads during clean parallel image builds.
ENV PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10
RUN pip install --no-cache-dir confluent-kafka==2.11.1 PyYAML==6.0.2
WORKDIR /app
COPY kafka_init.py kafka_ready.py /app/
ENTRYPOINT ["python", "/app/kafka_init.py"]
