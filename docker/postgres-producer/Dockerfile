FROM python:3.12.14-slim-bookworm
# Retry slow package-index downloads during clean parallel image builds.
ENV PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && useradd --create-home --uid 10001 --user-group app
COPY producer.py /app/producer.py
COPY build_ebrasil_dw.py /app/build_ebrasil_dw.py
COPY check_ebrasil_dw.py /app/check_ebrasil_dw.py
USER app
CMD ["python", "/app/producer.py"]
