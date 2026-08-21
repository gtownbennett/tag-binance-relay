FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/gtownbennett/tag-binance-relay"
LABEL org.opencontainers.image.description="TAG market-data relay v2.8.7 RC6.5 with Binance Vision grading fallback"
LABEL org.opencontainers.image.licenses="MIT"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /service

COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts/bootstrap_render_catalog.py ./scripts/bootstrap_render_catalog.py
COPY scripts/start_render_service.py ./scripts/start_render_service.py
COPY bootstrap_data/tagnext-rc4-external-catalog.pgcustom ./bootstrap_data/tagnext-rc4-external-catalog.pgcustom

CMD ["python", "scripts/start_render_service.py"]
