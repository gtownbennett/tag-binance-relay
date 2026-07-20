FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/gtownbennett/tag-binance-relay"
LABEL org.opencontainers.image.description="TAG market-data relay v2.5.0 with Chad durable intelligence, calibration and recoverable prediction ledger"
LABEL org.opencontainers.image.licenses="MIT"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
