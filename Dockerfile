FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/gtownbennett/tag-binance-relay"
LABEL org.opencontainers.image.description="TAG market-data relay v2.8.3 RC6 with bounded read-only Neon intelligence"
LABEL org.opencontainers.image.licenses="MIT"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
