from __future__ import annotations

import os

SYMBOL = os.getenv("BINANCE_SYMBOL", "TAGUSDT").upper()
DATABASE_URL = os.getenv(
    "TERMINAL_DATABASE_URL",
    os.getenv("DATABASE_URL", "sqlite:////tmp/tag_terminal_history.sqlite3"),
).strip()
# Render and several managed databases expose postgresql:// or postgres:// URLs.
# Force SQLAlchemy to use the bundled psycopg v3 driver instead of assuming
# psycopg2 is installed.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgres://"):]
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgresql://"):]
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "").strip()
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
COLLECT_SECONDS = max(30, int(os.getenv("COLLECT_SECONDS", "60")))
APP_VERSION = "2.6.0-rc3"

# Project-specific user context used only for risk framing, never for automatic orders.
TAG_BAG_TOKENS = float(os.getenv("TAG_BAG_TOKENS", "100812406"))
TAG_COST_BASIS = float(os.getenv("TAG_COST_BASIS", "0.00014105"))
