from __future__ import annotations

import os

SYSTEM_ID = os.getenv("SYSTEM_ID", "tagnext").strip().lower()
FORECAST_PRODUCER = os.getenv("FORECAST_PRODUCER", "tagnext").strip().lower()
SYMBOL = os.getenv("BINANCE_SYMBOL", "TAGUSDT").upper()
TAG_CONTRACT_ADDRESS = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
WBNB_CONTRACT_ADDRESS = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
PRIMARY_POOL_ADDRESS = "0xf0750c373ebbb3baeef7e03d8300caad1983d67c"

_tagnext_database_url = os.getenv("TAGNEXT_DATABASE_URL", "").strip()
_database_required = os.getenv("TAGNEXT_DATABASE_REQUIRED", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
if _database_required and not _tagnext_database_url:
    raise RuntimeError(
        "TAGNEXT_DATABASE_URL is required; TAGneXt will not fall back to the TAGalysis champion database."
    )
DATABASE_URL = (
    _tagnext_database_url
    or os.getenv("TERMINAL_DATABASE_URL", "").strip()
    or os.getenv("DATABASE_URL", "").strip()
    or "sqlite:////tmp/tagnext_challenger.sqlite3"
)
# Render and several managed databases expose postgresql:// or postgres:// URLs.
# Force SQLAlchemy to use the bundled psycopg v3 driver instead of assuming
# psycopg2 is installed.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgres://"):]
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgresql://"):]
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "").strip()
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
COLLECT_SECONDS = max(300, int(os.getenv("COLLECT_SECONDS", "300")))
APP_VERSION = "tagnext-1.0.0-alpha1"

# Project-specific user context used only for risk framing, never for automatic orders.
TAG_BAG_TOKENS = float(os.getenv("TAG_BAG_TOKENS", "100812406"))
TAG_COST_BASIS = float(os.getenv("TAG_COST_BASIS", "0.00014105"))
