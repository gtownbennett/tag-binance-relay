from __future__ import annotations

import hashlib
import os
from urllib.parse import urlsplit


SYSTEM_ID = os.getenv("SYSTEM_ID", "tagnext").strip().lower()
FORECAST_PRODUCER = os.getenv("FORECAST_PRODUCER", "tagnext").strip().lower()
SYMBOL = os.getenv("BINANCE_SYMBOL", "TAGUSDT").upper()
TAG_CONTRACT_ADDRESS = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
WBNB_CONTRACT_ADDRESS = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
PRIMARY_POOL_ADDRESS = "0xf0750c373ebbb3baeef7e03d8300caad1983d67c"


def _truthy(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _sqlalchemy_url(value: str) -> str:
    if value.startswith("postgres://"):
        return "postgresql+psycopg://" + value[len("postgres://"):]
    if value.startswith("postgresql://"):
        return "postgresql+psycopg://" + value[len("postgresql://"):]
    return value


def _connection_target(value: str) -> tuple[str, str, int | None, str] | None:
    """Return a credential-free database identity for collision checks."""
    candidate = value.strip()
    if not candidate:
        return None
    comparable = candidate.replace("postgresql+psycopg://", "postgresql://", 1)
    comparable = comparable.replace("postgres://", "postgresql://", 1)
    parsed = urlsplit(comparable)
    if parsed.scheme == "sqlite":
        return ("sqlite", "", None, parsed.path)
    if parsed.scheme != "postgresql":
        return (
            parsed.scheme.lower(), (parsed.hostname or "").lower(), parsed.port,
            parsed.path.lstrip("/"),
        )
    return (
        "postgresql", (parsed.hostname or "").lower(), parsed.port or 5432,
        parsed.path.lstrip("/"),
    )


TAGNEXT_RUNTIME_MODE = os.getenv("TAGNEXT_RUNTIME_MODE", "").strip().lower()
EXPLICIT_LOCAL_MODE = TAGNEXT_RUNTIME_MODE in {"local", "unit_test"}
_database_required = _truthy("TAGNEXT_DATABASE_REQUIRED")
_tagnext_database_url = os.getenv("TAGNEXT_DATABASE_URL", "").strip()

if SYSTEM_ID == "tagnext":
    if not _tagnext_database_url and (_database_required or not EXPLICIT_LOCAL_MODE):
        raise RuntimeError(
            "TAGNEXT_DATABASE_URL is required outside explicit local/unit-test mode; "
            "TAGneXt never falls back to champion database variables."
        )
    _raw_database_url = _tagnext_database_url or os.getenv(
        "TAGNEXT_LOCAL_DATABASE_URL", "sqlite:///./tagnext_challenger.sqlite3"
    ).strip()
    challenger_target = _connection_target(_raw_database_url)
    for champion_name in ("TERMINAL_DATABASE_URL", "DATABASE_URL"):
        champion_value = os.getenv(champion_name, "").strip()
        if champion_value and _connection_target(champion_value) == challenger_target:
            raise RuntimeError(
                f"TAGneXt write database collides with {champion_name}; champion connections "
                "may never be used as the challenger write target."
            )
    DATABASE_SOURCE = "TAGNEXT_DATABASE_URL" if _tagnext_database_url else "explicit_local_sqlite"
else:
    _raw_database_url = (
        os.getenv("TERMINAL_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
        or "sqlite:///./tagalysis_local.sqlite3"
    )
    DATABASE_SOURCE = "legacy_system_database"

if not _raw_database_url:
    raise RuntimeError("Configured database URL is empty.")

DATABASE_URL = _sqlalchemy_url(_raw_database_url)
DATABASE_FINGERPRINT = hashlib.sha256(DATABASE_URL.encode("utf-8")).hexdigest()[:16]
DATABASE_DIAGNOSTIC = {
    "systemId": SYSTEM_ID,
    "source": DATABASE_SOURCE,
    "dialect": DATABASE_URL.split(":", 1)[0].split("+", 1)[0],
    "fingerprint": DATABASE_FINGERPRINT,
    "explicitLocalMode": EXPLICIT_LOCAL_MODE,
}

RELAY_TOKEN = os.getenv("RELAY_TOKEN", "").strip()
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
# Ten-minute evidence packets keep the free public-provider footprint bounded.
# WebSocket market/depth state remains live between persisted packets.
COLLECT_SECONDS = max(600, int(os.getenv("COLLECT_SECONDS", "600")))
APP_VERSION = "tagnext-1.0.0-rc4"

# Project-specific user context used only for risk framing, never for automatic orders.
TAG_BAG_TOKENS = float(os.getenv("TAG_BAG_TOKENS", "100812406"))
TAG_COST_BASIS = float(os.getenv("TAG_COST_BASIS", "0.00014105"))
