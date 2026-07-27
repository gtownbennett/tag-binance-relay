from __future__ import annotations

import os
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


# Repair/off is intentionally the missing-variable default. Normal live work is
# enabled only after Eric approves budgets and Render receives explicit flags.
REPAIR_MODE = env_bool("REPAIR_MODE", True)
LIVE_COLLECTORS_ENABLED = env_bool("LIVE_COLLECTORS_ENABLED", False)
BACKFILL_ENABLED = env_bool("BACKFILL_ENABLED", False)
OPENAI_AUTOMATIC_ENABLED = env_bool("OPENAI_AUTOMATIC_ENABLED", False)
PUSH_ENABLED = env_bool("PUSH_ENABLED", False)

BUILD_ID = (
    os.getenv("RENDER_GIT_COMMIT")
    or os.getenv("GIT_COMMIT_SHA")
    or os.getenv("SOURCE_BUILD_ID")
    or "source-package"
).strip()


class UsageGovernor:
    """Small in-process circuit breaker for paid or high-volume work.

    The counters are deliberately conservative. They protect a single running
    relay instance; provider-side project limits remain the final backstop.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._day = ""
        self._month = ""
        self._daily: dict[str, int] = defaultdict(int)
        self._monthly: dict[str, int] = defaultdict(int)
        self._bytes: dict[str, int] = defaultdict(int)
        self._cache_hits = 0
        self._cache_misses = 0
        self._blocked: dict[str, int] = defaultdict(int)
        self._last_bounded_test_at: str | None = None
        self._limits = {
            "openai_call": (
                env_int("OPENAI_DAILY_CALL_LIMIT", 3),
                env_int("OPENAI_MONTHLY_CALL_LIMIT", 30),
            ),
            "external_request": (
                env_int("EXTERNAL_DAILY_REQUEST_LIMIT", 250),
                env_int("EXTERNAL_MONTHLY_REQUEST_LIMIT", 5_000),
            ),
            "expensive_route": (
                env_int("EXPENSIVE_ROUTE_DAILY_LIMIT", 250),
                env_int("EXPENSIVE_ROUTE_MONTHLY_LIMIT", 5_000),
            ),
            "manual_market_read": (
                env_int("MANUAL_REFRESH_DAILY_LIMIT", 25),
                env_int("MANUAL_REFRESH_MONTHLY_LIMIT", 500),
            ),
            "bounded_intelligence_read": (
                env_int("INTELLIGENCE_READ_DAILY_LIMIT", 25),
                env_int("INTELLIGENCE_READ_MONTHLY_LIMIT", 500),
            ),
            "database_query": (
                env_int("DATABASE_DAILY_QUERY_LIMIT", 10_000),
                env_int("DATABASE_MONTHLY_QUERY_LIMIT", 200_000),
            ),
            "notification_attempt": (
                env_int("PUSH_DAILY_ATTEMPT_LIMIT", 100),
                env_int("PUSH_MONTHLY_ATTEMPT_LIMIT", 2_000),
            ),
        }

    def _roll_windows(self) -> None:
        now = datetime.now(timezone.utc)
        day = now.date().isoformat()
        month = day[:7]
        if day != self._day:
            self._day = day
            self._daily = defaultdict(int)
        if month != self._month:
            self._month = month
            self._monthly = defaultdict(int)

    def authorize(self, category: str, *, automatic: bool = False) -> tuple[bool, str | None]:
        with self._lock:
            self._roll_windows()
            if automatic and REPAIR_MODE:
                self._blocked[category] += 1
                return False, "repair_mode"
            if automatic and category == "collector" and not LIVE_COLLECTORS_ENABLED:
                self._blocked[category] += 1
                return False, "collectors_disabled"
            if automatic and category == "openai_call" and not OPENAI_AUTOMATIC_ENABLED:
                self._blocked[category] += 1
                return False, "automatic_openai_disabled"
            if automatic and category == "notification_attempt" and not PUSH_ENABLED:
                self._blocked[category] += 1
                return False, "push_disabled"

            daily_limit, monthly_limit = self._limits.get(category, (0, 0))
            if daily_limit and self._daily[category] >= daily_limit:
                self._blocked[category] += 1
                return False, "daily_limit"
            if monthly_limit and self._monthly[category] >= monthly_limit:
                self._blocked[category] += 1
                return False, "monthly_limit"
            self._daily[category] += 1
            self._monthly[category] += 1
            return True, None

    def record(self, category: str, amount: int = 1, *, byte_count: int = 0) -> None:
        with self._lock:
            self._roll_windows()
            self._daily[category] += max(0, amount)
            self._monthly[category] += max(0, amount)
            if byte_count > 0:
                self._bytes[category] += byte_count

    def cache(self, hit: bool) -> None:
        with self._lock:
            if hit:
                self._cache_hits += 1
            else:
                self._cache_misses += 1

    def mark_bounded_test(self) -> None:
        with self._lock:
            self._last_bounded_test_at = datetime.now(timezone.utc).isoformat()

    def summary(self) -> dict[str, Any]:
        with self._lock:
            self._roll_windows()
            total_cache = self._cache_hits + self._cache_misses
            circuit_open = any(
                reason_count > 0
                for category, reason_count in self._blocked.items()
                if category in self._limits
            )
            return {
                "repairMode": REPAIR_MODE,
                "circuitOpen": circuit_open,
                "flags": {
                    "liveCollectorsEnabled": LIVE_COLLECTORS_ENABLED,
                    "backfillEnabled": BACKFILL_ENABLED,
                    "openAiAutomaticEnabled": OPENAI_AUTOMATIC_ENABLED,
                    "pushEnabled": PUSH_ENABLED,
                },
                "today": dict(self._daily),
                "month": dict(self._monthly),
                "bytes": dict(self._bytes),
                "blocked": dict(self._blocked),
                "cache": {
                    "hits": self._cache_hits,
                    "misses": self._cache_misses,
                    "hitRatePct": round(self._cache_hits / total_cache * 100.0, 1)
                    if total_cache
                    else None,
                },
                "limits": {
                    key: {"daily": value[0], "monthly": value[1]}
                    for key, value in self._limits.items()
                },
                "lastBoundedTestAt": self._last_bounded_test_at,
            }


usage_governor = UsageGovernor()


def operating_status(service_version: str) -> dict[str, Any]:
    return {
        "serviceVersion": service_version,
        "buildId": BUILD_ID,
        "repairMode": REPAIR_MODE,
        "mode": "REPAIR" if REPAIR_MODE else "NORMAL",
        "message": (
            "REPAIR MODE — LIVE MONITORING PAUSED"
            if REPAIR_MODE
            else "Normal mode"
        ),
        "usage": usage_governor.summary(),
    }
