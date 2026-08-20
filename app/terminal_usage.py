from __future__ import annotations

import os
import threading
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator


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
SERVER_JOBS_ENABLED = env_bool("SERVER_JOBS_ENABLED", True)
# A five-second heartbeat is required for direct exact-deadline outcome
# captures.  Collection itself remains separately bucketed at five minutes.
SERVER_JOB_POLL_SECONDS = env_int("SERVER_JOB_POLL_SECONDS", 5, minimum=5)
# External outcome work remains five-minute by default. A shorter bounded
# interval is supported for local scheduler acceptance so the same production
# queue/claim path can be proven without waiting for a cloud cadence.
EXTERNAL_GRADING_SECONDS = env_int("TAGNEXT_EXTERNAL_GRADING_SECONDS", 300, minimum=5)
DETERMINISTIC_GRADING_ENABLED = env_bool("DETERMINISTIC_GRADING_ENABLED", True)
# This bounded, database-only research job has no provider, paid-AI, or
# forecast-write path. It is independently visible in health and can be
# paused without disabling collection, grading, or historical maintenance.
HISTORICAL_RESEARCH_ENABLED = env_bool("HISTORICAL_RESEARCH_ENABLED", True)
# Phase 1 is a zero-paid-call phase. This is a separate, fail-closed gate so a
# stale collection of legacy Chad flags cannot accidentally enable a request.
PAID_AI_ENABLED = env_bool("PAID_AI_ENABLED", False)
OPENAI_AUTOMATIC_ENABLED = env_bool("OPENAI_AUTOMATIC_ENABLED", False)
PUSH_ENABLED = env_bool("PUSH_ENABLED", False)
CHAD_REACTIVATION_ENABLED = env_bool("CHAD_REACTIVATION_ENABLED", False)
CHAD_KILL_SWITCH = env_bool("CHAD_KILL_SWITCH", True)
CHAD_FORECAST_WRITES_ENABLED = env_bool("CHAD_FORECAST_WRITES_ENABLED", False)
CHAD_GRADING_ENABLED = env_bool("CHAD_GRADING_ENABLED", False)
CHAD_CACHE_WRITES_ENABLED = env_bool("CHAD_CACHE_WRITES_ENABLED", False)
OPENAI_PROJECT_BUDGET_CONFIRMED = env_bool(
    "OPENAI_PROJECT_BUDGET_CONFIRMED",
    False,
)
# The protected repair deployment already has a verified schema. Re-running
# create_all(), ALTER TABLE, CREATE INDEX, and ledger bootstrap work on every
# Render wake delays readiness and performs avoidable Neon queries. Normal mode
# keeps the existing bootstrap default; repair mode can opt back in explicitly.
DB_BOOTSTRAP_ON_START = env_bool("DB_BOOTSTRAP_ON_START", not REPAIR_MODE)

CHAD_REQUEST_DATABASE_QUERY_LIMIT = env_int(
    "CHAD_REQUEST_DATABASE_QUERY_LIMIT",
    40,
    minimum=1,
)
CHAD_REQUEST_DATABASE_WRITE_LIMIT = env_int(
    "CHAD_REQUEST_DATABASE_WRITE_LIMIT",
    12,
    minimum=0,
)
CHAD_REQUEST_EXTERNAL_REQUEST_LIMIT = env_int(
    "CHAD_REQUEST_EXTERNAL_REQUEST_LIMIT",
    20,
    minimum=1,
)
CHAD_REQUEST_OPENAI_CALL_LIMIT = env_int(
    "CHAD_REQUEST_OPENAI_CALL_LIMIT",
    1,
    minimum=1,
)

BUILD_ID = (
    os.getenv("RENDER_GIT_COMMIT")
    or os.getenv("GIT_COMMIT_SHA")
    or os.getenv("SOURCE_BUILD_ID")
    or "source-package"
).strip()

OPENAI_DAILY_CALL_LIMIT = env_int("OPENAI_DAILY_CALL_LIMIT", 1)
OPENAI_MONTHLY_CALL_LIMIT = env_int("OPENAI_MONTHLY_CALL_LIMIT", 20)
OPENAI_AUTO_RESERVE_DAILY = env_int("OPENAI_AUTO_RESERVE_DAILY", 1)
OPENAI_AUTO_RESERVE_MONTHLY = env_int("OPENAI_AUTO_RESERVE_MONTHLY", 4)
OPENAI_AUTO_EVENT_COOLDOWN_SECONDS = env_int(
    "OPENAI_AUTO_EVENT_COOLDOWN_SECONDS", 21_600, minimum=900
)
OPENAI_AUTO_MIN_CONFIRMATIONS = env_int(
    "OPENAI_AUTO_MIN_CONFIRMATIONS", 2, minimum=2
)


class RequestBudgetExceeded(RuntimeError):
    def __init__(self, category: str, limit: int) -> None:
        self.category = category
        self.limit = limit
        super().__init__(f"{category} request budget exceeded ({limit}).")


@dataclass
class RequestBudget:
    limits: dict[str, int]
    used: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def consume(self, category: str, amount: int = 1) -> None:
        safe_amount = max(0, int(amount))
        next_value = int(self.used.get(category, 0)) + safe_amount
        limit = int(self.limits.get(category, 0))
        if limit >= 0 and next_value > limit:
            raise RequestBudgetExceeded(category, limit)
        self.used[category] = next_value

    def summary(self) -> dict[str, Any]:
        return {
            "used": dict(self.used),
            "limits": dict(self.limits),
            "withinBudget": all(
                int(self.used.get(category, 0)) <= int(limit)
                for category, limit in self.limits.items()
            ),
        }


_request_budget: ContextVar[RequestBudget | None] = ContextVar(
    "tag_terminal_request_budget",
    default=None,
)


@contextmanager
def request_budget_scope(
    limits: dict[str, int] | None = None,
) -> Iterator[RequestBudget]:
    budget = RequestBudget(
        limits=limits
        or {
            "database_query": CHAD_REQUEST_DATABASE_QUERY_LIMIT,
            "database_write": CHAD_REQUEST_DATABASE_WRITE_LIMIT,
            "external_request": CHAD_REQUEST_EXTERNAL_REQUEST_LIMIT,
            "openai_call": CHAD_REQUEST_OPENAI_CALL_LIMIT,
        }
    )
    token: Token[RequestBudget | None] = _request_budget.set(budget)
    try:
        yield budget
    finally:
        _request_budget.reset(token)


def consume_request_budget(category: str, amount: int = 1) -> None:
    budget = _request_budget.get()
    if budget is not None:
        budget.consume(category, amount)


def account_database_statement(statement: str) -> None:
    consume_request_budget("database_query")
    command = statement.lstrip().split(None, 1)[0].upper() if statement.strip() else ""
    if command in {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP"}:
        consume_request_budget("database_write")


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
                OPENAI_DAILY_CALL_LIMIT,
                OPENAI_MONTHLY_CALL_LIMIT,
            ),
            "chad_request": (
                env_int("CHAD_DAILY_REQUEST_LIMIT", 2),
                env_int("CHAD_MONTHLY_REQUEST_LIMIT", 20),
            ),
            "chad_grade_batch": (
                env_int("CHAD_GRADE_DAILY_BATCH_LIMIT", 4),
                env_int("CHAD_GRADE_MONTHLY_BATCH_LIMIT", 80),
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
            if category in {"openai_call", "chad_request"} and not PAID_AI_ENABLED:
                self._blocked[category] += 1
                return False, "paid_ai_disabled"
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
                    "serverJobsEnabled": SERVER_JOBS_ENABLED,
                    "deterministicGradingEnabled": DETERMINISTIC_GRADING_ENABLED,
                    "historicalResearchEnabled": HISTORICAL_RESEARCH_ENABLED,
                    "paidAiEnabled": PAID_AI_ENABLED,
                    "openAiAutomaticEnabled": OPENAI_AUTOMATIC_ENABLED,
                    "pushEnabled": PUSH_ENABLED,
                    "databaseBootstrapOnStart": DB_BOOTSTRAP_ON_START,
                    "chadReactivationEnabled": CHAD_REACTIVATION_ENABLED,
                    "chadKillSwitch": CHAD_KILL_SWITCH,
                    "chadForecastWritesEnabled": CHAD_FORECAST_WRITES_ENABLED,
                    "chadGradingEnabled": CHAD_GRADING_ENABLED,
                    "chadCacheWritesEnabled": CHAD_CACHE_WRITES_ENABLED,
                    "openAiProjectBudgetConfirmed": OPENAI_PROJECT_BUDGET_CONFIRMED,
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
