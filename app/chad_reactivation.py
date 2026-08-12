from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from .terminal_usage import (
    BACKFILL_ENABLED,
    CHAD_CACHE_WRITES_ENABLED,
    CHAD_FORECAST_WRITES_ENABLED,
    CHAD_GRADING_ENABLED,
    CHAD_KILL_SWITCH,
    CHAD_REACTIVATION_ENABLED,
    LIVE_COLLECTORS_ENABLED,
    OPENAI_AUTOMATIC_ENABLED,
    OPENAI_PROJECT_BUDGET_CONFIRMED,
    PUSH_ENABLED,
    REPAIR_MODE,
    env_int,
    usage_governor,
)


CHAD_MIN_REQUEST_INTERVAL_SECONDS = env_int(
    "CHAD_MIN_REQUEST_INTERVAL_SECONDS",
    300,
    minimum=60,
)
CHAD_FORECAST_MIN_INTERVAL_SECONDS = env_int(
    "CHAD_FORECAST_MIN_INTERVAL_SECONDS",
    21_600,
    minimum=3_600,
)
CHAD_REGIME_CONFIRMATIONS_REQUIRED = env_int(
    "CHAD_REGIME_CONFIRMATIONS_REQUIRED",
    2,
    minimum=2,
)
CHAD_REGIME_DEBOUNCE_SECONDS = env_int(
    "CHAD_REGIME_DEBOUNCE_SECONDS",
    300,
    minimum=60,
)
CHAD_GRADING_MAX_PER_BATCH = env_int(
    "CHAD_GRADING_MAX_PER_BATCH",
    4,
    minimum=1,
)
FORECAST_GRADE_TOLERANCE_SECONDS = env_int(
    "FORECAST_GRADE_TOLERANCE_SECONDS",
    900,
    minimum=60,
)


class ChadReactivationGate:
    """Fail-closed runtime gate for manual, paid Chad analysis.

    This gate is deliberately in-memory and conservative. Render restarts clear
    cooldown and debounce observations, but do not unlock any feature flag,
    raise a spending limit, or bypass the durable evidence-hash cache.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_request_id: str | None = None
        self._last_accepted_at: float | None = None
        self._candidate_state: str | None = None
        self._candidate_first_seen_at: float | None = None
        self._candidate_observations = 0
        self._telemetry: dict[str, Any] = {
            "acceptedRequests": 0,
            "completedRequests": 0,
            "failedRequests": 0,
            "openAiAttempts": 0,
            "openAiRetries": 0,
            "cacheHits": 0,
            "cacheMisses": 0,
            "blocked": defaultdict(int),
            "lastStartedAt": None,
            "lastCompletedAt": None,
            "lastDurationMs": None,
            "lastFailureType": None,
            "lastRequestBudget": None,
            "lastOpenAiUsage": None,
        }

    @staticmethod
    def blockers(
        *,
        key_configured: bool,
        relay_token_configured: bool,
        call_mode: str = "manual",
    ) -> list[str]:
        blockers: list[str] = []
        mode = call_mode.lower()
        if mode not in {"manual", "automatic"}:
            blockers.append("invalid_call_mode")
        if not CHAD_REACTIVATION_ENABLED:
            blockers.append("chad_reactivation_disabled")
        if CHAD_KILL_SWITCH:
            blockers.append("chad_kill_switch_on")
        if not CHAD_FORECAST_WRITES_ENABLED:
            blockers.append("forecast_writes_disabled")
        if not CHAD_GRADING_ENABLED:
            blockers.append("grading_disabled")
        if not CHAD_CACHE_WRITES_ENABLED:
            blockers.append("durable_cache_writes_disabled")
        if not OPENAI_PROJECT_BUDGET_CONFIRMED:
            blockers.append("openai_project_budget_unconfirmed")
        if mode == "automatic" and not OPENAI_AUTOMATIC_ENABLED:
            blockers.append("automatic_openai_disabled")
        if not key_configured:
            blockers.append("openai_key_missing")
        if not relay_token_configured:
            blockers.append("relay_token_missing")
        return blockers

    def begin_request(
        self,
        *,
        key_configured: bool,
        relay_token_configured: bool,
        call_mode: str = "manual",
        now: float | None = None,
    ) -> tuple[str | None, str | None]:
        current = time.time() if now is None else float(now)
        blockers = self.blockers(
            key_configured=key_configured,
            relay_token_configured=relay_token_configured,
            call_mode=call_mode,
        )
        if blockers:
            reason = blockers[0]
            with self._lock:
                self._telemetry["blocked"][reason] += 1
            return None, reason

        with self._lock:
            if self._active_request_id is not None:
                self._telemetry["blocked"]["request_in_progress"] += 1
                return None, "request_in_progress"
            if (
                self._last_accepted_at is not None
                and current - self._last_accepted_at
                < CHAD_MIN_REQUEST_INTERVAL_SECONDS
            ):
                self._telemetry["blocked"]["cooldown"] += 1
                return None, "cooldown"

            allowed, reason = usage_governor.authorize(
                "chad_request", automatic=call_mode.lower() == "automatic"
            )
            if not allowed:
                safe_reason = f"request_{reason or 'limit'}"
                self._telemetry["blocked"][safe_reason] += 1
                return None, safe_reason

            request_id = uuid.uuid4().hex
            self._active_request_id = request_id
            self._last_accepted_at = current
            self._telemetry["acceptedRequests"] += 1
            self._telemetry["lastStartedAt"] = datetime.fromtimestamp(
                current,
                tz=timezone.utc,
            ).isoformat()
            self._telemetry["lastFailureType"] = None
            return request_id, None

    def record_openai_attempt(self, *, retry: bool = False) -> None:
        with self._lock:
            self._telemetry["openAiAttempts"] += 1
            if retry:
                self._telemetry["openAiRetries"] += 1

    def record_cache(self, *, hit: bool) -> None:
        with self._lock:
            key = "cacheHits" if hit else "cacheMisses"
            self._telemetry[key] += 1

    def record_openai_usage(self, raw: dict[str, Any]) -> None:
        usage = raw.get("usage") if isinstance(raw.get("usage"), dict) else {}
        with self._lock:
            self._telemetry["lastOpenAiUsage"] = {
                "inputTokens": int(usage.get("input_tokens") or 0),
                "outputTokens": int(usage.get("output_tokens") or 0),
            }

    def finish_request(
        self,
        request_id: str,
        *,
        success: bool,
        started_at: float,
        budget: dict[str, Any] | None,
        error: BaseException | None = None,
    ) -> None:
        finished_at = time.time()
        with self._lock:
            if self._active_request_id == request_id:
                self._active_request_id = None
            self._telemetry["completedRequests"] += 1
            if not success:
                self._telemetry["failedRequests"] += 1
            self._telemetry["lastCompletedAt"] = datetime.fromtimestamp(
                finished_at,
                tz=timezone.utc,
            ).isoformat()
            self._telemetry["lastDurationMs"] = max(
                0,
                int((finished_at - started_at) * 1000),
            )
            self._telemetry["lastFailureType"] = (
                type(error).__name__ if error is not None else None
            )
            self._telemetry["lastRequestBudget"] = budget

    def debounce_regime(
        self,
        *,
        candidate_state: str,
        previous_state: str | None,
        now: float | None = None,
    ) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        candidate = candidate_state.strip().lower() or "insufficient_data"
        previous = (
            previous_state.strip().lower()
            if isinstance(previous_state, str) and previous_state.strip()
            else None
        )

        with self._lock:
            if previous is None or candidate == previous:
                self._candidate_state = None
                self._candidate_first_seen_at = None
                self._candidate_observations = 0
                return {
                    "candidate": candidate,
                    "effective": candidate,
                    "confirmed": True,
                    "observations": 1,
                    "requiredObservations": CHAD_REGIME_CONFIRMATIONS_REQUIRED,
                    "minimumSeconds": CHAD_REGIME_DEBOUNCE_SECONDS,
                }

            if self._candidate_state != candidate:
                self._candidate_state = candidate
                self._candidate_first_seen_at = current
                self._candidate_observations = 1
            else:
                self._candidate_observations += 1

            elapsed = max(
                0.0,
                current - float(self._candidate_first_seen_at or current),
            )
            confirmed = (
                self._candidate_observations >= CHAD_REGIME_CONFIRMATIONS_REQUIRED
                and elapsed >= CHAD_REGIME_DEBOUNCE_SECONDS
            )
            effective = candidate if confirmed else previous
            result = {
                "candidate": candidate,
                "effective": effective,
                "confirmed": confirmed,
                "observations": self._candidate_observations,
                "requiredObservations": CHAD_REGIME_CONFIRMATIONS_REQUIRED,
                "elapsedSeconds": int(elapsed),
                "minimumSeconds": CHAD_REGIME_DEBOUNCE_SECONDS,
            }
            if confirmed:
                self._candidate_state = None
                self._candidate_first_seen_at = None
                self._candidate_observations = 0
            return result

    def status(
        self,
        *,
        key_configured: bool,
        relay_token_configured: bool,
    ) -> dict[str, Any]:
        blockers = self.blockers(
            key_configured=key_configured,
            relay_token_configured=relay_token_configured,
        )
        with self._lock:
            telemetry = dict(self._telemetry)
            telemetry["blocked"] = dict(self._telemetry["blocked"])
            active = self._active_request_id is not None
        return {
            "manualReady": not blockers,
            "automaticEnabled": False,
            "blockers": blockers,
            "activeRequest": active,
            "limits": {
                "minimumRequestIntervalSeconds": CHAD_MIN_REQUEST_INTERVAL_SECONDS,
                "forecastMinimumIntervalSeconds": CHAD_FORECAST_MIN_INTERVAL_SECONDS,
                "regimeConfirmationsRequired": CHAD_REGIME_CONFIRMATIONS_REQUIRED,
                "regimeDebounceSeconds": CHAD_REGIME_DEBOUNCE_SECONDS,
                "gradingMaxPerBatch": CHAD_GRADING_MAX_PER_BATCH,
                "gradeSampleToleranceSeconds": FORECAST_GRADE_TOLERANCE_SECONDS,
                "openAiRetriesPerRequest": 0,
            },
            "telemetry": telemetry,
        }

    def reset_for_tests(self) -> None:
        with self._lock:
            self._active_request_id = None
            self._last_accepted_at = None
            self._candidate_state = None
            self._candidate_first_seen_at = None
            self._candidate_observations = 0
            self._telemetry["acceptedRequests"] = 0
            self._telemetry["completedRequests"] = 0
            self._telemetry["failedRequests"] = 0
            self._telemetry["openAiAttempts"] = 0
            self._telemetry["openAiRetries"] = 0
            self._telemetry["cacheHits"] = 0
            self._telemetry["cacheMisses"] = 0
            self._telemetry["blocked"] = defaultdict(int)
            self._telemetry["lastStartedAt"] = None
            self._telemetry["lastCompletedAt"] = None
            self._telemetry["lastDurationMs"] = None
            self._telemetry["lastFailureType"] = None
            self._telemetry["lastRequestBudget"] = None
            self._telemetry["lastOpenAiUsage"] = None


chad_reactivation_gate = ChadReactivationGate()
