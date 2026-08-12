from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from app.ledger import PredictionLedger
from app.chad_reactivation import (
    CHAD_FORECAST_MIN_INTERVAL_SECONDS,
    CHAD_GRADING_MAX_PER_BATCH,
    FORECAST_GRADE_TOLERANCE_SECONDS,
    chad_reactivation_gate,
)
from app.terminal_config import COLLECT_SECONDS, DATABASE_URL as TERMINAL_DATABASE_URL
from app.terminal_addon import (
    APP_VERSION as TERMINAL_ADDON_VERSION,
    alert_feed as terminal_alert_feed,
    backfill_day as terminal_backfill_day,
    backfill_month as terminal_backfill_month,
    backfill_recent as terminal_backfill_recent,
    build_chad_report as build_terminal_chad_report,
    chad_history as terminal_chad_history,
    evaluate_alerts as evaluate_terminal_alerts,
    heatmap as terminal_heatmap,
    liquidation_feed as terminal_liquidation_feed,
    prediction_ledger as terminal_prediction_ledger,
    server_oi_history,
    share_report_text as terminal_share_report_text,
    terminal_addon,
)
from app.terminal_intelligence import insert_test_alert
from app.terminal_compact import (
    build_compact_terminal_payload,
    merge_compact_intelligence,
)
from app.terminal_multi_exchange import multi_exchange_service
from app.terminal_vision import (
    historical_futures_price_near as historical_vision_price_near,
)
from app.terminal_database import (
    OpenAIAnalysisCacheRow,
    json_dumps as terminal_json_dumps,
    session_scope,
    utc_now as terminal_utc_now,
)
from app.phase1_reliability import (
    AsyncCoalescingCache,
    authorize_persistent_usage,
    bounded_retry,
    build_canonical_evidence_packet,
    cache_put as phase1_cache_put,
    claim_due_job,
    complete_job,
    enqueue_job,
    fail_job,
    latest_evidence_packet,
    persist_evidence_packet,
    persist_helper_candidate,
    schedule_current_evidence_job,
    stable_hash,
    validate_helper_candidate,
)
from app.canonical_forecast import (
    ForecastValidationError,
    format_canonical_forecast,
    issue_due_tagalysis_forecasts,
    latest_canonical_forecast,
    persist_asset_truth_snapshot,
    persist_canonical_forecast,
    persist_portfolio_position_snapshot,
)
from app.supply_truth import (
    SupplyTruthError,
    verified_tag_supply_payload,
    verified_tag_supply_payload_from_cmc_and_gecko,
)
from app.phase3_learning import (
    Phase3ValidationError,
    active_alerts as phase3_active_alerts,
    capture_direct_deadline_outcome,
    capture_exact_due_outcomes,
    current_learning_version,
    current_user_levels,
    enqueue_phase3_jobs,
    finalize_alert,
    find_historical_analogs,
    grade_canonical_forecast,
    grade_report,
    grade_social_call,
    maintain_pattern_memory_from_latest_evidence,
    persist_learning_version,
    persist_pattern_sequence,
    persist_user_level_revision,
    persist_verified_outcome,
    process_alert_signal,
    process_level_alerts_from_latest_evidence,
    rollback_learning_version,
)
from app.phase4_control_center import canonical_control_center_snapshot
from app.historical_memory import (
    build_coverage_report,
    chad_history_evidence_package,
    historical_event_report,
    historical_maintenance,
    historical_production_summary,
)
from app.forecast_research import production_research_watermark, run_bounded_production_research
from app.predictive_tournament import persist_bounded_predictive_study
from app.prospective_learning import (
    evaluate_prospective_thresholds,
)
from app.event_driven_chad import (
    chad_usage_report,
    finish_chad_call,
    record_auto_event_decision,
    reserve_chad_call,
)
from app.terminal_paper_social import (
    alert_timeline as terminal_alert_timeline,
    cancel_paper_trade,
    close_paper_trade,
    enrich_social_calls_with_cmc_quotes,
    ingest_cmc_posts_response,
    ingest_social_call,
    paper_ledger as terminal_paper_ledger,
    place_paper_order,
    poll_cmc_social_calls,
    research_timestamp,
    social_ledger as terminal_social_ledger,
)
from app.terminal_usage import (
    BACKFILL_ENABLED,
    BUILD_ID,
    CHAD_CACHE_WRITES_ENABLED,
    CHAD_GRADING_ENABLED,
    DB_BOOTSTRAP_ON_START,
    DETERMINISTIC_GRADING_ENABLED,
    HISTORICAL_RESEARCH_ENABLED,
    LIVE_COLLECTORS_ENABLED,
    OPENAI_DAILY_CALL_LIMIT,
    OPENAI_MONTHLY_CALL_LIMIT,
    OPENAI_AUTOMATIC_ENABLED,
    PAID_AI_ENABLED,
    PUSH_ENABLED,
    REPAIR_MODE,
    SERVER_JOB_POLL_SECONDS,
    SERVER_JOBS_ENABLED,
    RequestBudgetExceeded,
    consume_request_budget,
    operating_status,
    request_budget_scope,
    usage_governor,
)


import httpx
import websockets
from fastapi import Body, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

SERVICE_VERSION = "2.11.0-phase3"

SYMBOL = os.getenv("BINANCE_SYMBOL", "TAGUSDT").upper()
REST_BASE = os.getenv("BINANCE_REST_BASE", "https://fapi.binance.com").rstrip("/")
WS_BASES = [
    os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com").rstrip("/"),
]
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "").strip()
CACHE_SECONDS = max(5, int(os.getenv("CACHE_SECONDS", "15")))
TERMINAL_RESPONSE_CACHE_SECONDS = max(
    60,
    int(os.getenv("TERMINAL_RESPONSE_CACHE_SECONDS", "300" if REPAIR_MODE else "60")),
)
INTELLIGENCE_READ_TIMEOUT_SECONDS = min(
    12,
    max(2, int(os.getenv("INTELLIGENCE_READ_TIMEOUT_SECONDS", "8"))),
)
MANUAL_RESPONSE_BUDGET_SECONDS = min(
    26,
    max(15, int(os.getenv("MANUAL_RESPONSE_BUDGET_SECONDS", "24"))),
)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.5").strip() or "gpt-5.5"

DEXSCREENER_BASE = os.getenv("DEXSCREENER_BASE", "https://api.dexscreener.com").rstrip("/")
DEX_CHAIN_ID = os.getenv("DEX_CHAIN_ID", "bsc").strip() or "bsc"
DEX_PAIR_ADDRESS = os.getenv(
    "DEX_PAIR_ADDRESS", "0xf0750c373EbBB3BaEEF7e03D8300cAaD1983d67c"
).strip()
OPENAI_REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "low").strip().lower()
OPENAI_MAX_OUTPUT_TOKENS = min(
    5_000,
    max(1_200, int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "4000"))),
)
OPENAI_TIMEOUT_SECONDS = max(20, int(os.getenv("OPENAI_TIMEOUT_SECONDS", "90")))

LEDGER_ENABLED = os.getenv("LEDGER_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
LEDGER_DB_PATH = os.getenv("LEDGER_DB_PATH", "/tmp/tag_prediction_ledger.sqlite3").strip() or "/tmp/tag_prediction_ledger.sqlite3"
LEDGER_BACKUP_PATH = os.getenv("LEDGER_BACKUP_PATH", f"{LEDGER_DB_PATH}.backup.json").strip()
LEDGER_AUTO_BACKUP = os.getenv("LEDGER_AUTO_BACKUP", "true").strip().lower() not in {"0", "false", "no", "off"}
LEDGER_AUTO_GRADE_SECONDS = max(60, int(os.getenv("LEDGER_AUTO_GRADE_SECONDS", "300")))
LEDGER_DEADBAND_PCT = max(0.1, float(os.getenv("LEDGER_DEADBAND_PCT", "1.0")))
LEDGER_MAX_RECORDS = max(100, int(os.getenv("LEDGER_MAX_RECORDS", "5000")))
FRESHNESS_WARN_SECONDS = max(15, int(os.getenv("FRESHNESS_WARN_SECONDS", "90")))
FRESHNESS_STALE_SECONDS = max(FRESHNESS_WARN_SECONDS + 15, int(os.getenv("FRESHNESS_STALE_SECONDS", "300")))

VALID_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}
if OPENAI_REASONING_EFFORT not in VALID_REASONING_EFFORTS:
    OPENAI_REASONING_EFFORT = "low"

VALID_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}

http_client: httpx.AsyncClient | None = None
openai_client: httpx.AsyncClient | None = None
snapshot_cache: dict[str, Any] = {"time": 0.0, "value": None}
spot_cache: dict[str, Any] = {"time": 0.0, "value": None}
terminal_response_cache: dict[str, dict[str, Any]] = {
    "stored": {"time": 0.0, "value": None},
    "manual": {"time": 0.0, "value": None},
}
cache_lock = asyncio.Lock()
spot_cache_lock = asyncio.Lock()
terminal_response_lock = asyncio.Lock()
ledger_lock = asyncio.Lock()
grader_state: dict[str, Any] = {
    "running": False,
    "lastRunAt": None,
    "lastResult": None,
    "lastError": None,
    "intervalSeconds": LEDGER_AUTO_GRADE_SECONDS,
}
phase1_state: dict[str, Any] = {
    "running": False,
    "lastRunAt": None,
    "lastCompletedJob": None,
    "lastEvidenceHash": None,
    "lastPacket": None,
    "lastError": None,
    "coalescedRequests": 0,
}
phase1_coalescer = AsyncCoalescingCache()
prediction_ledger = PredictionLedger(
    LEDGER_DB_PATH,
    deadband_pct=LEDGER_DEADBAND_PCT,
    max_records=LEDGER_MAX_RECORDS,
    backup_path=LEDGER_BACKUP_PATH,
    auto_backup=LEDGER_AUTO_BACKUP,
)

service_started_ms = int(time.time() * 1000)

liquidation_events: deque[dict[str, Any]] = deque(maxlen=20_000)
liquidation_lock = asyncio.Lock()

# Exact Binance trailing-hour taker tape. A reconnect resets continuity so a
# partial/gapped window is never mislabeled as exact.
recent_agg_trades: deque[tuple[int, bool, float]] = deque()
agg_trade_window_started_ms: int | None = None
agg_trade_lock = asyncio.Lock()

stream_lock = asyncio.Lock()


class ChadAnalyzeRequest(BaseModel):
    question: str = Field(
        default=(
            "What is TAG doing right now, why is it moving, what should I watch next, "
            "and what would invalidate the current view?"
        ),
        min_length=1,
        max_length=1200,
    )
    historyPeriod: str = Field(default="5m")
    historyLimit: int = Field(default=72, ge=12, le=240)
    positionTag: float | None = Field(default=None, ge=0)
    averageEntryUsd: float | None = Field(default=None, ge=0)
    forceFresh: bool = Field(default=False)
    includeRawHistory: bool = Field(default=False)
    allowPaidCall: bool = Field(
        default=False,
        description="Must be true for an explicit user-approved paid analysis through the narrow reactivation gate.",
    )
    allowForecastWrite: bool = Field(
        default=False,
        description="Must be true to lock a new immutable forecast.",
    )
    allowGrading: bool = Field(
        default=False,
        description="Must be true to grade due locked forecasts.",
    )
    trigger: str = Field(default="explicit-user-request", max_length=80)


class HelperResultRequest(BaseModel):
    idempotencyKey: str = Field(min_length=8, max_length=160)
    jobId: str | None = Field(default=None, max_length=64)
    evidenceSnapshotId: str | None = Field(default=None, max_length=64)
    producerId: str = Field(min_length=1, max_length=100)
    modelVersion: str = Field(min_length=1, max_length=100)
    origin: str = Field(pattern="^(android|windows)$")
    createdAt: str | None = Field(default=None, max_length=80)
    payload: dict[str, Any] = Field(default_factory=dict)


FULL_FORECAST_HORIZONS = (
    "15m", "1h", "2h", "4h", "6h", "12h", "24h",
    "3d", "7d", "2w", "3w", "1mo", "3mo", "6mo", "1y",
    "2026", "2027", "2028", "2029", "2030",
    "2y", "3y", "4y", "5y", "6y",
)


CHAD_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "headline": {"type": "string"},
        "marketState": {
            "type": "string",
            "enum": ["bullish", "bearish", "neutral", "mixed", "insufficient_data"],
        },
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        "plainEnglishSummary": {"type": "string"},
        "whatHappened": {"type": "array", "items": {"type": "string"}},
        "whyItMatters": {"type": "array", "items": {"type": "string"}},
        "leverageAssessment": {
            "type": "object",
            "properties": {
                "openInterest": {"type": "string"},
                "funding": {"type": "string"},
                "takerFlow": {"type": "string"},
                "longShortPositioning": {"type": "string"},
                "liquidations": {"type": "string"},
                "overall": {"type": "string"},
            },
            "required": [
                "openInterest",
                "funding",
                "takerFlow",
                "longShortPositioning",
                "liquidations",
                "overall",
            ],
            "additionalProperties": False,
        },
        "spotConfirmation": {"type": "string"},
        "keyLevels": {
            "type": "object",
            "properties": {
                "bullishConfirmation": {"type": "array", "items": {"type": "string"}},
                "bearishConfirmation": {"type": "array", "items": {"type": "string"}},
                "invalidation": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["bullishConfirmation", "bearishConfirmation", "invalidation"],
            "additionalProperties": False,
        },
        "scenarios": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "probability": {"type": "integer", "minimum": 0, "maximum": 100},
                    "trigger": {"type": "string"},
                    "expectedMove": {"type": "string"},
                    "invalidation": {"type": "string"},
                },
                "required": ["name", "probability", "trigger", "expectedMove", "invalidation"],
                "additionalProperties": False,
            },
        },
        "actionableGuidance": {"type": "array", "items": {"type": "string"}},
        "forecastLedger": {
            "type": "object",
            "properties": {
                "thesis": {"type": "string"},
                "confidenceAdjustment": {"type": "string"},
                "horizons": {
                    "type": "array",
                    "minItems": len(FULL_FORECAST_HORIZONS),
                    "maxItems": len(FULL_FORECAST_HORIZONS),
                    "items": {
                        "type": "object",
                        "properties": {
                            "horizon": {
                                "type": "string",
                                "enum": list(FULL_FORECAST_HORIZONS),
                            },
                            "direction": {
                                "type": "string",
                                "enum": ["up", "down", "sideways"],
                            },
                            "probability": {"type": "integer", "minimum": 0, "maximum": 100},
                            "targetLowUsd": {"type": "number", "minimum": 0},
                            "targetHighUsd": {"type": "number", "minimum": 0},
                            "invalidationUsd": {"type": "number", "minimum": 0},
                            "reasoning": {"type": "string"},
                        },
                        "required": [
                            "horizon",
                            "direction",
                            "probability",
                            "targetLowUsd",
                            "targetHighUsd",
                            "invalidationUsd",
                            "reasoning",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["thesis", "confidenceAdjustment", "horizons"],
            "additionalProperties": False,
        },
        "dataQuality": {
            "type": "object",
            "properties": {
                "score": {"type": "integer", "minimum": 0, "maximum": 100},
                "missingData": {"type": "array", "items": {"type": "string"}},
                "warnings": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["score", "missingData", "warnings"],
            "additionalProperties": False,
        },
    },
    "required": [
        "headline",
        "marketState",
        "confidence",
        "plainEnglishSummary",
        "whatHappened",
        "whyItMatters",
        "leverageAssessment",
        "spotConfirmation",
        "keyLevels",
        "scenarios",
        "actionableGuidance",
        "forecastLedger",
        "dataQuality",
    ],
    "additionalProperties": False,
}


CHAD_INSTRUCTIONS = """
You are Chad, the plain-English, leverage-first market analyst inside TAG Terminal.
Analyze TAGUSDT using ONLY the verified data supplied in the user message.

Required analysis order:
1. Derivatives structure: price versus open interest, funding, taker flow, long/short ratios, liquidations, basis and order-book imbalance.
2. Spot confirmation. Use the supplied DEX spot object for price, market cap, liquidity, total volume and buy/sell transaction counts. Transaction counts are not dollar buy/sell volume; do not mislabel them. If spot data is unavailable, state that clearly and lower confidence.
3. Catalysts. If no verified catalyst data was supplied, say unavailable; never invent news.
4. Technical structure and confirmation/invalidation levels.

Interpret price and open interest carefully:
- price up + OI up: new leverage entering; stronger momentum but higher liquidation risk.
- price up + OI down: likely short covering/deleveraging; may fade without spot demand.
- price down + OI up: fresh shorting or trapped longs; downside risk rises.
- price down + OI down: leverage flush/deleveraging; bearish now but can prepare a reset.

Rules:
- Plain English first, then technical detail.
- Quote exact supplied numbers when useful.
- Never claim a market cap, DEX volume, catalyst, whale move, or spot-buying confirmation unless it is in the supplied data.
- Compare futures movement with DEX spot participation. A futures-led move with weak DEX volume or weak transaction confirmation deserves lower durability confidence.
- Missing, delayed, stale, or contradictory data must reduce confidence and obey the supplied confidence cap.
- Give exactly three scenarios whose probabilities total 100.
- Produce exactly 25 machine-readable forecast horizons, once each and in this exact order: 15m, 1h, 2h, 4h, 6h, 12h, 24h, 3d, 7d, 2w, 3w, 1mo, 3mo, 6mo, 1y, 2026, 2027, 2028, 2029, 2030, 2y, 3y, 4y, 5y, 6y.
- Use the supplied primary DEX spot price as the anchor. Each horizon needs one direction, one stated probability, a realistic target range and a clear invalidation price. Keep each reasoning field to one short sentence so the phone response stays compact and cost-bounded.
- Near-term horizons may use current market structure. Multi-month, calendar-year, and 2y–6y ranges must become progressively wider and probabilities more conservative. Explicitly label their reasoning as low confidence when verified TAG history or catalyst evidence is insufficient; do not pretend they are calibrated.
- targetLowUsd must be less than or equal to targetHighUsd. Avoid false precision and do not make every horizon point in the same direction unless the evidence supports it.
- Use prediction-ledger performance only when enough graded outcomes exist. If fewer than 8 horizons are graded, explicitly say the sample is too small for meaningful calibration.
- Similar historical setups are project-specific analogs, not proof that the same outcome will repeat.
- Separate a temporary squeeze from a durable trend.
- Do not promise profit or present a trade as certain.
- Keep the response concise enough for a phone screen but detailed enough to explain why the view changed.
- Use the supplied calibration profile only when learningReady is true. Weak historical horizons should lower confidence rather than force a directional call.
""".strip()


stream_state: dict[str, Any] = {
    # Regular market stream: mark price, ticker and liquidations.
    "connected": False,
    "endpoint": None,
    "lastMessageAt": None,
    "lastError": None,

    # Dedicated high-frequency public order-book stream.
    "depthConnected": False,
    "depthEndpoint": None,
    "depthLastMessageAt": None,
    "depthLastError": None,
    "depthMode": None,

    "markPrice": None,
    "indexPrice": None,
    "fundingRate": None,
    "nextFundingTime": None,
    "markEventTime": None,
    "priceChange24hPct": None,
    "volume24hContracts": None,
    "quoteVolume24hUsd": None,
    "tradeCount24h": None,
    "tickerEventTime": None,
    "depth": None,
    "depthEventTime": None,
}


def utc_iso(ms: int | None = None) -> str:
    timestamp = (ms / 1000) if ms is not None else time.time()
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _parse_time_for_api(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def timestamp_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric / 1000.0 if numeric > 10_000_000_000 else numeric
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
    return None


def freshness_item(name: str, timestamp: Any, *, critical: bool = True, note: str | None = None) -> dict[str, Any]:
    parsed = timestamp_seconds(timestamp)
    age = max(0.0, time.time() - parsed) if parsed is not None else None
    if age is None:
        status = "missing"
    elif age > FRESHNESS_STALE_SECONDS:
        status = "stale"
    elif age > FRESHNESS_WARN_SECONDS:
        status = "delayed"
    else:
        status = "fresh"
    return {
        "name": name,
        "timestamp": utc_iso(int(parsed * 1000)) if parsed is not None else None,
        "ageSeconds": round(age, 1) if age is not None else None,
        "status": status,
        "critical": critical,
        "note": note,
    }


def build_freshness_report(
    snapshot: dict[str, Any],
    spot: dict[str, Any],
    history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    items = [
        freshness_item(
            "Binance market event",
            snapshot.get("binanceEventTime"),
            note="Exchange event timestamp from mark/ticker/OI data.",
        ),
        freshness_item(
            "Binance market stream",
            snapshot.get("marketStreamLastMessageAt"),
            note="Last mark-price/ticker WebSocket message observed by the relay.",
        ),
        freshness_item(
            "Binance depth stream",
            snapshot.get("depthStreamLastMessageAt"),
            note="Last order-book WebSocket message observed by the relay.",
        ),
        freshness_item(
            "PancakeSwap spot fetch",
            spot.get("generatedAt"),
            note="DEX Screener fetch time; the upstream pair API does not supply a trade-event timestamp.",
        ),
    ]
    if history is not None:
        items.append(
            freshness_item(
                "Binance history fetch",
                history.get("generatedAt"),
                critical=False,
            )
        )

    critical_items = [item for item in items if item["critical"]]
    statuses = [item["status"] for item in critical_items]
    warnings: list[str] = []
    for item in items:
        if item["status"] != "fresh":
            warnings.append(f"{item['name']} is {item['status']}.")

    if "stale" in statuses:
        overall = "stale"
        score = 40
        confidence_cap = 50
    elif "missing" in statuses:
        overall = "incomplete"
        score = 55
        confidence_cap = 60
    elif "delayed" in statuses:
        overall = "delayed"
        score = 72
        confidence_cap = 72
    else:
        overall = "fresh"
        score = 95
        confidence_cap = None

    return {
        "overall": overall,
        "score": score,
        "confidenceCap": confidence_cap,
        "warnAfterSeconds": FRESHNESS_WARN_SECONDS,
        "staleAfterSeconds": FRESHNESS_STALE_SECONDS,
        "sources": items,
        "warnings": warnings,
        "generatedAt": utc_iso(),
    }


def apply_confidence_controls(
    analysis: dict[str, Any],
    *,
    freshness: dict[str, Any],
    calibration: dict[str, Any],
) -> dict[str, Any]:
    original = as_int(analysis.get("confidence"))
    caps: list[int] = []
    freshness_cap = as_int(freshness.get("confidenceCap"))
    if freshness_cap is not None:
        caps.append(freshness_cap)
    calibration_cap = as_int(calibration.get("suggestedConfidenceCap"))
    if calibration.get("learningReady") and calibration_cap is not None:
        caps.append(calibration_cap)
    applied_cap = min(caps) if caps else None
    adjusted = min(original, applied_cap) if original is not None and applied_cap is not None else original
    if adjusted is not None:
        analysis["confidence"] = adjusted

    quality = analysis.get("dataQuality") if isinstance(analysis.get("dataQuality"), dict) else {}
    warnings = quality.get("warnings") if isinstance(quality.get("warnings"), list) else []
    for warning in freshness.get("warnings", []):
        if warning not in warnings:
            warnings.append(warning)
    if original is not None and adjusted is not None and adjusted < original:
        warnings.append(f"Confidence was capped from {original} to {adjusted} by deterministic freshness/calibration controls.")
    quality["warnings"] = warnings
    if as_int(quality.get("score")) is not None:
        quality["score"] = min(as_int(quality.get("score")) or 0, as_int(freshness.get("score")) or 100)
    analysis["dataQuality"] = quality
    analysis["confidenceControls"] = {
        "originalConfidence": original,
        "adjustedConfidence": adjusted,
        "appliedCap": applied_cap,
        "freshnessCap": freshness_cap,
        "calibrationCap": calibration_cap if calibration.get("learningReady") else None,
    }
    return analysis


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pct_change(new_value: float | None, old_value: float | None) -> float | None:
    if new_value is None or old_value in (None, 0):
        return None
    return ((new_value / old_value) - 1.0) * 100.0


def latest_item(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value:
        item = value[-1]
        return item if isinstance(item, dict) else None
    if isinstance(value, dict):
        return value
    return None


def require_relay_key(x_relay_key: str | None) -> None:
    if not RELAY_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Authenticated API is unavailable until RELAY_TOKEN is configured server-side.",
        )
    supplied = (x_relay_key or "").strip()
    if not supplied or not hmac.compare_digest(supplied, RELAY_TOKEN):
        raise HTTPException(status_code=401, detail="Missing or invalid X-Relay-Key.")


def require_chad_access(x_relay_key: str | None) -> None:
    # A server-side OpenAI key can create billable requests. Refuse to expose the
    # endpoint publicly unless the relay has a shared access token configured.
    if not RELAY_TOKEN:
        raise HTTPException(
            status_code=503,
            detail=(
                "Chad is disabled until RELAY_TOKEN is configured in Render. "
                "This protects the OpenAI API key from public abuse."
            ),
        )
    require_relay_key(x_relay_key)

    if not OPENAI_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="OPENAI_API_KEY is not configured on the server.",
        )
    if openai_client is None:
        raise HTTPException(status_code=503, detail="OpenAI client has not started.")


def require_repair_writes_unlocked(action: str) -> None:
    if REPAIR_MODE:
        raise HTTPException(
            status_code=423,
            detail=f"Repair mode is active; {action} is paused.",
        )


async def get_json(path: str, params: dict[str, Any] | None = None) -> Any:
    if http_client is None:
        raise RuntimeError("HTTP client has not started.")
    consume_request_budget("external_request")
    allowed, reason = usage_governor.authorize("external_request")
    if not allowed:
        raise RuntimeError(f"External request circuit is open ({reason}).")

    response = await http_client.get(f"{REST_BASE}{path}", params=params)

    if response.status_code == 451:
        raise RuntimeError(
            "Binance returned HTTP 451. The relay region cannot access this endpoint."
        )

    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Binance returned non-JSON data for {path}.") from exc


async def get_dex_json(path: str) -> Any:
    if http_client is None:
        raise RuntimeError("HTTP client has not started.")
    consume_request_budget("external_request")
    allowed, reason = usage_governor.authorize("external_request")
    if not allowed:
        raise RuntimeError(f"External request circuit is open ({reason}).")

    response = await http_client.get(f"{DEXSCREENER_BASE}{path}")
    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"DEX Screener returned non-JSON data for {path}.") from exc


def _window_value(container: Any, window: str, field: str | None = None) -> Any:
    if not isinstance(container, dict):
        return None
    value = container.get(window)
    if field is None:
        return value
    if not isinstance(value, dict):
        return None
    return value.get(field)


async def collect_spot_data() -> dict[str, Any]:
    errors: list[str] = []
    path = f"/latest/dex/pairs/{DEX_CHAIN_ID}/{DEX_PAIR_ADDRESS}"

    try:
        payload = await get_dex_json(path)
    except Exception as exc:
        return {
            "source": "DEX Screener pair API",
            "chainId": DEX_CHAIN_ID,
            "pairAddress": DEX_PAIR_ADDRESS,
            "generatedAt": utc_iso(),
            "available": False,
            "errors": [str(exc)],
        }

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    pair = pairs[0] if isinstance(pairs, list) and pairs and isinstance(pairs[0], dict) else None
    if pair is None:
        return {
            "source": "DEX Screener pair API",
            "chainId": DEX_CHAIN_ID,
            "pairAddress": DEX_PAIR_ADDRESS,
            "generatedAt": utc_iso(),
            "available": False,
            "errors": ["TAG/WBNB pair was not returned by DEX Screener."],
        }

    price_usd = as_float(pair.get("priceUsd"))
    api_market_cap = as_float(pair.get("marketCap"))

    # The collector cannot infer circulating supply. Preserve the provider's
    # labelled pair value; canonical forecasts derive their own market-cap and
    # portfolio values only from an immutable verified supply snapshot.
    market_cap = api_market_cap

    liquidity = pair.get("liquidity") if isinstance(pair.get("liquidity"), dict) else {}
    volume = pair.get("volume") if isinstance(pair.get("volume"), dict) else {}
    price_change = (
        pair.get("priceChange") if isinstance(pair.get("priceChange"), dict) else {}
    )
    txns = pair.get("txns") if isinstance(pair.get("txns"), dict) else {}

    def txn_window(window: str) -> dict[str, Any]:
        buys = as_int(_window_value(txns, window, "buys"))
        sells = as_int(_window_value(txns, window, "sells"))
        total = (buys or 0) + (sells or 0)
        return {
            "buys": buys,
            "sells": sells,
            "total": total if buys is not None or sells is not None else None,
            "buySharePct": ((buys or 0) / total * 100.0) if total else None,
            "buySellRatio": ((buys or 0) / sells) if sells else None,
        }

    base_token = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
    quote_token = pair.get("quoteToken") if isinstance(pair.get("quoteToken"), dict) else {}

    return {
        "source": "DEX Screener pair API",
        "sourceUrl": pair.get("url"),
        "generatedAt": utc_iso(),
        "available": True,
        "chainId": pair.get("chainId") or DEX_CHAIN_ID,
        "dexId": pair.get("dexId"),
        "pairAddress": pair.get("pairAddress") or DEX_PAIR_ADDRESS,
        "baseToken": {
            "address": base_token.get("address"),
            "name": base_token.get("name"),
            "symbol": base_token.get("symbol"),
        },
        "quoteToken": {
            "address": quote_token.get("address"),
            "name": quote_token.get("name"),
            "symbol": quote_token.get("symbol"),
        },
        "priceUsd": price_usd,
        "priceNative": as_float(pair.get("priceNative")),
        "marketCapUsd": market_cap,
        "marketCapSource": (
            "DEX Screener pair.marketCap (provider-labelled; supply not inferred)"
        ),
        "dexScreenerMarketCapUsd": api_market_cap,
        "fdvUsd": as_float(pair.get("fdv")),
        "liquidityUsd": as_float(liquidity.get("usd")),
        "volumeUsd": {
            "m5": as_float(volume.get("m5")),
            "h1": as_float(volume.get("h1")),
            "h6": as_float(volume.get("h6")),
            "h24": as_float(volume.get("h24")),
        },
        "priceChangePct": {
            "m5": as_float(price_change.get("m5")),
            "h1": as_float(price_change.get("h1")),
            "h6": as_float(price_change.get("h6")),
            "h24": as_float(price_change.get("h24")),
        },
        "transactions": {
            "m5": txn_window("m5"),
            "h1": txn_window("h1"),
            "h6": txn_window("h6"),
            "h24": txn_window("h24"),
        },
        "pairCreatedAt": as_int(pair.get("pairCreatedAt")),
        "errors": errors,
        "notes": [
            "DEX transaction buys/sells are transaction counts, not dollar buy/sell volume.",
            "Volume fields are total traded notional for each window.",
        ],
    }


async def cached_spot(
    force: bool = False,
    *,
    allow_repair_override: bool = False,
) -> dict[str, Any]:
    now = time.monotonic()
    cached = spot_cache.get("value")
    if REPAIR_MODE and not allow_repair_override:
        if cached is not None:
            usage_governor.cache(True)
            return cached
        raise HTTPException(
            status_code=423,
            detail="Repair mode is active; fresh spot collection is paused.",
        )
    if not force and cached is not None and now - spot_cache["time"] < CACHE_SECONDS:
        usage_governor.cache(True)
        return cached

    async with spot_cache_lock:
        now = time.monotonic()
        cached = spot_cache.get("value")
        if not force and cached is not None and now - spot_cache["time"] < CACHE_SECONDS:
            usage_governor.cache(True)
            return cached

        usage_governor.cache(False)
        value = await collect_spot_data()
        spot_cache["time"] = time.monotonic()
        spot_cache["value"] = value
        return value


async def collect_history_data(period: str, limit: int) -> dict[str, Any]:
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"period must be one of: {', '.join(sorted(VALID_PERIODS))}",
        )

    results = await asyncio.gather(
        get_json(
            "/futures/data/openInterestHist",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/topLongShortPositionRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/takerlongshortRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        return_exceptions=True,
    )

    names = [
        "openInterest",
        "globalLongShort",
        "topAccounts",
        "topPositions",
        "takerBuySell",
    ]

    payload: dict[str, Any] = {
        "symbol": SYMBOL,
        "period": period,
        "limit": limit,
        "generatedAt": utc_iso(),
        "errors": [],
    }

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            payload[name] = []
            payload["errors"].append(f"{name}: {result}")
        else:
            payload[name] = result

    return payload


def compact_history_for_chad(history: dict[str, Any]) -> dict[str, Any]:
    field_map: dict[str, tuple[str, ...]] = {
        "openInterest": ("timestamp", "sumOpenInterest", "sumOpenInterestValue"),
        "globalLongShort": ("timestamp", "longShortRatio", "longAccount", "shortAccount"),
        "topAccounts": ("timestamp", "longShortRatio", "longAccount", "shortAccount"),
        "topPositions": ("timestamp", "longShortRatio", "longAccount", "shortAccount"),
        "takerBuySell": ("timestamp", "buySellRatio", "buyVol", "sellVol"),
    }

    compact: dict[str, Any] = {
        "symbol": history.get("symbol"),
        "period": history.get("period"),
        "limit": history.get("limit"),
        "generatedAt": history.get("generatedAt"),
        "errors": history.get("errors", []),
    }

    for name, fields in field_map.items():
        rows = history.get(name)
        if not isinstance(rows, list):
            compact[name] = []
            continue
        compact[name] = [
            {field: row.get(field) for field in fields if field in row}
            for row in rows
            if isinstance(row, dict)
        ]

    return compact



def current_market_features(snapshot: dict[str, Any], spot: dict[str, Any]) -> dict[str, Any]:
    spot_changes = spot.get("priceChangePct") if isinstance(spot.get("priceChangePct"), dict) else {}
    spot_txns = spot.get("transactions") if isinstance(spot.get("transactions"), dict) else {}
    spot_h1 = spot_txns.get("h1") if isinstance(spot_txns.get("h1"), dict) else {}
    spot_volume = spot.get("volumeUsd") if isinstance(spot.get("volumeUsd"), dict) else {}
    return {
        "priceUsd": as_float(spot.get("priceUsd")) or as_float(snapshot.get("markPrice")),
        "marketCapUsd": as_float(spot.get("marketCapUsd")),
        "openInterestUsd": as_float(snapshot.get("openInterestUsd")),
        "oiChange1hPct": as_float(snapshot.get("oiChange1hPct")),
        "oiChange4hPct": as_float(snapshot.get("oiChange4hPct")),
        "fundingRate": as_float(snapshot.get("fundingRate")),
        "takerBuySellRatio": as_float(snapshot.get("takerBuySellRatio")),
        "globalLongShortRatio": as_float(snapshot.get("globalLongShortRatio")),
        "topPositionRatio": as_float(snapshot.get("topPositionRatio")),
        "basisBps": as_float(snapshot.get("basisBps")),
        "futuresPriceChange24hPct": as_float(snapshot.get("futuresPriceChange24hPct")),
        "futuresQuoteVolume24hUsd": as_float(snapshot.get("futuresQuoteVolume24hUsd")),
        "spotPriceChange1hPct": as_float(spot_changes.get("h1")),
        "spotPriceChange24hPct": as_float(spot_changes.get("h24")),
        "spotVolume1hUsd": as_float(spot_volume.get("h1")),
        "spotVolume24hUsd": as_float(spot_volume.get("h24")),
        "spotBuyShare1hPct": as_float(spot_h1.get("buySharePct")),
        "spotLiquidityUsd": as_float(spot.get("liquidityUsd")),
    }


async def historical_futures_price_near(
    due_ts: float,
) -> tuple[float | None, str | None, float | None, str | None, str | None]:
    return await historical_vision_price_near(
        due_ts,
        tolerance_seconds=FORECAST_GRADE_TOLERANCE_SECONDS,
        interval="5m",
    )


async def grade_due_ledger_predictions(limit: int = 50) -> dict[str, Any]:
    if not LEDGER_ENABLED or not CHAD_GRADING_ENABLED:
        return {
            "enabled": False,
            "graded": 0,
            "pendingDue": 0,
            "errors": [],
            "reason": "ledger_disabled" if not LEDGER_ENABLED else "grading_disabled",
        }

    async with ledger_lock:
        due = prediction_ledger.due_horizons(
            now_ts=time.time() - 5 * 60,
            limit=max(1, min(CHAD_GRADING_MAX_PER_BATCH, int(limit))),
        )
        if not due:
            return {
                "enabled": True,
                "graded": 0,
                "pendingDue": 0,
                "errors": [],
            }
        allowed, reason = usage_governor.authorize("chad_grade_batch")
        if not allowed:
            return {
                "enabled": True,
                "graded": 0,
                "pendingDue": len(due),
                "errors": [f"Grading batch circuit is open ({reason})."],
            }
        graded: list[dict[str, Any]] = []
        errors: list[str] = []
        price_cache: dict[
            int,
            tuple[float | None, str | None, float | None, str | None, str | None],
        ] = {}

        for item in due:
            due_bucket = int(float(item["due_ts"]) // 300 * 300)
            if due_bucket not in price_cache:
                price_cache[due_bucket] = await historical_futures_price_near(float(item["due_ts"]))
            actual_price, actual_at, _, actual_source, error = price_cache[due_bucket]
            if actual_price is None or actual_at is None:
                errors.append(
                    f"{item['prediction_id']} {item['horizon']}: {error or 'price unavailable'}"
                )
                continue
            try:
                graded.append(
                    prediction_ledger.grade_horizon(
                        prediction_id=str(item["prediction_id"]),
                        horizon=str(item["horizon"]),
                        actual_price_usd=actual_price,
                        actual_at=actual_at,
                        actual_source=actual_source or "Verified Binance futures history",
                        max_sample_offset_seconds=FORECAST_GRADE_TOLERANCE_SECONDS,
                    )
                )
            except Exception as exc:
                errors.append(f"{item['prediction_id']} {item['horizon']}: {exc}")

        return {
            "enabled": True,
            "graded": len(graded),
            "gradedItems": graded,
            "pendingDue": max(0, len(due) - len(graded)),
            "errors": errors,
        }


def extract_openai_text(payload: dict[str, Any]) -> str:
    output = payload.get("output")
    if not isinstance(output, list):
        raise RuntimeError("OpenAI response did not include an output array.")

    refusal_messages: list[str] = []
    text_parts: list[str] = []

    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                text_parts.append(part["text"])
            elif part.get("type") == "refusal" and isinstance(part.get("refusal"), str):
                refusal_messages.append(part["refusal"])

    if text_parts:
        return "".join(text_parts)
    if refusal_messages:
        raise HTTPException(status_code=422, detail="OpenAI refused the analysis request.")
    raise RuntimeError("OpenAI returned no text output.")


def openai_error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except Exception:
        return f"OpenAI request failed with HTTP {response.status_code}."

    error = body.get("error") if isinstance(body, dict) else None
    message = error.get("message") if isinstance(error, dict) else None
    if isinstance(message, str) and message.strip():
        return message.strip()[:500]
    return f"OpenAI request failed with HTTP {response.status_code}."


def _stable_evidence(value: Any) -> Any:
    volatile = {
        "generatedat",
        "relaygeneratedat",
        "updatedat",
        "lastmessageat",
        "servicetime",
        "windowstartms",
        "windowendms",
        "time",
        "timestamp",
    }
    if isinstance(value, dict):
        return {
            str(key): _stable_evidence(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key).lower() not in volatile
        }
    if isinstance(value, list):
        return [_stable_evidence(item) for item in value]
    if isinstance(value, float):
        return round(value, 10)
    return value


def _analysis_evidence_hash(
    *,
    question: str,
    snapshot: dict[str, Any],
    history: dict[str, Any],
    liquidation_data: dict[str, Any],
    spot: dict[str, Any],
    historical_memory: dict[str, Any] | None = None,
) -> str:
    material = {
        "question": " ".join(question.lower().split()),
        "snapshot": snapshot,
        "history": compact_history_for_chad(history),
        "liquidations": liquidation_data,
        "spot": spot,
        "historicalMemory": historical_memory or {},
    }
    normalized = json.dumps(
        _stable_evidence(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _cached_openai_analysis(
    evidence_hash: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    with session_scope() as session:
        row = session.get(OpenAIAnalysisCacheRow, evidence_hash)
        if row is None:
            return None
        payload = json.loads(row.response_json)
        analysis = payload.get("analysis")
        if not isinstance(analysis, dict):
            return None
        raw = {
            "id": row.request_id,
            "model": row.model,
            "usage": {
                "input_tokens": row.input_tokens,
                "output_tokens": row.output_tokens,
            },
            "cacheHit": True,
            "evidenceHash": evidence_hash,
        }
        return analysis, raw


def _store_openai_analysis(
    *,
    evidence_hash: str,
    trigger: str,
    analysis: dict[str, Any],
    raw: dict[str, Any],
) -> None:
    if not CHAD_CACHE_WRITES_ENABLED:
        raise RuntimeError("Durable Chad cache writes are disabled.")
    usage = raw.get("usage") if isinstance(raw.get("usage"), dict) else {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    input_rate = float(os.getenv("OPENAI_INPUT_USD_PER_1M", "0") or 0)
    output_rate = float(os.getenv("OPENAI_OUTPUT_USD_PER_1M", "0") or 0)
    estimated_cost = (
        input_tokens * input_rate / 1_000_000
        + output_tokens * output_rate / 1_000_000
    )
    with session_scope() as session:
        existing = session.get(OpenAIAnalysisCacheRow, evidence_hash)
        if existing is not None:
            return
        session.add(
            OpenAIAnalysisCacheRow(
                evidence_hash=evidence_hash,
                created_at=terminal_utc_now(),
                model=str(raw.get("model") or OPENAI_MODEL),
                trigger=trigger[:80],
                request_id=str(raw.get("id") or "") or None,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                estimated_cost_usd=estimated_cost if input_rate or output_rate else None,
                cache_hit_count=0,
                response_json=terminal_json_dumps({"analysis": analysis}),
            )
        )


async def request_chad_analysis(
    *,
    question: str,
    snapshot: dict[str, Any],
    history: dict[str, Any],
    liquidation_data: dict[str, Any],
    spot_data: dict[str, Any],
    position_tag: float | None,
    average_entry_usd: float | None,
    ledger_performance: dict[str, Any],
    calibration_profile: dict[str, Any],
    similar_setups: list[dict[str, Any]],
    freshness: dict[str, Any],
    historical_memory: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not PAID_AI_ENABLED:
        raise HTTPException(
            status_code=423,
            detail="Paid AI is disabled by the Phase 2 server gate.",
        )
    if openai_client is None:
        raise RuntimeError("OpenAI client has not started.")

    user_context: dict[str, Any] = {
        "question": question,
        "positionTag": position_tag,
        "averageEntryUsd": average_entry_usd,
        "importantUserLevelsUsd": [0.00075, 0.00080, 0.00088, 0.00100, 0.00105, 0.00110],
        "dataNotes": [
            "snapshot and history are Binance USD-M futures market data",
            "spot is the primary TAG/WBNB PancakeSwap pair from the DEX Screener pair API",
            "DEX buys/sells are transaction counts, not dollar buy/sell volume",
            "liquidation totals only cover events observed while the relay was running",
            "no news, catalyst, or whale data is supplied in this request",
        ],
        "snapshot": snapshot,
        "spot": spot_data,
        "history": compact_history_for_chad(history),
        "liquidations": liquidation_data,
        "freshness": freshness,
        "tagHistoricalMemory": historical_memory or {
            "historicalMemoryStatus": "unavailable",
            "failure": {"reason": "Canonical TAG historical memory was unavailable."},
        },
        "predictionLedger": {
            "performance": ledger_performance,
            "calibrationProfile": calibration_profile,
            "similarHistoricalSetups": similar_setups,
            "gradingMethod": (
                "Forecasts are graded against the Binance futures 5-minute close nearest each due time. "
                "Direction uses a configured deadband; target-range hits and midpoint error are tracked separately."
            ),
        },
    }

    request_body: dict[str, Any] = {
        "model": OPENAI_MODEL,
        "instructions": CHAD_INSTRUCTIONS,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(user_context, separators=(",", ":"), ensure_ascii=False),
                    }
                ],
            }
        ],
        "reasoning": {"effort": OPENAI_REASONING_EFFORT},
        "text": {
            "verbosity": "medium",
            "format": {
                "type": "json_schema",
                "name": "chad_tag_market_analysis",
                "description": "Structured leverage-first TAG market analysis for the TAG Terminal app.",
                "strict": True,
                "schema": CHAD_RESPONSE_SCHEMA,
            },
        },
        "max_output_tokens": OPENAI_MAX_OUTPUT_TOKENS,
        "store": False,
    }

    consume_request_budget("openai_call")
    chad_reactivation_gate.record_openai_attempt(retry=False)
    try:
        response = await openai_client.post(
            f"{OPENAI_API_BASE}/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=request_body,
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail="OpenAI timed out. The request was not retried.",
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"OpenAI request failed without retry ({type(exc).__name__}).",
        ) from exc

    if response.status_code >= 400:
        detail = openai_error_detail(response)
        if response.status_code == 401:
            detail = "OpenAI rejected the API key. Confirm OPENAI_API_KEY in Render."
        elif response.status_code == 429:
            detail = (
                "OpenAI rate limit or billing limit reached. Check API billing and project limits. "
                f"Details: {detail}"
            )
        raise HTTPException(status_code=502, detail=detail)

    try:
        raw = response.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="OpenAI returned non-JSON data.") from exc

    incomplete = raw.get("incomplete_details") if isinstance(raw, dict) else None
    if incomplete:
        raise HTTPException(
            status_code=502,
            detail=f"OpenAI returned an incomplete response: {incomplete}",
        )

    text = extract_openai_text(raw)
    try:
        analysis = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=502,
            detail="OpenAI returned text that was not valid structured JSON.",
        ) from exc

    chad_reactivation_gate.record_openai_usage(raw)
    return analysis, raw


def book_metrics(depth: dict[str, Any] | None, mark_price: float | None) -> dict[str, float | None]:
    depth = depth or {}
    bids_raw = depth.get("bids") or depth.get("b") or []
    asks_raw = depth.get("asks") or depth.get("a") or []

    bids = [
        (as_float(row[0]), as_float(row[1]))
        for row in bids_raw
        if isinstance(row, list) and len(row) >= 2
    ]
    asks = [
        (as_float(row[0]), as_float(row[1]))
        for row in asks_raw
        if isinstance(row, list) and len(row) >= 2
    ]
    bids = [(p, q) for p, q in bids if p is not None and q is not None]
    asks = [(p, q) for p, q in asks if p is not None and q is not None]

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    mid = (
        (best_bid + best_ask) / 2.0
        if best_bid is not None and best_ask is not None
        else mark_price
    )

    def notional(rows: list[tuple[float, float]]) -> float:
        return sum(price * quantity for price, quantity in rows)

    def within(rows: list[tuple[float, float]], percent: float, side: str) -> float | None:
        if mid is None:
            return None
        if side == "bid":
            threshold = mid * (1.0 - percent / 100.0)
            selected = [(p, q) for p, q in rows if p >= threshold]
        else:
            threshold = mid * (1.0 + percent / 100.0)
            selected = [(p, q) for p, q in rows if p <= threshold]
        return notional(selected)

    bid_total = notional(bids)
    ask_total = notional(asks)
    denominator = bid_total + ask_total
    imbalance = ((bid_total - ask_total) / denominator * 100.0) if denominator else None

    spread_bps = None
    if mid and best_bid is not None and best_ask is not None:
        spread_bps = ((best_ask - best_bid) / mid) * 10_000.0

    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadBps": spread_bps,
        "bidDepthUsdTop20": bid_total,
        "askDepthUsdTop20": ask_total,
        # Keep the original field names so the Android replacement still works.
        "bidDepthUsdTop100": bid_total,
        "askDepthUsdTop100": ask_total,
        "orderBookImbalancePct": imbalance,
        "bidDepthUsdWithin0_5Pct": within(bids, 0.5, "bid"),
        "askDepthUsdWithin0_5Pct": within(asks, 0.5, "ask"),
        "bidDepthUsdWithin1Pct": within(bids, 1.0, "bid"),
        "askDepthUsdWithin1Pct": within(asks, 1.0, "ask"),
    }


async def liquidation_summary() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    one_hour_ago = now_ms - 60 * 60 * 1000
    twenty_four_hours_ago = now_ms - 24 * 60 * 60 * 1000

    async with liquidation_lock:
        while liquidation_events and liquidation_events[0]["time"] < twenty_four_hours_ago:
            liquidation_events.popleft()
        events = list(liquidation_events)

    def total(side: str, since_ms: int) -> float:
        return sum(
            event["notionalUsd"]
            for event in events
            if event["liquidationSide"] == side and event["time"] >= since_ms
        )

    async with stream_lock:
        connected = bool(stream_state["connected"])
        endpoint = stream_state["endpoint"]
        last_message_at = stream_state["lastMessageAt"]
        last_error = stream_state["lastError"]

    return {
        "trackerConnected": connected,
        "trackerEndpoint": endpoint,
        "trackerStartedAt": utc_iso(service_started_ms),
        "lastMessageAt": last_message_at,
        "lastError": last_error,
        "eventsTracked24h": len(events),
        "longLiquidation1hUsd": total("LONG", one_hour_ago),
        "shortLiquidation1hUsd": total("SHORT", one_hour_ago),
        "longLiquidationTracked24hUsd": total("LONG", twenty_four_hours_ago),
        "shortLiquidationTracked24hUsd": total("SHORT", twenty_four_hours_ago),
        "note": (
            "Liquidation totals include only events observed while this relay was running. "
            "The Binance stream provides liquidation snapshots, not a complete historical ledger."
        ),
    }


async def collect_snapshot(*, include_rest_state: bool = False) -> dict[str, Any]:
    errors: list[str] = []

    # These /futures/data endpoints are working from the current Render relay.
    calls = {
        "oi_hist": get_json(
            "/futures/data/openInterestHist",
            {"symbol": SYMBOL, "period": "5m", "limit": 60},
        ),
        "global_ratio": get_json(
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "top_account": get_json(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "top_position": get_json(
            "/futures/data/topLongShortPositionRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "taker": get_json(
            "/futures/data/takerlongshortRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "taker_1h": get_json(
            "/futures/data/takerlongshortRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 12},
        ),
    }
    if include_rest_state:
        # Repair mode keeps WebSockets off. An explicit manual refresh therefore
        # obtains the missing point-in-time mark, ticker and book through three
        # bounded public REST reads. Nothing here writes to the database or
        # starts a background loop.
        calls.update(
            {
                "premium": get_json("/fapi/v1/premiumIndex", {"symbol": SYMBOL}),
                "ticker": get_json("/fapi/v1/ticker/24hr", {"symbol": SYMBOL}),
                "depth": get_json(
                    "/fapi/v1/depth",
                    {"symbol": SYMBOL, "limit": 20},
                ),
            }
        )

    names = list(calls.keys())
    results = await asyncio.gather(*calls.values(), return_exceptions=True)
    data: dict[str, Any] = {}

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            errors.append(f"{name}: {result}")
            data[name] = None
        else:
            data[name] = result

    async with stream_lock:
        live = dict(stream_state)

    premium = data.get("premium") if isinstance(data.get("premium"), dict) else {}
    ticker = data.get("ticker") if isinstance(data.get("ticker"), dict) else {}
    depth = data.get("depth") if isinstance(data.get("depth"), dict) else {}

    def preferred_float(primary: Any, fallback: Any) -> float | None:
        value = as_float(primary)
        return value if value is not None else as_float(fallback)

    def preferred_int(primary: Any, fallback: Any) -> int | None:
        value = as_int(primary)
        return value if value is not None else as_int(fallback)

    mark_price = preferred_float(live.get("markPrice"), premium.get("markPrice"))
    index_price = preferred_float(live.get("indexPrice"), premium.get("indexPrice"))
    funding_rate = as_float(live.get("fundingRate"))
    if funding_rate is None:
        funding_rate = as_float(premium.get("lastFundingRate"))
    next_funding_time = preferred_int(
        live.get("nextFundingTime"),
        premium.get("nextFundingTime")
    )

    oi_history = data.get("oi_hist") if isinstance(data.get("oi_hist"), list) else []
    oi_history = [row for row in oi_history if isinstance(row, dict)]

    latest_oi_row = oi_history[-1] if oi_history else {}
    open_interest_contracts = as_float(latest_oi_row.get("sumOpenInterest"))
    open_interest_usd = as_float(latest_oi_row.get("sumOpenInterestValue"))

    oi_values = [as_float(row.get("sumOpenInterestValue")) for row in oi_history]
    oi_values = [value for value in oi_values if value is not None]
    latest_oi_hist = oi_values[-1] if oi_values else None

    def historical_change(bars_back: int) -> float | None:
        if len(oi_values) <= bars_back:
            return None
        return pct_change(oi_values[-1], oi_values[-1 - bars_back])

    global_ratio = latest_item(data.get("global_ratio")) or {}
    top_account = latest_item(data.get("top_account")) or {}
    top_position = latest_item(data.get("top_position")) or {}
    taker = latest_item(data.get("taker")) or {}
    rolling_taker = await rolling_taker_1h()
    taker_history = data.get("taker_1h") if isinstance(data.get("taker_1h"), list) else []
    fallback_buy_contracts = sum(as_float(row.get("buyVol")) or 0.0 for row in taker_history if isinstance(row, dict))
    fallback_sell_contracts = sum(as_float(row.get("sellVol")) or 0.0 for row in taker_history if isinstance(row, dict))
    fallback_buy_usd = fallback_buy_contracts * mark_price if mark_price is not None and fallback_buy_contracts > 0 else None
    fallback_sell_usd = fallback_sell_contracts * mark_price if mark_price is not None and fallback_sell_contracts > 0 else None
    fallback_ratio = (fallback_buy_contracts / fallback_sell_contracts) if fallback_sell_contracts > 0 else None
    use_exact_taker = rolling_taker.get("quality") == "live-exact"
    taker_buy_usd_1h = rolling_taker.get("buyUsd") if use_exact_taker else fallback_buy_usd
    taker_sell_usd_1h = rolling_taker.get("sellUsd") if use_exact_taker else fallback_sell_usd
    taker_ratio_1h = rolling_taker.get("ratio") if use_exact_taker else fallback_ratio
    taker_quality = "live-exact" if use_exact_taker else ("binance-5m-history" if fallback_ratio is not None else "warming-up")

    basis_bps = None
    if mark_price is not None and index_price not in (None, 0):
        basis_bps = ((mark_price - index_price) / index_price) * 10_000.0

    raw_depth = live.get("depth") if isinstance(live.get("depth"), dict) else depth
    order_book = book_metrics(raw_depth, mark_price)
    def normalized_levels(rows: Any) -> list[list[float]]:
        output: list[list[float]] = []
        if not isinstance(rows, list):
            return output
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                continue
            price = as_float(row[0])
            quantity = as_float(row[1])
            if price is not None and quantity is not None:
                output.append([price, quantity, price * quantity])
        return output
    depth_levels = {
        "bids": normalized_levels(raw_depth.get("bids") or raw_depth.get("b") or []),
        "asks": normalized_levels(raw_depth.get("asks") or raw_depth.get("a") or []),
    }
    liquidations = await liquidation_summary()

    if not live.get("connected") and not include_rest_state:
        errors.append("Binance market WebSocket is reconnecting.")
    if mark_price is None:
        errors.append("Mark-price stream has not produced a value yet.")
    if not raw_depth:
        detail = live.get("depthLastError")
        errors.append(
            "Depth stream has not produced a value yet."
            + (f" Last error: {detail}" if detail else "")
        )

    return {
        "symbol": SYMBOL,
        "source": "Binance USDⓈ-M Futures public market data",
        "relayGeneratedAt": utc_iso(),
        "binanceEventTime": (
            as_int(live.get("markEventTime"))
            or as_int(live.get("tickerEventTime"))
            or as_int(latest_oi_row.get("timestamp"))
        ),
        "marketStreamConnected": bool(live.get("connected")),
        "marketStreamLastMessageAt": live.get("lastMessageAt"),
        "depthStreamConnected": bool(live.get("depthConnected")),
        "depthStreamEndpoint": live.get("depthEndpoint"),
        "depthStreamLastMessageAt": live.get("depthLastMessageAt"),
        "depthStreamMode": live.get("depthMode"),
        "markPrice": mark_price,
        "indexPrice": index_price,
        "basisBps": basis_bps,
        "fundingRate": funding_rate,
        "nextFundingTime": next_funding_time,
        "openInterestContracts": open_interest_contracts,
        "openInterestUsd": open_interest_usd,
        "openInterestHistoryLatestUsd": latest_oi_hist,
        "oiChange5mPct": historical_change(1),
        "oiChange15mPct": historical_change(3),
        "oiChange1hPct": historical_change(12),
        "oiChange4hPct": historical_change(48),
        "globalLongShortRatio": as_float(global_ratio.get("longShortRatio")),
        "globalLongAccountPct": (
            as_float(global_ratio.get("longAccount")) * 100.0
            if as_float(global_ratio.get("longAccount")) is not None
            else None
        ),
        "globalShortAccountPct": (
            as_float(global_ratio.get("shortAccount")) * 100.0
            if as_float(global_ratio.get("shortAccount")) is not None
            else None
        ),
        "topAccountRatio": as_float(top_account.get("longShortRatio")),
        "topAccountLongPct": (
            as_float(top_account.get("longAccount")) * 100.0
            if as_float(top_account.get("longAccount")) is not None
            else None
        ),
        "topAccountShortPct": (
            as_float(top_account.get("shortAccount")) * 100.0
            if as_float(top_account.get("shortAccount")) is not None
            else None
        ),
        "topPositionRatio": as_float(top_position.get("longShortRatio")),
        "topPositionLongPct": (
            as_float(top_position.get("longAccount")) * 100.0
            if as_float(top_position.get("longAccount")) is not None
            else None
        ),
        "topPositionShortPct": (
            as_float(top_position.get("shortAccount")) * 100.0
            if as_float(top_position.get("shortAccount")) is not None
            else None
        ),
        "takerBuySellRatio": taker_ratio_1h,
        "takerBuySellRatio1h": taker_ratio_1h,
        "takerBuyVolumeUsd1h": taker_buy_usd_1h,
        "takerSellVolumeUsd1h": taker_sell_usd_1h,
        "takerWindowCoverageSeconds": rolling_taker.get("coverageSeconds"),
        "takerWindowStartTime": utc_iso(rolling_taker.get("windowStartMs")) if rolling_taker.get("windowStartMs") else None,
        "takerWindowEndTime": utc_iso(rolling_taker.get("windowEndMs")) if rolling_taker.get("windowEndMs") else None,
        "takerWindowQuality": taker_quality,
        "takerBuyVolumeContracts5m": as_float(taker.get("buyVol")),
        "takerSellVolumeContracts5m": as_float(taker.get("sellVol")),
        "depthLevels": depth_levels,
        "futuresPriceChange24hPct": (
            preferred_float(
                live.get("priceChange24hPct"),
                ticker.get("priceChangePercent"),
            )
        ),
        "futuresVolume24hContracts": (
            preferred_float(
                live.get("volume24hContracts"),
                ticker.get("volume"),
            )
        ),
        "futuresQuoteVolume24hUsd": (
            preferred_float(
                live.get("quoteVolume24hUsd"),
                ticker.get("quoteVolume"),
            )
        ),
        "futuresTradeCount24h": (
            preferred_int(
                live.get("tradeCount24h"),
                ticker.get("count"),
            )
        ),
        "sourceStatus": "manual-live" if include_rest_state else None,
        "manualPointInTime": include_rest_state,
        **order_book,
        **liquidations,
        "errors": errors,
    }


async def cached_snapshot(
    force: bool = False,
    *,
    allow_repair_override: bool = False,
) -> dict[str, Any]:
    now = time.monotonic()
    cached = snapshot_cache.get("value")
    if REPAIR_MODE and not allow_repair_override:
        if cached is not None:
            usage_governor.cache(True)
            return cached
        raise HTTPException(
            status_code=423,
            detail="Repair mode is active; fresh futures collection is paused.",
        )
    if not force and cached is not None and now - snapshot_cache["time"] < CACHE_SECONDS:
        usage_governor.cache(True)
        return cached

    async with cache_lock:
        now = time.monotonic()
        cached = snapshot_cache.get("value")
        if not force and cached is not None and now - snapshot_cache["time"] < CACHE_SECONDS:
            usage_governor.cache(True)
            return cached

        usage_governor.cache(False)
        value = await collect_snapshot(include_rest_state=allow_repair_override)
        snapshot_cache["time"] = time.monotonic()
        snapshot_cache["value"] = value
        return value


async def record_agg_trade(payload: dict[str, Any], event_time: int) -> None:
    price = as_float(payload.get("p"))
    quantity = as_float(payload.get("q"))
    if price is None or quantity is None:
        return
    # buyer-is-maker means the seller crossed the spread: taker sell.
    is_taker_buy = not bool(payload.get("m"))
    notional = price * quantity
    cutoff = int(time.time() * 1000) - 60 * 60 * 1000
    async with agg_trade_lock:
        if event_time >= cutoff:
            recent_agg_trades.append((event_time, is_taker_buy, notional))
        while recent_agg_trades and recent_agg_trades[0][0] < cutoff:
            recent_agg_trades.popleft()


async def rolling_taker_1h() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - 60 * 60 * 1000
    async with agg_trade_lock:
        while recent_agg_trades and recent_agg_trades[0][0] < cutoff:
            recent_agg_trades.popleft()
        rows = list(recent_agg_trades)
        started = agg_trade_window_started_ms

    buy = sum(notional for event_time, is_buy, notional in rows if event_time >= cutoff and is_buy)
    sell = sum(notional for event_time, is_buy, notional in rows if event_time >= cutoff and not is_buy)
    coverage_seconds = max(0, int((now_ms - started) / 1000)) if started else 0
    exact = coverage_seconds >= 3600
    return {
        "buyUsd": buy if buy > 0 else None,
        "sellUsd": sell if sell > 0 else None,
        "ratio": (buy / sell) if sell > 0 else None,
        "tradeCount": len(rows),
        "coverageSeconds": min(coverage_seconds, 3600),
        "windowStartMs": cutoff,
        "windowEndMs": now_ms,
        "quality": "live-exact" if exact else "warming-up",
    }


async def record_liquidation(payload: dict[str, Any]) -> None:
    order = payload.get("o")
    if not isinstance(order, dict):
        return
    if str(order.get("s", "")).upper() != SYMBOL:
        return

    side = str(order.get("S", "")).upper()
    liquidation_side = "LONG" if side == "SELL" else "SHORT"

    price = as_float(order.get("ap")) or as_float(order.get("p"))
    quantity = (
        as_float(order.get("z"))
        or as_float(order.get("l"))
        or as_float(order.get("q"))
    )
    event_time = (
        as_int(order.get("T"))
        or as_int(payload.get("E"))
        or int(time.time() * 1000)
    )

    if price is None or quantity is None:
        return

    event = {
        "time": event_time,
        "timeIso": utc_iso(event_time),
        "liquidationSide": liquidation_side,
        "orderSide": side,
        "price": price,
        "quantity": quantity,
        "notionalUsd": price * quantity,
    }

    async with liquidation_lock:
        liquidation_events.append(event)
    terminal_addon.persist_liquidation(event)


async def market_stream_listener() -> None:
    global agg_trade_window_started_ms
    """
    Regular market data belongs on Binance's /market route.
    Order-book depth is intentionally handled by depth_stream_listener().
    """
    symbol = SYMBOL.lower()
    streams = "/".join(
        [
            f"{symbol}@forceOrder",
            f"{symbol}@markPrice@1s",
            f"{symbol}@ticker",
            f"{symbol}@aggTrade",
        ]
    )
    retry_seconds = 2

    while True:
        connected_this_round = False

        for base in WS_BASES:
            endpoint = f"{base}/market/stream?streams={streams}"

            async with stream_lock:
                stream_state["endpoint"] = endpoint

            try:
                async with websockets.connect(
                    endpoint,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=4_000_000,
                ) as websocket:
                    connected_this_round = True
                    retry_seconds = 2
                    async with agg_trade_lock:
                        recent_agg_trades.clear()
                        agg_trade_window_started_ms = int(time.time() * 1000)

                    async with stream_lock:
                        stream_state["connected"] = True
                        stream_state["lastError"] = None

                    async for raw_message in websocket:
                        wrapper = json.loads(raw_message)
                        payload = (
                            wrapper.get("data", wrapper)
                            if isinstance(wrapper, dict)
                            else {}
                        )
                        if not isinstance(payload, dict):
                            continue

                        event_type = str(payload.get("e", ""))
                        event_time = (
                            as_int(payload.get("E"))
                            or int(time.time() * 1000)
                        )

                        async with stream_lock:
                            stream_state["lastMessageAt"] = utc_iso(event_time)

                        if event_type == "forceOrder":
                            await record_liquidation(payload)

                        elif event_type == "markPriceUpdate":
                            async with stream_lock:
                                stream_state["markPrice"] = as_float(payload.get("p"))
                                stream_state["indexPrice"] = as_float(payload.get("i"))
                                stream_state["fundingRate"] = as_float(payload.get("r"))
                                stream_state["nextFundingTime"] = as_int(payload.get("T"))
                                stream_state["markEventTime"] = event_time

                        elif event_type == "24hrTicker":
                            async with stream_lock:
                                stream_state["priceChange24hPct"] = as_float(payload.get("P"))
                                stream_state["volume24hContracts"] = as_float(payload.get("v"))
                                stream_state["quoteVolume24hUsd"] = as_float(payload.get("q"))
                                stream_state["tradeCount24h"] = as_int(payload.get("n"))
                                stream_state["tickerEventTime"] = event_time

                        elif event_type == "aggTrade":
                            await record_agg_trade(payload, event_time)

            except asyncio.CancelledError:
                async with stream_lock:
                    stream_state["connected"] = False
                raise
            except Exception as exc:
                async with stream_lock:
                    stream_state["connected"] = False
                    stream_state["lastError"] = f"{type(exc).__name__}: {exc}"

        if not connected_this_round:
            async with stream_lock:
                stream_state["connected"] = False

        await asyncio.sleep(retry_seconds)
        retry_seconds = min(retry_seconds * 2, 60)


async def depth_stream_listener() -> None:
    """
    High-frequency order-book data belongs on Binance's /public route.

    Try partial-depth snapshots first. If a particular depth level does not emit
    for TAGUSDT, fall back to the individual bookTicker stream so the relay still
    returns best bid, best ask, spread and top-level imbalance.
    """
    symbol = SYMBOL.lower()
    candidates = [
        (f"{symbol}@depth20@100ms", "partial-depth-20"),
        (f"{symbol}@depth10@100ms", "partial-depth-10"),
        (f"{symbol}@depth5@100ms", "partial-depth-5"),
        (f"{symbol}@bookTicker", "book-ticker-fallback"),
    ]
    retry_seconds = 2

    while True:
        got_any_message = False

        for base in WS_BASES:
            for stream_name, mode in candidates:
                endpoint = f"{base}/public/ws/{stream_name}"

                async with stream_lock:
                    stream_state["depthEndpoint"] = endpoint
                    stream_state["depthMode"] = mode
                    stream_state["depthConnected"] = False

                try:
                    async with websockets.connect(
                        endpoint,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=10,
                        max_size=4_000_000,
                    ) as websocket:
                        async with stream_lock:
                            stream_state["depthConnected"] = True
                            stream_state["depthLastError"] = None

                        while True:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=20,
                            )
                            payload = json.loads(raw_message)
                            if not isinstance(payload, dict):
                                continue

                            event_type = str(payload.get("e", ""))
                            event_time = (
                                as_int(payload.get("E"))
                                or int(time.time() * 1000)
                            )

                            if event_type == "depthUpdate":
                                bids = payload.get("b") or []
                                asks = payload.get("a") or []
                            elif event_type == "bookTicker":
                                bid_price = payload.get("b")
                                bid_qty = payload.get("B")
                                ask_price = payload.get("a")
                                ask_qty = payload.get("A")
                                bids = (
                                    [[bid_price, bid_qty]]
                                    if bid_price is not None and bid_qty is not None
                                    else []
                                )
                                asks = (
                                    [[ask_price, ask_qty]]
                                    if ask_price is not None and ask_qty is not None
                                    else []
                                )
                            else:
                                continue

                            async with stream_lock:
                                stream_state["depth"] = {
                                    "bids": bids,
                                    "asks": asks,
                                }
                                stream_state["depthEventTime"] = event_time
                                stream_state["depthLastMessageAt"] = utc_iso(event_time)
                                stream_state["depthConnected"] = True
                                stream_state["depthMode"] = mode
                                stream_state["depthLastError"] = None

                            got_any_message = True

                except asyncio.CancelledError:
                    async with stream_lock:
                        stream_state["depthConnected"] = False
                    raise
                except Exception as exc:
                    async with stream_lock:
                        stream_state["depthConnected"] = False
                        stream_state["depthLastError"] = (
                            f"{mode}: {type(exc).__name__}: {exc}"
                        )

                    # When a stream had already produced data, retry it on reconnect.
                    if got_any_message:
                        break

            if got_any_message:
                break

        await asyncio.sleep(retry_seconds)
        retry_seconds = min(retry_seconds * 2, 30)


async def ledger_grader_loop() -> None:
    grader_state["running"] = True
    try:
        await asyncio.sleep(30)
        while True:
            try:
                result = await grade_due_ledger_predictions(limit=200)
                grader_state["lastRunAt"] = utc_iso()
                grader_state["lastResult"] = result
                grader_state["lastError"] = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                grader_state["lastRunAt"] = utc_iso()
                grader_state["lastError"] = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(LEDGER_AUTO_GRADE_SECONDS)
    finally:
        grader_state["running"] = False


async def collect_canonical_evidence_once() -> dict[str, Any]:
    """Collect and persist one coalesced server-authoritative evidence packet."""

    bucket = int(time.time()) // max(60, COLLECT_SECONDS)

    async def operation() -> dict[str, Any]:
        async def collect() -> dict[str, Any]:
            collected = await terminal_addon.collect_once(cached_snapshot, cached_spot)
            market = collected.get("market") if isinstance(collected, dict) else None
            if not isinstance(market, dict):
                raise RuntimeError("Collector returned no market packet.")
            # Independent CEX spot is additive evidence.  It is never used as
            # a fallback price for DEX truth and failures remain visible.
            market = dict(market)
            market["cexSpot"] = await collect_verified_cex_spot_once()
            packet = build_canonical_evidence_packet(market)
            storage = await asyncio.to_thread(persist_evidence_packet, packet)
            await asyncio.to_thread(
                phase1_cache_put,
                cache_key=f"canonical-evidence:{packet['evidenceHash']}",
                category="canonical_evidence",
                payload=packet,
                ttl_seconds=max(60, COLLECT_SECONDS),
                evidence_hash=packet["evidenceHash"],
                origin="server",
            )
            result = {
                "packet": packet,
                "storage": storage,
                "sourceErrors": collected.get("sourceErrors", []),
                "maintenanceErrors": collected.get("maintenanceErrors", []),
            }
            phase1_state["lastPacket"] = packet
            phase1_state["lastEvidenceHash"] = packet["evidenceHash"]
            return result

        return await bounded_retry(collect, attempts=2)

    result, coalesced = await phase1_coalescer.run(
        f"canonical-evidence-bucket:{bucket}",
        operation,
        ttl_seconds=min(60, max(15, COLLECT_SECONDS // 5)),
    )
    if coalesced:
        phase1_state["coalescedRequests"] = int(
            phase1_state.get("coalescedRequests") or 0
        ) + 1
    return {**result, "coalesced": coalesced}


async def collect_verified_cex_spot_once() -> list[dict[str, Any]]:
    """Fetch the two confirmed TAG/USDT spot tickers with bounded public I/O."""
    if http_client is None:
        return [{"exchange": "Gate", "available": False, "failureReason": "HTTP client unavailable"}, {"exchange": "MEXC", "available": False, "failureReason": "HTTP client unavailable"}]

    async def gate() -> dict[str, Any]:
        try:
            response = await http_client.get("https://api.gateio.ws/api/v4/spot/tickers", params={"currency_pair": "TAG_USDT"})
            response.raise_for_status()
            rows = response.json()
            row = rows[0] if isinstance(rows, list) and rows else {}
            observed_at = terminal_utc_now().isoformat()
            return {"exchange": "Gate", "available": True, "priceUsd": as_float(row.get("last")), "volumeUsd24h": as_float(row.get("quote_volume")), "priceChange24hPct": as_float(row.get("change_percentage")), "observedAt": observed_at, "updatedAt": observed_at, "marketType": "spot"}
        except Exception as exc:
            return {"exchange": "Gate", "available": False, "failureReason": f"{type(exc).__name__}: {exc}"}

    async def mexc() -> dict[str, Any]:
        try:
            response = await http_client.get("https://api.mexc.com/api/v3/ticker/24hr", params={"symbol": "TAGUSDT"})
            response.raise_for_status()
            row = response.json() if isinstance(response.json(), dict) else {}
            observed_at = terminal_utc_now().isoformat()
            return {"exchange": "MEXC", "available": True, "priceUsd": as_float(row.get("lastPrice")), "volumeUsd24h": as_float(row.get("quoteVolume")), "priceChange24hPct": as_float(row.get("priceChangePercent")), "observedAt": observed_at, "updatedAt": observed_at, "marketType": "spot"}
        except Exception as exc:
            return {"exchange": "MEXC", "available": False, "failureReason": f"{type(exc).__name__}: {exc}"}

    return list(await asyncio.gather(gate(), mexc()))


async def collect_verified_tag_supply_once() -> dict[str, Any]:
    """Persist current TAG supply only after independent public-source checks."""

    if http_client is None:
        raise RuntimeError("HTTP client is unavailable")

    async def collect() -> dict[str, Any]:
        coin_gecko_response, coin_market_cap_response, bsc_response, gecko_response = await asyncio.gather(
            http_client.get(
                "https://api.coingecko.com/api/v3/coins/tagger",
                params={
                    "localization": "false",
                    "tickers": "false",
                    "market_data": "true",
                    "community_data": "false",
                    "developer_data": "false",
                    "sparkline": "false",
                },
            ),
            http_client.get("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail?slug=tagger"),
            http_client.post(
                "https://bsc-dataseed.binance.org/",
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_call",
                    "params": [
                        {"to": "0x208bf3e7da9639f1eaefa2de78c23396b0682025", "data": "0x18160ddd"},
                        "latest",
                    ],
                },
            ),
            http_client.get(
                f"https://api.geckoterminal.com/api/v2/networks/{DEX_CHAIN_ID}/pools/{DEX_PAIR_ADDRESS}",
                headers={"Accept": "application/json;version=20230302"},
            ),
        )
        for response in (coin_market_cap_response, bsc_response, gecko_response):
            response.raise_for_status()
        cmc_document = coin_market_cap_response.json()
        cmc_payload = cmc_document.get("data") if isinstance(cmc_document, dict) else {}
        bsc_total = (bsc_response.json() or {}).get("result")
        retrieved_at = terminal_utc_now()
        if coin_gecko_response.is_success:
            payload = verified_tag_supply_payload(
                coingecko=coin_gecko_response.json(),
                coinmarketcap=cmc_payload,
                bsc_total_supply_hex=bsc_total,
                retrieved_at=retrieved_at,
            )
        else:
            payload = verified_tag_supply_payload_from_cmc_and_gecko(
                coinmarketcap=cmc_payload,
                geckoterminal=gecko_response.json(),
                bsc_total_supply_hex=bsc_total,
                retrieved_at=retrieved_at,
                unavailable_sources=(f"CoinGecko HTTP {coin_gecko_response.status_code}",),
            )
        stored = await asyncio.to_thread(persist_asset_truth_snapshot, payload)
        return {
            **stored,
            "source": "cross-checked-public-supply-truth",
            "automaticPaidAiCalls": 0,
        }

    try:
        return await bounded_retry(collect, attempts=2)
    except SupplyTruthError as exc:
        # A failed source is visible and leaves existing verified truth intact.
        return {"stored": False, "verified": False, "reason": str(exc), "automaticPaidAiCalls": 0}


async def evaluate_event_driven_chad() -> dict[str, Any]:
    """Evaluate deterministic major-event gates; never call Chad for ordinary noise."""

    packet = await asyncio.to_thread(latest_evidence_packet)
    if not isinstance(packet, dict):
        return {"evaluated": False, "automaticCall": False, "reason": "no canonical evidence"}
    alerts = await asyncio.to_thread(phase3_active_alerts, limit=20)
    major = next(
        (
            row
            for row in alerts
            if row.get("stage") in {"CONFIRMED", "URGENT ACTION"}
            and float(row.get("signalScore") or 0.0) >= 75.0
        ),
        None,
    )
    items = packet.get("items") if isinstance(packet.get("items"), list) else []
    futures = [
        row for row in items
        if isinstance(row, dict)
        and row.get("category") == "futures"
        and row.get("validationStatus") not in {"invalid", "unavailable"}
    ]
    spot = next(
        (
            row for row in items
            if isinstance(row, dict)
            and row.get("category") == "dex_spot"
            and row.get("validationStatus") not in {"invalid", "unavailable"}
        ),
        None,
    )
    compact_features: dict[str, Any] = {}
    for row in futures:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        for source, target in (
            ("oiChange1hPct", "openInterestChange"),
            ("fundingRate", "funding"),
            ("takerBuySellRatio", "takerImbalance"),
        ):
            if source in payload and target not in compact_features:
                compact_features[target] = payload[source]
    if isinstance(spot, dict) and isinstance(spot.get("payload"), dict):
        spot_payload = spot["payload"]
        compact_features["spotConfirmation"] = (spot_payload.get("priceChangePct") or {}).get("h1") if isinstance(spot_payload.get("priceChangePct"), dict) else None
        compact_features["liquidity"] = spot_payload.get("liquidityUsd")
    historical = await asyncio.to_thread(
        chad_history_evidence_package,
        compact_features,
        data_as_of=packet.get("dataAsOf") or packet.get("serverCreatedAt"),
        evidence_snapshot_id=packet["snapshotId"],
    )
    top_analog = next(iter(historical.get("rankedTagHistoricalAnalogs") or []), None)
    exceptional_analog = isinstance(top_analog, dict) and float(top_analog.get("similarityScore") or 0.0) >= 85.0
    if major is None and not exceptional_analog:
        return {
            "evaluated": True,
            "automaticCall": False,
            "reason": "ordinary market conditions; no defined major event threshold",
            "routineDailyCall": False,
        }

    reason_text = f"{major.get('alertType')} {major.get('reason')}".upper() if major else ""
    family = "EXCEPTIONAL_HISTORICAL_ANALOG" if exceptional_analog and major is None else "HISTORICALLY_UNUSUAL_SETUP"
    for needle, candidate in (
        ("ATH", "ATH_BREAK"),
        ("PANIC", "PANIC_CAPITULATION"),
        ("LIQUIDATION", "LIQUIDATION_CASCADE"),
        ("SHORT SQUEEZE", "SHORT_SQUEEZE"),
        ("LONG SQUEEZE", "LONG_SQUEEZE"),
        ("BREAKDOWN", "EXTREME_BREAKDOWN"),
        ("BREAKOUT", "EXTREME_BREAKOUT"),
        ("SUPPORT", "MAJOR_SUPPORT_FAILURE"),
        ("RECLAIM", "MAJOR_RECLAIM"),
        ("VOLUME", "ABNORMAL_VOLUME_EXPANSION"),
        ("OPEN INTEREST", "ABNORMAL_OI_EXPANSION"),
        ("OI FLUSH", "OI_FLUSH"),
        ("REGIME", "MATERIAL_REGIME_CHANGE"),
    ):
        if needle in reason_text:
            family = candidate
            break
    confirmations: list[dict[str, Any]] = []
    if major is not None:
        confirmations.append(
            {
                "signalFamily": "technical_structure",
                "source": "canonical staged alert",
                "evidence": {"alertId": major["alertId"], "score": major["signalScore"]},
            }
        )
    if futures:
        confirmations.append(
            {
                "signalFamily": "leverage",
                "source": ", ".join(sorted({str(row.get('sourceName') or row.get('sourceId')) for row in futures})),
                "evidence": compact_features,
            }
        )
    if spot is not None:
        confirmations.append(
            {
                "signalFamily": "spot",
                "source": str(spot.get("sourceName") or spot.get("sourceId")),
                "evidence": compact_features.get("spotConfirmation"),
            }
        )
    if top_analog is not None:
        confirmations.append(
            {
                "signalFamily": "historical_analog",
                "source": historical["engineVersion"],
                "evidence": {
                    "eventVersionId": top_analog.get("eventVersionId"),
                    "similarityScore": top_analog.get("similarityScore"),
                },
            }
        )
    event_key = (
        str(major.get("alertId"))
        if major is not None
        else f"analog:{top_analog.get('eventVersionId')}"
    )
    decision = await asyncio.to_thread(
        record_auto_event_decision,
        {
            "eventKey": event_key,
            "eventFamily": family,
            "evidenceHash": packet["evidenceHash"],
            "regimeFingerprint": stable_hash(compact_features),
            "detectedAt": major.get("firstDetectedAt") if major else packet.get("dataAsOf"),
            "severityScore": max(
                float(major.get("signalScore") or 0.0) if major else 0.0,
                float(top_analog.get("similarityScore") or 0.0) if top_analog else 0.0,
            ),
            "historicalAnalogScore": float(top_analog.get("similarityScore") or 0.0) if top_analog else 0.0,
            "confirmations": confirmations,
            "evidence": {
                "snapshotId": packet["snapshotId"],
                "alert": major,
                "historicalAnalog": top_analog,
            },
        },
    )
    if not decision.get("eligible") or not PAID_AI_ENABLED or not OPENAI_AUTOMATIC_ENABLED:
        return {
            "evaluated": True,
            "automaticCall": False,
            "decision": decision,
            "reason": (
                decision.get("decisionReason")
                if not decision.get("eligible")
                else "event qualified, but automatic paid Chad is disabled"
            ),
        }

    blockers = chad_reactivation_gate.blockers(
        key_configured=bool(OPENAI_API_KEY),
        relay_token_configured=bool(RELAY_TOKEN),
        call_mode="automatic",
    )
    if blockers:
        return {"evaluated": True, "automaticCall": False, "decision": decision, "reason": blockers[0]}
    request_id, blocked_reason = chad_reactivation_gate.begin_request(
        key_configured=bool(OPENAI_API_KEY),
        relay_token_configured=bool(RELAY_TOKEN),
        call_mode="automatic",
    )
    if request_id is None:
        return {"evaluated": True, "automaticCall": False, "decision": decision, "reason": blocked_reason}
    started_at = time.time()
    error: BaseException | None = None
    success = False
    budget_summary: dict[str, Any] | None = None
    try:
        with request_budget_scope() as budget:
            result = await _run_chad_analysis(
                ChadAnalyzeRequest(
                    question=(
                        "An extreme TAG event was detected. Independently analyze the frozen evidence, "
                        "historical analogs, differences, risks, and invalidation."
                    ),
                    allowPaidCall=True,
                    allowForecastWrite=True,
                    allowGrading=True,
                    trigger="auto-extreme-event",
                ),
                call_mode="automatic",
                auto_event=decision,
            )
            budget_summary = budget.summary()
        success = True
        return {"evaluated": True, "automaticCall": True, "decision": decision, "result": result}
    except BaseException as exc:
        error = exc
        raise
    finally:
        chad_reactivation_gate.finish_request(
            request_id,
            success=success,
            started_at=started_at,
            budget=budget_summary,
            error=error,
        )


async def _run_claimed_phase1_job(job: dict[str, Any]) -> dict[str, Any]:
    job_type = str(job.get("jobType") or "")
    if job_type == "collect_canonical_evidence":
        return await collect_canonical_evidence_once()
    if job_type == "collect_verified_tag_supply":
        return await collect_verified_tag_supply_once()
    if job_type == "issue_due_tagalysis_forecasts":
        # Deterministic server-side TAGalysis issuance has no Chad/OpenAI path.
        return await asyncio.to_thread(issue_due_tagalysis_forecasts)
    if job_type == "validate_helper_candidate":
        candidate_id = str((job.get("payload") or {}).get("candidateId") or "")
        if not candidate_id:
            raise ValueError("Helper validation job is missing candidateId.")
        return await asyncio.to_thread(validate_helper_candidate, candidate_id)
    if job_type == "grade_due_canonical_forecasts":
        return await asyncio.to_thread(capture_exact_due_outcomes)
    if job_type == "capture_canonical_deadline_observation":
        forecast_id = str((job.get("payload") or {}).get("forecastId") or "")
        if not forecast_id:
            raise ValueError("deadline-capture job is missing forecastId")
        # Force one fresh DEX read only at the scheduled immutable deadline.
        # This is deterministic market collection, never a paid-AI call.
        spot = await cached_spot(force=True)
        return await asyncio.to_thread(capture_direct_deadline_outcome, forecast_id, spot=spot)
    if job_type == "maintain_pattern_memory":
        return await asyncio.to_thread(maintain_pattern_memory_from_latest_evidence)
    if job_type == "process_staged_alerts":
        return await asyncio.to_thread(process_level_alerts_from_latest_evidence)
    if job_type == "maintain_historical_memory":
        archive_catch_up: dict[str, Any] = {
            "enabled": bool(BACKFILL_ENABLED and not REPAIR_MODE),
            "paidAiCalls": 0,
        }
        if BACKFILL_ENABLED and not REPAIR_MODE:
            archive_catch_up["result"] = await terminal_backfill_recent(2, "5m")
        maintenance = await asyncio.to_thread(
            historical_maintenance,
            include_recent_detection=not bool((job.get("payload") or {}).get("importActivation")),
        )
        return {**maintenance, "archiveCatchUp": archive_catch_up}
    if job_type == "run_bounded_forecast_research":
        if not HISTORICAL_RESEARCH_ENABLED:
            return {"enabled": False, "reason": "historical_research_disabled", "automaticPaidAiCalls": 0}
        horizon = str((job.get("payload") or {}).get("horizon") or "")
        return await asyncio.to_thread(run_bounded_production_research, max_observations=20_000, horizons=(horizon,))
    if job_type == "run_bounded_predictive_tournament":
        if not HISTORICAL_RESEARCH_ENABLED:
            return {"enabled": False, "reason": "historical_research_disabled", "automaticPaidAiCalls": 0}
        return await asyncio.to_thread(persist_bounded_predictive_study, max_rows=25_000)
    if job_type == "evaluate_prospective_learning":
        return await asyncio.to_thread(evaluate_prospective_thresholds)
    if job_type == "evaluate_event_driven_chad":
        return await evaluate_event_driven_chad()
    raise ValueError(f"Unsupported server job type: {job_type}")


async def phase1_job_loop() -> None:
    """Run persistent, idempotent jobs and catch up expired work after wake."""

    worker_id = f"render:{BUILD_ID}:{uuid.uuid4().hex[:12]}"
    phase1_state["running"] = True
    try:
        await asyncio.sleep(2)
        while True:
            phase1_state["lastRunAt"] = utc_iso()
            try:
                await asyncio.to_thread(
                    schedule_current_evidence_job,
                    interval_seconds=COLLECT_SECONDS,
                )
                supply_bucket = int(time.time()) // 3_600 * 3_600
                await asyncio.to_thread(
                    enqueue_job,
                    job_type="collect_verified_tag_supply",
                    idempotency_key=f"collect-verified-tag-supply:{supply_bucket}",
                    origin="server-scheduler",
                    payload={"bucket": supply_bucket},
                    max_attempts=2,
                )
                forecast_bucket = int(time.time()) // max(300, COLLECT_SECONDS) * max(300, COLLECT_SECONDS)
                await asyncio.to_thread(
                    enqueue_job,
                    job_type="issue_due_tagalysis_forecasts",
                    idempotency_key=f"issue-tagalysis-forecasts:{forecast_bucket}",
                    origin="server-scheduler",
                    payload={"bucket": forecast_bucket},
                    max_attempts=2,
                )
                await asyncio.to_thread(
                    enqueue_phase3_jobs,
                    interval_seconds=max(300, COLLECT_SECONDS),
                )
                history_bucket = int(time.time()) // 3_600 * 3_600
                await asyncio.to_thread(
                    enqueue_job,
                    job_type="maintain_historical_memory",
                    idempotency_key=f"phase6-history:{history_bucket}",
                    origin="server-scheduler",
                    payload={"bucket": history_bucket},
                    max_attempts=2,
                )
                # Lowest-priority deterministic learning job: keyed to the
                # historical watermark so unchanged history cannot trigger a
                # repeated full replay, and scheduled after live work.
                research_watermark = await asyncio.to_thread(production_research_watermark)
                if research_watermark is not None:
                    for research_horizon in ("1h", "4h", "24h", "7d", "30d", "90d", "180d", "1y"):
                        await asyncio.to_thread(
                            enqueue_job,
                            job_type="run_bounded_forecast_research",
                            # v5 supersedes the prior single-attempt v3/v4 keys.
                            # Re-running is safe: research persistence is
                            # content-hash idempotent and the long lease now
                            # survives normal Render wake/deploy windows.
                            idempotency_key=f"phase9-bounded-research-v5:{research_watermark}:{research_horizon}",
                            origin="server-scheduler",
                            payload={"historyWatermark": research_watermark, "horizon": research_horizon, "priority": "lowest"},
                            # Research is idempotent at the persisted run and
                            # feature-profile hashes.  Permit one delayed
                            # recovery after a Render restart, never an
                            # unbounded retry loop.
                            max_attempts=2,
                        )
                    await asyncio.to_thread(
                        enqueue_job,
                        job_type="run_bounded_predictive_tournament",
                        idempotency_key=f"phase10-canonical-tournament-v1:{research_watermark}",
                        origin="server-scheduler",
                        payload={"historyWatermark": research_watermark, "priority": "lowest"},
                        max_attempts=2,
                    )
                chad_event_bucket = int(time.time()) // max(300, COLLECT_SECONDS) * max(300, COLLECT_SECONDS)
                await asyncio.to_thread(
                    enqueue_job,
                    job_type="evaluate_event_driven_chad",
                    idempotency_key=f"phase6-chad-event:{chad_event_bucket}",
                    origin="server-scheduler",
                    payload={"bucket": chad_event_bucket},
                    max_attempts=2,
                )
                prospective_bucket = int(time.time()) // 3_600 * 3_600
                await asyncio.to_thread(
                    enqueue_job,
                    job_type="evaluate_prospective_learning",
                    idempotency_key=f"phase12-prospective-learning-v1:{prospective_bucket}",
                    origin="server-scheduler",
                    payload={"bucket": prospective_bucket, "priority": "after-live-grading"},
                    max_attempts=2,
                )
                processed = 0
                while processed < 20:
                    job = await asyncio.to_thread(
                        claim_due_job,
                        worker_id=worker_id,
                        lock_seconds=max(120, COLLECT_SECONDS),
                    )
                    if job is None:
                        break
                    try:
                        phase1_state["activeJob"] = {
                            "jobId": job["jobId"], "jobType": job["jobType"], "startedAt": utc_iso(),
                        }
                        result = await _run_claimed_phase1_job(job)
                        await asyncio.to_thread(complete_job, job["jobId"], result)
                        phase1_state["lastCompletedJob"] = {
                            "jobId": job["jobId"],
                            "jobType": job["jobType"],
                            "completedAt": utc_iso(),
                        }
                        phase1_state["lastError"] = None
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        await asyncio.to_thread(fail_job, job["jobId"], exc)
                        phase1_state["lastError"] = f"{type(exc).__name__}: {str(exc)[:500]}"
                    finally:
                        phase1_state["activeJob"] = None
                    processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                phase1_state["lastError"] = f"{type(exc).__name__}: {str(exc)[:500]}"
            await asyncio.sleep(SERVER_JOB_POLL_SECONDS)
    finally:
        phase1_state["running"] = False


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client, openai_client

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=10.0),
        headers={"User-Agent": f"TAG-Terminal-Relay/{SERVICE_VERSION}"},
        follow_redirects=True,
    )
    if LEDGER_ENABLED and DB_BOOTSTRAP_ON_START:
        prediction_ledger.initialize()
    automatic_live_work = not REPAIR_MODE and LIVE_COLLECTORS_ENABLED
    await terminal_addon.start(
        enable_external_clients=automatic_live_work,
        bootstrap_database=DB_BOOTSTRAP_ON_START,
    )

    openai_client = httpx.AsyncClient(
        timeout=httpx.Timeout(float(OPENAI_TIMEOUT_SECONDS), connect=15.0),
        headers={"User-Agent": f"TAG-Terminal-Chad/{SERVICE_VERSION}"},
        follow_redirects=True,
    )
    market_task = (
        asyncio.create_task(market_stream_listener())
        if automatic_live_work
        else None
    )
    depth_task = (
        asyncio.create_task(depth_stream_listener())
        if automatic_live_work
        else None
    )
    grader_task = (
        asyncio.create_task(ledger_grader_loop())
        if LEDGER_ENABLED and automatic_live_work and DETERMINISTIC_GRADING_ENABLED
        else None
    )
    phase1_task = (
        asyncio.create_task(phase1_job_loop(), name="tagalysis-phase1-server-jobs")
        if automatic_live_work and SERVER_JOBS_ENABLED
        else None
    )
    # Retain the legacy loop only as an optional fallback. Phase 1 server jobs
    # own collection when enabled so two schedulers cannot create duplicate work.
    if automatic_live_work and not SERVER_JOBS_ENABLED:
        terminal_addon.start_collector(cached_snapshot, cached_spot)

    try:
        yield
    finally:
        if market_task is not None:
            market_task.cancel()
        if depth_task is not None:
            depth_task.cancel()
        if grader_task is not None:
            grader_task.cancel()
        if phase1_task is not None:
            phase1_task.cancel()
        tasks = [
            task
            for task in (market_task, depth_task, grader_task, phase1_task)
            if task is not None
        ]
        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        if http_client is not None:
            await http_client.aclose()
        await terminal_addon.stop()
        if openai_client is not None:
            await openai_client.aclose()
        http_client = None
        openai_client = None


app = FastAPI(
    title="TAG Market Data Relay",
    version=SERVICE_VERSION,
    description=(
        "Read-only relay for public Binance USDⓈ-M futures data, primary-pair DEX spot data, "
        "a protected server-side Chad analysis endpoint, durable forecast grading, alert-state history, "
        "a paper-only isolated-margin simulator, and a verified-timestamp social-call ledger. "
        "There is no exchange login, wallet access, real-money execution, or live order routing."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "TAG Market Data Relay",
        "version": SERVICE_VERSION,
        "symbol": SYMBOL,
        "marketDataReadOnly": True,
        "paperSimulationOnly": True,
        "realTradingEnabled": False,
        "docs": "/docs",
        "health": "/health",
        "sourceHealth": "/v1/tag/source-health",
        "connection": "/v1/tag/connection",
        "canonicalEvidence": "/v1/tag/evidence/current",
        "helperCandidate": "/v1/tag/assist/results",
        "snapshot": "/v1/tag/snapshot",
        "spot": "/v1/tag/spot",
        "history": "/v1/tag/history?period=5m&limit=100",
        "chad": "/v1/chad/analyze",
        "ledger": "/v1/chad/ledger",
        "performance": "/v1/chad/performance",
        "freshness": "/v1/tag/freshness",
        "calibration": "/v1/chad/calibration",
        "changes": "/v1/chad/changes",
        "ledgerImport": "/v1/chad/ledger/import",
        "ledgerBackup": "/v1/chad/ledger/backup",
        "terminal": "/v1/tag/terminal",
        "market": "/v1/tag/market",
        "heatmap": "/v1/tag/heatmap",
        "liquidations": "/v1/tag/liquidations",
        "alerts": "/v1/tag/alerts",
        "alertTimeline": "/v1/tag/alert-timeline",
        "testNotification": "/v1/tag/alerts/test",
        "paper": "/v1/tag/paper",
        "social": "/v1/tag/social",
        "forecast": "/v1/tag/forecast",
        "patterns": "/v1/tag/patterns",
        "shareReport": "/v1/tag/share-report",
        "visionBackfill": "/v1/admin/binance-vision/backfill",
        "operatingStatus": operating_status(SERVICE_VERSION),
    }


def phase1_health_state() -> dict[str, Any]:
    """Small in-memory snapshot safe for side-effect-free health routes."""

    return {
        key: value
        for key, value in phase1_state.items()
        if key != "lastPacket"
    } | {"lastPacketAvailable": phase1_state.get("lastPacket") is not None}


@app.get("/health")
async def health() -> dict[str, Any]:
    async with stream_lock:
        connected = bool(stream_state["connected"])
        last_message = stream_state["lastMessageAt"]
        depth_connected = bool(stream_state["depthConnected"])
        depth_message = stream_state["depthLastMessageAt"]
        depth_mode = stream_state["depthMode"]

    terminal_storage_persistent = (
        TERMINAL_DATABASE_URL.startswith("postgresql+")
        or (
            TERMINAL_DATABASE_URL.startswith("sqlite")
            and "/tmp/" not in TERMINAL_DATABASE_URL
        )
    )

    return {
        # Render health checks must never wake Neon or ping paid/external
        # sources. Component health is exposed from in-memory state only.
        "ok": True if REPAIR_MODE else connected and depth_connected,
        "serviceTime": utc_iso(),
        "version": SERVICE_VERSION,
        "buildId": BUILD_ID,
        "symbol": SYMBOL,
        "operatingStatus": operating_status(SERVICE_VERSION),
        "healthCheckSideEffects": "none",
        "binanceRestPingReachable": None,
        "binanceRestPingError": None,
        "marketStreamConnected": connected,
        "marketStreamLastMessageAt": last_message,
        "marketStreamLastError": None,
        "depthStreamConnected": depth_connected,
        "depthStreamLastMessageAt": depth_message,
        "depthStreamMode": depth_mode,
        "depthStreamLastError": None,
        "chadConfigured": bool(OPENAI_API_KEY and RELAY_TOKEN),
        "chadProtected": bool(RELAY_TOKEN),
        "chadReactivation": chad_reactivation_gate.status(
            key_configured=bool(OPENAI_API_KEY),
            relay_token_configured=bool(RELAY_TOKEN),
        ),
        "openAIModel": OPENAI_MODEL,
        "predictionLedgerEnabled": LEDGER_ENABLED,
        "predictionLedgerPath": LEDGER_DB_PATH if LEDGER_ENABLED else None,
        "predictionLedgerStorageWarning": prediction_ledger.persistent_hint if LEDGER_ENABLED else None,
        "predictionLedgerStorage": {
            "configured": LEDGER_ENABLED,
            "checkedByHealthRoute": False,
        },
        # Retained for clients that previously consumed this field.  It is
        # exclusively the legacy Chad-ledger grader and is intentionally
        # disabled with paid Chad.  It must not be mistaken for the
        # deterministic canonical TAGalysis exact-deadline grader below.
        "automaticGrader": (
            {**dict(grader_state), "scope": "legacy_chad_ledger_only", "canonicalForecastGrading": "separate_server_job"}
            if LEDGER_ENABLED else None
        ),
        "canonicalForecastGrader": {
            "enabled": bool(DETERMINISTIC_GRADING_ENABLED and SERVER_JOBS_ENABLED),
            "scheduler": "grade_due_canonical_forecasts",
            "exactDeadlineOnly": True,
            "paidAiIndependent": True,
            "workerRunning": bool(phase1_state.get("running")),
            "state": "server_job_loop" if phase1_state.get("running") else "awaiting_server_job_loop",
        },
        "terminalAddonVersion": TERMINAL_ADDON_VERSION,
        "terminalCollectorRunning": bool(
            phase1_state.get("running")
            or (terminal_addon.collector_task and not terminal_addon.collector_task.done())
        ),
        "phase1ServerJobs": phase1_health_state(),
        "terminalHistoryCounts": None,
        "terminalStoragePersistent": terminal_storage_persistent,
        "terminalStorageWarning": (
            None
            if terminal_storage_persistent
            else "Terminal history uses ephemeral/default storage. Configure TERMINAL_DATABASE_URL with PostgreSQL or a persistent disk before treating history as durable."
        ),
        "terminalStorageError": None,
    }


async def source_health_payload(*, authenticated: bool) -> dict[str, Any]:
    """Return side-effect-free operational health for TAGalyst.

    This deliberately reads only process memory and configuration. It must not
    wake Neon, poll a provider, start a collector, grade a forecast, or call AI.
    """
    async with stream_lock:
        market_connected = bool(stream_state["connected"])
        market_last_message = stream_state["lastMessageAt"]
        depth_connected = bool(stream_state["depthConnected"])
        depth_last_message = stream_state["depthLastMessageAt"]

    storage_persistent = (
        TERMINAL_DATABASE_URL.startswith("postgresql+")
        or (
            TERMINAL_DATABASE_URL.startswith("sqlite")
            and "/tmp/" not in TERMINAL_DATABASE_URL
        )
    )
    collector_running = bool(
        phase1_state.get("running")
        or (
            terminal_addon.collector_task
            and not terminal_addon.collector_task.done()
        )
    )
    grader_running = bool(grader_state.get("running", False))
    grader_last_result = (
        grader_state.get("lastResult")
        if isinstance(grader_state.get("lastResult"), dict)
        else {}
    )
    grader_errors = [
        str(value)
        for value in grader_last_result.get("errors", [])
        if str(value).strip()
    ]
    grader_healthy = grader_running and not grader_errors
    flags = operating_status(SERVICE_VERSION)["usage"]["flags"]
    minimum_ready = bool(
        authenticated
        and not REPAIR_MODE
        and flags["liveCollectorsEnabled"]
        and collector_running
        and flags["deterministicGradingEnabled"]
        and grader_healthy
        and storage_persistent
    )
    blockers: list[str] = []
    if not authenticated:
        blockers.append("Authorization required")
    if REPAIR_MODE:
        blockers.append("Repair Mode is active")
    if not flags["liveCollectorsEnabled"]:
        blockers.append("Current-data collection is paused")
    elif not collector_running:
        blockers.append("Collector job is not running")
    if not flags["deterministicGradingEnabled"]:
        blockers.append("Deterministic grading is disabled")
    elif not grader_running:
        blockers.append("Forecast grader is not running")
    if grader_errors:
        blockers.append(
            f"Forecast grader is degraded ({len(grader_errors)} unresolved item error(s))"
        )
    if not storage_persistent:
        blockers.append("Persistent database storage is not configured")

    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "serviceVersion": SERVICE_VERSION,
        "buildId": BUILD_ID,
        "authenticated": authenticated,
        "sideEffects": "none",
        "repairMode": REPAIR_MODE,
        "minimumLiveServicesReady": minimum_ready,
        "blockers": blockers,
        "services": {
            "collection": {
                "enabled": flags["liveCollectorsEnabled"],
                "running": collector_running,
                "authoritativeScheduler": (
                    "server_jobs" if flags.get("serverJobsEnabled") else "legacy_collector"
                ),
                "lastCompletedAt": terminal_addon.collector_state.get("lastCompletedAt"),
                "lastError": terminal_addon.collector_state.get("lastError"),
                "sourceErrors": terminal_addon.collector_state.get("lastSourceErrors", []),
            },
            "grading": {
                "enabled": flags["deterministicGradingEnabled"],
                "running": grader_running,
                "healthy": grader_healthy,
                "lastRunAt": grader_state.get("lastRunAt"),
                "gradedLastRun": grader_last_result.get("graded", 0),
                "pendingDue": grader_last_result.get("pendingDue", 0),
                "errorCount": len(grader_errors),
                "errors": grader_errors[:3],
            },
            "backfill": {"enabled": flags["backfillEnabled"]},
            "optionalAiSynthesis": {
                "enabled": flags["openAiAutomaticEnabled"] and flags.get("paidAiEnabled", False),
            },
            "notifications": {"enabled": flags["pushEnabled"]},
            "forecastWrites": {
                "enabled": flags["chadForecastWritesEnabled"],
            },
            "cacheWrites": {
                "enabled": flags["chadCacheWritesEnabled"],
            },
        },
        "sources": [
            {
                "id": "binance-market-stream",
                "status": "CURRENT" if market_connected else "PAUSED",
                "connected": market_connected,
                "lastObservedAt": market_last_message,
            },
            {
                "id": "binance-depth-stream",
                "status": "CURRENT" if depth_connected else "PAUSED",
                "connected": depth_connected,
                "lastObservedAt": depth_last_message,
            },
        ],
        "storage": {
            "persistent": storage_persistent,
            "databaseCheckedByThisRequest": False,
        },
        "serverJobs": phase1_health_state(),
        "nextAction": (
            "Minimum live services are ready"
            if minimum_ready
            else blockers[0] if blockers else "Review service configuration"
        ),
    }


@app.get("/v1/tag/source-health")
async def tag_source_health() -> dict[str, Any]:
    return await source_health_payload(authenticated=False)


@app.get("/v1/tag/connection")
async def tag_connection(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await source_health_payload(authenticated=True)


@app.get("/v1/tag/evidence/current")
async def tag_current_evidence(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """Return last durable evidence and coalesce one safe wake/catch-up job."""

    require_relay_key(x_relay_key)
    read_error: str | None = None
    try:
        packet = await asyncio.to_thread(latest_evidence_packet)
    except Exception as exc:
        packet = phase1_state.get("lastPacket")
        read_error = f"{type(exc).__name__}: {str(exc)[:300]}"

    packet_created = _parse_time_for_api(
        packet.get("serverCreatedAt") if isinstance(packet, dict) else None
    )
    age_seconds = (
        max(0.0, (datetime.now(timezone.utc) - packet_created).total_seconds())
        if packet_created is not None
        else None
    )
    needs_catch_up = packet is None or age_seconds is None or age_seconds > COLLECT_SECONDS
    wake_job: dict[str, Any] | None = None
    wake_error: str | None = None
    if needs_catch_up and SERVER_JOBS_ENABLED and not REPAIR_MODE:
        try:
            wake_job = await asyncio.to_thread(
                schedule_current_evidence_job,
                interval_seconds=COLLECT_SECONDS,
            )
        except Exception as exc:
            wake_error = f"{type(exc).__name__}: {str(exc)[:300]}"

    return {
        "ok": packet is not None,
        "authoritative": True,
        "authority": "TAGalysis server / Neon PostgreSQL",
        "serverTime": utc_iso(),
        "packet": packet,
        "freshness": {
            "ageSeconds": round(age_seconds, 3) if age_seconds is not None else None,
            "status": (
                "current"
                if age_seconds is not None and age_seconds <= COLLECT_SECONDS
                else "stale" if packet is not None else "unavailable"
            ),
        },
        "catchUp": {
            "needed": needs_catch_up,
            "job": wake_job,
            "error": wake_error,
            "coalescing": "database idempotency key per collection window",
        },
        "storageReadError": read_error,
        "lastValidRetained": packet is not None,
    }


@app.post("/v1/tag/assist/results")
async def tag_assist_result(
    request: HelperResultRequest,
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("helper candidate uploads")
    try:
        result = await asyncio.to_thread(
            persist_helper_candidate,
            request.model_dump(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "accepted": True,
        "authoritative": False,
        "message": "Helper output is a non-authoritative candidate until server validation completes.",
        **result,
    }


@app.post("/v1/tag/forecast/truth/supply")
async def tag_forecast_supply_truth(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("verified supply snapshot writes")
    try:
        result = await asyncio.to_thread(persist_asset_truth_snapshot, payload)
    except ForecastValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, **result}


@app.post("/v1/tag/forecast/truth/portfolio")
async def tag_forecast_portfolio_truth(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("verified portfolio snapshot writes")
    try:
        result = await asyncio.to_thread(persist_portfolio_position_snapshot, payload)
    except ForecastValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, **result}


@app.post("/v1/tag/forecasts/canonical")
async def tag_canonical_forecast_write(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("canonical forecast writes")
    try:
        result = await asyncio.to_thread(persist_canonical_forecast, payload)
    except ForecastValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.get("/v1/tag/forecasts/canonical/latest")
async def tag_canonical_forecast_latest(
    producer: str = Query("tagalysis"),
    horizon: str = Query("24h"),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    try:
        record = await asyncio.to_thread(
            latest_canonical_forecast,
            producer=producer.lower(),
            horizon=horizon.lower(),
        )
        formatted = format_canonical_forecast(record) if record is not None else None
    except ForecastValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "ok": record is not None,
        "authoritative": True,
        "record": record,
        "presentation": formatted,
    }


@app.get("/v1/tag/control-center")
async def tag_control_center(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """One authenticated, side-effect-free payload for the Phase 4 Android UI."""
    require_relay_key(x_relay_key)
    return {
        "ok": True,
        **await asyncio.to_thread(canonical_control_center_snapshot),
    }


@app.get("/v1/tag/history/coverage")
async def tag_history_coverage(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """Authenticated read-only history coverage; missing fields remain explicit."""
    require_relay_key(x_relay_key)
    return {"ok": True, "authoritative": True, **await asyncio.to_thread(build_coverage_report)}


@app.get("/v1/tag/history/events")
async def tag_history_events(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return {"ok": True, "authoritative": True, **await asyncio.to_thread(historical_event_report)}


@app.get("/v1/tag/history/summary")
async def tag_history_summary(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """Authenticated compact production-history status; raw warehouse rows remain server-only."""
    require_relay_key(x_relay_key)
    return {"ok": True, "authoritative": True, **await asyncio.to_thread(historical_production_summary)}


@app.get("/v1/chad/usage")
async def tag_chad_usage(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return {"ok": True, **await asyncio.to_thread(chad_usage_report)}


@app.post("/v1/tag/outcomes/verified")
async def tag_verified_outcome_write(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("verified exact-deadline outcome writes")
    try:
        result = await asyncio.to_thread(persist_verified_outcome, payload)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.post("/v1/tag/forecasts/{forecast_id}/grade")
async def tag_canonical_forecast_grade(
    forecast_id: str,
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("canonical deterministic grading")
    try:
        result = await asyncio.to_thread(
            grade_canonical_forecast,
            forecast_id,
            str(payload.get("outcomeId") or ""),
            evaluation_kind=str(payload.get("evaluationKind") or "live"),
        )
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.post("/v1/tag/social-calls/{social_call_id}/grade")
async def tag_canonical_social_call_grade(
    social_call_id: str,
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("canonical social-call grading")
    frozen_call = payload.get("frozenCall") if isinstance(payload.get("frozenCall"), dict) else {}
    frozen_call = {**frozen_call, "socialCallId": social_call_id}
    try:
        result = await asyncio.to_thread(
            grade_social_call,
            frozen_call,
            str(payload.get("outcomeId") or ""),
            evaluation_kind=str(payload.get("evaluationKind") or "live"),
        )
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.get("/v1/tag/grades/report")
async def tag_canonical_grade_report(
    producer: str = Query("tagalysis"),
    horizon: str = Query("24h"),
    evaluation_kind: str = Query("live", alias="evaluationKind"),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    try:
        result = await asyncio.to_thread(
            grade_report,
            producer=producer.lower(),
            horizon=horizon.lower(),
            evaluation_kind=evaluation_kind.lower(),
        )
    except (Phase3ValidationError, KeyError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, **result}


@app.post("/v1/tag/patterns/sequences")
async def tag_pattern_sequence_write(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("canonical ChadTAG pattern memory writes")
    try:
        result = await asyncio.to_thread(persist_pattern_sequence, payload)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "memoryOwner": "canonical-chadtag-memory", **result}


@app.get("/v1/tag/patterns/{sequence_id}/analogs")
async def tag_pattern_analogs(
    sequence_id: str,
    limit: int = Query(5, ge=1, le=20),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    try:
        results = await asyncio.to_thread(find_historical_analogs, sequence_id, limit=limit)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "analogs": results}


@app.post("/v1/tag/learning/versions")
async def tag_learning_version_write(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("bounded adaptive-learning version writes")
    try:
        result = await asyncio.to_thread(persist_learning_version, payload)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.get("/v1/tag/learning/versions/current")
async def tag_learning_version_current(
    component: str = Query("canonical-forecast-weighting"),
    producer: str = Query("champion"),
    horizon: str = Query("24h"),
    regime: str = Query("ALL"),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    result = await asyncio.to_thread(
        current_learning_version,
        component=component,
        producer=producer.lower(),
        horizon=horizon.lower(),
        regime=regime,
    )
    return {"ok": result is not None, "authoritative": True, "version": result}


@app.post("/v1/tag/learning/versions/{version_id}/rollback")
async def tag_learning_version_rollback(
    version_id: str,
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("adaptive-learning rollback version writes")
    try:
        result = await asyncio.to_thread(
            rollback_learning_version,
            version_id,
            reason=str(payload.get("reason") or "manual rollback"),
        )
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.post("/v1/tag/alerts/canonical/process")
async def tag_canonical_alert_process(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("canonical staged-alert processing")
    try:
        result = await asyncio.to_thread(process_alert_signal, payload)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutableTimeline": True, **result}


@app.post("/v1/tag/alerts/canonical/finalize")
async def tag_canonical_alert_finalize(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("canonical alert outcome writes")
    try:
        result = await asyncio.to_thread(finalize_alert, payload)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutable": True, **result}


@app.get("/v1/tag/alerts/canonical/active")
async def tag_canonical_alert_active(
    limit: int = Query(50, ge=1, le=200),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return {"ok": True, "authoritative": True, "alerts": await asyncio.to_thread(phase3_active_alerts, limit=limit)}


@app.get("/v1/tag/settings/market-cap-levels")
async def tag_market_cap_levels(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return {"ok": True, "authoritative": True, "levels": await asyncio.to_thread(current_user_levels)}


@app.post("/v1/tag/settings/market-cap-levels")
async def tag_market_cap_level_write(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("authenticated market-cap level revisions")
    try:
        result = await asyncio.to_thread(persist_user_level_revision, payload)
    except Phase3ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "authoritative": True, "immutableRevision": True, **result}


@app.get("/v1/tag/snapshot")
async def tag_snapshot(
    force: bool = Query(False, description="Ignore the short cache and request fresh data."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await cached_snapshot(force=force)


@app.get("/v1/tag/spot")
async def tag_spot(
    force: bool = Query(False, description="Ignore the short cache and request fresh DEX data."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await cached_spot(force=force)


@app.get("/v1/tag/freshness")
async def tag_freshness(
    force: bool = Query(False, description="Request fresh futures and spot data before scoring freshness."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    snapshot, spot = await asyncio.gather(
        cached_snapshot(force=force),
        cached_spot(force=force),
    )
    return {
        "ok": True,
        "symbol": SYMBOL,
        "freshness": build_freshness_report(snapshot, spot),
    }


@app.get("/v1/tag/history")
async def tag_history(
    period: str = Query("5m"),
    limit: int = Query(100, ge=2, le=500),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    if REPAIR_MODE:
        raise HTTPException(
            status_code=423,
            detail="Repair mode is active; external history collection is paused.",
        )
    return await collect_history_data(period=period, limit=limit)


@app.get("/v1/tag/liquidations")
async def tag_liquidations(
    limit: int = Query(100, ge=1, le=1000),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)

    async with liquidation_lock:
        events = list(liquidation_events)[-limit:]

    return {
        "symbol": SYMBOL,
        "generatedAt": utc_iso(),
        "summary": await liquidation_summary(),
        "events": events,
    }


@app.get("/v1/chad/calibration")
async def chad_calibration(
    gradeDue: bool = Query(False),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    grading = await grade_due_ledger_predictions(limit=200) if gradeDue and not REPAIR_MODE else None
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "grading": grading,
        "performance": prediction_ledger.performance_summary(),
        "calibration": prediction_ledger.calibration_profile(),
    }


@app.get("/v1/chad/changes")
async def chad_changes(
    limit: int = Query(25, ge=1, le=100),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "changes": prediction_ledger.changes_history(limit=limit),
    }


@app.post("/v1/chad/ledger/import")
async def chad_ledger_import(
    payload: dict[str, Any],
    merge: bool = Query(True, description="Merge with current records. Set false to replace the ledger."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_repair_writes_unlocked("ledger imports")
    require_chad_access(x_relay_key)
    try:
        imported = prediction_ledger.import_data(payload, merge=merge)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "import": imported,
        "storage": prediction_ledger.storage_status(),
        "performance": prediction_ledger.performance_summary(),
    }


@app.post("/v1/chad/ledger/backup")
async def chad_ledger_backup(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_repair_writes_unlocked("ledger backups")
    require_chad_access(x_relay_key)
    try:
        backup = prediction_ledger.backup_now()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ledger backup failed: {exc}") from exc
    return {
        "ok": bool(backup.get("ok")),
        "backup": backup,
        "storage": prediction_ledger.storage_status(),
    }


@app.post("/v1/chad/ledger/grade")
async def chad_ledger_grade(
    limit: int = Query(50, ge=1, le=200),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_repair_writes_unlocked("forecast grading")
    require_chad_access(x_relay_key)
    result = await grade_due_ledger_predictions(limit=limit)
    result["performance"] = prediction_ledger.performance_summary()
    return result


@app.get("/v1/chad/performance")
async def chad_performance(
    gradeDue: bool = Query(False, description="Grade due forecasts before returning performance."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    grading = await grade_due_ledger_predictions(limit=100) if gradeDue and not REPAIR_MODE else None
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "grading": grading,
        "performance": prediction_ledger.performance_summary(),
        "calibration": prediction_ledger.calibration_profile(),
        "automaticGrader": dict(grader_state),
        "storage": prediction_ledger.storage_status(),
    }


@app.get("/v1/chad/ledger")
async def chad_ledger(
    limit: int = Query(50, ge=1, le=500),
    status: str = Query("all", pattern="^(all|pending|graded)$"),
    includeAnalysis: bool = Query(False),
    gradeDue: bool = Query(False),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    grading = await grade_due_ledger_predictions(limit=100) if gradeDue and not REPAIR_MODE else None
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "grading": grading,
        "performance": prediction_ledger.performance_summary(),
        "calibration": prediction_ledger.calibration_profile(),
        "storage": prediction_ledger.storage_status(),
        "predictions": prediction_ledger.list_predictions(
            limit=limit,
            status=status,
            include_analysis=includeAnalysis,
        ),
    }


@app.get("/v1/chad/ledger/export")
async def chad_ledger_export(
    limit: int = Query(5000, ge=1, le=5000),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    if not REPAIR_MODE:
        await grade_due_ledger_predictions(limit=200)
    return prediction_ledger.export_data(limit=min(limit, 200) if REPAIR_MODE else limit)


async def _run_chad_analysis(
    request: ChadAnalyzeRequest,
    *,
    call_mode: str = "manual",
    auto_event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if request.historyPeriod not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"historyPeriod must be one of: {', '.join(sorted(VALID_PERIODS))}",
        )

    snapshot, history, spot = await asyncio.gather(
        cached_snapshot(
            force=request.forceFresh,
            allow_repair_override=True,
        ),
        collect_history_data(request.historyPeriod, request.historyLimit),
        cached_spot(
            force=request.forceFresh,
            allow_repair_override=True,
        ),
    )

    async with liquidation_lock:
        recent_events = list(liquidation_events)[-100:]

    liquidation_data = {
        "summary": await liquidation_summary(),
        "recentEvents": recent_events,
    }
    features = current_market_features(snapshot, spot)
    history_audit_warning: str | None = None
    try:
        canonical_packet = await asyncio.to_thread(latest_evidence_packet)
    except Exception as exc:
        # Migration-safe rollout: a manual request may still use the already
        # collected live evidence, but the missing durable history is explicit.
        canonical_packet = None
        history_audit_warning = (
            "Canonical historical evidence was unavailable: "
            f"{type(exc).__name__}: {str(exc)[:240]}"
        )
    history_data_as_of = (
        canonical_packet.get("dataAsOf") or canonical_packet.get("serverCreatedAt")
        if isinstance(canonical_packet, dict)
        else utc_iso()
    )
    history_evidence_id = (
        str(canonical_packet.get("snapshotId") or "")
        if isinstance(canonical_packet, dict)
        else "unpersisted-live-evidence"
    )
    historical_memory = await asyncio.to_thread(
        chad_history_evidence_package,
        features,
        data_as_of=history_data_as_of,
        evidence_snapshot_id=history_evidence_id,
    )
    evidence_hash = _analysis_evidence_hash(
        question=request.question,
        snapshot=snapshot,
        history=history,
        liquidation_data=liquidation_data,
        spot=spot,
        historical_memory=historical_memory,
    )

    cached_analysis = _cached_openai_analysis(evidence_hash)
    cache_hit = cached_analysis is not None
    chad_reactivation_gate.record_cache(hit=cache_hit)
    usage_governor.cache(cache_hit)

    lock_status = (
        prediction_ledger.forecast_lock_status(
            lock_key=evidence_hash,
            minimum_interval_seconds=CHAD_FORECAST_MIN_INTERVAL_SECONDS,
        )
        if LEDGER_ENABLED
        else {"allowed": False, "reason": "ledger_disabled", "predictionId": None}
    )
    if not cache_hit and not lock_status.get("allowed"):
        retry_after = int(lock_status.get("retryAfterSeconds") or 0)
        suffix = f" Retry after {retry_after} seconds." if retry_after else ""
        raise HTTPException(
            status_code=429,
            detail=(
                "A paid Chad call was blocked before OpenAI because a new immutable "
                f"forecast cannot be locked ({lock_status.get('reason')}).{suffix}"
            ),
        )

    grading_before = await grade_due_ledger_predictions(
        limit=CHAD_GRADING_MAX_PER_BATCH
    )
    freshness = build_freshness_report(snapshot, spot, history)
    ledger_performance = (
        prediction_ledger.performance_summary()
        if LEDGER_ENABLED
        else {"enabled": False, "learningReady": False}
    )
    calibration_profile = (
        prediction_ledger.calibration_profile()
        if LEDGER_ENABLED
        else {"learningReady": False, "gradedHorizons": 0}
    )
    previous_prediction = prediction_ledger.latest_prediction(include_analysis=True) if LEDGER_ENABLED else None
    similar_setups = (
        prediction_ledger.similar_setups(features, limit=3)
        if LEDGER_ENABLED and ledger_performance.get("learningReady")
        else []
    )

    if cached_analysis is not None:
        analysis, raw_response = cached_analysis
    else:
        allowed, reason = usage_governor.authorize(
            "openai_call", automatic=call_mode == "automatic"
        )
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"OpenAI cost circuit is open ({reason}).",
            )
        try:
            reservation = await asyncio.to_thread(
                reserve_chad_call,
                call_mode=call_mode,
                idempotency_key=(
                    f"auto:{auto_event.get('eventKey')}:{evidence_hash}"
                    if call_mode == "automatic" and isinstance(auto_event, dict)
                    else f"manual:{evidence_hash}"
                ),
                evidence_hash=evidence_hash,
                trigger_reason=(
                    str(auto_event.get("decisionReason") or auto_event.get("eventFamily") or "major TAG event")
                    if call_mode == "automatic" and isinstance(auto_event, dict)
                    else "User explicitly selected Ask Chad / Run Chad Now."
                ),
                event_id=(str(auto_event.get("eventKey")) if isinstance(auto_event, dict) else None),
                regime_fingerprint=(
                    str(auto_event.get("regimeFingerprint")) if isinstance(auto_event, dict) else None
                ),
                confirmations=(auto_event.get("confirmations") if isinstance(auto_event, dict) else []),
                evidence={
                    "historicalMemory": historical_memory,
                    "snapshotId": history_evidence_id,
                },
                paid_enabled=PAID_AI_ENABLED,
                automatic_enabled=OPENAI_AUTOMATIC_ENABLED,
            )
        except Exception as exc:
            # During a migration rollout the new detailed audit table may not
            # exist yet. Retain the pre-existing durable hard limit, disclose
            # the degraded audit state, and never use this fallback for auto.
            if call_mode != "manual":
                raise HTTPException(
                    status_code=503,
                    detail="Automatic Chad is disabled until its event audit schema is available.",
                ) from exc
            legacy_allowed, legacy_reason = await asyncio.to_thread(
                authorize_persistent_usage,
                "openai_call",
                daily_limit=OPENAI_DAILY_CALL_LIMIT,
                monthly_limit=OPENAI_MONTHLY_CALL_LIMIT,
            )
            reservation = {
                "reserved": legacy_allowed,
                "reason": legacy_reason,
                "callId": None,
                "auditStatus": "degraded-migration-pending",
                "auditError": f"{type(exc).__name__}: {str(exc)[:240]}",
            }
        if not reservation.get("reserved"):
            raise HTTPException(
                status_code=429,
                detail=f"Central OpenAI usage limit is closed ({reservation.get('reason')}).",
            )
        usage_governor.mark_bounded_test()
        try:
            analysis, raw_response = await request_chad_analysis(
                question=request.question,
                snapshot=snapshot,
                history=history,
                liquidation_data=liquidation_data,
                spot_data=spot,
                position_tag=request.positionTag,
                average_entry_usd=request.averageEntryUsd,
                ledger_performance=ledger_performance,
                calibration_profile=calibration_profile,
                similar_setups=similar_setups,
                freshness=freshness,
                historical_memory=historical_memory,
            )
        except Exception as exc:
            if reservation.get("callId"):
                await asyncio.to_thread(
                    finish_chad_call,
                    reservation["callId"],
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                )
            raise
        if reservation.get("callId"):
            await asyncio.to_thread(
                finish_chad_call,
                reservation["callId"],
                status="completed",
                provider_response=raw_response,
            )
        _store_openai_analysis(
            evidence_hash=evidence_hash,
            trigger=request.trigger,
            analysis=analysis,
            raw=raw_response,
        )

    analysis = apply_confidence_controls(analysis, freshness=freshness, calibration=calibration_profile)
    previous_state = None
    if isinstance(previous_prediction, dict):
        previous_analysis = (
            previous_prediction.get("analysis")
            if isinstance(previous_prediction.get("analysis"), dict)
            else {}
        )
        previous_state = str(
            previous_analysis.get("marketState")
            or previous_prediction.get("marketState")
            or ""
        )
    regime_debounce = chad_reactivation_gate.debounce_regime(
        candidate_state=str(analysis.get("marketState") or "insufficient_data"),
        previous_state=previous_state,
    )
    analysis["regimeDebounce"] = regime_debounce
    if not regime_debounce["confirmed"]:
        analysis["marketState"] = regime_debounce["effective"]
        why = analysis.get("whyItMatters")
        if not isinstance(why, list):
            why = []
        why.insert(
            0,
            (
                "A possible regime change is still inside the debounce window; "
                "Chad kept the prior effective state and did not lock a new forecast."
            ),
        )
        analysis["whyItMatters"] = why

    decision_change = (
        prediction_ledger.decision_delta(
            current_analysis=analysis,
            current_features=features,
            previous=previous_prediction,
        )
        if LEDGER_ENABLED
        else None
    )

    ledger_saved = False
    ledger_prediction_id: str | None = None
    ledger_error: str | None = None
    forecast_ledger = analysis.get("forecastLedger") if isinstance(analysis, dict) else None
    start_price = as_float(spot.get("priceUsd")) or as_float(snapshot.get("markPrice"))

    cache_hit = bool(raw_response.get("cacheHit"))
    if (
        LEDGER_ENABLED
        and bool(lock_status.get("allowed"))
        and bool(regime_debounce.get("confirmed"))
        and isinstance(forecast_ledger, dict)
        and start_price is not None
    ):
        try:
            locked_at = utc_iso()
            analysis["forecastLock"] = {
                "predictionId": lock_status.get("predictionId"),
                "evidenceHash": evidence_hash,
                "lockedAt": locked_at,
                "immutable": True,
            }
            ledger_prediction_id = prediction_ledger.save_prediction(
                model=str(raw_response.get("model", OPENAI_MODEL)),
                question=request.question,
                start_price_usd=start_price,
                market_cap_usd=as_float(spot.get("marketCapUsd")),
                market_state=str(analysis.get("marketState", "")),
                confidence=as_int(analysis.get("confidence")),
                data_quality=(
                    as_int(analysis.get("dataQuality", {}).get("score"))
                    if isinstance(analysis.get("dataQuality"), dict)
                    else None
                ),
                thesis=str(forecast_ledger.get("thesis", "")),
                horizons=(
                    forecast_ledger.get("horizons")
                    if isinstance(forecast_ledger.get("horizons"), list)
                    else []
                ),
                features=features,
                analysis=analysis,
                snapshot=snapshot,
                spot=spot,
                lock_key=evidence_hash,
            )
            ledger_saved = True
        except Exception as exc:
            ledger_error = str(exc)
    elif LEDGER_ENABLED:
        if cache_hit:
            ledger_prediction_id = lock_status.get("predictionId")
            ledger_error = (
                "Unchanged evidence reused the original locked forecast; "
                "no duplicate was created."
            )
        elif not regime_debounce.get("confirmed"):
            ledger_error = "Forecast was not locked while the regime change is unconfirmed."
        else:
            ledger_error = (
                "Prediction was not saved because forecastLedger or a valid start price was missing."
            )

    result: dict[str, Any] = {
        "ok": True,
        "generatedAt": utc_iso(),
        "symbol": SYMBOL,
        "model": raw_response.get("model", OPENAI_MODEL),
        "openAIResponseId": raw_response.get("id"),
        "cacheHit": cache_hit,
        "evidenceHash": evidence_hash,
        "openAIUsage": {
            "inputTokens": int(
                (raw_response.get("usage") or {}).get("input_tokens") or 0
            ),
            "outputTokens": int(
                (raw_response.get("usage") or {}).get("output_tokens") or 0
            ),
        },
        "analysis": analysis,
        "decisionChange": decision_change,
        "freshness": freshness,
        "predictionLedger": {
            "enabled": LEDGER_ENABLED,
            "saved": ledger_saved,
            "predictionId": ledger_prediction_id,
            "saveError": ledger_error,
            "lockStatus": lock_status,
            "regimeDebounce": regime_debounce,
            "gradedBeforeAnalysis": grading_before,
            "performance": prediction_ledger.performance_summary() if LEDGER_ENABLED else None,
            "calibration": prediction_ledger.calibration_profile() if LEDGER_ENABLED else None,
            "similarSetupsUsed": similar_setups,
            "storageWarning": prediction_ledger.persistent_hint if LEDGER_ENABLED else None,
            "storage": prediction_ledger.storage_status() if LEDGER_ENABLED else None,
            "automaticGrader": dict(grader_state) if LEDGER_ENABLED else None,
        },
        "dataUsed": {
            "snapshot": snapshot,
            "spot": spot,
            "historyPeriod": request.historyPeriod,
            "historyLimit": request.historyLimit,
            "historyErrors": history.get("errors", []),
            "liquidationSummary": liquidation_data["summary"],
            "historicalMemory": historical_memory,
        },
        "chadCall": {
            "mode": call_mode,
            "label": "MANUAL CHAD" if call_mode == "manual" else "AUTO CHAD — EXTREME EVENT",
            "triggerReason": (
                auto_event.get("decisionReason") if isinstance(auto_event, dict)
                else "User explicitly requested analysis."
            ),
            "eventId": auto_event.get("eventKey") if isinstance(auto_event, dict) else None,
            "paidProviderInvoked": not cache_hit,
            "auditStatus": (
                reservation.get("auditStatus", "complete")
                if not cache_hit
                else "cache-hit-no-new-spend"
            ),
            "historicalEvidenceWarning": history_audit_warning,
        },
    }

    if request.includeRawHistory:
        result["dataUsed"]["history"] = compact_history_for_chad(history)

    return result


@app.post("/v1/chad/analyze")
async def chad_analyze(
    request: ChadAnalyzeRequest,
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    if not PAID_AI_ENABLED:
        raise HTTPException(
            status_code=423,
            detail="Paid Chad/OpenAI calls are disabled for Phase 2.",
        )
    blockers = chad_reactivation_gate.blockers(
        key_configured=bool(OPENAI_API_KEY),
        relay_token_configured=bool(RELAY_TOKEN),
    )
    if blockers:
        raise HTTPException(
            status_code=423,
            detail=f"Chad reactivation gate is closed ({blockers[0]}).",
        )
    require_chad_access(x_relay_key)
    if not request.allowPaidCall:
        raise HTTPException(
            status_code=423,
            detail="Paid Chad analysis requires allowPaidCall=true.",
        )
    if not request.allowForecastWrite or not request.allowGrading:
        raise HTTPException(
            status_code=423,
            detail=(
                "Chad reactivation requires explicit forecast locking and grading "
                "approval on the request."
            ),
        )

    request_id, blocked_reason = chad_reactivation_gate.begin_request(
        key_configured=bool(OPENAI_API_KEY),
        relay_token_configured=bool(RELAY_TOKEN),
    )
    if request_id is None:
        raise HTTPException(
            status_code=429,
            detail=f"Chad request was blocked ({blocked_reason or 'circuit'}).",
        )

    started_at = time.time()
    request_budget: dict[str, Any] | None = None
    budget_object: Any = None
    error: BaseException | None = None
    success = False
    try:
        with request_budget_scope() as budget:
            budget_object = budget
            result = await _run_chad_analysis(request)
            request_budget = budget.summary()
            result["requestBudget"] = request_budget
            result["reactivationRequestId"] = request_id
            success = True
            return result
    except RequestBudgetExceeded as exc:
        if budget_object is not None:
            request_budget = budget_object.summary()
        error = exc
        raise HTTPException(
            status_code=429,
            detail=(
                "Chad stopped before exceeding its per-request "
                f"{exc.category} budget."
            ),
        ) from exc
    except BaseException as exc:
        if budget_object is not None:
            request_budget = budget_object.summary()
        error = exc
        raise
    finally:
        chad_reactivation_gate.finish_request(
            request_id,
            success=success,
            started_at=started_at,
            budget=request_budget,
            error=error,
        )



# ---------------------------------------------------------------------------
# TAG Terminal v0.5 compatibility and persistent-intelligence endpoints.
# These are additive. All v2.5 Durable Intelligence routes above remain intact.
# ---------------------------------------------------------------------------

async def collect_terminal_market(
    force: bool = False,
    *,
    manual_live: bool = False,
) -> dict[str, Any]:
    if REPAIR_MODE and not manual_live:
        return terminal_addon.stored_market()
    independent_task: asyncio.Task[list[Any]] | None = None
    if REPAIR_MODE and manual_live:
        await terminal_addon.ensure_external_clients()
        independent_task = asyncio.create_task(
            multi_exchange_service.collect_independent(),
            name="manual-independent-exchanges",
        )
    source_reads: list[Any] = [
        cached_snapshot(
            force=force,
            allow_repair_override=REPAIR_MODE and manual_live,
        ),
        cached_spot(
            force=force,
            allow_repair_override=REPAIR_MODE and manual_live,
        ),
    ]
    if independent_task is not None:
        source_reads.append(independent_task)
    source_results = await asyncio.gather(*source_reads)
    binance = source_results[0]
    spot = source_results[1]
    independent_results = source_results[2] if len(source_results) > 2 else None
    return await terminal_addon.collect_market(
        binance,
        spot,
        force=force,
        persist=False,
        independent_results=independent_results,
        include_server_history=not (REPAIR_MODE and manual_live),
    )


def require_terminal_admin(x_admin_key: str | None) -> None:
    from app.terminal_config import ADMIN_KEY

    if not ADMIN_KEY:
        raise HTTPException(status_code=503, detail="ADMIN_KEY is not configured.")
    supplied = (x_admin_key or "").strip()
    if not supplied or not hmac.compare_digest(supplied, ADMIN_KEY):
        raise HTTPException(status_code=401, detail="Missing or invalid X-Admin-Key.")


@app.get("/v1/tag/market")
async def terminal_market(
    force: bool = Query(False),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await collect_terminal_market(force=force)


@app.post("/v1/tag/client-snapshot")
async def terminal_client_snapshot(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    if REPAIR_MODE:
        return {
            "accepted": False,
            "repairMode": True,
            "message": "Repair mode is active; refreshes do not create database snapshots.",
        }
    # Legacy Android uploads are preserved for compatibility, but Phase 1 no
    # longer lets a device clock or device-computed aggregate become canonical
    # market history. The server stores it as a deduplicated candidate and
    # validates it asynchronously against a server evidence snapshot.
    idempotency_key = str(payload.get("idempotencyKey") or "").strip()
    if not idempotency_key:
        idempotency_key = f"legacy-android-snapshot:{stable_hash(payload)}"
    result = await asyncio.to_thread(
        persist_helper_candidate,
        {
            "idempotencyKey": idempotency_key,
            "jobId": payload.get("jobId"),
            "evidenceSnapshotId": payload.get("evidenceSnapshotId"),
            "producerId": payload.get("producerId") or "tagalysis-android-legacy",
            "modelVersion": payload.get("modelVersion") or "legacy-client-snapshot-v1",
            "origin": "android",
            "createdAt": payload.get("recordedAt"),
            "payload": payload,
        },
    )
    return {
        "accepted": True,
        "authoritative": False,
        "legacyCompatibility": True,
        **result,
    }


@app.get("/v1/tag/heatmap")
async def terminal_heatmap_endpoint(
    hours: int = Query(24, ge=1, le=168),
    bins: int = Query(32, ge=12, le=80),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return terminal_heatmap(hours, bins)


async def build_unified_forecast_ledger(
    limit: int = 200,
    *,
    grade: bool = False,
) -> dict[str, Any]:
    """Consolidate the deterministic Terminal ledger and OpenAI Chad ledger.

    The underlying stores remain independently auditable, while this read model
    prevents the Android app from presenting partial counts as the whole record.
    """
    safe_limit = min(max(int(limit), 1), 500)
    ledger_warning: str | None = None
    if LEDGER_ENABLED:
        try:
            if grade:
                prediction_ledger.initialize()
            grading = (
                await grade_due_ledger_predictions(limit=100)
                if grade
                else {
                    "enabled": True,
                    "graded": 0,
                    "pendingDue": 0,
                    "errors": [],
                    "status": "read-only; grading runs only in the enabled collector or an explicit grade action",
                }
            )
        except Exception as exc:
            ledger_warning = f"OpenAI Chad ledger unavailable: {type(exc).__name__}: {exc}"
            grading = {"enabled": True, "graded": 0, "pendingDue": 0, "errors": [ledger_warning]}
    else:
        grading = {"enabled": False, "graded": 0, "pendingDue": 0, "errors": []}
    terminal = terminal_prediction_ledger(safe_limit, grade=grade)
    terminal_records = []
    for row in terminal.get("reports", []):
        if not isinstance(row, dict):
            continue
        terminal_records.append({
            "source": "deterministic-terminal",
            "predictionId": None,
            "time": row.get("time"),
            "horizon": row.get("horizon"),
            "scenario": row.get("scenario"),
            "probability": row.get("probability"),
            "targetLow": row.get("targetLow"),
            "targetHigh": row.get("targetHigh"),
            "status": row.get("status"),
            "correct": row.get("correct"),
            "score": 100.0 if row.get("correct") is True else 0.0 if row.get("correct") is False else None,
            "outcome": row.get("outcome"),
            "postMortem": None,
        })

    try:
        openai_predictions = (
            prediction_ledger.list_predictions(limit=safe_limit, status="all", include_analysis=False)
            if LEDGER_ENABLED and ledger_warning is None
            else []
        )
    except Exception as exc:
        ledger_warning = ledger_warning or f"OpenAI Chad ledger unavailable: {type(exc).__name__}: {exc}"
        openai_predictions = []
    openai_records = []
    for prediction in openai_predictions:
        if not isinstance(prediction, dict):
            continue
        for horizon in prediction.get("horizons", []):
            if not isinstance(horizon, dict):
                continue
            openai_records.append({
                "source": "openai-chad",
                "predictionId": prediction.get("predictionId"),
                "time": prediction.get("createdAt"),
                "horizon": horizon.get("horizon"),
                "scenario": horizon.get("predictedDirection"),
                "probability": horizon.get("probability"),
                "targetLow": horizon.get("targetLowUsd"),
                "targetHigh": horizon.get("targetHighUsd"),
                "status": horizon.get("status"),
                "correct": horizon.get("directionCorrect"),
                "score": horizon.get("score"),
                "outcome": horizon.get("actualDirection"),
                "postMortem": horizon.get("postMortem"),
            })

    records = terminal_records + openai_records
    records.sort(key=lambda row: str(row.get("time") or ""), reverse=True)
    graded_records = [row for row in records if row.get("correct") is not None]
    pending_records = [row for row in records if str(row.get("status") or "").lower() in {"pending", "candidate"}]
    by_horizon: dict[str, dict[str, Any]] = {}
    for label in sorted({str(row.get("horizon")) for row in records if row.get("horizon")}):
        matching = [row for row in records if str(row.get("horizon")) == label]
        graded = [row for row in matching if row.get("correct") is not None]
        correct = [row for row in graded if row.get("correct") is True]
        scores = [float(row["score"]) for row in graded if isinstance(row.get("score"), (int, float))]
        by_horizon[label] = {
            "records": len(matching),
            "graded": len(graded),
            "correct": len(correct),
            "accuracyPct": round(len(correct) / len(graded) * 100.0, 1) if graded else None,
            "averageScore": round(sum(scores) / len(scores), 1) if scores else None,
            "calibrated": len(graded) >= 25,
        }

    try:
        openai_performance = prediction_ledger.performance_summary() if LEDGER_ENABLED and ledger_warning is None else {}
    except Exception as exc:
        ledger_warning = ledger_warning or f"OpenAI Chad ledger unavailable: {type(exc).__name__}: {exc}"
        openai_performance = {}
    required_ready_horizons = {"1h", "4h", "24h", "7d", "30d", "3mo"}
    calibrated_horizons = {
        label
        for label, value in by_horizon.items()
        if bool(value.get("calibrated"))
    }
    learning_ready = required_ready_horizons.issubset(calibrated_horizons)
    learning_status = (
        "READY"
        if learning_ready
        else "PARTIALLY CALIBRATED"
        if calibrated_horizons
        else "WARMING"
        if graded_records
        else "COLLECTING"
    )
    return {
        "generatedAt": utc_iso(),
        "status": learning_status,
        "predictionCount": (
            int(openai_performance.get("predictionCount") or 0)
            + len({(row.get("time"), row.get("source")) for row in terminal_records})
        ),
        "horizonRecordCount": len(records),
        "gradedCount": len(graded_records),
        "pendingCount": len(pending_records),
        "learningReady": learning_ready,
        "sourceCounts": {
            "openaiChadPredictions": int(openai_performance.get("predictionCount") or 0),
            "openaiChadHorizons": len(openai_records),
            "deterministicTerminalHorizons": len(terminal_records),
        },
        "byHorizon": by_horizon,
        "automaticGrader": grading,
        "records": records[:safe_limit],
        "warning": ledger_warning,
        "note": (
            "Both durable forecast stores are shown together and graded automatically from timestamped market history. "
            "They remain source-labelled so duplicate or incompatible records are never silently merged."
        ),
    }


@app.get("/v1/tag/predictions/unified")
async def terminal_unified_predictions_endpoint(
    limit: int = Query(200, ge=1, le=500),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await build_unified_forecast_ledger(limit)


@app.get("/v1/tag/predictions")
async def terminal_predictions_endpoint(
    limit: int = Query(50, ge=1, le=300),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return terminal_prediction_ledger(limit, grade=False)


@app.get("/v1/tag/alerts")
async def terminal_alerts_endpoint(
    limit: int = Query(30, ge=1, le=200),
    evaluate: bool = Query(False),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    if evaluate and REPAIR_MODE:
        raise HTTPException(
            status_code=423,
            detail="Repair mode is active; alert evaluation is paused.",
        )
    if evaluate:
        await collect_terminal_market()
        report = build_terminal_chad_report(store=True)
        evaluate_terminal_alerts(report)
    return terminal_alert_feed(limit)


@app.get("/v1/tag/alert-timeline")
async def terminal_alert_timeline_endpoint(
    limit: int = Query(100, ge=1, le=500),
    state_key: str | None = Query(default=None),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return terminal_alert_timeline(limit=limit, state_key=state_key)


@app.post("/v1/tag/alerts/test")
async def terminal_test_alert_endpoint(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("test-alert writes")
    alert = insert_test_alert()
    return {
        "ok": True,
        "testOnly": True,
        "message": "A test alert was inserted. The Android background worker should deliver it on its next check.",
        "alert": alert,
    }


@app.get("/v1/tag/paper")
async def terminal_paper_endpoint(
    limit: int = Query(100, ge=1, le=500),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return terminal_paper_ledger(
        limit=limit,
        evaluate=False,
        create_account=False,
    )


@app.post("/v1/tag/paper/orders")
async def terminal_paper_order_endpoint(
    payload: dict[str, Any] = Body(...),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("paper-order writes")
    try:
        return place_paper_order(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/v1/tag/paper/trades/{trade_id}/close")
async def terminal_paper_close_endpoint(
    trade_id: int,
    payload: dict[str, Any] | None = Body(default=None),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("paper-trade writes")
    reason = str((payload or {}).get("reason") or "manual close")[:60]
    try:
        return close_paper_trade(trade_id, reason=reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/v1/tag/paper/orders/{trade_id}/cancel")
async def terminal_paper_cancel_endpoint(
    trade_id: int,
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    require_repair_writes_unlocked("paper-order writes")
    try:
        return cancel_paper_trade(trade_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/v1/tag/social")
async def terminal_social_endpoint(
    limit: int = Query(100, ge=1, le=500),
    caller_id: int | None = Query(default=None),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return terminal_social_ledger(
        limit=limit,
        caller_id=caller_id,
        refresh_grades=False,
    )


@app.post("/v1/admin/social/calls/import")
async def terminal_social_import_endpoint(
    payload: dict[str, Any] = Body(...),
    x_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_terminal_admin(x_admin_key)
    require_repair_writes_unlocked("social-call imports")
    try:
        return {"ok": True, "call": ingest_social_call(payload)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/v1/admin/social/cmc/import")
async def terminal_social_cmc_import_endpoint(
    payload: dict[str, Any] = Body(...),
    x_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_terminal_admin(x_admin_key)
    require_repair_writes_unlocked("social-call imports")
    return {"ok": True, **ingest_cmc_posts_response(payload)}


@app.post("/v1/admin/social/research-timestamp")
async def terminal_social_timestamp_endpoint(
    payload: dict[str, Any] = Body(...),
    x_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_terminal_admin(x_admin_key)
    require_repair_writes_unlocked("social timestamp research")
    return {"ok": True, **research_timestamp(payload)}


@app.post("/v1/admin/social/cmc/history-enrich")
async def terminal_social_cmc_history_endpoint(
    limit: int = Query(5, ge=1, le=20),
    x_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_terminal_admin(x_admin_key)
    if REPAIR_MODE:
        raise HTTPException(
            status_code=423,
            detail="Repair mode is active; paid social enrichment is paused.",
        )
    return {"ok": True, **(await enrich_social_calls_with_cmc_quotes(limit=limit))}


@app.post("/v1/admin/social/cmc/poll")
async def terminal_social_cmc_poll_endpoint(
    x_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_terminal_admin(x_admin_key)
    if REPAIR_MODE:
        raise HTTPException(
            status_code=423,
            detail="Repair mode is active; social polling is paused.",
        )
    return {"ok": True, **(await poll_cmc_social_calls(force=True))}


@app.get("/v1/tag/chad")
async def terminal_chad_endpoint(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return build_terminal_chad_report(store=False)


@app.get("/v1/tag/chad/history")
async def terminal_chad_history_endpoint(
    limit: int = Query(30, ge=1, le=200),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return terminal_chad_history(limit)


@app.get("/v1/tag/forecast")
async def terminal_forecast_endpoint(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    report = build_terminal_chad_report(store=False)
    return {
        "generatedAt": report.get("generatedAt"),
        "futurePaths": report.get("futurePaths", []),
        "horizons": report.get("forecastHorizons", []),
        "opportunities": report.get("opportunities", []),
        "ledger": terminal_prediction_ledger(30, grade=False),
        "dataQuality": report.get("dataQuality"),
        "warnings": report.get("dataWarnings", []),
    }


@app.get("/v1/tag/patterns")
async def terminal_patterns_endpoint(
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    report = build_terminal_chad_report(store=False)
    specialists = report.get("specialistConsensus") if isinstance(report.get("specialistConsensus"), list) else []
    return {
        "generatedAt": report.get("generatedAt"),
        "regime": report.get("regime"),
        "matches": report.get("historicalAnalogs", []),
        "specialist": next(
            (
                row
                for row in specialists
                if isinstance(row, dict) and row.get("name") == "Pattern specialist"
            ),
            None,
        ),
        "status": (
            "TAG-specific analog memory is active"
            if report.get("historicalAnalogs")
            else "Collecting comparable TAG states"
        ),
    }


@app.get("/v1/tag/share-report", response_class=PlainTextResponse)
async def terminal_share_report_endpoint(
    x_relay_key: str | None = Header(default=None),
) -> str:
    require_relay_key(x_relay_key)
    return terminal_share_report_text(build_terminal_chad_report(store=False))


@app.get("/v1/tag/terminal")
async def terminal_bundle_endpoint(
    manual: bool = Query(False),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    cache_key = "manual" if REPAIR_MODE and manual else "stored"
    cache_entry = terminal_response_cache[cache_key]
    now = time.monotonic()
    cached = cache_entry.get("value")
    if (
        cached is not None
        and now - cache_entry["time"] < TERMINAL_RESPONSE_CACHE_SECONDS
    ):
        usage_governor.cache(True)
        return cached
    async with terminal_response_lock:
        cache_entry = terminal_response_cache[cache_key]
        now = time.monotonic()
        cached = cache_entry.get("value")
        if (
            cached is not None
            and now - cache_entry["time"] < TERMINAL_RESPONSE_CACHE_SECONDS
        ):
            usage_governor.cache(True)
            return cached
        if REPAIR_MODE and manual:
            allowed, reason = usage_governor.authorize("manual_market_read")
            if not allowed:
                raise HTTPException(
                    status_code=429,
                    detail=f"Manual market-read circuit is open ({reason}).",
                )
        allowed, reason = usage_governor.authorize("expensive_route")
        if not allowed and cached is not None:
            return cached
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Terminal response circuit is open ({reason}).",
            )
        usage_governor.cache(False)
        compact_task: asyncio.Task[dict[str, Any]] | None = None
        compact_block_reason: str | None = None
        manual_request_started = time.monotonic()
        if REPAIR_MODE and manual:
            compact_allowed, compact_block_reason = usage_governor.authorize(
                "bounded_intelligence_read"
            )
            if compact_allowed:
                compact_task = asyncio.create_task(
                    asyncio.to_thread(build_compact_terminal_payload),
                    name="bounded-neon-intelligence",
                )
        try:
            market = await collect_terminal_market(manual_live=manual)
        except Exception:
            if compact_task is not None and not compact_task.done():
                compact_task.cancel()
            raise
        packet_warnings: list[str] = []
        if REPAIR_MODE and manual:
            compact_payload: dict[str, Any] | None = None
            if compact_task is not None:
                try:
                    if compact_task.done():
                        compact_payload = compact_task.result()
                    else:
                        remaining = (
                            MANUAL_RESPONSE_BUDGET_SECONDS
                            - (time.monotonic() - manual_request_started)
                        )
                        if remaining <= 0:
                            compact_task.cancel()
                            raise TimeoutError
                        compact_payload = await asyncio.wait_for(
                            compact_task,
                            timeout=min(
                                INTELLIGENCE_READ_TIMEOUT_SECONDS,
                                remaining,
                            ),
                        )
                except TimeoutError:
                    packet_warnings.append(
                        "The bounded read-only intelligence slice exceeded its "
                        "time limit; live market data remains valid."
                    )
                except Exception as exc:
                    packet_warnings.append(
                        "The bounded read-only intelligence slice is temporarily "
                        f"unavailable ({type(exc).__name__}); live market data remains valid."
                    )
            else:
                packet_warnings.append(
                    "The bounded read-only intelligence circuit is open "
                    f"({compact_block_reason or 'limit'}); live market data remains valid."
                )
            if compact_payload is not None:
                terminal = merge_compact_intelligence(compact_payload, market)
                unified_predictions = (
                    terminal.pop("unifiedPredictions")
                    if isinstance(terminal.get("unifiedPredictions"), dict)
                    else {}
                )
            else:
                terminal = {
                    "generatedAt": utc_iso(),
                    "serverOiHistory": market.get("serverOiHistory") or {},
                    "heatmap": {},
                    "liquidations": {},
                    "chad": {
                        "regime": "MANUAL MARKET SNAPSHOT",
                        "summary": (
                            "Core spot and leverage data loaded. Stored forecasting, "
                            "alerts and history were deferred by the bounded read circuit."
                        ),
                        "recommendedPosture": "WAIT FOR COMPLETE CONFIRMATION",
                        "dataWarnings": list(packet_warnings),
                    },
                    "chadHistory": [],
                    "predictions": {},
                    "alerts": [],
                    "alertTimeline": {},
                    "paper": {},
                }
                unified_predictions = {}
        else:
            try:
                terminal = terminal_addon.build_terminal_payload(
                    store=False,
                    evaluate=False,
                )
            except Exception as exc:
                packet_warnings.append(
                    "Stored intelligence is temporarily unavailable; the core "
                    "market snapshot is still valid "
                    f"({type(exc).__name__})."
                )
                terminal = {
                    "generatedAt": utc_iso(),
                    "serverOiHistory": market.get("serverOiHistory") or {},
                    "heatmap": {},
                    "liquidations": {},
                    "chad": {
                        "regime": "MANUAL MARKET SNAPSHOT",
                        "summary": (
                            "Core spot and leverage data loaded. Stored forecasting, "
                            "alerts and history remain isolated while repair mode is active."
                        ),
                        "recommendedPosture": "WAIT FOR COMPLETE CONFIRMATION",
                        "dataWarnings": list(packet_warnings),
                    },
                    "chadHistory": [],
                    "predictions": {},
                    "alerts": [],
                    "alertTimeline": {},
                    "paper": {},
                }
            try:
                unified_predictions = await build_unified_forecast_ledger(
                    60,
                    grade=False,
                )
            except Exception as exc:
                packet_warnings.append(
                    "Stored forecast ledger is temporarily unavailable "
                    f"({type(exc).__name__})."
                )
                unified_predictions = {}
        if (
            packet_warnings
            and not (REPAIR_MODE and manual)
            and isinstance(terminal.get("chad"), dict)
        ):
            chad_warnings = terminal["chad"].get("dataWarnings")
            terminal["chad"]["dataWarnings"] = list(
                dict.fromkeys(
                    [
                        *(
                            chad_warnings
                            if isinstance(chad_warnings, list)
                            else []
                        ),
                        *packet_warnings,
                    ]
                )
            )
        result = {
            "generatedAt": terminal.get("generatedAt"),
            "spot": market.get("spot"),
            "futures": market.get("futures"),
            "binance": market.get("binance"),
            **terminal,
            "unifiedPredictions": unified_predictions,
            "operatingStatus": operating_status(SERVICE_VERSION),
            "manualLiveRead": bool(REPAIR_MODE and manual),
            "readOnly": True,
            "databaseWrites": 0,
            "automaticWorkStarted": False,
            "packetWarnings": packet_warnings,
        }
        cache_entry["time"] = time.monotonic()
        cache_entry["value"] = result
        return result


@app.post("/v1/admin/binance-vision/backfill")
async def terminal_backfill_endpoint(
    day: str | None = Query(default=None),
    month: str | None = Query(default=None),
    days: int = Query(2, ge=1, le=31),
    interval: str = Query("5m"),
    x_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_terminal_admin(x_admin_key)
    if REPAIR_MODE or not BACKFILL_ENABLED:
        raise HTTPException(
            status_code=423,
            detail="Backfills are disabled during repair mode.",
        )
    if day and month:
        raise HTTPException(status_code=400, detail="Use day or month, not both.")
    if day:
        return await terminal_backfill_day(day, interval)
    if month:
        return await terminal_backfill_month(month)
    return await terminal_backfill_recent(days, interval)
