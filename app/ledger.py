from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

HORIZON_HOURS: dict[str, int] = {
    "6h": 6,
    "24h": 24,
    "3d": 72,
    "7d": 168,
}

FEATURE_SCALES: dict[str, float] = {
    "oiChange1hPct": 5.0,
    "oiChange4hPct": 15.0,
    "fundingRate": 0.0002,
    "takerBuySellRatio": 0.50,
    "globalLongShortRatio": 0.50,
    "topPositionRatio": 0.50,
    "basisBps": 20.0,
    "spotPriceChange1hPct": 5.0,
    "spotPriceChange24hPct": 15.0,
    "spotBuyShare1hPct": 10.0,
}


def utc_iso(ts_seconds: float | None = None) -> str:
    ts = time.time() if ts_seconds is None else ts_seconds
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _load_json(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _actual_direction(start_price: float, actual_price: float, deadband_pct: float) -> str:
    move_pct = ((actual_price / start_price) - 1.0) * 100.0
    if move_pct > deadband_pct:
        return "up"
    if move_pct < -deadband_pct:
        return "down"
    return "sideways"


def _score_forecast(
    *,
    predicted_direction: str,
    actual_direction: str,
    target_low: float,
    target_high: float,
    actual_price: float,
) -> tuple[bool, bool, float, float]:
    low, high = sorted((target_low, target_high))
    range_hit = low <= actual_price <= high
    direction_correct = predicted_direction == actual_direction
    midpoint = (low + high) / 2.0
    midpoint_error_pct = (
        abs(actual_price - midpoint) / midpoint * 100.0 if midpoint > 0 else 100.0
    )

    if range_hit:
        score = 100.0
    elif direction_correct:
        score = max(55.0, 86.0 - min(31.0, midpoint_error_pct * 1.6))
    elif actual_direction == "sideways":
        score = max(25.0, 45.0 - min(20.0, midpoint_error_pct))
    else:
        score = max(0.0, 35.0 - min(35.0, midpoint_error_pct))

    return direction_correct, range_hit, midpoint_error_pct, round(score, 2)


def _post_mortem(
    *,
    predicted_direction: str,
    actual_direction: str,
    direction_correct: bool,
    range_hit: bool,
    midpoint_error_pct: float,
    invalidation_hit: bool,
    actual_price: float,
    target_low: float,
    target_high: float,
) -> str:
    low, high = sorted((target_low, target_high))
    if range_hit and direction_correct:
        return "Direction was correct and the realized price finished inside Chad's target range."
    if range_hit:
        return (
            "The realized price landed inside the target range, but the direction label did not "
            "match the deadband-based outcome."
        )
    if direction_correct:
        location = "above" if actual_price > high else "below"
        return (
            f"Direction was correct, but the move finished {location} the target range; "
            f"midpoint error was {midpoint_error_pct:.2f}%."
        )
    if invalidation_hit:
        return (
            f"Forecast expected {predicted_direction}, but the realized move was {actual_direction} "
            "and crossed the stated invalidation level."
        )
    return (
        f"Forecast expected {predicted_direction}, but the realized move was {actual_direction}; "
        f"midpoint error was {midpoint_error_pct:.2f}%."
    )


class PredictionLedger:
    def __init__(
        self,
        db_path: str,
        *,
        deadband_pct: float = 1.0,
        max_records: int = 5000,
    ) -> None:
        self.db_path = db_path
        self.deadband_pct = max(0.1, float(deadband_pct))
        self.max_records = max(100, int(max_records))
        self._lock = threading.RLock()

    @property
    def persistent_hint(self) -> str:
        normalized = os.path.abspath(self.db_path)
        if normalized.startswith("/tmp/") or normalized == "/tmp":
            return (
                "The ledger is using temporary container storage. It can be lost after a Render "
                "restart or redeploy. Use LEDGER_DB_PATH on a persistent disk for durable memory."
            )
        return "The ledger is stored at the configured LEDGER_DB_PATH."

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        os.makedirs(parent, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def initialize(self) -> None:
        with self._lock, self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS predictions (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    created_ts REAL NOT NULL,
                    model TEXT,
                    question TEXT,
                    start_price_usd REAL NOT NULL,
                    market_cap_usd REAL,
                    market_state TEXT,
                    confidence INTEGER,
                    data_quality INTEGER,
                    thesis TEXT,
                    features_json TEXT NOT NULL,
                    analysis_json TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    spot_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS forecast_horizons (
                    prediction_id TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    horizon_hours INTEGER NOT NULL,
                    due_ts REAL NOT NULL,
                    predicted_direction TEXT NOT NULL,
                    probability INTEGER NOT NULL,
                    target_low_usd REAL NOT NULL,
                    target_high_usd REAL NOT NULL,
                    invalidation_usd REAL NOT NULL,
                    reasoning TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    actual_price_usd REAL,
                    actual_at TEXT,
                    actual_source TEXT,
                    actual_direction TEXT,
                    direction_correct INTEGER,
                    range_hit INTEGER,
                    invalidation_hit INTEGER,
                    midpoint_error_pct REAL,
                    score REAL,
                    post_mortem TEXT,
                    PRIMARY KEY (prediction_id, horizon),
                    FOREIGN KEY (prediction_id) REFERENCES predictions(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_horizons_due
                    ON forecast_horizons(status, due_ts);
                CREATE INDEX IF NOT EXISTS idx_predictions_created
                    ON predictions(created_ts DESC);
                """
            )
            db.commit()

    def save_prediction(
        self,
        *,
        model: str,
        question: str,
        start_price_usd: float,
        market_cap_usd: float | None,
        market_state: str | None,
        confidence: int | None,
        data_quality: int | None,
        thesis: str,
        horizons: list[dict[str, Any]],
        features: dict[str, Any],
        analysis: dict[str, Any],
        snapshot: dict[str, Any],
        spot: dict[str, Any],
    ) -> str:
        prediction_id = uuid.uuid4().hex
        created_ts = time.time()
        created_at = utc_iso(created_ts)

        normalized: dict[str, dict[str, Any]] = {}
        for item in horizons:
            if not isinstance(item, dict):
                continue
            label = str(item.get("horizon", "")).strip()
            if label not in HORIZON_HOURS or label in normalized:
                continue
            direction = str(item.get("direction", "")).strip().lower()
            if direction not in {"up", "down", "sideways"}:
                continue
            target_low = _as_float(item.get("targetLowUsd"))
            target_high = _as_float(item.get("targetHighUsd"))
            invalidation = _as_float(item.get("invalidationUsd"))
            probability = item.get("probability")
            reasoning = str(item.get("reasoning", "")).strip()
            if (
                target_low is None
                or target_high is None
                or invalidation is None
                or target_low <= 0
                or target_high <= 0
                or invalidation <= 0
            ):
                continue
            try:
                probability_int = max(0, min(100, int(probability)))
            except (TypeError, ValueError):
                continue
            normalized[label] = {
                "direction": direction,
                "probability": probability_int,
                "targetLowUsd": min(target_low, target_high),
                "targetHighUsd": max(target_low, target_high),
                "invalidationUsd": invalidation,
                "reasoning": reasoning,
            }

        if set(normalized) != set(HORIZON_HOURS):
            missing = sorted(set(HORIZON_HOURS) - set(normalized))
            raise ValueError(f"Forecast ledger is missing valid horizons: {', '.join(missing)}")

        with self._lock, self._connect() as db:
            db.execute(
                """
                INSERT INTO predictions (
                    id, created_at, created_ts, model, question, start_price_usd,
                    market_cap_usd, market_state, confidence, data_quality, thesis,
                    features_json, analysis_json, snapshot_json, spot_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    created_at,
                    created_ts,
                    model,
                    question,
                    start_price_usd,
                    market_cap_usd,
                    market_state,
                    confidence,
                    data_quality,
                    thesis,
                    _json(features),
                    _json(analysis),
                    _json(snapshot),
                    _json(spot),
                ),
            )

            for label, item in normalized.items():
                hours = HORIZON_HOURS[label]
                db.execute(
                    """
                    INSERT INTO forecast_horizons (
                        prediction_id, horizon, horizon_hours, due_ts,
                        predicted_direction, probability, target_low_usd,
                        target_high_usd, invalidation_usd, reasoning
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        prediction_id,
                        label,
                        hours,
                        created_ts + hours * 3600,
                        item["direction"],
                        item["probability"],
                        item["targetLowUsd"],
                        item["targetHighUsd"],
                        item["invalidationUsd"],
                        item["reasoning"],
                    ),
                )

            self._prune_locked(db)
            db.commit()

        return prediction_id

    def _prune_locked(self, db: sqlite3.Connection) -> None:
        count = db.execute("SELECT COUNT(*) AS c FROM predictions").fetchone()["c"]
        excess = int(count) - self.max_records
        if excess <= 0:
            return
        rows = db.execute(
            """
            SELECT p.id
            FROM predictions p
            WHERE NOT EXISTS (
                SELECT 1 FROM forecast_horizons h
                WHERE h.prediction_id = p.id AND h.status = 'pending'
            )
            ORDER BY p.created_ts ASC
            LIMIT ?
            """,
            (excess,),
        ).fetchall()
        for row in rows:
            db.execute("DELETE FROM predictions WHERE id = ?", (row["id"],))

    def due_horizons(self, *, now_ts: float | None = None, limit: int = 50) -> list[dict[str, Any]]:
        now = time.time() if now_ts is None else now_ts
        with self._lock, self._connect() as db:
            rows = db.execute(
                """
                SELECT h.*, p.start_price_usd, p.created_at
                FROM forecast_horizons h
                JOIN predictions p ON p.id = h.prediction_id
                WHERE h.status = 'pending' AND h.due_ts <= ?
                ORDER BY h.due_ts ASC
                LIMIT ?
                """,
                (now, max(1, min(500, int(limit)))),
            ).fetchall()
        return [dict(row) for row in rows]

    def grade_horizon(
        self,
        *,
        prediction_id: str,
        horizon: str,
        actual_price_usd: float,
        actual_at: str,
        actual_source: str,
    ) -> dict[str, Any]:
        with self._lock, self._connect() as db:
            row = db.execute(
                """
                SELECT h.*, p.start_price_usd
                FROM forecast_horizons h
                JOIN predictions p ON p.id = h.prediction_id
                WHERE h.prediction_id = ? AND h.horizon = ?
                """,
                (prediction_id, horizon),
            ).fetchone()
            if row is None:
                raise KeyError("Forecast horizon was not found.")
            if row["status"] == "graded":
                return dict(row)

            start_price = float(row["start_price_usd"])
            target_low = float(row["target_low_usd"])
            target_high = float(row["target_high_usd"])
            invalidation = float(row["invalidation_usd"])
            predicted_direction = str(row["predicted_direction"])
            actual_direction = _actual_direction(
                start_price,
                actual_price_usd,
                self.deadband_pct,
            )
            direction_correct, range_hit, midpoint_error_pct, score = _score_forecast(
                predicted_direction=predicted_direction,
                actual_direction=actual_direction,
                target_low=target_low,
                target_high=target_high,
                actual_price=actual_price_usd,
            )
            invalidation_hit = (
                (predicted_direction == "up" and actual_price_usd <= invalidation)
                or (predicted_direction == "down" and actual_price_usd >= invalidation)
                or (
                    predicted_direction == "sideways"
                    and not (target_low <= actual_price_usd <= target_high)
                )
            )
            post_mortem = _post_mortem(
                predicted_direction=predicted_direction,
                actual_direction=actual_direction,
                direction_correct=direction_correct,
                range_hit=range_hit,
                midpoint_error_pct=midpoint_error_pct,
                invalidation_hit=invalidation_hit,
                actual_price=actual_price_usd,
                target_low=target_low,
                target_high=target_high,
            )

            db.execute(
                """
                UPDATE forecast_horizons
                SET status = 'graded', actual_price_usd = ?, actual_at = ?,
                    actual_source = ?, actual_direction = ?, direction_correct = ?,
                    range_hit = ?, invalidation_hit = ?, midpoint_error_pct = ?,
                    score = ?, post_mortem = ?
                WHERE prediction_id = ? AND horizon = ?
                """,
                (
                    actual_price_usd,
                    actual_at,
                    actual_source,
                    actual_direction,
                    int(direction_correct),
                    int(range_hit),
                    int(invalidation_hit),
                    midpoint_error_pct,
                    score,
                    post_mortem,
                    prediction_id,
                    horizon,
                ),
            )
            db.commit()

        return {
            "predictionId": prediction_id,
            "horizon": horizon,
            "actualPriceUsd": actual_price_usd,
            "actualAt": actual_at,
            "actualSource": actual_source,
            "actualDirection": actual_direction,
            "directionCorrect": direction_correct,
            "rangeHit": range_hit,
            "invalidationHit": invalidation_hit,
            "midpointErrorPct": round(midpoint_error_pct, 4),
            "score": score,
            "postMortem": post_mortem,
        }

    def performance_summary(self) -> dict[str, Any]:
        with self._lock, self._connect() as db:
            prediction_count = db.execute(
                "SELECT COUNT(*) AS c FROM predictions"
            ).fetchone()["c"]
            pending_count = db.execute(
                "SELECT COUNT(*) AS c FROM forecast_horizons WHERE status = 'pending'"
            ).fetchone()["c"]
            rows = db.execute(
                """
                SELECT horizon, probability, direction_correct, range_hit,
                       invalidation_hit, midpoint_error_pct, score
                FROM forecast_horizons
                WHERE status = 'graded'
                """
            ).fetchall()

        graded = len(rows)

        def metrics(items: list[sqlite3.Row]) -> dict[str, Any]:
            if not items:
                return {
                    "graded": 0,
                    "directionAccuracyPct": None,
                    "rangeHitRatePct": None,
                    "invalidationRatePct": None,
                    "averageMidpointErrorPct": None,
                    "averageScore": None,
                    "averageStatedProbability": None,
                }
            return {
                "graded": len(items),
                "directionAccuracyPct": round(
                    sum(int(row["direction_correct"] or 0) for row in items)
                    / len(items)
                    * 100.0,
                    2,
                ),
                "rangeHitRatePct": round(
                    sum(int(row["range_hit"] or 0) for row in items)
                    / len(items)
                    * 100.0,
                    2,
                ),
                "invalidationRatePct": round(
                    sum(int(row["invalidation_hit"] or 0) for row in items)
                    / len(items)
                    * 100.0,
                    2,
                ),
                "averageMidpointErrorPct": round(
                    sum(float(row["midpoint_error_pct"] or 0.0) for row in items)
                    / len(items),
                    3,
                ),
                "averageScore": round(
                    sum(float(row["score"] or 0.0) for row in items) / len(items),
                    2,
                ),
                "averageStatedProbability": round(
                    sum(float(row["probability"] or 0.0) for row in items) / len(items),
                    2,
                ),
            }

        by_horizon: dict[str, Any] = {}
        for label in HORIZON_HOURS:
            by_horizon[label] = metrics([row for row in rows if row["horizon"] == label])

        return {
            "predictionCount": int(prediction_count),
            "gradedHorizons": graded,
            "pendingHorizons": int(pending_count),
            "overall": metrics(rows),
            "byHorizon": by_horizon,
            "learningReady": graded >= 8,
            "learningNote": (
                "Enough graded horizons exist for cautious calibration."
                if graded >= 8
                else "Chad needs at least 8 graded horizons before treating performance as a meaningful learning signal."
            ),
            "deadbandPct": self.deadband_pct,
            "storage": {
                "dbPath": self.db_path,
                "warning": self.persistent_hint,
            },
            "generatedAt": utc_iso(),
        }

    def list_predictions(
        self,
        *,
        limit: int = 50,
        status: str = "all",
        include_analysis: bool = False,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(500, int(limit)))
        status = status if status in {"all", "pending", "graded"} else "all"
        with self._lock, self._connect() as db:
            if status == "all":
                prediction_rows = db.execute(
                    "SELECT * FROM predictions ORDER BY created_ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                prediction_rows = db.execute(
                    """
                    SELECT DISTINCT p.*
                    FROM predictions p
                    JOIN forecast_horizons h ON h.prediction_id = p.id
                    WHERE h.status = ?
                    ORDER BY p.created_ts DESC
                    LIMIT ?
                    """,
                    (status, limit),
                ).fetchall()

            result: list[dict[str, Any]] = []
            for row in prediction_rows:
                horizons = db.execute(
                    """
                    SELECT * FROM forecast_horizons
                    WHERE prediction_id = ?
                    ORDER BY horizon_hours ASC
                    """,
                    (row["id"],),
                ).fetchall()
                item = {
                    "predictionId": row["id"],
                    "createdAt": row["created_at"],
                    "model": row["model"],
                    "question": row["question"],
                    "startPriceUsd": row["start_price_usd"],
                    "marketCapUsd": row["market_cap_usd"],
                    "marketState": row["market_state"],
                    "confidence": row["confidence"],
                    "dataQuality": row["data_quality"],
                    "thesis": row["thesis"],
                    "features": _load_json(row["features_json"], {}),
                    "horizons": [
                        {
                            "horizon": h["horizon"],
                            "dueAt": utc_iso(float(h["due_ts"])),
                            "predictedDirection": h["predicted_direction"],
                            "probability": h["probability"],
                            "targetLowUsd": h["target_low_usd"],
                            "targetHighUsd": h["target_high_usd"],
                            "invalidationUsd": h["invalidation_usd"],
                            "reasoning": h["reasoning"],
                            "status": h["status"],
                            "actualPriceUsd": h["actual_price_usd"],
                            "actualAt": h["actual_at"],
                            "actualSource": h["actual_source"],
                            "actualDirection": h["actual_direction"],
                            "directionCorrect": (
                                bool(h["direction_correct"])
                                if h["direction_correct"] is not None
                                else None
                            ),
                            "rangeHit": (
                                bool(h["range_hit"]) if h["range_hit"] is not None else None
                            ),
                            "invalidationHit": (
                                bool(h["invalidation_hit"])
                                if h["invalidation_hit"] is not None
                                else None
                            ),
                            "midpointErrorPct": h["midpoint_error_pct"],
                            "score": h["score"],
                            "postMortem": h["post_mortem"],
                        }
                        for h in horizons
                    ],
                }
                if include_analysis:
                    item["analysis"] = _load_json(row["analysis_json"], {})
                result.append(item)
        return result

    def similar_setups(self, features: dict[str, Any], *, limit: int = 3) -> list[dict[str, Any]]:
        with self._lock, self._connect() as db:
            rows = db.execute(
                """
                SELECT p.id, p.created_at, p.start_price_usd, p.market_state,
                       p.features_json, h.predicted_direction, h.actual_direction,
                       h.actual_price_usd, h.score, h.post_mortem
                FROM predictions p
                JOIN forecast_horizons h ON h.prediction_id = p.id
                WHERE h.horizon = '24h' AND h.status = 'graded'
                ORDER BY p.created_ts DESC
                LIMIT 500
                """
            ).fetchall()

        scored: list[tuple[float, sqlite3.Row, list[str]]] = []
        for row in rows:
            past = _load_json(row["features_json"], {})
            distances: list[float] = []
            matched: list[str] = []
            for key, scale in FEATURE_SCALES.items():
                current_value = _as_float(features.get(key))
                past_value = _as_float(past.get(key))
                if current_value is None or past_value is None:
                    continue
                distances.append(abs(current_value - past_value) / scale)
                if abs(current_value - past_value) / scale <= 0.5:
                    matched.append(key)
            if len(distances) < 3:
                continue
            avg_distance = sum(distances) / len(distances)
            similarity = 100.0 / (1.0 + avg_distance)
            scored.append((similarity, row, matched))

        scored.sort(key=lambda item: item[0], reverse=True)
        result: list[dict[str, Any]] = []
        for similarity, row, matched in scored[: max(1, min(10, int(limit)))]:
            result.append(
                {
                    "predictionId": row["id"],
                    "createdAt": row["created_at"],
                    "similarityPct": round(similarity, 2),
                    "startPriceUsd": row["start_price_usd"],
                    "marketState": row["market_state"],
                    "forecastDirection24h": row["predicted_direction"],
                    "actualDirection24h": row["actual_direction"],
                    "actualPriceUsd24h": row["actual_price_usd"],
                    "score24h": row["score"],
                    "matchedFeatures": matched[:6],
                    "postMortem": row["post_mortem"],
                }
            )
        return result

    def export_data(self, *, limit: int = 5000) -> dict[str, Any]:
        return {
            "format": "tag-terminal-prediction-ledger-v1",
            "exportedAt": utc_iso(),
            "performance": self.performance_summary(),
            "predictions": self.list_predictions(
                limit=max(1, min(self.max_records, int(limit))),
                status="all",
                include_analysis=True,
            ),
        }
