from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.engine import Connection, Engine

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


class _SqlAlchemyRow:
    """Small mapping wrapper so SQLAlchemy rows behave like sqlite3.Row."""

    def __init__(self, row: Any) -> None:
        self._data = dict(row._mapping)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def keys(self):
        return self._data.keys()


class _SqlAlchemyResult:
    def __init__(self, result: Any) -> None:
        self._result = result

    def fetchone(self) -> _SqlAlchemyRow | None:
        row = self._result.fetchone()
        return _SqlAlchemyRow(row) if row is not None else None

    def fetchall(self) -> list[_SqlAlchemyRow]:
        return [_SqlAlchemyRow(row) for row in self._result.fetchall()]


class _SqlAlchemyConnection:
    """Compatibility layer for the small sqlite API surface used by PredictionLedger."""

    def __init__(self, engine: Engine) -> None:
        self._connection: Connection = engine.connect()
        self.total_changes = 0

    @staticmethod
    def _prepare(statement: str, params: Any = None) -> tuple[str, dict[str, Any]]:
        sql = statement.strip()
        if sql.upper().startswith("INSERT OR IGNORE INTO"):
            sql = "INSERT INTO" + sql[len("INSERT OR IGNORE INTO") :]
            sql = f"{sql} ON CONFLICT DO NOTHING"

        if params is None:
            return sql, {}
        if isinstance(params, dict):
            return sql, params

        values = tuple(params)
        placeholders = sql.count("?")
        if placeholders != len(values):
            raise ValueError(
                f"SQL placeholder count ({placeholders}) does not match parameter count ({len(values)})."
            )

        pieces = sql.split("?")
        rebuilt: list[str] = [pieces[0]]
        bindings: dict[str, Any] = {}
        for index, value in enumerate(values):
            name = f"p{index}"
            rebuilt.append(f":{name}")
            rebuilt.append(pieces[index + 1])
            bindings[name] = value
        return "".join(rebuilt), bindings

    def execute(self, statement: str, params: Any = None) -> _SqlAlchemyResult:
        sql, bindings = self._prepare(statement, params)
        result = self._connection.execute(sql_text(sql), bindings)
        command = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        if command in {"INSERT", "UPDATE", "DELETE"} and result.rowcount and result.rowcount > 0:
            self.total_changes += int(result.rowcount)
        return _SqlAlchemyResult(result)

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            statement = statement.strip()
            if statement:
                self.execute(statement)

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


class PredictionLedger:
    def __init__(
        self,
        db_path: str,
        *,
        deadband_pct: float = 1.0,
        max_records: int = 5000,
        backup_path: str | None = None,
        auto_backup: bool = True,
    ) -> None:
        self.db_path = db_path
        configured_database_url = (
            os.getenv("LEDGER_DATABASE_URL", "").strip()
            or os.getenv("TERMINAL_DATABASE_URL", "").strip()
        )
        self._database_url: str | None = None
        self._engine: Engine | None = None

        # The legacy caller still passes /tmp/tag_prediction_ledger.sqlite3.
        # When PostgreSQL is configured for TAG Terminal, use that durable database
        # for the ledger instead of temporary container storage.
        normalized_path = os.path.abspath(db_path)
        if configured_database_url and (
            normalized_path.startswith("/tmp/") or normalized_path == "/tmp"
        ):
            database_url = configured_database_url
            if database_url.startswith("postgres://"):
                database_url = "postgresql+psycopg://" + database_url[len("postgres://") :]
            elif database_url.startswith("postgresql://"):
                database_url = "postgresql+psycopg://" + database_url[len("postgresql://") :]
            self._database_url = database_url
            self._engine = create_engine(
                database_url,
                pool_pre_ping=True,
                future=True,
            )

        self.deadband_pct = max(0.1, float(deadband_pct))
        self.max_records = max(100, int(max_records))
        self.backup_path = (backup_path or f"{db_path}.backup.json").strip()
        self.auto_backup = bool(auto_backup)
        self._lock = threading.RLock()
        self._last_backup_error: str | None = None
        self._last_backup_at: str | None = None
        self._last_restore: dict[str, Any] | None = None

    @property
    def persistent_hint(self) -> str:
        if self._database_url:
            return "The ledger is stored durably in PostgreSQL through TERMINAL_DATABASE_URL."
        normalized = os.path.abspath(self.db_path)
        if normalized.startswith("/tmp/") or normalized == "/tmp":
            return (
                "The ledger is using temporary container storage. It can be lost after a Render "
                "restart or redeploy. Use LEDGER_DB_PATH on a persistent disk or configure "
                "TERMINAL_DATABASE_URL for durable memory."
            )
        return "The ledger is stored at the configured LEDGER_DB_PATH."

    def _connect(self) -> sqlite3.Connection | _SqlAlchemyConnection:
        if self._engine is not None:
            return _SqlAlchemyConnection(self._engine)

        parent = os.path.dirname(os.path.abspath(self.db_path))
        os.makedirs(parent, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def initialize(self) -> None:
        with self._lock, closing(self._connect()) as db:
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

        self._last_restore = self.restore_from_backup_if_empty()

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

        with self._lock, closing(self._connect()) as db:
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

        self._maybe_backup()
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
        with self._lock, closing(self._connect()) as db:
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
        with self._lock, closing(self._connect()) as db:
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

        self._maybe_backup()
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
        with self._lock, closing(self._connect()) as db:
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
        with self._lock, closing(self._connect()) as db:
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
        with self._lock, closing(self._connect()) as db:
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


    def prediction_count(self) -> int:
        with self._lock, closing(self._connect()) as db:
            return int(db.execute("SELECT COUNT(*) AS c FROM predictions").fetchone()["c"])

    def latest_prediction(self, *, include_analysis: bool = True) -> dict[str, Any] | None:
        rows = self.list_predictions(limit=1, status="all", include_analysis=include_analysis)
        return rows[0] if rows else None

    def calibration_profile(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db:
            rows = db.execute(
                """
                SELECT horizon, probability, direction_correct, range_hit, score, midpoint_error_pct
                FROM forecast_horizons
                WHERE status = 'graded'
                ORDER BY due_ts ASC
                """
            ).fetchall()

        bins = [(0, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 100)]
        probability_bins: list[dict[str, Any]] = []
        for low, high in bins:
            items = [row for row in rows if low <= int(row["probability"] or 0) <= high]
            probability_bins.append(
                {
                    "label": f"{low}-{high}%",
                    "count": len(items),
                    "statedProbabilityAvg": (
                        round(sum(float(row["probability"] or 0.0) for row in items) / len(items), 2)
                        if items else None
                    ),
                    "directionAccuracyPct": (
                        round(sum(int(row["direction_correct"] or 0) for row in items) / len(items) * 100.0, 2)
                        if items else None
                    ),
                    "rangeHitRatePct": (
                        round(sum(int(row["range_hit"] or 0) for row in items) / len(items) * 100.0, 2)
                        if items else None
                    ),
                }
            )

        by_horizon: dict[str, Any] = {}
        weak_horizons: list[str] = []
        for label in HORIZON_HOURS:
            items = [row for row in rows if row["horizon"] == label]
            avg_score = (
                sum(float(row["score"] or 0.0) for row in items) / len(items) if items else None
            )
            accuracy = (
                sum(int(row["direction_correct"] or 0) for row in items) / len(items) * 100.0
                if items else None
            )
            multiplier = None
            if len(items) >= 4 and avg_score is not None:
                multiplier = round(max(0.65, min(1.10, avg_score / 70.0)), 3)
                if multiplier < 0.85:
                    weak_horizons.append(label)
            by_horizon[label] = {
                "graded": len(items),
                "directionAccuracyPct": round(accuracy, 2) if accuracy is not None else None,
                "averageScore": round(avg_score, 2) if avg_score is not None else None,
                "confidenceMultiplier": multiplier,
            }

        graded = len(rows)
        average_score = (
            sum(float(row["score"] or 0.0) for row in rows) / graded if graded else None
        )
        confidence_cap = None
        if graded >= 8 and average_score is not None:
            confidence_cap = int(round(max(45.0, min(88.0, 45.0 + average_score * 0.55))))

        return {
            "learningReady": graded >= 8,
            "gradedHorizons": graded,
            "averageScore": round(average_score, 2) if average_score is not None else None,
            "suggestedConfidenceCap": confidence_cap,
            "byHorizon": by_horizon,
            "weakHorizons": weak_horizons,
            "probabilityCalibration": probability_bins,
            "note": (
                "Calibration is active and should be applied cautiously."
                if graded >= 8
                else "At least 8 graded horizons are required before calibration changes confidence."
            ),
            "generatedAt": utc_iso(),
        }

    def decision_delta(
        self,
        *,
        current_analysis: dict[str, Any],
        current_features: dict[str, Any],
        previous: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        previous = previous or self.latest_prediction(include_analysis=True)
        if not previous:
            return {
                "changed": False,
                "firstAnalysis": True,
                "summary": "This is the first durable server-side Chad analysis, so there is no prior view to compare.",
                "evidenceChanges": [],
            }

        prior_analysis = previous.get("analysis") if isinstance(previous.get("analysis"), dict) else {}
        prior_features = previous.get("features") if isinstance(previous.get("features"), dict) else {}
        prior_confidence = _as_float(prior_analysis.get("confidence"))
        current_confidence = _as_float(current_analysis.get("confidence"))
        prior_state = str(prior_analysis.get("marketState") or previous.get("marketState") or "unknown")
        current_state = str(current_analysis.get("marketState") or "unknown")

        evidence: list[dict[str, Any]] = []
        feature_labels = {
            "priceUsd": "price",
            "oiChange1hPct": "1h open interest change",
            "oiChange4hPct": "4h open interest change",
            "fundingRate": "funding",
            "takerBuySellRatio": "taker buy/sell ratio",
            "basisBps": "futures basis",
            "spotPriceChange1hPct": "1h spot change",
            "spotVolume1hUsd": "1h spot volume",
            "spotBuyShare1hPct": "1h spot buy transaction share",
        }
        for key, label in feature_labels.items():
            old = _as_float(prior_features.get(key))
            new = _as_float(current_features.get(key))
            if old is None or new is None:
                continue
            scale = FEATURE_SCALES.get(key, max(abs(old), 1e-9))
            normalized_change = abs(new - old) / max(scale, 1e-9)
            if normalized_change < 0.15:
                continue
            evidence.append(
                {
                    "field": key,
                    "label": label,
                    "previous": old,
                    "current": new,
                    "direction": "higher" if new > old else "lower",
                    "importance": (
                        "high" if normalized_change >= 1.0 else "medium" if normalized_change >= 0.4 else "low"
                    ),
                }
            )

        evidence.sort(key=lambda item: {"high": 3, "medium": 2, "low": 1}[item["importance"]], reverse=True)
        confidence_delta = (
            round(current_confidence - prior_confidence, 1)
            if current_confidence is not None and prior_confidence is not None
            else None
        )
        changed = prior_state != current_state or bool(evidence) or (confidence_delta not in (None, 0.0))
        summary_parts: list[str] = []
        if prior_state != current_state:
            summary_parts.append(f"Market state changed from {prior_state} to {current_state}.")
        if confidence_delta is not None and abs(confidence_delta) >= 1:
            summary_parts.append(
                f"Confidence {'rose' if confidence_delta > 0 else 'fell'} by {abs(confidence_delta):.0f} points."
            )
        if evidence:
            summary_parts.append(
                "Largest evidence changes: " + ", ".join(item["label"] for item in evidence[:3]) + "."
            )
        if not summary_parts:
            summary_parts.append("The new analysis is materially similar to the previous durable view.")

        return {
            "changed": changed,
            "firstAnalysis": False,
            "previousPredictionId": previous.get("predictionId"),
            "previousCreatedAt": previous.get("createdAt"),
            "previousHeadline": prior_analysis.get("headline"),
            "currentHeadline": current_analysis.get("headline"),
            "previousMarketState": prior_state,
            "currentMarketState": current_state,
            "previousConfidence": prior_confidence,
            "currentConfidence": current_confidence,
            "confidenceDelta": confidence_delta,
            "summary": " ".join(summary_parts),
            "evidenceChanges": evidence[:8],
        }

    def changes_history(self, *, limit: int = 25) -> list[dict[str, Any]]:
        predictions = list(reversed(self.list_predictions(
            limit=max(2, min(200, int(limit) + 1)),
            status="all",
            include_analysis=True,
        )))
        changes: list[dict[str, Any]] = []
        previous: dict[str, Any] | None = None
        for item in predictions:
            analysis = item.get("analysis") if isinstance(item.get("analysis"), dict) else {}
            if previous is not None:
                delta = self.decision_delta(
                    current_analysis=analysis,
                    current_features=item.get("features") if isinstance(item.get("features"), dict) else {},
                    previous=previous,
                )
                delta["predictionId"] = item.get("predictionId")
                delta["createdAt"] = item.get("createdAt")
                changes.append(delta)
            previous = item
        return list(reversed(changes[-max(1, min(100, int(limit))):]))

    def import_data(self, payload: dict[str, Any], *, merge: bool = True) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Ledger import body must be a JSON object.")
        if payload.get("format") != "tag-terminal-prediction-ledger-v1":
            raise ValueError("Unsupported ledger export format.")
        predictions = payload.get("predictions")
        if not isinstance(predictions, list):
            raise ValueError("Ledger export is missing a predictions array.")

        inserted_predictions = 0
        inserted_horizons = 0
        skipped = 0
        with self._lock, closing(self._connect()) as db:
            if not merge:
                db.execute("DELETE FROM forecast_horizons")
                db.execute("DELETE FROM predictions")

            for item in predictions:
                if not isinstance(item, dict):
                    skipped += 1
                    continue
                prediction_id = str(item.get("predictionId") or "").strip()
                start_price = _as_float(item.get("startPriceUsd"))
                created_at = str(item.get("createdAt") or "").strip()
                if not prediction_id or start_price is None or start_price <= 0 or not created_at:
                    skipped += 1
                    continue
                try:
                    created_ts = datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
                except Exception:
                    created_ts = time.time()
                before = db.total_changes
                db.execute(
                    """
                    INSERT OR IGNORE INTO predictions (
                        id, created_at, created_ts, model, question, start_price_usd,
                        market_cap_usd, market_state, confidence, data_quality, thesis,
                        features_json, analysis_json, snapshot_json, spot_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        prediction_id,
                        created_at,
                        created_ts,
                        item.get("model"),
                        item.get("question"),
                        start_price,
                        _as_float(item.get("marketCapUsd")),
                        item.get("marketState"),
                        item.get("confidence"),
                        item.get("dataQuality"),
                        item.get("thesis"),
                        _json(item.get("features") if isinstance(item.get("features"), dict) else {}),
                        _json(item.get("analysis") if isinstance(item.get("analysis"), dict) else {}),
                        _json(item.get("snapshot") if isinstance(item.get("snapshot"), dict) else {}),
                        _json(item.get("spot") if isinstance(item.get("spot"), dict) else {}),
                    ),
                )
                if db.total_changes > before:
                    inserted_predictions += 1

                horizons = item.get("horizons") if isinstance(item.get("horizons"), list) else []
                for h in horizons:
                    if not isinstance(h, dict):
                        continue
                    label = str(h.get("horizon") or "").strip()
                    if label not in HORIZON_HOURS:
                        continue
                    target_low = _as_float(h.get("targetLowUsd"))
                    target_high = _as_float(h.get("targetHighUsd"))
                    invalidation = _as_float(h.get("invalidationUsd"))
                    if target_low is None or target_high is None or invalidation is None:
                        continue
                    due_at = str(h.get("dueAt") or "").strip()
                    try:
                        due_ts = datetime.fromisoformat(due_at.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        due_ts = created_ts + HORIZON_HOURS[label] * 3600
                    before_h = db.total_changes
                    db.execute(
                        """
                        INSERT OR IGNORE INTO forecast_horizons (
                            prediction_id, horizon, horizon_hours, due_ts, predicted_direction, probability,
                            target_low_usd, target_high_usd, invalidation_usd, reasoning, status,
                            actual_price_usd, actual_at, actual_source, actual_direction, direction_correct,
                            range_hit, invalidation_hit, midpoint_error_pct, score, post_mortem
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            prediction_id,
                            label,
                            HORIZON_HOURS[label],
                            due_ts,
                            h.get("predictedDirection") or h.get("direction") or "sideways",
                            int(h.get("probability") or 0),
                            min(target_low, target_high),
                            max(target_low, target_high),
                            invalidation,
                            str(h.get("reasoning") or ""),
                            h.get("status") if h.get("status") in {"pending", "graded"} else "pending",
                            _as_float(h.get("actualPriceUsd")),
                            h.get("actualAt"),
                            h.get("actualSource"),
                            h.get("actualDirection"),
                            int(bool(h.get("directionCorrect"))) if h.get("directionCorrect") is not None else None,
                            int(bool(h.get("rangeHit"))) if h.get("rangeHit") is not None else None,
                            int(bool(h.get("invalidationHit"))) if h.get("invalidationHit") is not None else None,
                            _as_float(h.get("midpointErrorPct")),
                            _as_float(h.get("score")),
                            h.get("postMortem"),
                        ),
                    )
                    if db.total_changes > before_h:
                        inserted_horizons += 1
            self._prune_locked(db)
            db.commit()

        self._maybe_backup()
        return {
            "insertedPredictions": inserted_predictions,
            "insertedHorizons": inserted_horizons,
            "skippedPredictions": skipped,
            "predictionCount": self.prediction_count(),
            "merge": merge,
        }

    def backup_now(self) -> dict[str, Any]:
        if not self.backup_path:
            return {"ok": False, "error": "LEDGER_BACKUP_PATH is empty."}
        data = self.export_data(limit=self.max_records)
        encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        parent = os.path.dirname(os.path.abspath(self.backup_path))
        os.makedirs(parent, exist_ok=True)
        temp_path = f"{self.backup_path}.tmp"
        with open(temp_path, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, self.backup_path)
        self._last_backup_at = utc_iso()
        self._last_backup_error = None
        return {
            "ok": True,
            "path": self.backup_path,
            "bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "backedUpAt": self._last_backup_at,
            "predictionCount": len(data.get("predictions", [])),
        }

    def _maybe_backup(self) -> None:
        if not self.auto_backup:
            return
        try:
            self.backup_now()
        except Exception as exc:
            self._last_backup_error = f"{type(exc).__name__}: {exc}"

    def restore_from_backup_if_empty(self) -> dict[str, Any]:
        if self.prediction_count() > 0:
            return {"restored": False, "reason": "database_not_empty"}
        if not self.backup_path or not os.path.exists(self.backup_path):
            return {"restored": False, "reason": "backup_not_found"}
        try:
            with open(self.backup_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            result = self.import_data(payload, merge=True)
            return {"restored": True, **result}
        except Exception as exc:
            return {"restored": False, "reason": "restore_failed", "error": f"{type(exc).__name__}: {exc}"}

    def storage_status(self) -> dict[str, Any]:
        backup_path = os.path.abspath(self.backup_path) if self.backup_path else None
        backup_exists = bool(backup_path and os.path.exists(backup_path))

        if self._database_url:
            return {
                "backend": "postgresql",
                "dbPath": "PostgreSQL via TERMINAL_DATABASE_URL",
                "dbExists": True,
                "dbBytes": 0,
                "backupPath": self.backup_path,
                "backupExists": backup_exists,
                "backupBytes": os.path.getsize(backup_path) if backup_exists and backup_path else 0,
                "autoBackup": self.auto_backup,
                "lastBackupAt": self._last_backup_at,
                "lastBackupError": self._last_backup_error,
                "startupRestore": self._last_restore,
                "predictionCount": self.prediction_count(),
                "persistentHint": self.persistent_hint,
                "durableServerStorage": True,
                "recoveryReady": True,
            }

        db_path = os.path.abspath(self.db_path)
        db_exists = os.path.exists(db_path)
        return {
            "backend": "sqlite",
            "dbPath": self.db_path,
            "dbExists": db_exists,
            "dbBytes": os.path.getsize(db_path) if db_exists else 0,
            "backupPath": self.backup_path,
            "backupExists": backup_exists,
            "backupBytes": os.path.getsize(backup_path) if backup_exists and backup_path else 0,
            "autoBackup": self.auto_backup,
            "lastBackupAt": self._last_backup_at,
            "lastBackupError": self._last_backup_error,
            "startupRestore": self._last_restore,
            "predictionCount": self.prediction_count(),
            "persistentHint": self.persistent_hint,
            "durableServerStorage": not db_path.startswith("/tmp/"),
            "recoveryReady": backup_exists or not db_path.startswith("/tmp/"),
        }

