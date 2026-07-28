from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .terminal_config import DATABASE_URL
from .terminal_usage import account_database_statement, usage_governor


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class SpotSnapshotRow(Base):
    __tablename__ = "spot_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    price: Mapped[float | None] = mapped_column(Float)
    market_cap: Mapped[float | None] = mapped_column(Float)
    liquidity_usd: Mapped[float | None] = mapped_column(Float)
    price_change_1h: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class BinanceSnapshot(Base):
    __tablename__ = "binance_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    price: Mapped[float | None] = mapped_column(Float)
    open_interest_usd: Mapped[float | None] = mapped_column(Float)
    funding_rate: Mapped[float | None] = mapped_column(Float)
    global_long_short: Mapped[float | None] = mapped_column(Float)
    top_account_ratio: Mapped[float | None] = mapped_column(Float)
    top_position_ratio: Mapped[float | None] = mapped_column(Float)
    taker_ratio_1h: Mapped[float | None] = mapped_column(Float)
    taker_buy_usd_1h: Mapped[float | None] = mapped_column(Float)
    taker_sell_usd_1h: Mapped[float | None] = mapped_column(Float)
    book_imbalance_pct: Mapped[float | None] = mapped_column(Float)
    long_liq_usd_1h: Mapped[float | None] = mapped_column(Float)
    short_liq_usd_1h: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class ExchangeSnapshotRow(Base):
    __tablename__ = "exchange_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    exchange: Mapped[str] = mapped_column(String(40), index=True)
    symbol: Mapped[str] = mapped_column(String(40))
    available: Mapped[bool] = mapped_column(Boolean, default=False)
    mark_price: Mapped[float | None] = mapped_column(Float)
    open_interest_usd: Mapped[float | None] = mapped_column(Float)
    open_interest_tokens: Mapped[float | None] = mapped_column(Float)
    volume_usd_24h: Mapped[float | None] = mapped_column(Float)
    funding_rate: Mapped[float | None] = mapped_column(Float)
    price_change_24h: Mapped[float | None] = mapped_column(Float)
    bid_depth_1pct: Mapped[float | None] = mapped_column(Float)
    ask_depth_1pct: Mapped[float | None] = mapped_column(Float)
    source_status: Mapped[str] = mapped_column(String(30), default="unknown")
    payload_json: Mapped[str] = mapped_column(Text)


class AggregateSnapshotRow(Base):
    __tablename__ = "aggregate_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    coverage_key: Mapped[str] = mapped_column(String(200), index=True)
    price: Mapped[float | None] = mapped_column(Float)
    price_change_1h: Mapped[float | None] = mapped_column(Float)
    aggregate_oi_usd: Mapped[float | None] = mapped_column(Float)
    funding_pct: Mapped[float | None] = mapped_column(Float)
    active_exchange_count: Mapped[int] = mapped_column(Integer, default=0)
    payload_json: Mapped[str] = mapped_column(Text)


class ClientSnapshot(Base):
    __tablename__ = "client_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    coverage_key: Mapped[str] = mapped_column(String(200), index=True)
    price: Mapped[float | None] = mapped_column(Float)
    price_change_1h: Mapped[float | None] = mapped_column(Float)
    aggregate_oi_usd: Mapped[float | None] = mapped_column(Float)
    funding_pct: Mapped[float | None] = mapped_column(Float)
    active_exchange_count: Mapped[int] = mapped_column(Integer, default=0)
    payload_json: Mapped[str] = mapped_column(Text)


class TakerMinute(Base):
    __tablename__ = "taker_minutes"
    __table_args__ = (UniqueConstraint("minute_ms", name="uq_taker_minute"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    minute_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    buy_notional_usd: Mapped[float] = mapped_column(Float, default=0.0)
    sell_notional_usd: Mapped[float] = mapped_column(Float, default=0.0)
    buy_quantity: Mapped[float] = mapped_column(Float, default=0.0)
    sell_quantity: Mapped[float] = mapped_column(Float, default=0.0)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)


class LiquidationEvent(Base):
    __tablename__ = "liquidation_events"
    __table_args__ = (
        UniqueConstraint(
            "event_time_ms",
            "side",
            "price",
            "quantity",
            name="uq_liquidation_event",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    side: Mapped[str] = mapped_column(String(10), index=True)
    price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[float] = mapped_column(Float)
    notional_usd: Mapped[float] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class OrderBookSnapshot(Base):
    __tablename__ = "orderbook_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    mark_price: Mapped[float | None] = mapped_column(Float)
    bid_depth_1pct: Mapped[float | None] = mapped_column(Float)
    ask_depth_1pct: Mapped[float | None] = mapped_column(Float)
    imbalance_pct: Mapped[float | None] = mapped_column(Float)
    levels_json: Mapped[str] = mapped_column(Text)


class ChadReportRow(Base):
    __tablename__ = "chad_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    baseline_price: Mapped[float | None] = mapped_column(Float)
    regime: Mapped[str] = mapped_column(String(80), index=True)
    confidence: Mapped[float] = mapped_column(Float)
    data_quality: Mapped[float] = mapped_column(Float)
    scenario_6h: Mapped[str | None] = mapped_column(String(30))
    scenario_24h: Mapped[str | None] = mapped_column(String(30))
    outcome_6h: Mapped[str | None] = mapped_column(String(30))
    outcome_24h: Mapped[str | None] = mapped_column(String(30))
    correct_6h: Mapped[bool | None] = mapped_column(Boolean)
    correct_24h: Mapped[bool | None] = mapped_column(Boolean)
    payload_json: Mapped[str] = mapped_column(Text)


class ForecastRecordRow(Base):
    __tablename__ = "forecast_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    horizon_minutes: Mapped[int] = mapped_column(Integer, index=True)
    horizon_label: Mapped[str] = mapped_column(String(30), index=True)
    baseline_price: Mapped[float | None] = mapped_column(Float)
    regime: Mapped[str] = mapped_column(String(80), index=True)
    model_id: Mapped[str] = mapped_column(String(80), default="champion-rules-v1")
    scenario: Mapped[str | None] = mapped_column(String(40))
    probability: Mapped[float | None] = mapped_column(Float)
    target_low: Mapped[float | None] = mapped_column(Float)
    target_high: Mapped[float | None] = mapped_column(Float)
    outcome: Mapped[str | None] = mapped_column(String(40))
    correct: Mapped[bool | None] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(String(30), default="candidate")
    payload_json: Mapped[str] = mapped_column(Text)


class AlertEventRow(Base):
    __tablename__ = "alert_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    alert_type: Mapped[str] = mapped_column(String(50), index=True)
    severity: Mapped[str] = mapped_column(String(20), index=True)
    state_key: Mapped[str] = mapped_column(String(120), index=True)
    title: Mapped[str] = mapped_column(String(160))
    message: Mapped[str] = mapped_column(Text)
    price: Mapped[float | None] = mapped_column(Float)
    market_cap: Mapped[float | None] = mapped_column(Float)
    confidence: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class AlertTimelineRow(Base):
    __tablename__ = "alert_timeline"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    state_key: Mapped[str] = mapped_column(String(120), index=True)
    stage: Mapped[str] = mapped_column(String(30), index=True)
    alert_type: Mapped[str] = mapped_column(String(50), index=True)
    severity: Mapped[str] = mapped_column(String(20), default="info")
    title: Mapped[str] = mapped_column(String(160))
    message: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(80), default="Chad rules engine")
    evidence_hash: Mapped[str] = mapped_column(String(80), default="")
    price: Mapped[float | None] = mapped_column(Float)
    market_cap: Mapped[float | None] = mapped_column(Float)
    confidence: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class SocialCallerRow(Base):
    __tablename__ = "social_callers"
    __table_args__ = (UniqueConstraint("platform", "handle", name="uq_social_caller"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform: Mapped[str] = mapped_column(String(40), index=True)
    handle: Mapped[str] = mapped_column(String(120), index=True)
    display_name: Mapped[str] = mapped_column(String(160), default="")
    profile_url: Mapped[str | None] = mapped_column(Text)
    avatar_url: Mapped[str | None] = mapped_column(Text)
    verified: Mapped[bool] = mapped_column(Boolean, default=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    call_count: Mapped[int] = mapped_column(Integer, default=0)
    graded_count: Mapped[int] = mapped_column(Integer, default=0)
    wins: Mapped[int] = mapped_column(Integer, default=0)
    losses: Mapped[int] = mapped_column(Integer, default=0)
    win_rate_pct: Mapped[float | None] = mapped_column(Float)
    average_return_pct: Mapped[float | None] = mapped_column(Float)
    total_return_pct: Mapped[float | None] = mapped_column(Float)
    best_return_pct: Mapped[float | None] = mapped_column(Float)
    worst_return_pct: Mapped[float | None] = mapped_column(Float)
    grade: Mapped[str] = mapped_column(String(20), default="WARMING")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class SocialCallRow(Base):
    __tablename__ = "social_calls"
    __table_args__ = (UniqueConstraint("platform", "external_id", name="uq_social_call"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    caller_id: Mapped[int] = mapped_column(Integer, index=True)
    platform: Mapped[str] = mapped_column(String(40), index=True)
    external_id: Mapped[str] = mapped_column(String(160), index=True)
    post_url: Mapped[str | None] = mapped_column(Text)
    profile_url: Mapped[str | None] = mapped_column(Text)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    timestamp_status: Mapped[str] = mapped_column(String(40), default="unavailable")
    timestamp_source: Mapped[str] = mapped_column(String(120), default="")
    symbol: Mapped[str] = mapped_column(String(30), default="TAG", index=True)
    direction: Mapped[str] = mapped_column(String(20), default="NEUTRAL", index=True)
    text_content: Mapped[str] = mapped_column(Text, default="")
    entry_price: Mapped[float | None] = mapped_column(Float)
    entry_market_cap: Mapped[float | None] = mapped_column(Float)
    entry_price_status: Mapped[str] = mapped_column(String(40), default="unavailable")
    entry_price_source: Mapped[str] = mapped_column(String(120), default="")
    target_price: Mapped[float | None] = mapped_column(Float)
    invalidation_price: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(30), default="open", index=True)
    return_1h_pct: Mapped[float | None] = mapped_column(Float)
    return_4h_pct: Mapped[float | None] = mapped_column(Float)
    return_24h_pct: Mapped[float | None] = mapped_column(Float)
    return_3d_pct: Mapped[float | None] = mapped_column(Float)
    return_7d_pct: Mapped[float | None] = mapped_column(Float)
    max_favorable_pct: Mapped[float | None] = mapped_column(Float)
    max_adverse_pct: Mapped[float | None] = mapped_column(Float)
    outcome: Mapped[str | None] = mapped_column(String(40))
    grade_score: Mapped[float | None] = mapped_column(Float)
    grade: Mapped[str] = mapped_column(String(20), default="WARMING")
    why_result: Mapped[str] = mapped_column(Text, default="")
    evidence_json: Mapped[str] = mapped_column(Text, default="{}")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class PaperAccountRow(Base):
    __tablename__ = "paper_accounts"
    __table_args__ = (UniqueConstraint("account_key", name="uq_paper_account"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_key: Mapped[str] = mapped_column(String(80), index=True)
    name: Mapped[str] = mapped_column(String(160), default="TAG Derivatives Paper Wallet")
    starting_balance: Mapped[float] = mapped_column(Float, default=10000.0)
    cash_balance: Mapped[float] = mapped_column(Float, default=10000.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    total_fees: Mapped[float] = mapped_column(Float, default=0.0)
    total_funding: Mapped[float] = mapped_column(Float, default=0.0)
    closed_trades: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class PaperTradeRow(Base):
    __tablename__ = "paper_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    side: Mapped[str] = mapped_column(String(10), index=True)
    order_type: Mapped[str] = mapped_column(String(20), default="MARKET")
    margin_mode: Mapped[str] = mapped_column(String(20), default="ISOLATED")
    leverage: Mapped[float] = mapped_column(Float, default=1.0)
    margin_usdt: Mapped[float] = mapped_column(Float)
    quantity_tag: Mapped[float | None] = mapped_column(Float)
    requested_price: Mapped[float | None] = mapped_column(Float)
    trigger_price: Mapped[float | None] = mapped_column(Float)
    entry_price: Mapped[float | None] = mapped_column(Float)
    exit_price: Mapped[float | None] = mapped_column(Float)
    stop_loss: Mapped[float | None] = mapped_column(Float)
    take_profit: Mapped[float | None] = mapped_column(Float)
    liquidation_price: Mapped[float | None] = mapped_column(Float)
    highest_price: Mapped[float | None] = mapped_column(Float)
    lowest_price: Mapped[float | None] = mapped_column(Float)
    opening_fee: Mapped[float] = mapped_column(Float, default=0.0)
    closing_fee: Mapped[float] = mapped_column(Float, default=0.0)
    funding_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float | None] = mapped_column(Float)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float)
    return_on_margin_pct: Mapped[float | None] = mapped_column(Float)
    close_reason: Mapped[str | None] = mapped_column(String(60))
    signal_source: Mapped[str] = mapped_column(String(120), default="manual paper trade")
    alert_id: Mapped[int | None] = mapped_column(Integer, index=True)
    social_call_id: Mapped[int | None] = mapped_column(Integer, index=True)
    thesis: Mapped[str] = mapped_column(Text, default="")
    postmortem: Mapped[str] = mapped_column(Text, default="")
    grade: Mapped[str] = mapped_column(String(20), default="OPEN")
    assumptions_json: Mapped[str] = mapped_column(Text, default="{}")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class PaperEquityRow(Base):
    __tablename__ = "paper_equity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(Integer, index=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    cash_balance: Mapped[float] = mapped_column(Float)
    reserved_margin: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float)
    equity: Mapped[float] = mapped_column(Float)
    mark_price: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class VisionRow(Base):
    __tablename__ = "vision_rows"
    __table_args__ = (
        UniqueConstraint("dataset", "event_time_ms", "interval", name="uq_vision_row"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset: Mapped[str] = mapped_column(String(50), index=True)
    event_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    interval: Mapped[str] = mapped_column(String(20), default="")
    open_price: Mapped[float | None] = mapped_column(Float)
    high_price: Mapped[float | None] = mapped_column(Float)
    low_price: Mapped[float | None] = mapped_column(Float)
    close_price: Mapped[float | None] = mapped_column(Float)
    volume: Mapped[float | None] = mapped_column(Float)
    buy_notional_usd: Mapped[float | None] = mapped_column(Float)
    sell_notional_usd: Mapped[float | None] = mapped_column(Float)
    value: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class OpenAIAnalysisCacheRow(Base):
    __tablename__ = "openai_analysis_cache"

    evidence_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    model: Mapped[str] = mapped_column(String(120))
    trigger: Mapped[str] = mapped_column(String(80), default="explicit-user-request")
    request_id: Mapped[str | None] = mapped_column(String(160))
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    estimated_cost_usd: Mapped[float | None] = mapped_column(Float)
    cache_hit_count: Mapped[int] = mapped_column(Integer, default=0)
    response_json: Mapped[str] = mapped_column(Text)


connect_args: dict[str, Any] = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine_options: dict[str, Any] = {
    "pool_pre_ping": True,
    "connect_args": connect_args,
}
if not DATABASE_URL.startswith("sqlite"):
    # A small LIFO pool avoids Render opening the default five idle
    # connections against a serverless preview database.
    engine_options.update(
        pool_size=2,
        max_overflow=0,
        pool_timeout=10,
        pool_recycle=300,
        pool_use_lifo=True,
    )

engine = create_engine(DATABASE_URL, **engine_options)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


@event.listens_for(engine, "before_cursor_execute")
def _enforce_request_query_budget(
    _: Any,
    __: Any,
    statement: str,
    ___: Any,
    ____: Any,
    _____: Any,
) -> None:
    account_database_statement(statement)


@event.listens_for(engine, "after_cursor_execute")
def _account_query(
    _: Any,
    __: Any,
    statement: str,
    ___: Any,
    ____: Any,
    _____: Any,
) -> None:
    usage_governor.record("database_query")
    command = statement.lstrip().split(None, 1)[0].upper() if statement.strip() else ""
    if command in {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP"}:
        usage_governor.record("database_write")


def _migrate_postgres_timestamp_columns() -> None:
    """Upgrade millisecond epoch columns to BIGINT on existing PostgreSQL databases."""
    if engine.dialect.name != "postgresql":
        return

    statements = (
        "ALTER TABLE taker_minutes ALTER COLUMN minute_ms TYPE BIGINT USING minute_ms::bigint",
        "ALTER TABLE liquidation_events ALTER COLUMN event_time_ms TYPE BIGINT USING event_time_ms::bigint",
        "ALTER TABLE vision_rows ALTER COLUMN event_time_ms TYPE BIGINT USING event_time_ms::bigint",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def init_db() -> None:
    Base.metadata.create_all(engine)
    _migrate_postgres_timestamp_columns()
    # Existing PostgreSQL tables predate these composite indexes. create_all()
    # does not add them to an already-created table, so create them explicitly
    # and idempotently.
    statements = (
        "CREATE INDEX IF NOT EXISTS ix_aggregate_coverage_recorded ON aggregate_snapshots (coverage_key, recorded_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_forecast_due ON forecast_records (status, created_at, horizon_minutes)",
        "CREATE INDEX IF NOT EXISTS ix_alert_state_created ON alert_events (state_key, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_alert_timeline_state_created ON alert_timeline (state_key, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_social_calls_caller_discovered ON social_calls (caller_id, discovered_at DESC)",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def latest_client_snapshot(session: Session) -> ClientSnapshot | None:
    return session.scalar(
        select(ClientSnapshot).order_by(ClientSnapshot.recorded_at.desc()).limit(1)
    )


def latest_binance_snapshot(session: Session) -> BinanceSnapshot | None:
    return session.scalar(
        select(BinanceSnapshot).order_by(BinanceSnapshot.recorded_at.desc()).limit(1)
    )


def latest_aggregate_snapshot(session: Session) -> AggregateSnapshotRow | None:
    return session.scalar(
        select(AggregateSnapshotRow).order_by(AggregateSnapshotRow.recorded_at.desc()).limit(1)
    )
