from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    func,
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


class HistoricalMarketRow(Base):
    """Source-specific, point-in-time TAG history; conflicting venues never merge."""

    __tablename__ = "historical_market_rows"
    __table_args__ = (
        UniqueConstraint("source_row_key", name="uq_historical_market_source_key"),
        CheckConstraint("observed_at <= retrieved_at", name="ck_historical_market_time_order"),
        CheckConstraint(
            "category IN ('futures','cex_spot','dex_spot','liquidity','on_chain','catalyst','social','aggregate')",
            name="ck_historical_market_category",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_row_key: Mapped[str] = mapped_column(String(64), index=True)
    observation_hash: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str] = mapped_column(String(100), index=True)
    source_type: Mapped[str] = mapped_column(String(40), index=True)
    exchange: Mapped[str | None] = mapped_column(String(60), index=True)
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    contract_address: Mapped[str | None] = mapped_column(String(100), index=True)
    category: Mapped[str] = mapped_column(String(24), index=True)
    dataset: Mapped[str] = mapped_column(String(60), index=True)
    resolution: Mapped[str] = mapped_column(String(16), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    retrieved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    reliability_status: Mapped[str] = mapped_column(String(24), index=True)
    validation_status: Mapped[str] = mapped_column(String(24), index=True)
    open_price: Mapped[float | None] = mapped_column(Float)
    high_price: Mapped[float | None] = mapped_column(Float)
    low_price: Mapped[float | None] = mapped_column(Float)
    close_price: Mapped[float | None] = mapped_column(Float)
    base_volume: Mapped[float | None] = mapped_column(Float)
    quote_volume: Mapped[float | None] = mapped_column(Float)
    trade_count: Mapped[int | None] = mapped_column(Integer)
    taker_buy_quote: Mapped[float | None] = mapped_column(Float)
    taker_sell_quote: Mapped[float | None] = mapped_column(Float)
    market_cap_usd: Mapped[float | None] = mapped_column(Float)
    circulating_supply: Mapped[float | None] = mapped_column(Float)
    fdv_usd: Mapped[float | None] = mapped_column(Float)
    liquidity_usd: Mapped[float | None] = mapped_column(Float)
    mark_price: Mapped[float | None] = mapped_column(Float)
    index_price: Mapped[float | None] = mapped_column(Float)
    open_interest_usd: Mapped[float | None] = mapped_column(Float)
    open_interest_tokens: Mapped[float | None] = mapped_column(Float)
    funding_rate: Mapped[float | None] = mapped_column(Float)
    global_long_short_ratio: Mapped[float | None] = mapped_column(Float)
    top_account_ratio: Mapped[float | None] = mapped_column(Float)
    top_position_ratio: Mapped[float | None] = mapped_column(Float)
    taker_ratio: Mapped[float | None] = mapped_column(Float)
    long_liquidations_usd: Mapped[float | None] = mapped_column(Float)
    short_liquidations_usd: Mapped[float | None] = mapped_column(Float)
    basis_pct: Mapped[float | None] = mapped_column(Float)
    provenance_json: Mapped[str] = mapped_column(Text)
    values_json: Mapped[str] = mapped_column(Text)


class HistoricalBackfillRangeRow(Base):
    """Mutable operational checkpoint for resumable, bounded archive acquisition."""

    __tablename__ = "historical_backfill_ranges"
    __table_args__ = (
        UniqueConstraint(
            "source", "dataset", "symbol", "resolution", "range_start", "range_end",
            name="uq_historical_backfill_range",
        ),
        CheckConstraint("range_end > range_start", name="ck_historical_backfill_time_order"),
        CheckConstraint(
            "status IN ('pending','running','complete','partial','unavailable','failed')",
            name="ck_historical_backfill_status",
        ),
    )

    range_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(100), index=True)
    dataset: Mapped[str] = mapped_column(String(60), index=True)
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    resolution: Mapped[str] = mapped_column(String(16), index=True)
    range_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    range_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    rows_seen: Mapped[int] = mapped_column(Integer, default=0)
    rows_stored: Mapped[int] = mapped_column(Integer, default=0)
    cursor: Mapped[str | None] = mapped_column(String(200))
    archive_reference: Mapped[str | None] = mapped_column(Text)
    archive_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    last_error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class HistoricalEventVersionRow(Base):
    """Append-only version of a detected or named TAG historical episode."""

    __tablename__ = "historical_event_versions"
    __table_args__ = (
        UniqueConstraint("event_key", "event_version", name="uq_historical_event_version"),
        CheckConstraint("end_at >= start_at", name="ck_historical_event_time_order"),
        CheckConstraint("evidence_cutoff_at <= end_at", name="ck_historical_event_cutoff"),
    )

    event_version_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_key: Mapped[str] = mapped_column(String(120), index=True)
    event_version: Mapped[int] = mapped_column(Integer)
    event_name: Mapped[str] = mapped_column(String(160), index=True)
    event_family: Mapped[str] = mapped_column(String(60), index=True)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ignition_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    breakout_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    peak_trough_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    evidence_cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    start_price: Mapped[float] = mapped_column(Float)
    peak_price: Mapped[float] = mapped_column(Float)
    trough_price: Mapped[float] = mapped_column(Float)
    end_price: Mapped[float] = mapped_column(Float)
    percent_move: Mapped[float] = mapped_column(Float)
    duration_seconds: Mapped[int] = mapped_column(BigInteger)
    detection_version: Mapped[str] = mapped_column(String(80), index=True)
    success_classification: Mapped[str] = mapped_column(String(80), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    timeline_json: Mapped[str] = mapped_column(Text)
    features_json: Mapped[str] = mapped_column(Text)
    confirmation_json: Mapped[str] = mapped_column(Text)
    invalidation_json: Mapped[str] = mapped_column(Text)
    outcome_json: Mapped[str] = mapped_column(Text)
    provenance_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class HistoricalCoverageSnapshotRow(Base):
    __tablename__ = "historical_coverage_snapshots"
    __table_args__ = (
        UniqueConstraint("report_id", "month", "source", name="uq_historical_coverage_cell"),
        CheckConstraint(
            "coverage_status IN ('COMPLETE','STRONG','PARTIAL','MINIMAL','MISSING')",
            name="ck_historical_coverage_status",
        ),
    )

    coverage_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    report_id: Mapped[str] = mapped_column(String(64), index=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    month: Mapped[str] = mapped_column(String(7), index=True)
    source: Mapped[str] = mapped_column(String(100), index=True)
    first_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    row_count: Mapped[int] = mapped_column(Integer)
    resolutions_json: Mapped[str] = mapped_column(Text)
    fields_json: Mapped[str] = mapped_column(Text)
    coverage_status: Mapped[str] = mapped_column(String(16), index=True)
    missing_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class ForecastHistoricalContextRow(Base):
    """Frozen historical-memory run attached to one immutable canonical forecast."""

    __tablename__ = "forecast_historical_contexts"
    __table_args__ = (
        UniqueConstraint("forecast_id", name="uq_forecast_historical_context"),
        CheckConstraint(
            "status IN ('available','degraded','unavailable')",
            name="ck_forecast_historical_context_status",
        ),
    )

    context_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    context_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    forecast_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("canonical_forecasts.forecast_id"), index=True
    )
    producer: Mapped[str] = mapped_column(String(24), index=True)
    horizon: Mapped[str] = mapped_column(String(12), index=True)
    evidence_snapshot_id: Mapped[str] = mapped_column(String(64), index=True)
    engine_version: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(24), index=True)
    data_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    considered_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    analogs_json: Mapped[str] = mapped_column(Text)
    influenced_json: Mapped[str] = mapped_column(Text)
    override_json: Mapped[str] = mapped_column(Text)
    failure_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class HistoricalReplayRunRow(Base):
    __tablename__ = "historical_replay_runs"
    __table_args__ = (
        UniqueConstraint("run_hash", name="uq_historical_replay_run_hash"),
        CheckConstraint("training_end_at < evaluation_start_at", name="ck_historical_replay_no_lookahead"),
        CheckConstraint("evaluation_end_at >= evaluation_start_at", name="ck_historical_replay_eval_order"),
    )

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_hash: Mapped[str] = mapped_column(String(64), index=True)
    model_version: Mapped[str] = mapped_column(String(120))
    evaluation_kind: Mapped[str] = mapped_column(String(32), default="historical_replay")
    training_start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    training_end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    evaluation_start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    evaluation_end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    baseline_metrics_json: Mapped[str] = mapped_column(Text)
    analog_metrics_json: Mapped[str] = mapped_column(Text)
    comparison_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class MarketStructureRegimeVersionRow(Base):
    """Immutable online/retrospective regime evidence for research and replay."""

    __tablename__ = "market_structure_regime_versions"
    __table_args__ = (
        UniqueConstraint("regime_key", "version", name="uq_market_structure_regime_version"),
        CheckConstraint("effective_to >= effective_from", name="ck_market_structure_regime_time_order"),
    )

    regime_version_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    regime_key: Mapped[str] = mapped_column(String(120), index=True)
    version: Mapped[int] = mapped_column(Integer)
    detector_version: Mapped[str] = mapped_column(String(80), index=True)
    online_label: Mapped[str] = mapped_column(String(80), index=True)
    retrospective_label: Mapped[str | None] = mapped_column(String(80), index=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    effective_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    online_confidence: Mapped[float] = mapped_column(Float)
    retrospective_confidence: Mapped[float | None] = mapped_column(Float)
    features_json: Mapped[str] = mapped_column(Text)
    source_coverage_json: Mapped[str] = mapped_column(Text)
    missingness_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class ForecastResearchRunRow(Base):
    """Immutable deterministic replay, ablation, benchmark, or drift result."""

    __tablename__ = "forecast_research_runs"
    __table_args__ = (UniqueConstraint("run_hash", name="uq_forecast_research_run_hash"),)

    research_run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_hash: Mapped[str] = mapped_column(String(64), index=True)
    run_kind: Mapped[str] = mapped_column(String(48), index=True)
    model_version: Mapped[str] = mapped_column(String(120))
    horizon: Mapped[str | None] = mapped_column(String(12), index=True)
    evaluation_start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    evaluation_end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    raw_case_count: Mapped[int] = mapped_column(Integer)
    effective_sample_count: Mapped[int] = mapped_column(Integer)
    no_lookahead: Mapped[bool] = mapped_column(Boolean)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class FeatureReliabilityProfileRow(Base):
    """Versioned out-of-sample reliability by feature, horizon, and regime."""

    __tablename__ = "feature_reliability_profiles"
    __table_args__ = (
        UniqueConstraint("profile_hash", name="uq_feature_reliability_profile_hash"),
        CheckConstraint("sample_count >= 0", name="ck_feature_reliability_samples"),
    )

    profile_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    profile_hash: Mapped[str] = mapped_column(String(64), index=True)
    feature_family: Mapped[str] = mapped_column(String(80), index=True)
    horizon: Mapped[str] = mapped_column(String(12), index=True)
    regime: Mapped[str] = mapped_column(String(80), index=True)
    sample_count: Mapped[int] = mapped_column(Integer)
    effective_sample_count: Mapped[int] = mapped_column(Integer)
    skill_delta: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class ChadCallAuditRow(Base):
    """Durable manual/automatic paid-call reservation and provider-usage audit."""

    __tablename__ = "chad_call_audit"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_chad_call_audit_idempotency"),
        CheckConstraint("call_mode IN ('manual','automatic')", name="ck_chad_call_mode"),
        CheckConstraint(
            "status IN ('reserved','completed','failed','blocked')",
            name="ck_chad_call_status",
        ),
    )

    call_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), index=True)
    call_mode: Mapped[str] = mapped_column(String(16), index=True)
    label: Mapped[str] = mapped_column(String(40))
    trigger_reason: Mapped[str] = mapped_column(Text)
    event_id: Mapped[str | None] = mapped_column(String(120), index=True)
    evidence_hash: Mapped[str] = mapped_column(String(64), index=True)
    regime_fingerprint: Mapped[str | None] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    reserved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    estimated_cost_usd: Mapped[float | None] = mapped_column(Float)
    provider_request_id: Mapped[str | None] = mapped_column(String(160))
    confirmations_json: Mapped[str] = mapped_column(Text)
    evidence_json: Mapped[str] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)


class ChadAutoEventStateRow(Base):
    """One deterministic major-event decision used for auto-call dedupe/cooldown."""

    __tablename__ = "chad_auto_event_states"
    __table_args__ = (
        UniqueConstraint("event_key", "evidence_hash", name="uq_chad_auto_event_evidence"),
        CheckConstraint("confirmation_count >= 0", name="ck_chad_auto_confirmation_count"),
    )

    state_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_key: Mapped[str] = mapped_column(String(120), index=True)
    event_family: Mapped[str] = mapped_column(String(60), index=True)
    evidence_hash: Mapped[str] = mapped_column(String(64), index=True)
    regime_fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    confirmation_count: Mapped[int] = mapped_column(Integer)
    severity_score: Mapped[float] = mapped_column(Float)
    eligible: Mapped[bool] = mapped_column(Boolean, index=True)
    decision_reason: Mapped[str] = mapped_column(Text)
    cooldown_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    call_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("chad_call_audit.call_id"), index=True)
    confirmations_json: Mapped[str] = mapped_column(Text)
    evidence_json: Mapped[str] = mapped_column(Text)


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


class CanonicalEvidenceSnapshotRow(Base):
    """One immutable server-owned evidence packet.

    The evidence hash is the idempotency boundary shared by every producer.
    Device and helper clocks are retained only inside provenance payloads; the
    database/server timestamp is authoritative for receipt and persistence.
    """

    __tablename__ = "canonical_evidence_snapshots"

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    evidence_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    origin: Mapped[str] = mapped_column(String(40), index=True)
    producer_id: Mapped[str] = mapped_column(String(80))
    data_as_of: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    available_source_count: Mapped[int] = mapped_column(Integer, default=0)
    payload_json: Mapped[str] = mapped_column(Text)


class CanonicalEvidenceItemRow(Base):
    __tablename__ = "canonical_evidence_items"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "source_id",
            "category",
            name="uq_canonical_evidence_item",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("canonical_evidence_snapshots.snapshot_id", ondelete="CASCADE"),
        index=True,
    )
    source_id: Mapped[str] = mapped_column(String(100), index=True)
    source_name: Mapped[str] = mapped_column(String(100))
    source_type: Mapped[str] = mapped_column(String(40), index=True)
    category: Mapped[str] = mapped_column(String(40), index=True)
    priority: Mapped[int] = mapped_column(Integer)
    symbol_identity: Mapped[str] = mapped_column(String(160))
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    retrieved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    freshness: Mapped[str] = mapped_column(String(24), index=True)
    validation_status: Mapped[str] = mapped_column(String(24), index=True)
    degradation_status: Mapped[str] = mapped_column(String(24), index=True)
    origin: Mapped[str] = mapped_column(String(40), index=True)
    content_hash: Mapped[str] = mapped_column(String(64), index=True)
    provenance_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class AssetTruthSnapshotRow(Base):
    """Immutable verified asset-supply input used by issued forecasts."""

    __tablename__ = "asset_truth_snapshots"
    __table_args__ = (
        CheckConstraint("circulating_supply > 0", name="ck_asset_truth_positive_supply"),
        CheckConstraint(
            "verification_status = 'verified'",
            name="ck_asset_truth_verified",
        ),
    )

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    asset_symbol: Mapped[str] = mapped_column(String(30), index=True)
    network: Mapped[str] = mapped_column(String(80))
    contract_address: Mapped[str] = mapped_column(String(100), index=True)
    circulating_supply: Mapped[float] = mapped_column(Float)
    fully_diluted_supply: Mapped[float | None] = mapped_column(Float)
    source_name: Mapped[str] = mapped_column(String(160))
    source_reference: Mapped[str] = mapped_column(Text)
    verification_status: Mapped[str] = mapped_column(String(24), index=True)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    payload_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class PortfolioPositionSnapshotRow(Base):
    """Immutable persisted quantity/cost-basis snapshot; never a UI default."""

    __tablename__ = "portfolio_position_snapshots"
    __table_args__ = (
        CheckConstraint("quantity >= 0", name="ck_portfolio_snapshot_quantity"),
        CheckConstraint(
            "verification_status = 'verified'",
            name="ck_portfolio_snapshot_verified",
        ),
    )

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    portfolio_key: Mapped[str] = mapped_column(String(80), index=True)
    asset_symbol: Mapped[str] = mapped_column(String(30), index=True)
    quantity: Mapped[float] = mapped_column(Float)
    cost_basis_usd: Mapped[float | None] = mapped_column(Float)
    source_name: Mapped[str] = mapped_column(String(160))
    source_reference: Mapped[str] = mapped_column(Text)
    verification_status: Mapped[str] = mapped_column(String(24), index=True)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    payload_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class CanonicalForecastRow(Base):
    """One immutable forecast contract shared by every named producer."""

    __tablename__ = "canonical_forecasts"
    __table_args__ = (
        CheckConstraint(
            "producer IN ('tagalysis','chad','final_call','baseline','champion','challenger')",
            name="ck_canonical_forecast_producer",
        ),
        CheckConstraint(
            "horizon IN ('1h','4h','12h','24h','3d','7d','30d','3m','6m','1y','3y','5y')",
            name="ck_canonical_forecast_horizon",
        ),
        CheckConstraint(
            "direction IN ('HIGHER','LOWER','SIDEWAYS','NEUTRAL')",
            name="ck_canonical_forecast_direction",
        ),
        CheckConstraint(
            "status IN ('issued','active','completed','superseded','invalid','rejected')",
            name="ck_canonical_forecast_status",
        ),
        CheckConstraint("deadline > issued_at", name="ck_canonical_forecast_deadline"),
        CheckConstraint("data_as_of <= issued_at", name="ck_canonical_forecast_data_as_of"),
        CheckConstraint("current_price > 0", name="ck_canonical_forecast_current_price"),
        CheckConstraint("verified_supply > 0", name="ck_canonical_forecast_supply"),
        CheckConstraint("point_forecast > 0", name="ck_canonical_forecast_point"),
        CheckConstraint("p50 > 0", name="ck_canonical_forecast_p50"),
        CheckConstraint("q10 <= q25 AND q25 <= p50 AND p50 <= q75 AND q75 <= q90", name="ck_canonical_forecast_quantiles"),
        CheckConstraint(
            "probability_up >= 0 AND probability_up <= 1 "
            "AND probability_down >= 0 AND probability_down <= 1 "
            "AND probability_sideways >= 0 AND probability_sideways <= 1",
            name="ck_canonical_forecast_probabilities",
        ),
        CheckConstraint("confidence >= 0 AND confidence <= 100", name="ck_canonical_forecast_confidence"),
    )

    forecast_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    forecast_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    producer: Mapped[str] = mapped_column(String(24), index=True)
    evidence_snapshot_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("canonical_evidence_snapshots.snapshot_id"),
        index=True,
    )
    supply_snapshot_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("asset_truth_snapshots.snapshot_id"),
        index=True,
    )
    portfolio_snapshot_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("portfolio_position_snapshots.snapshot_id"),
        index=True,
    )
    revision_parent_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("canonical_forecasts.forecast_id"),
        index=True,
    )
    forecast_version: Mapped[str] = mapped_column(String(80))
    model_version: Mapped[str] = mapped_column(String(120))
    prompt_version: Mapped[str | None] = mapped_column(String(120))
    horizon: Mapped[str] = mapped_column(String(12), index=True)
    horizon_minutes: Mapped[int] = mapped_column(Integer)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    data_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    current_price: Mapped[float] = mapped_column(Float)
    verified_supply: Mapped[float] = mapped_column(Float)
    fully_diluted_supply: Mapped[float | None] = mapped_column(Float)
    portfolio_quantity: Mapped[float | None] = mapped_column(Float)
    portfolio_cost_basis_usd: Mapped[float | None] = mapped_column(Float)
    point_forecast: Mapped[float] = mapped_column(Float)
    p50: Mapped[float] = mapped_column(Float)
    q10: Mapped[float] = mapped_column(Float)
    q25: Mapped[float] = mapped_column(Float)
    q75: Mapped[float] = mapped_column(Float)
    q90: Mapped[float] = mapped_column(Float)
    probability_up: Mapped[float] = mapped_column(Float)
    probability_down: Mapped[float] = mapped_column(Float)
    probability_sideways: Mapped[float] = mapped_column(Float)
    direction: Mapped[str] = mapped_column(String(16), index=True)
    confidence: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(24), index=True)
    scenarios_json: Mapped[str] = mapped_column(Text)
    source_availability_json: Mapped[str] = mapped_column(Text)
    field_completeness_json: Mapped[str] = mapped_column(Text)
    freshness_json: Mapped[str] = mapped_column(Text)
    confidence_penalties_json: Mapped[str] = mapped_column(Text)
    green_confirmation_json: Mapped[str] = mapped_column(Text)
    red_invalidation_json: Mapped[str] = mapped_column(Text)
    evidence_summary: Mapped[str] = mapped_column(Text)
    evidence_references_json: Mapped[str] = mapped_column(Text)
    calibration_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class VerifiedOutcomeRow(Base):
    """Immutable exact-deadline price observation used for live grading."""

    __tablename__ = "verified_outcomes"
    __table_args__ = (
        UniqueConstraint("asset_symbol", "observed_at", "source_name", name="uq_verified_outcome_observation"),
        CheckConstraint("price_usd > 0", name="ck_verified_outcome_price"),
        CheckConstraint("verification_status = 'verified'", name="ck_verified_outcome_status"),
    )

    outcome_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    outcome_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    asset_symbol: Mapped[str] = mapped_column(String(30), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    retrieved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    price_usd: Mapped[float] = mapped_column(Float)
    source_name: Mapped[str] = mapped_column(String(160))
    source_reference: Mapped[str] = mapped_column(Text)
    evidence_snapshot_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("canonical_evidence_snapshots.snapshot_id"), index=True
    )
    verification_status: Mapped[str] = mapped_column(String(24), index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class CanonicalForecastGradeRow(Base):
    """Immutable grade; live, historical, producer and social memories never merge."""

    __tablename__ = "canonical_forecast_grades"
    __table_args__ = (
        UniqueConstraint(
            "subject_type", "subject_id", "evaluation_kind", "grade_version",
            name="uq_canonical_grade_subject_version",
        ),
        CheckConstraint(
            "producer IN ('tagalysis','chad','final_call','baseline','champion','challenger','social_call')",
            name="ck_canonical_grade_producer",
        ),
        CheckConstraint(
            "evaluation_kind IN ('live','historical_backtest')",
            name="ck_canonical_grade_kind",
        ),
        CheckConstraint("composite_score >= 0 AND composite_score <= 100", name="ck_canonical_grade_composite"),
    )

    grade_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    grade_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    subject_type: Mapped[str] = mapped_column(String(24), index=True)
    subject_id: Mapped[str] = mapped_column(String(80), index=True)
    forecast_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("canonical_forecasts.forecast_id"), index=True
    )
    outcome_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("verified_outcomes.outcome_id"), index=True
    )
    producer: Mapped[str] = mapped_column(String(24), index=True)
    horizon: Mapped[str] = mapped_column(String(12), index=True)
    evaluation_kind: Mapped[str] = mapped_column(String(24), index=True)
    grade_version: Mapped[str] = mapped_column(String(80))
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    graded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    independent_sample: Mapped[bool] = mapped_column(Boolean, index=True)
    independence_group: Mapped[str] = mapped_column(String(80), index=True)
    direction_correct: Mapped[bool] = mapped_column(Boolean)
    point_error_pct: Mapped[float] = mapped_column(Float)
    market_cap_error_pct: Mapped[float] = mapped_column(Float)
    position_value_error_pct: Mapped[float | None] = mapped_column(Float)
    interval_covered: Mapped[bool] = mapped_column(Boolean)
    interval_sharpness_pct: Mapped[float] = mapped_column(Float)
    weighted_interval_score: Mapped[float] = mapped_column(Float)
    probability_brier_score: Mapped[float] = mapped_column(Float)
    baseline_relative_skill: Mapped[float | None] = mapped_column(Float)
    volatility_tolerance_pct: Mapped[float] = mapped_column(Float)
    composite_score: Mapped[float] = mapped_column(Float)
    grade_label: Mapped[str] = mapped_column(String(32), index=True)
    metrics_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class ForecastInvalidationRuleRow(Base):
    """Immutable, predeclared rule that may invalidate a future forecast."""

    __tablename__ = "forecast_invalidation_rules"
    __table_args__ = (
        UniqueConstraint("rule_version", name="uq_forecast_invalidation_rule_version"),
        CheckConstraint("effective_at >= registered_at", name="ck_invalidation_rule_time_order"),
    )

    rule_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    rule_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    rule_version: Mapped[str] = mapped_column(String(80), index=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    trigger_type: Mapped[str] = mapped_column(String(80))
    threshold_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class ForecastEvaluationDispositionRow(Base):
    """One immutable terminal evaluation category per canonical forecast."""

    __tablename__ = "forecast_evaluation_dispositions"
    __table_args__ = (
        UniqueConstraint("forecast_id", name="uq_forecast_terminal_disposition"),
        CheckConstraint(
            "category IN ('valid_completed','prospectively_invalidated','ungradable','legacy_pre_repair','practice')",
            name="ck_forecast_disposition_category",
        ),
        CheckConstraint(
            "category <> 'prospectively_invalidated' OR (rule_id IS NOT NULL AND trigger_evidence_snapshot_id IS NOT NULL AND triggered_at IS NOT NULL)",
            name="ck_forecast_invalidation_provenance",
        ),
        CheckConstraint(
            "category <> 'valid_completed' OR grade_id IS NOT NULL",
            name="ck_forecast_valid_grade",
        ),
    )

    disposition_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    disposition_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    forecast_id: Mapped[str] = mapped_column(String(64), ForeignKey("canonical_forecasts.forecast_id"), index=True)
    category: Mapped[str] = mapped_column(String(40), index=True)
    grade_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("canonical_forecast_grades.grade_id"), index=True)
    rule_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("forecast_invalidation_rules.rule_id"), index=True)
    trigger_evidence_snapshot_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("canonical_evidence_snapshots.snapshot_id"), index=True
    )
    triggered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    warning_early_enough: Mapped[bool | None] = mapped_column(Boolean)
    invalidation_confirmed: Mapped[bool | None] = mapped_column(Boolean)
    reason: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class MarketRegimeRow(Base):
    __tablename__ = "market_regimes"
    __table_args__ = (UniqueConstraint("evidence_snapshot_id", "detector_version", name="uq_market_regime_snapshot_version"),)

    regime_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    evidence_snapshot_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("canonical_evidence_snapshots.snapshot_id"), index=True
    )
    detector_version: Mapped[str] = mapped_column(String(80))
    regime: Mapped[str] = mapped_column(String(80), index=True)
    confidence: Mapped[float] = mapped_column(Float)
    data_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    features_json: Mapped[str] = mapped_column(Text)
    reasons_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class PatternSequenceRow(Base):
    __tablename__ = "pattern_sequences"
    __table_args__ = (
        CheckConstraint("memory_kind IN ('live','historical_backtest')", name="ck_pattern_sequence_memory_kind"),
        CheckConstraint("ended_at >= started_at", name="ck_pattern_sequence_time_order"),
    )

    sequence_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    sequence_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    evidence_snapshot_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("canonical_evidence_snapshots.snapshot_id"), index=True
    )
    regime_id: Mapped[str] = mapped_column(String(64), ForeignKey("market_regimes.regime_id"), index=True)
    memory_kind: Mapped[str] = mapped_column(String(24), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    precursor_json: Mapped[str] = mapped_column(Text)
    timeline_json: Mapped[str] = mapped_column(Text)
    outcome_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class HistoricalAnalogRow(Base):
    __tablename__ = "historical_analogs"
    __table_args__ = (UniqueConstraint("current_sequence_id", "historical_sequence_id", "matcher_version", name="uq_historical_analog_pair"),)

    analog_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    current_sequence_id: Mapped[str] = mapped_column(String(64), ForeignKey("pattern_sequences.sequence_id"), index=True)
    historical_sequence_id: Mapped[str] = mapped_column(String(64), ForeignKey("pattern_sequences.sequence_id"), index=True)
    matcher_version: Mapped[str] = mapped_column(String(80))
    similarity_score: Mapped[float] = mapped_column(Float, index=True)
    sample_size: Mapped[int] = mapped_column(Integer)
    current_validity: Mapped[str] = mapped_column(String(24), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    matching_conditions_json: Mapped[str] = mapped_column(Text)
    differences_json: Mapped[str] = mapped_column(Text)
    prior_outcomes_json: Mapped[str] = mapped_column(Text)
    failure_reasons_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class LearningVersionRow(Base):
    __tablename__ = "learning_versions"
    __table_args__ = (
        CheckConstraint("decision IN ('candidate','champion','rollback')", name="ck_learning_version_decision"),
        CheckConstraint("minimum_samples > 0 AND independent_samples >= 0", name="ck_learning_version_samples"),
    )

    version_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    parent_version_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("learning_versions.version_id"), index=True)
    rollback_of_version_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("learning_versions.version_id"), index=True)
    component: Mapped[str] = mapped_column(String(80), index=True)
    producer: Mapped[str] = mapped_column(String(24), index=True)
    horizon: Mapped[str] = mapped_column(String(12), index=True)
    regime: Mapped[str] = mapped_column(String(80), index=True)
    decision: Mapped[str] = mapped_column(String(24), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    minimum_samples: Mapped[int] = mapped_column(Integer)
    independent_samples: Mapped[int] = mapped_column(Integer)
    out_of_sample_improvement: Mapped[float | None] = mapped_column(Float)
    weights_json: Mapped[str] = mapped_column(Text)
    walk_forward_json: Mapped[str] = mapped_column(Text)
    comparison_json: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class AlertCaseRow(Base):
    __tablename__ = "canonical_alert_cases"

    alert_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    case_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    alert_type: Mapped[str] = mapped_column(String(80), index=True)
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    initial_evidence_snapshot_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("canonical_evidence_snapshots.snapshot_id"), index=True
    )
    level_version_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("user_market_cap_level_versions.level_version_id"), index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    payload_json: Mapped[str] = mapped_column(Text)


class AlertStageEventRow(Base):
    __tablename__ = "canonical_alert_stage_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_canonical_alert_event_idempotency"),
        UniqueConstraint("alert_id", "sequence_number", name="uq_canonical_alert_event_sequence"),
        CheckConstraint(
            "stage IN ('OBSERVING','EARLY WATCH','DEVELOPING','CONFIRMED','URGENT ACTION')",
            name="ck_canonical_alert_event_stage",
        ),
        CheckConstraint("signal_score >= 0 AND signal_score <= 100", name="ck_canonical_alert_event_score"),
    )

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    alert_id: Mapped[str] = mapped_column(String(64), ForeignKey("canonical_alert_cases.alert_id"), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), index=True)
    sequence_number: Mapped[int] = mapped_column(Integer)
    stage: Mapped[str] = mapped_column(String(32), index=True)
    stage_changed: Mapped[bool] = mapped_column(Boolean)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    evidence_snapshot_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("canonical_evidence_snapshots.snapshot_id"), index=True
    )
    evidence_hash: Mapped[str] = mapped_column(String(64), index=True)
    signal_score: Mapped[float] = mapped_column(Float)
    price_usd: Mapped[float | None] = mapped_column(Float)
    market_cap_usd: Mapped[float | None] = mapped_column(Float)
    notification_allowed: Mapped[bool] = mapped_column(Boolean)
    reason: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)


class AlertOutcomeRow(Base):
    __tablename__ = "canonical_alert_outcomes"
    __table_args__ = (
        CheckConstraint(
            "result_class IN ('early','timely','late','false_alarm','missed')",
            name="ck_canonical_alert_outcome_class",
        ),
    )

    outcome_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    audit_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    alert_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("canonical_alert_cases.alert_id"), index=True)
    finalized_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    final_outcome: Mapped[str] = mapped_column(String(80), index=True)
    result_class: Mapped[str] = mapped_column(String(24), index=True)
    confirmation_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expiration_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    invalidation_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lead_time_seconds: Mapped[float | None] = mapped_column(Float)
    maximum_favorable_pct: Mapped[float | None] = mapped_column(Float)
    maximum_adverse_pct: Mapped[float | None] = mapped_column(Float)
    payload_json: Mapped[str] = mapped_column(Text)


class UserMarketCapLevelVersionRow(Base):
    __tablename__ = "user_market_cap_level_versions"
    __table_args__ = (
        UniqueConstraint("owner_key", "level_key", "version", name="uq_user_level_version"),
        CheckConstraint("low_usd > 0 AND high_usd >= low_usd", name="ck_user_level_range"),
        CheckConstraint("version > 0", name="ck_user_level_version_positive"),
    )

    level_version_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    parent_version_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("user_market_cap_level_versions.level_version_id"), index=True)
    owner_key: Mapped[str] = mapped_column(String(80), index=True)
    level_key: Mapped[str] = mapped_column(String(80), index=True)
    version: Mapped[int] = mapped_column(Integer)
    label: Mapped[str] = mapped_column(String(160))
    low_usd: Mapped[float] = mapped_column(Float)
    high_usd: Mapped[float] = mapped_column(Float)
    meaning: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean)
    source: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    payload_json: Mapped[str] = mapped_column(Text)


class ServerJobRow(Base):
    __tablename__ = "server_jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_type: Mapped[str] = mapped_column(String(80), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    origin: Mapped[str] = mapped_column(String(40), index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    locked_by: Mapped[str | None] = mapped_column(String(120), index=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    evidence_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    result_json: Mapped[str | None] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)


class HelperCandidateRow(Base):
    __tablename__ = "helper_candidates"

    candidate_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    job_id: Mapped[str | None] = mapped_column(String(64), index=True)
    evidence_snapshot_id: Mapped[str | None] = mapped_column(String(64), index=True)
    producer_id: Mapped[str] = mapped_column(String(100))
    model_version: Mapped[str] = mapped_column(String(100))
    origin: Mapped[str] = mapped_column(String(24), index=True)
    client_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    server_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(24), index=True)
    payload_hash: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    validation_json: Mapped[str | None] = mapped_column(Text)


class UsageCounterRow(Base):
    __tablename__ = "usage_counters"

    category: Mapped[str] = mapped_column(String(80), primary_key=True)
    window_type: Mapped[str] = mapped_column(String(16), primary_key=True)
    window_key: Mapped[str] = mapped_column(String(16), primary_key=True)
    used_count: Mapped[int] = mapped_column(Integer, default=0)
    byte_count: Mapped[int] = mapped_column(BigInteger, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class RequestCacheRow(Base):
    __tablename__ = "request_cache"

    cache_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    category: Mapped[str] = mapped_column(String(80), index=True)
    evidence_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    origin: Mapped[str] = mapped_column(String(40), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class ProviderUsageSnapshotRow(Base):
    """Immutable, normalized provider billing/allowance observation."""

    __tablename__ = "provider_usage_snapshots"
    __table_args__ = (
        UniqueConstraint("provider", "fingerprint", name="uq_provider_usage_fingerprint"),
        CheckConstraint(
            "value_status IN ('EXACT','ESTIMATED','MANUAL','STALE','UNAVAILABLE')",
            name="ck_provider_usage_value_status",
        ),
        CheckConstraint(
            "status IN ('GOOD','CAUTION','DANGER','BLOCKED')",
            name="ck_provider_usage_status",
        ),
    )

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    provider: Mapped[str] = mapped_column(String(40), index=True)
    fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    source_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    cycle_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cycle_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    value_status: Mapped[str] = mapped_column(String(16), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


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


def _migrate_canonical_horizon_constraints() -> None:
    """Extend existing production checks for scenario-only 6m and 3y rows."""

    if engine.dialect.name != "postgresql":
        return
    definitions = {
        "ck_canonical_forecast_horizon": (
            "horizon IN ('1h','4h','12h','24h','3d','7d','30d','3m','6m','1y','3y','5y')"
        ),
        "ck_canonical_forecast_horizon_minutes": (
            "(horizon = '1h' AND horizon_minutes = 60) OR "
            "(horizon = '4h' AND horizon_minutes = 240) OR "
            "(horizon = '12h' AND horizon_minutes = 720) OR "
            "(horizon = '24h' AND horizon_minutes = 1440) OR "
            "(horizon = '3d' AND horizon_minutes = 4320) OR "
            "(horizon = '7d' AND horizon_minutes = 10080) OR "
            "(horizon = '30d' AND horizon_minutes = 43200) OR "
            "(horizon = '3m' AND horizon_minutes = 129600) OR "
            "(horizon = '6m' AND horizon_minutes = 262800) OR "
            "(horizon = '1y' AND horizon_minutes = 525600) OR "
            "(horizon = '3y' AND horizon_minutes = 1576800) OR "
            "(horizon = '5y' AND horizon_minutes = 2628000)"
        ),
    }
    with engine.begin() as connection:
        existing = {
            row.conname: row.definition
            for row in connection.execute(
                text(
                    "SELECT conname, pg_get_constraintdef(oid) AS definition "
                    "FROM pg_constraint WHERE conrelid = 'canonical_forecasts'::regclass "
                    "AND conname IN ('ck_canonical_forecast_horizon', "
                    "'ck_canonical_forecast_horizon_minutes')"
                )
            )
        }
        for name, definition in definitions.items():
            current = str(existing.get(name) or "")
            if "'6m'" in current and "'3y'" in current:
                continue
            replacement = f"{name}_v2"
            connection.execute(
                text(
                    f"ALTER TABLE canonical_forecasts ADD CONSTRAINT {replacement} "
                    f"CHECK ({definition}) NOT VALID"
                )
            )
            connection.execute(
                text(f"ALTER TABLE canonical_forecasts VALIDATE CONSTRAINT {replacement}")
            )
            if current:
                connection.execute(
                    text(f"ALTER TABLE canonical_forecasts DROP CONSTRAINT {name}")
                )
            connection.execute(
                text(
                    f"ALTER TABLE canonical_forecasts RENAME CONSTRAINT {replacement} TO {name}"
                )
            )


def init_db() -> None:
    Base.metadata.create_all(engine)
    _migrate_postgres_timestamp_columns()
    _migrate_canonical_horizon_constraints()
    # Existing PostgreSQL tables predate these composite indexes. create_all()
    # does not add them to an already-created table, so create them explicitly
    # and idempotently.
    statements = (
        "CREATE INDEX IF NOT EXISTS ix_aggregate_coverage_recorded ON aggregate_snapshots (coverage_key, recorded_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_forecast_due ON forecast_records (status, created_at, horizon_minutes)",
        "CREATE INDEX IF NOT EXISTS ix_alert_state_created ON alert_events (state_key, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_alert_timeline_state_created ON alert_timeline (state_key, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_social_calls_caller_discovered ON social_calls (caller_id, discovered_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_provider_usage_latest ON provider_usage_snapshots (provider, source_timestamp DESC)",
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
