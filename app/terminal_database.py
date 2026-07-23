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
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .terminal_config import DATABASE_URL


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


connect_args: dict[str, Any] = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


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
