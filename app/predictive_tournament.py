"""Bounded, point-in-time-safe tournament for deterministic TAG forecasts.

The module is deliberately provider-free and storage-free.  Callers supply
already-persisted historical rows and may persist the compact result through
the immutable Phase 9 research ledger.  It evaluates the *same* deterministic
projection used by canonical TAGalysis issuance, never manufactures a LIVE
forecast or a future evidence packet.
"""
from __future__ import annotations

import math
import statistics
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from .canonical_forecast import deterministic_horizon_projection
from .forecast_research import confirmed_online_regime_sequence, purged_embargoed_cases


SHORT_HORIZON_STEPS = {"1h": 12, "4h": 48, "24h": 288}
SHORT_HORIZON_WINDOWS = {"1h": 12, "4h": 48, "24h": 288}
_EPSILON = 1e-12


def _finite(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _clip(value: float, scale: float) -> float:
    return max(-1.0, min(1.0, value / max(scale, _EPSILON)))


def _time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _return(now: float, before: float | None) -> float | None:
    return None if before is None or before <= 0 else now / before - 1.0


def _realized_volatility(prices: Sequence[float]) -> float:
    changes = [math.log(right / left) for left, right in zip(prices, prices[1:]) if left > 0 and right > 0]
    return statistics.pstdev(changes) * 100.0 if len(changes) > 1 else 0.0


def _actual_direction(actual_return: float, neutral: float = 0.0025) -> str:
    return "HIGHER" if actual_return > neutral else "LOWER" if actual_return < -neutral else "SIDEWAYS"


def historical_feature_cases(
    rows: Sequence[Mapping[str, Any]], *, horizon: str,
) -> list[dict[str, Any]]:
    """Build frozen 5-minute cases using only rows at or before each cutoff."""
    if horizon not in SHORT_HORIZON_STEPS:
        raise ValueError(f"unsupported short research horizon: {horizon}")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        at, close = _time(row["observedAt"]), _finite(row.get("close"))
        if close is not None and close > 0:
            normalized.append({**dict(row), "observedAt": at, "close": close})
    normalized.sort(key=lambda row: row["observedAt"])
    times = [row["observedAt"] for row in normalized]
    steps, window = SHORT_HORIZON_STEPS[horizon], SHORT_HORIZON_WINDOWS[horizon]
    cases: list[dict[str, Any]] = []
    # One cutoff per hour is deliberately enough for the raw replay grid.  The
    # later purge still enforces independent outcome windows; avoiding every
    # five-minute row keeps the low-priority study bounded on a Render worker.
    for index in range(max(window, 48), len(normalized), 12):
        current = normalized[index]
        target_at = current["observedAt"] + timedelta(minutes=5 * steps)
        future_index = bisect_left(times, target_at, lo=index + 1)
        if future_index >= len(normalized) or times[future_index] != target_at:
            continue
        history = normalized[index - window:index + 1]
        price = current["close"]
        change_1h = _return(price, normalized[index - 12]["close"])
        change_4h = _return(price, normalized[index - 48]["close"])
        change_24h = _return(price, normalized[index - 288]["close"]) if index >= 288 else None
        prices = [entry["close"] for entry in history]
        oi_now, oi_1h, oi_4h = (_finite(current.get("openInterestUsd")), _finite(normalized[index - 12].get("openInterestUsd")), _finite(normalized[index - 48].get("openInterestUsd")))
        oi_change_1h, oi_change_4h = _return(oi_now, oi_1h) if oi_now else None, _return(oi_now, oi_4h) if oi_now else None
        quote = sum((_finite(entry.get("quoteVolume")) or 0.0) for entry in normalized[index - 11:index + 1])
        buy = sum((_finite(entry.get("takerBuyQuote")) or 0.0) for entry in normalized[index - 11:index + 1])
        taker = (2.0 * buy / quote - 1.0) if quote > 0 and buy > 0 else None
        funding_values = [_finite(entry.get("fundingRate")) for entry in normalized[index - 48:index + 1]]
        funding_values = [value for value in funding_values if value is not None]
        funding = statistics.mean(funding_values) if funding_values else None
        long_liq = sum((_finite(entry.get("longLiquidationsUsd")) or 0.0) for entry in normalized[index - 11:index + 1])
        short_liq = sum((_finite(entry.get("shortLiquidationsUsd")) or 0.0) for entry in normalized[index - 11:index + 1])
        liquidation = (short_liq - long_liq) / max(short_liq + long_liq, 1.0) if short_liq + long_liq > 0 else None
        features: dict[str, float] = {}
        if change_1h is not None:
            features["priceChange1h"] = _clip(change_1h, 0.025)
        if oi_change_1h is not None:
            features["oiChange1h"] = _clip(oi_change_1h, 0.15)
        if taker is not None:
            features["takerImbalance1h"] = max(-1.0, min(1.0, taker))
        if liquidation is not None:
            features["liquidationPressure1h"] = max(-1.0, min(1.0, liquidation))
        if change_4h is not None:
            features["priceChange4h"] = _clip(change_4h, 0.08)
            features["spotConfirmation4h"] = _clip(change_4h, 0.08) if quote > 0 else 0.0
        if oi_change_4h is not None:
            features["oiChange4h"] = _clip(oi_change_4h, 0.30)
        if funding is not None:
            features["fundingTrend4h"] = _clip(funding, 0.003)
        if change_24h is not None:
            features["priceChange24h"] = _clip(change_24h, 0.20)
            features["spotVolume24h"] = _clip(math.log1p(quote) - math.log1p(sum((_finite(entry.get("quoteVolume")) or 0.0) for entry in normalized[index - 287:index - 11])), 4.0)
            features["liquidityChange24h"] = 0.0
        # Only historical data at/before cutoff enters this online regime.
        regime_features = {
            "oiChange": _clip(oi_change_1h or 0.0, 0.15),
            "funding": _clip(funding or 0.0, 0.003),
            "spotConfirmation": _clip(change_4h or 0.0, 0.08),
            "realizedVolatility": _clip(_realized_volatility(prices), 2.0),
            "liquidationPressure": liquidation or 0.0,
        }
        # The canonical projection expects volatility for the forecast horizon,
        # not the standard deviation of a single five-minute return.  Scale a
        # trailing realized-volatility estimate to the matching outcome window.
        features.update({
            "realizedVolatility1hPct": max(0.05, _realized_volatility([entry["close"] for entry in normalized[index - 12:index + 1]]) * math.sqrt(12)),
            "realizedVolatility4hPct": max(0.05, _realized_volatility([entry["close"] for entry in normalized[index - 48:index + 1]]) * math.sqrt(48)),
            "realizedVolatility24hPct": max(0.05, _realized_volatility([entry["close"] for entry in normalized[index - 288:index + 1]]) * math.sqrt(288)) if index >= 288 else 5.0,
        })
        cases.append({
            "cutoff": current["observedAt"], "current": price, "actual": normalized[future_index]["close"],
            "features": features, "regimeFeatures": regime_features,
            "raw": {"oiDelta1h": oi_change_1h, "funding": funding, "taker": taker, "liquidation": liquidation,
                    "futuresReturn1h": change_1h, "futuresReturn4h": change_4h},
        })
    return cases


def _candidate(case: Mapping[str, Any], horizon: str, name: str) -> dict[str, float]:
    current = float(case["current"])
    features = dict(case["features"])
    if name == "canonical_without_oi":
        features.pop("oiChange1h", None); features.pop("oiChange4h", None)
    if name == "canonical_without_taker":
        features.pop("takerImbalance1h", None)
    if name == "canonical_without_funding":
        features.pop("fundingTrend4h", None)
    if name.startswith("canonical"):
        projected = deterministic_horizon_projection(horizon=horizon, current_price=current, features=features)
        return {"point": projected["pointForecastUsd"], "lower": projected["q10Usd"], "upper": projected["q90Usd"], "probabilityUp": projected["probabilityUp"]}
    return_1h = float(case["raw"].get("futuresReturn1h") or 0.0)
    volatility = max(0.005, float(features.get("realizedVolatility1hPct") or 1.0) / 100.0)
    if name == "persistence":
        projected_return = 0.0
    elif name == "momentum":
        projected_return = max(-0.10, min(0.10, return_1h * 0.35))
    elif name == "volatility_adjusted_momentum":
        projected_return = max(-0.10, min(0.10, return_1h * min(0.60, 0.01 / volatility)))
    elif name == "mean_reversion":
        projected_return = max(-0.10, min(0.10, -return_1h * 0.20))
    else:
        raise ValueError(f"unknown candidate {name}")
    width = max(0.01, min(0.75, volatility * math.sqrt(SHORT_HORIZON_STEPS[horizon]) * 1.35))
    probability_up = max(0.05, min(0.95, 0.5 + projected_return / max(width * 2.0, 0.01)))
    return {"point": current * (1.0 + projected_return), "lower": current * (1.0 + projected_return - width), "upper": current * (1.0 + projected_return + width), "probabilityUp": probability_up}


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"status": "INSUFFICIENT_DATA", "sampleCount": 0}
    errors = [abs(float(row["point"]) / float(row["actual"]) - 1.0) for row in rows]
    coverage = [float(row["lower"]) <= float(row["actual"]) <= float(row["upper"]) for row in rows]
    widths = [(float(row["upper"]) - float(row["lower"])) / float(row["current"]) for row in rows]
    outcomes = [float(row["actual"]) / float(row["current"]) - 1.0 for row in rows]
    directions = [_actual_direction(value) for value in outcomes]
    predicted = [_actual_direction(float(row["point"]) / float(row["current"]) - 1.0) for row in rows]
    brier = [(float(row["probabilityUp"]) - (1.0 if actual > 0.0025 else 0.0)) ** 2 for row, actual in zip(rows, outcomes)]
    wis = [
        ((float(row["upper"]) - float(row["lower"])) + 10 * max(0.0, float(row["lower"]) - float(row["actual"])) + 10 * max(0.0, float(row["actual"]) - float(row["upper"]))) / float(row["current"])
        for row in rows
    ]
    ece = 0.0
    for low in (0.0, 0.2, 0.4, 0.6, 0.8):
        bucket = [(float(row["probabilityUp"]), actual > 0.0025) for row, actual in zip(rows, outcomes) if low <= float(row["probabilityUp"]) < low + 0.2 or (low == 0.8 and float(row["probabilityUp"]) == 1.0)]
        if bucket:
            ece += len(bucket) / len(rows) * abs(statistics.mean(x[0] for x in bucket) - statistics.mean(float(x[1]) for x in bucket))
    return {
        "status": "AVAILABLE", "sampleCount": len(rows), "directionAccuracy": sum(a == b for a, b in zip(predicted, directions)) / len(rows),
        "maePct": 100.0 * statistics.mean(errors), "medianAbsoluteErrorPct": 100.0 * statistics.median(errors),
        "weightedIntervalScorePct": 100.0 * statistics.mean(wis), "intervalCoverage": statistics.mean(coverage),
        "intervalWidthPct": 100.0 * statistics.mean(widths), "brierUp": statistics.mean(brier), "expectedCalibrationError": ece,
    }


def _independent_rows(rows: Sequence[Mapping[str, Any]], horizon: str) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    selection = purged_embargoed_cases([row["cutoff"] for row in rows], horizon=timedelta(minutes=SHORT_HORIZON_STEPS[horizon] * 5))
    accepted = set(selection.pop("acceptedCutoffs"))
    return [row for row in rows if row["cutoff"].isoformat() in accepted], selection


def evaluate_canonical_tournament(rows: Sequence[Mapping[str, Any]], *, horizon: str) -> dict[str, Any]:
    """Evaluate fixed candidates on the final chronological 40% only."""
    cases = historical_feature_cases(rows, horizon=horizon)
    if len(cases) < 100:
        return {"horizon": horizon, "status": "INSUFFICIENT_DATA", "rawCaseCount": len(cases), "noLookahead": True}
    split_at = cases[int(len(cases) * 0.60)]["cutoff"]
    test_cases = [case for case in cases if case["cutoff"] >= split_at]
    independent, independence = _independent_rows(test_cases, horizon)
    candidates = ("canonical_current", "persistence", "momentum", "volatility_adjusted_momentum", "mean_reversion", "canonical_without_oi", "canonical_without_taker", "canonical_without_funding")
    results: dict[str, Any] = {}
    for name in candidates:
        predicted = [{**case, **_candidate(case, horizon, name)} for case in independent]
        results[name] = _metrics(predicted)
    baseline = results["persistence"]
    for name, result in results.items():
        if result["status"] == "AVAILABLE":
            result["maeDeltaVsPersistencePct"] = baseline["maePct"] - result["maePct"]
            result["wisDeltaVsPersistencePct"] = baseline["weightedIntervalScorePct"] - result["weightedIntervalScorePct"]
    current = results["canonical_current"]
    eligible = len(independent) >= 30
    promotion = [
        name for name, result in results.items()
        if name not in {"canonical_current", "persistence"}
        and eligible
        # A challenger must beat the canonical model *and* the simple
        # persistence baseline.  A relative canonical win is not useful when
        # both models are still worse than doing nothing.
        and result["maePct"] < min(current["maePct"], baseline["maePct"])
        and result["weightedIntervalScorePct"] <= min(current["weightedIntervalScorePct"], baseline["weightedIntervalScorePct"])
        and result["expectedCalibrationError"] <= min(current["expectedCalibrationError"], baseline["expectedCalibrationError"])
    ]
    return {
        "runKind": "canonical_deterministic_tournament", "modelVersion": "tagalysis-horizon-specialists-v1", "horizon": horizon,
        "status": "AVAILABLE", "noLookahead": True, "trainEndAt": split_at.isoformat(), "evaluationStartAt": split_at.isoformat(),
        "evaluationEndAt": cases[-1]["cutoff"].isoformat(), "rawCaseCount": len(test_cases),
        "effectiveIndependentSampleCount": len(independent), "independence": independence, "candidates": results,
        "promotionEligible": eligible, "promotionCandidates": promotion,
        "learningHealth": "IMPROVING" if promotion else ("STALLED" if eligible else "INSUFFICIENT_DATA"),
    }


def discover_online_regimes(
    rows: Sequence[Mapping[str, Any]], *, spacing: timedelta = timedelta(hours=4), confirmations: int = 6,
) -> dict[str, Any]:
    """Discover durable confirmed online states; retrospective labels are excluded.

    Six four-hour observations deliberately require roughly a day of
    point-in-time evidence before a market-structure transition becomes a
    persisted research candidate.  This prevents an intraday indicator flip
    from being misrepresented as a structural change point.
    """
    observations: list[dict[str, Any]] = []
    last: datetime | None = None
    for row in rows:
        at = _time(row.get("cutoff") or row["observedAt"])
        if last is not None and at < last + spacing:
            continue
        last = at
        observations.append({"observedAt": at.isoformat(), "features": dict(row["regimeFeatures"])})
    transitions = confirmed_online_regime_sequence(observations, confirmations=confirmations)
    return {"detectorVersion": "online-distribution-safe-v1", "observationCount": len(observations), "confirmationsRequired": confirmations, "spacingSeconds": int(spacing.total_seconds()), "transitions": transitions, "noLookahead": True}


def lead_lag_summary(cases: Sequence[Mapping[str, Any]], *, field: str, horizon: str) -> dict[str, Any]:
    values = [(float(case["raw"][field]), float(case["actual"]) / float(case["current"]) - 1.0) for case in cases if _finite(case["raw"].get(field)) is not None]
    if len(values) < 40:
        return {"field": field, "horizon": horizon, "status": "INSUFFICIENT_DATA", "sampleCount": len(values), "noLookahead": True}
    values.sort(key=lambda row: row[0])
    bucket = max(1, len(values) // 5)
    low, high = values[:bucket], values[-bucket:]
    effect = statistics.mean(row[1] for row in high) - statistics.mean(row[1] for row in low)
    return {"field": field, "horizon": horizon, "status": "AVAILABLE", "sampleCount": len(values), "quantileEffectPct": 100.0 * effect,
            "lowQuintileReturnPct": 100.0 * statistics.mean(row[1] for row in low), "highQuintileReturnPct": 100.0 * statistics.mean(row[1] for row in high), "noLookahead": True,
            "interpretation": "descriptive out-of-sample association; not causal and not promoted without tournament improvement"}


def run_bounded_predictive_study(rows: Sequence[Mapping[str, Any]], *, horizons: Iterable[str] = ("1h", "4h", "24h")) -> dict[str, Any]:
    result: dict[str, Any] = {"version": "predictive-tournament-v1", "automaticPaidAiCalls": 0, "liveForecastWeightsChanged": False, "noLookahead": True, "horizons": {}}
    all_cases: dict[str, list[dict[str, Any]]] = {}
    for horizon in horizons:
        all_cases[horizon] = historical_feature_cases(rows, horizon=horizon)
        result["horizons"][horizon] = evaluate_canonical_tournament(rows, horizon=horizon)
    regime_source = all_cases.get("1h") or []
    result["onlineRegimes"] = discover_online_regimes(regime_source)
    result["leadLag"] = {field: lead_lag_summary(regime_source, field=field, horizon="1h") for field in ("oiDelta1h", "funding", "taker", "liquidation")}
    return result


def _persisted_rows(*, max_rows: int = 120_000) -> list[dict[str, Any]]:
    """Read the minimum Phase 6 columns required for a bounded study."""
    from sqlalchemy import select
    from .terminal_database import HistoricalMarketRow, session_scope

    with session_scope() as session:
        klines = list(session.execute(
            select(
                HistoricalMarketRow.observed_at, HistoricalMarketRow.close_price,
                HistoricalMarketRow.quote_volume, HistoricalMarketRow.taker_buy_quote,
            ).where(
                HistoricalMarketRow.source == "Binance Vision",
                HistoricalMarketRow.dataset == "klines",
                HistoricalMarketRow.validation_status == "valid",
                HistoricalMarketRow.close_price.is_not(None),
            ).order_by(HistoricalMarketRow.observed_at.desc()).limit(max_rows)
        ))
        metrics = list(session.execute(
            select(
                HistoricalMarketRow.observed_at, HistoricalMarketRow.open_interest_usd,
                HistoricalMarketRow.long_liquidations_usd, HistoricalMarketRow.short_liquidations_usd,
            ).where(
                HistoricalMarketRow.source == "Binance Vision",
                HistoricalMarketRow.dataset == "metrics",
                HistoricalMarketRow.validation_status == "valid",
            )
        ))
        funding = list(session.execute(
            select(HistoricalMarketRow.observed_at, HistoricalMarketRow.funding_rate).where(
                HistoricalMarketRow.source == "Binance Vision",
                HistoricalMarketRow.dataset == "fundingRate",
                HistoricalMarketRow.validation_status == "valid",
            ).order_by(HistoricalMarketRow.observed_at)
        ))
    metric_by_time = {row[0]: row for row in metrics}
    funding_at = list(funding)
    index, last_funding = 0, None
    result: list[dict[str, Any]] = []
    for at, close, volume, buy in reversed(klines):
        while index < len(funding_at) and funding_at[index][0] <= at:
            last_funding = funding_at[index][1]
            index += 1
        metric = metric_by_time.get(at)
        result.append({
            "observedAt": _time(at).isoformat(), "close": close, "quoteVolume": volume,
            "takerBuyQuote": buy, "fundingRate": last_funding,
            "openInterestUsd": metric[1] if metric else None,
            "longLiquidationsUsd": metric[2] if metric else None,
            "shortLiquidationsUsd": metric[3] if metric else None,
        })
    return result


def persist_bounded_predictive_study(*, max_rows: int = 120_000) -> dict[str, Any]:
    """Persist compact Phase 10 research only; never alters LIVE weights."""
    from .forecast_research import persist_feature_reliability, persist_regime_version, persist_research_run

    study = run_bounded_predictive_study(_persisted_rows(max_rows=max_rows))
    stored_runs: dict[str, Any] = {}
    profiles: list[dict[str, Any]] = []
    for horizon, result in study["horizons"].items():
        stored_runs[horizon] = persist_research_run({
            "runKind": "canonical_deterministic_tournament", "horizon": horizon,
            "modelVersion": "tagalysis-horizon-specialists-v1",
            "evaluationStartAt": result["evaluationStartAt"], "evaluationEndAt": result["evaluationEndAt"],
            "rawCaseCount": result["rawCaseCount"], "effectiveSampleCount": result["effectiveIndependentSampleCount"],
            "noLookahead": True, "results": result,
        })
        current = result["candidates"]["canonical_current"]
        for candidate in ("canonical_without_oi", "canonical_without_taker", "canonical_without_funding"):
            candidate_metrics = result["candidates"][candidate]
            delta = current["maePct"] - candidate_metrics["maePct"]
            profiles.append(persist_feature_reliability({
                "featureFamily": candidate.removeprefix("canonical_without_") or candidate,
                "horizon": horizon, "regime": "ALL",
                "sampleCount": result["rawCaseCount"], "effectiveSampleCount": result["effectiveIndependentSampleCount"],
                "skillDelta": delta,
                "status": "EVALUATED" if delta > 0 else "DISQUALIFIED",
                "results": {"withFeatureMaePct": current["maePct"], "withoutFeatureMaePct": candidate_metrics["maePct"],
                            "metric": "canonical MAE minus ablated MAE; positive means the feature helped"},
            }))
    regimes = []
    for transition in study["onlineRegimes"]["transitions"]:
        regimes.append(persist_regime_version({
            "regimeKey": f"online:{transition['onlineLabel']}:{transition['effectiveFrom']}",
            "detectorVersion": study["onlineRegimes"]["detectorVersion"],
            "effectiveFrom": transition["effectiveFrom"], "effectiveTo": transition["detectedAt"],
            "detectedAt": transition["detectedAt"], "onlineLabel": transition["onlineLabel"],
            "onlineConfidence": transition["onlineConfidence"], "features": {"reasons": transition["reasons"]},
            "sourceCoverage": {"Binance Vision futures": "available"},
            "missingness": {"liquidations": "not available in 5m metrics archive"}, "noLookahead": True,
        }))
    health = "STALLED" if all(row["learningHealth"] == "STALLED" for row in study["horizons"].values()) else "INCONCLUSIVE"
    return {**study, "persistence": {"researchRuns": stored_runs, "featureProfiles": profiles, "regimes": regimes}, "learningHealth": health,
            "automaticPaidAiCalls": 0, "liveForecastWeightsChanged": False}
