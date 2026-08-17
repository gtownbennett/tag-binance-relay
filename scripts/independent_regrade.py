#!/usr/bin/env python3
"""Independent, standard-library-only TAGneXt forecast regrader.

This file is embedded verbatim in every complete-brain export.  It intentionally
does not import TAGneXt application code or require a database connection.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


VERSION = "tagnext-independent-regrade-v1"
GRADE_VERSION = "canonical-grade-v1"
HORIZON_BASE_TOLERANCE_PCT = {
    "1h": 0.75, "4h": 1.25, "6h": 1.5, "12h": 1.75, "24h": 2.25,
    "3d": 3.5, "7d": 5.0, "30d": 8.0, "3m": 12.0, "6m": 16.0,
    "1y": 20.0, "2026": 16.0, "2027": 25.0, "2028": 30.0,
    "2029": 35.0, "2030": 35.0, "3y": 30.0, "5y": 35.0,
}
METRIC_KEYS = (
    "actualPriceUsd", "actualClass", "directionCorrect", "pointErrorPct",
    "marketCapErrorPct", "positionValueErrorPct", "intervalCovered",
    "intervalSharpnessPct", "weightedIntervalScore",
    "directionProbabilityBrierScore", "scenarioProbabilityBrierScore",
    "probabilityBrierScore", "actualScenario", "baselineRelativeSkill",
    "volatilityTolerancePct", "compositeScore",
)


class RegradeError(ValueError):
    pass


def _iso(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("payload")
    if isinstance(value, dict):
        return dict(value)
    value = row.get("payload_json")
    if isinstance(value, str) and value.strip():
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return dict(row)


def _finite(value: Any, label: str, *, positive: bool = False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise RegradeError(f"{label} must be numeric") from exc
    if not math.isfinite(number) or (positive and number <= 0):
        raise RegradeError(f"{label} must be finite" + (" and positive" if positive else ""))
    return number


def _interval_score(low: float, high: float, actual: float, alpha: float) -> float:
    width = high - low
    below = (2.0 / alpha) * (low - actual) if actual < low else 0.0
    above = (2.0 / alpha) * (actual - high) if actual > high else 0.0
    return width + below + above


def weighted_interval_score(record: Mapping[str, Any], actual: float) -> float:
    quantiles = record["quantilesUsd"]
    p50 = _finite(record["p50Usd"], "p50Usd", positive=True)
    weighted_sum = (
        0.5 * abs(actual - p50)
        + 0.1 * _interval_score(
            _finite(quantiles["p10"], "p10", positive=True),
            _finite(quantiles["p90"], "p90", positive=True), actual, 0.2,
        )
        + 0.25 * _interval_score(
            _finite(quantiles["p25"], "p25", positive=True),
            _finite(quantiles["p75"], "p75", positive=True), actual, 0.5,
        )
    )
    return weighted_sum / 2.5 / actual * 100.0


def _volatility_tolerance(record: Mapping[str, Any]) -> float:
    horizon = str(record["horizon"])
    if horizon not in HORIZON_BASE_TOLERANCE_PCT:
        raise RegradeError(f"unsupported horizon: {horizon}")
    base = HORIZON_BASE_TOLERANCE_PCT[horizon]
    method = record.get("forecastMethod") if isinstance(record.get("forecastMethod"), dict) else {}
    try:
        volatility = float(method.get("volatilityPct"))
    except (TypeError, ValueError):
        volatility = base
    return round(max(base, min(base * 2.0, base + max(0.0, volatility) * 0.15)), 6)


def _actual_class(record: Mapping[str, Any], actual: float, tolerance: float) -> str:
    change = (actual / _finite(record["currentPriceUsd"], "currentPriceUsd", positive=True) - 1.0) * 100.0
    if change > tolerance:
        return "HIGHER"
    if change < -tolerance:
        return "LOWER"
    return "SIDEWAYS"


def _direction_brier(record: Mapping[str, Any], actual_class: str) -> float:
    values = record["directionProbability"]
    probabilities = {
        "HIGHER": _finite(values["up"], "direction up"),
        "LOWER": _finite(values["down"], "direction down"),
        "SIDEWAYS": _finite(values["sideways"], "direction sideways"),
    }
    return sum((probability - (1.0 if label == actual_class else 0.0)) ** 2
               for label, probability in probabilities.items()) / 3.0


def _scenario_brier(record: Mapping[str, Any], actual: float) -> tuple[float, str]:
    candidates: list[tuple[str, float, float, bool]] = []
    for row in record.get("scenarios", []):
        if not isinstance(row, dict):
            continue
        scenario_id = str(row.get("id") or "").strip()
        probability = _finite(row.get("probability"), f"scenario {scenario_id} probability")
        point = _finite(row.get("priceUsd"), f"scenario {scenario_id} price", positive=True)
        region = row.get("priceRegionUsd") if isinstance(row.get("priceRegionUsd"), dict) else {}
        low = _finite(region.get("low", point), f"scenario {scenario_id} low", positive=True)
        high = _finite(region.get("high", point), f"scenario {scenario_id} high", positive=True)
        candidates.append((scenario_id, probability, abs(point - actual), low <= actual <= high))
    if not candidates:
        raise RegradeError("scenario probability grading requires frozen scenario records")
    contained = [row for row in candidates if row[3]]
    actual_id = min(contained or candidates, key=lambda row: row[2])[0]
    score = sum((probability - (1.0 if scenario_id == actual_id else 0.0)) ** 2
                for scenario_id, probability, _, _ in candidates) / len(candidates)
    return score, actual_id


def canonical_metrics(
    forecast_row: Mapping[str, Any],
    outcome_row: Mapping[str, Any],
    *,
    baseline_weighted_interval_score: float | None = None,
) -> dict[str, Any]:
    """Recompute the deterministic canonical metrics from immutable inputs."""
    record = _payload(forecast_row)
    outcome = _payload(outcome_row)
    actual = _finite(
        outcome_row.get("price_usd", outcome.get("priceUsd")), "actual price", positive=True
    )
    point = _finite(record["pointForecastUsd"], "pointForecastUsd", positive=True)
    current = _finite(record["currentPriceUsd"], "currentPriceUsd", positive=True)
    supply = _finite(record["verifiedSupplyTokens"], "verifiedSupplyTokens", positive=True)
    tolerance = _volatility_tolerance(record)
    actual_class = _actual_class(record, actual, tolerance)
    forecast_class = "SIDEWAYS" if str(record["direction"]) == "NEUTRAL" else str(record["direction"])
    direction_correct = forecast_class == actual_class
    point_error = abs(point - actual) / actual * 100.0
    predicted_market_cap = point * supply
    actual_market_cap = actual * supply
    market_cap_error = abs(predicted_market_cap - actual_market_cap) / actual_market_cap * 100.0
    quantity = record.get("portfolioQuantityTokens")
    position_error = None
    if quantity is not None:
        quantity_value = _finite(quantity, "portfolioQuantityTokens")
        predicted_position = point * quantity_value
        actual_position = actual * quantity_value
        position_error = abs(predicted_position - actual_position) / actual_position * 100.0
    quantiles = record["quantilesUsd"]
    q10 = _finite(quantiles["p10"], "p10", positive=True)
    q90 = _finite(quantiles["p90"], "p90", positive=True)
    covered = q10 <= actual <= q90
    sharpness = (q90 - q10) / current * 100.0
    wis = weighted_interval_score(record, actual)
    direction_brier = _direction_brier(record, actual_class)
    scenario_brier, actual_scenario = _scenario_brier(record, actual)
    baseline_skill = None
    if baseline_weighted_interval_score is not None and baseline_weighted_interval_score > 0:
        baseline_skill = (baseline_weighted_interval_score - wis) / baseline_weighted_interval_score
    components: list[tuple[float, float]] = [
        (1.0 if direction_correct else 0.0, 0.16),
        (max(0.0, 1.0 - point_error / max(tolerance * 4.0, 1e-9)), 0.14),
        (max(0.0, 1.0 - market_cap_error / max(tolerance * 4.0, 1e-9)), 0.08),
        (max(0.0, 1.0 - wis / max(tolerance * 6.0, 1e-9)), 0.22),
        (max(0.0, 1.0 - ((direction_brier + scenario_brier) / 2.0) / (2.0 / 3.0)), 0.14),
        (1.0 if covered else 0.0, 0.09),
        (max(0.0, 1.0 - sharpness / max(tolerance * 12.0, 1e-9)), 0.07),
    ]
    if position_error is not None:
        components.append((max(0.0, 1.0 - position_error / max(tolerance * 4.0, 1e-9)), 0.04))
    if baseline_skill is not None:
        components.append((max(0.0, min(1.0, 0.5 + baseline_skill)), 0.06))
    total_weight = sum(weight for _, weight in components)
    composite = round(sum(value * weight for value, weight in components) / total_weight * 100.0, 4)
    return {
        "actualPriceUsd": actual,
        "actualClass": actual_class,
        "directionCorrect": direction_correct,
        "pointErrorPct": point_error,
        "marketCapErrorPct": market_cap_error,
        "positionValueErrorPct": position_error,
        "intervalCovered": covered,
        "intervalSharpnessPct": sharpness,
        "weightedIntervalScore": wis,
        "directionProbabilityBrierScore": direction_brier,
        "scenarioProbabilityBrierScore": scenario_brier,
        "probabilityBrierScore": scenario_brier,
        "actualScenario": actual_scenario,
        "baselineRelativeSkill": baseline_skill,
        "volatilityTolerancePct": tolerance,
        "compositeScore": composite,
    }


def _equal(expected: Any, actual: Any, tolerance: float = 1e-8) -> bool:
    if expected is None or actual is None:
        return expected is actual
    if isinstance(expected, bool) or isinstance(actual, bool):
        return expected is actual
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return math.isclose(float(expected), float(actual), rel_tol=tolerance, abs_tol=tolerance)
    return expected == actual


def _jsonl(content: bytes) -> list[dict[str, Any]]:
    return [json.loads(line) for line in content.decode("utf-8").splitlines() if line.strip()]


def _verify_checksums(archive: zipfile.ZipFile) -> list[str]:
    failures: list[str] = []
    for line in archive.read("SHA256SUMS.txt").decode("utf-8").splitlines():
        expected, name = line.split("  ", 1)
        actual = hashlib.sha256(archive.read(name)).hexdigest()
        if actual != expected:
            failures.append(name)
    return failures


def verify_brain_zip(path: str | Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as archive:
        checksum_failures = _verify_checksums(archive)
        forecasts = _jsonl(archive.read("canonical_forecasts_full.jsonl"))
        outcomes = _jsonl(archive.read("verified_outcomes.jsonl"))
        grades = _jsonl(archive.read("canonical_forecast_grades_full.jsonl"))
    outcomes_by_id = {str(row["outcome_id"]): row for row in outcomes}
    forecast_by_id = {str(row["forecast_id"]): row for row in forecasts}
    baseline_grades = {
        (str(row["horizon"]), str(row["deadline"])): row
        for row in grades if row.get("producer") == "baseline"
    }
    mismatches: list[dict[str, Any]] = []
    recomputed = 0
    for grade in grades:
        if grade.get("producer") != "tagnext" or grade.get("grade_version") != GRADE_VERSION:
            continue
        forecast = forecast_by_id.get(str(grade.get("forecast_id")))
        outcome = outcomes_by_id.get(str(grade.get("outcome_id")))
        if forecast is None or outcome is None:
            mismatches.append({"gradeId": grade.get("grade_id"), "reason": "missing frozen forecast or outcome"})
            continue
        if _iso(forecast["deadline"]) != _iso(outcome["observed_at"]):
            mismatches.append({"gradeId": grade.get("grade_id"), "reason": "outcome is not at exact deadline"})
            continue
        baseline = baseline_grades.get((str(grade["horizon"]), str(grade["deadline"])))
        baseline_wis = float(baseline["weighted_interval_score"]) if baseline else None
        calculated = canonical_metrics(forecast, outcome, baseline_weighted_interval_score=baseline_wis)
        stored = json.loads(grade.get("metrics_json") or "{}")
        metric_failures = [key for key in METRIC_KEYS if not _equal(stored.get(key), calculated.get(key))]
        if metric_failures:
            mismatches.append({"gradeId": grade.get("grade_id"), "metrics": metric_failures})
        recomputed += 1
    tagnext = [row for row in forecasts if row.get("producer") == "tagnext"]
    exact_outcome_keys = {(str(row.get("asset_symbol")), _iso(row["observed_at"])) for row in outcomes}
    now = datetime.now(timezone.utc)
    pending = sum(_iso(row["deadline"]) > now for row in tagnext)
    matured = len(tagnext) - pending
    exact_available = sum(
        ("TAG", _iso(row["deadline"])) in exact_outcome_keys for row in tagnext
    )
    report = {
        "version": VERSION,
        "brainZip": str(Path(path).resolve()),
        "internalChecksumFailures": checksum_failures,
        "tagnextForecastRows": len(tagnext),
        "verifiedOutcomeRows": len(outcomes),
        "storedTAGneXtGrades": sum(row.get("producer") == "tagnext" for row in grades),
        "pendingNotMatured": pending,
        "maturedForecasts": matured,
        "forecastsWithExactOutcome": exact_available,
        "recomputedStoredGrades": recomputed,
        "gradeMismatches": mismatches,
        "passed": not checksum_failures and not mismatches,
        "interpretation": "Pending forecasts are not graded until an immutable exact-deadline outcome exists.",
    }
    return report


def _self_test() -> dict[str, Any]:
    forecast = {
        "horizon": "1h", "currentPriceUsd": 1.0, "pointForecastUsd": 1.02,
        "p50Usd": 1.02, "verifiedSupplyTokens": 100.0, "portfolioQuantityTokens": None,
        "quantilesUsd": {"p10": 0.9, "p25": 0.96, "p50": 1.02, "p75": 1.08, "p90": 1.14},
        "direction": "HIGHER", "directionProbability": {"up": 0.6, "down": 0.2, "sideways": 0.2},
        "scenarios": [
            {"id": "bear", "probability": 0.2, "priceUsd": 0.92, "priceRegionUsd": {"low": 0.88, "high": 0.96}},
            {"id": "base", "probability": 0.3, "priceUsd": 1.0, "priceRegionUsd": {"low": 0.97, "high": 1.03}},
            {"id": "bull", "probability": 0.5, "priceUsd": 1.08, "priceRegionUsd": {"low": 1.04, "high": 1.12}},
        ],
        "forecastMethod": {"volatilityPct": 1.0},
    }
    result = canonical_metrics(forecast, {"price_usd": 1.08})
    if result["actualClass"] != "HIGHER" or not result["directionCorrect"] or result["actualScenario"] != "bull":
        raise AssertionError("reference calculation failed")
    for horizon in HORIZON_BASE_TOLERANCE_PCT:
        _volatility_tolerance({"horizon": horizon, "forecastMethod": {}})
    return {"version": VERSION, "passed": True, "supportedHorizons": sorted(HORIZON_BASE_TOLERANCE_PCT)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brain-zip", type=Path)
    parser.add_argument("--forecast-json", type=Path)
    parser.add_argument("--outcome-json", type=Path)
    parser.add_argument("--output", type=Path, help="optional JSON report destination")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        result = _self_test()
    elif args.brain_zip:
        result = verify_brain_zip(args.brain_zip)
    elif args.forecast_json and args.outcome_json:
        result = canonical_metrics(
            json.loads(args.forecast_json.read_text(encoding="utf-8")),
            json.loads(args.outcome_json.read_text(encoding="utf-8")),
        )
    else:
        parser.error("use --self-test, --brain-zip, or both --forecast-json and --outcome-json")
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result.get("passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
