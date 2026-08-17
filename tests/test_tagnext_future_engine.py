from __future__ import annotations

import pytest

from app.tagnext_future_engine import EVENT_TYPES, PATH_DEFINITIONS


def test_future_engine_defines_all_required_path_classes() -> None:
    assert {row[0] for row in PATH_DEFINITIONS} == {
        "healthy_continuation", "consolidation", "failed_reclaim", "deeper_breakdown",
        "capitulation", "v_recovery", "long_squeeze", "short_squeeze", "trap",
    }


def test_event_ledger_acceptance_surface_is_explicit() -> None:
    assert {"breakout", "breakdown", "failed_reclaim", "support_reversal", "capitulation", "v_recovery", "squeeze", "trap"} <= EVENT_TYPES
    assert {"funding_extreme", "oi_divergence", "whale_event", "exchange_flow", "liquidity_event", "catalyst", "social_anomaly"} <= EVENT_TYPES
