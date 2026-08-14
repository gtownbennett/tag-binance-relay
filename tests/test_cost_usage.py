from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from app import cost_usage
from app.cost_usage import cost_usage_report, persist_snapshot, refresh_cost_usage
from app.terminal_database import init_db


def setup_module() -> None:
    init_db()


def test_normalized_snapshot_projects_and_surfaces_threshold() -> None:
    now = datetime.now(timezone.utc)
    card = persist_snapshot(
        "render",
        {
            "currentPlan": "Fixture plan",
            "cycleStart": (now - timedelta(days=20)).isoformat(),
            "cycleEnd": (now + timedelta(days=10)).isoformat(),
            "currentSpendingUsd": 8.0,
            "budgetUsd": 10.0,
            "sourceTimestamp": now.isoformat(),
            "dataSource": "test fixture",
            "valueStatus": "MANUAL",
            "explanation": "Fixture only.",
        },
    )

    assert card["projectedCostUsd"] is not None
    assert card["projectedCostUsd"] >= 11.9
    assert card["status"] == "DANGER"
    report = cost_usage_report()
    render = next(item for item in report["providers"] if item["provider"] == "render")
    assert render["valueStatus"] == "MANUAL"
    assert report["rules"]["providerFailuresRetainLastValidSnapshot"] is True


def test_failed_refresh_keeps_last_valid_snapshot() -> None:
    now = datetime.now(timezone.utc)
    persist_snapshot(
        "github",
        {
            "currentPlan": "GitHub Free",
            "currentSpendingUsd": 0.0,
            "sourceTimestamp": now.isoformat(),
            "dataSource": "test fixture",
            "valueStatus": "MANUAL",
            "status": "GOOD",
            "explanation": "Fixture only.",
        },
    )
    client = AsyncMock()
    client.get.side_effect = RuntimeError("provider unavailable")
    with (
        patch.dict(
            "os.environ",
            {"GITHUB_BILLING_TOKEN": "fixture", "GITHUB_BILLING_USER": "fixture"},
            clear=False,
        ),
        patch.object(cost_usage, "_last_refresh_attempt", None),
    ):
        result = asyncio.run(refresh_cost_usage(client, force=True))

    github = next(item for item in result["providers"] if item["provider"] == "github")
    assert github["currentPlan"] == "GitHub Free"
    assert github["currentSpendingUsd"] == 0.0
    assert "github" in result["refreshErrors"]


def test_unavailable_cards_do_not_claim_invoice_truth() -> None:
    report = cost_usage_report()
    codex = next(item for item in report["providers"] if item["provider"] == "codex")
    assert codex["currentSpendingUsd"] is None
    assert codex["valueStatus"] == "UNAVAILABLE"
    assert codex["status"] == "BLOCKED"
