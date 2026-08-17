from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.tagnext_external_adapters import (
    adapter_for_url,
    deduplicate_claims,
    normalize_horizon,
    parse_document,
)


FIXTURES = Path(__file__).parent / "fixtures" / "external_forecast_adapters.json"


@pytest.mark.parametrize("case", json.loads(FIXTURES.read_text(encoding="utf-8")))
def test_approved_source_fixtures_preserve_every_horizon_and_semantics(case: dict) -> None:
    fetched = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
    adapter = adapter_for_url(case["url"])
    assert adapter is not None
    document = parse_document(url=case["url"], html=case["html"], fetched_at=fetched)
    claims = adapter.parse(source_id=case["sourceId"], document=document, source_issue_at=fetched)
    actual = {
        (
            claim["normalizedHorizon"], claim["targetSemantics"],
            float(claim["targetPrice"]) if claim["targetPrice"] is not None else None,
        )
        for claim in claims
    }
    expected = {
        (horizon, semantics, float(value) if value is not None else None)
        for horizon, semantics, value in case["expected"]
    }
    assert actual == expected
    assert all(claim["sourceId"] == case["sourceId"] for claim in claims)


def test_horizon_case_distinguishes_months_from_minutes() -> None:
    issued = datetime(2026, 8, 17, tzinfo=timezone.utc)
    months = normalize_horizon("3M", issue_at=issued)
    minutes = normalize_horizon("3m", issue_at=issued)
    assert months["normalizedHorizon"] == "3m"
    assert minutes["normalizedHorizon"] == "3m"
    assert (months["deadline"] - issued).days == 90
    assert (minutes["deadline"] - issued).total_seconds() == 180
    assert normalize_horizon("3 months", issue_at=issued)["normalizedHorizon"] == "3m"
    assert normalize_horizon("15m", issue_at=issued)["normalizedHorizon"] == "15m"


def test_navigation_ads_and_wrong_token_without_tagger_are_not_claims() -> None:
    fetched = datetime(2026, 8, 17, tzinfo=timezone.utc)
    url = "https://coincodex.com/crypto/another-tag/price-prediction/"
    html = "<html><title>TAG token prediction</title><nav>Forecast 2030 USD 99</nav><div>Prediction 2030 $1</div></html>"
    adapter = adapter_for_url(url)
    assert adapter is not None
    document = parse_document(url=url, html=html, fetched_at=fetched)
    assert adapter.parse(source_id="wrong-token", document=document) == []


def test_repeat_fetch_is_semantically_stable_and_real_revision_changes_hash_input() -> None:
    fetched = datetime(2026, 8, 17, tzinfo=timezone.utc)
    adapter = adapter_for_url("https://coincodex.com/crypto/tagger/price-prediction/")
    first = parse_document(
        url="https://coincodex.com/crypto/tagger/price-prediction/",
        html="<title>TAGGER forecast</title><p>Forecast 2030 target USD 0.01.</p>",
        fetched_at=fetched,
    )
    layout_change = parse_document(
        url=first.url,
        html="<title>TAGGER forecast</title><header>new menu</header><p>Forecast 2030 target USD 0.01.</p>",
        fetched_at=fetched,
    )
    revision = parse_document(
        url=first.url,
        html="<title>TAGGER forecast</title><p>Forecast 2030 target USD 0.02.</p>",
        fetched_at=fetched,
    )
    first_claims = adapter.parse(source_id="coincodex-tagger", document=first)
    assert deduplicate_claims(first_claims) == deduplicate_claims(
        adapter.parse(source_id="coincodex-tagger", document=layout_change)
    )
    assert first_claims != adapter.parse(source_id="coincodex-tagger", document=revision)
