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


def test_rendered_beincrypto_table_preserves_min_average_and_maximum() -> None:
    fetched = datetime(2026, 8, 17, 21, tzinfo=timezone.utc)
    url = "https://beincrypto.com/price/tagger/price-prediction/"
    document = parse_document(
        url=url,
        html=(
            "TAGGER (TAG) Price Prediction Mon, 17 Aug 2026 "
            "Year Min Price Avg Price Max Price 2027 $0.00030 $0.00088 $0.00174"
        ),
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="beincrypto-tagger", document=document)
    assert [(row["targetSemantics"], row["targetPrice"]) for row in claims] == [
        ("period_minimum", 0.00030),
        ("period_average", 0.00088),
        ("period_maximum", 0.00174),
    ]
    assert all(row["sourceIssueAt"] == datetime(2026, 8, 17, tzinfo=timezone.utc) for row in claims)


def test_tradersunion_monthly_rows_do_not_become_false_annual_rows() -> None:
    fetched = datetime(2026, 8, 17, 21, tzinfo=timezone.utc)
    url = "https://tradersunion.com/currencies/forecast/tag-usd/"
    document = parse_document(
        url=url,
        html=(
            "TAGGER TAG/USD Month Minimum Price Maximum Price Average Price "
            "September 2026 $0.0029 $0.0031 $0.0030 "
            "Year Price in the middle of the year Price at the end of the year "
            "2027 $0.0079 $0.0043 How our forecasts work"
        ),
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="tradersunion-tagger", document=document)
    assert len(claims) == 5
    assert sum(row["normalizedHorizon"] == "2026-09" for row in claims) == 3
    assert {(row["normalizedHorizon"], row["targetSemantics"]) for row in claims[-2:]} == {
        ("2027", "mid_year"), ("2027", "year_end")
    }


def test_gate_native_currency_table_is_never_silently_labeled_usd() -> None:
    fetched = datetime(2026, 8, 17, 21, tzinfo=timezone.utc)
    url = "https://miniapp.gate.com/zh/price-prediction/tagger-tag"
    document = parse_document(
        url=url,
        html="TAGGER TAG 2027\t¥0.005217\t¥0.007759\t¥0.006689",
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="gate-tagger-forecast", document=document)
    assert len(claims) == 3
    assert all(row["targetCurrency"] == "CNY" for row in claims)
    assert all(row["targetPrice"] is None for row in claims)
    assert {row["targetNativePrice"] for row in claims} == {0.005217, 0.007759, 0.006689}


def test_exchange_calculator_is_conditional_and_preserves_native_currency() -> None:
    fetched = datetime(2026, 8, 17, 21, tzinfo=timezone.utc)
    url = "https://www.tapbit.com/en/price-prediction/tagger"
    document = parse_document(
        url=url,
        html="TAGGER TAG price prediction CAD scenarios based on 5% 2027 $0.000989",
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="tapbit-tagger-calculator", document=document)
    assert len(claims) == 1
    assert claims[0]["targetSemantics"] == "scenario_calculator"
    assert claims[0]["scenarioClass"] == "user_input_growth_calculator"
    assert claims[0]["targetCurrency"] == "CAD"
    assert claims[0]["targetPrice"] is None
    assert claims[0]["targetNativePrice"] == 0.000989


def test_nonpositive_target_is_not_a_forecast_claim() -> None:
    fetched = datetime(2026, 8, 17, 21, tzinfo=timezone.utc)
    url = "https://coincheckup.com/coins/tagger/predictions"
    document = parse_document(
        url=url,
        html=(
            "<html><title>TAGGER price prediction</title>"
            "<table><tr><th>Year</th><th>Forecast price</th></tr>"
            "<tr><td>2027</td><td>$0</td></tr></table></html>"
        ),
        fetched_at=fetched,
    )
    assert adapter_for_url(url).parse(source_id="coincheckup-tagger", document=document) == []


def test_dmc_news_article_money_is_not_a_forecast_claim() -> None:
    fetched = datetime(2026, 8, 17, 21, tzinfo=timezone.utc)
    url = "https://dmcnews.org/prices/tagger"
    document = parse_document(
        url=url,
        html=(
            "<html><title>TAGGER Price Today</title><p>TAG $0.0013736 live price.</p>"
            "<article>In 2026 another company announced a $225 million acquisition.</article></html>"
        ),
        fetched_at=fetched,
    )
    assert adapter_for_url(url).parse(source_id="dmcnews-tagger", document=document) == []


def test_govcapital_accepts_only_explicit_dated_forecast_sentence() -> None:
    fetched = datetime(2026, 8, 18, 21, tzinfo=timezone.utc)
    url = "https://gov.capital/crypto/tagger/"
    document = parse_document(
        url=url,
        html=(
            "<html><title>Tagger (TAG) Forecast is 0.001197 USD for 2027-08-18; "
            "And 0.000671 USD for 2031-08-18!</title>"
            "<p>Advertisement: save $225 and see rank 2026.</p></html>"
        ),
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="govcapital-tagger", document=document)
    assert [(row["normalizedHorizon"], row["targetPrice"]) for row in claims] == [
        ("2027-08-18", 0.001197),
        ("2031-08-18", 0.000671),
    ]
    assert all("labelled_title_sentence" in row["methodologyVersion"] for row in claims)


def test_bitscreener_uses_visible_annual_rows_not_monthly_json_years() -> None:
    fetched = datetime(2026, 8, 18, 21, tzinfo=timezone.utc)
    url = "https://bitscreener.com/coins/tagger/price-prediction"
    document = parse_document(
        url=url,
        html=(
            "<html><title>Tagger (TAG) Price Prediction</title>"
            "<p>Year Average Price (USD) Low Price (USD) High Price (USD) Change from today's price "
            "2026 $0.0008433 $0.0002542 $0.002159 -7.53% "
            "2027 $0.001666 $0.0008118 $0.002741 +82.73% "
            "Tagger Price Prediction for 2026</p>"
            "<script type='application/json'>"
            '{"year":2026,"month":"February","low":0.000254194444055034,"high":0.000407392162231132}'
            "</script></html>"
        ),
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="bitscreener-tagger", document=document)
    assert len(claims) == 6
    assert {(row["normalizedHorizon"], row["targetSemantics"], row["targetPrice"]) for row in claims} == {
        ("2026", "period_minimum", 0.0002542),
        ("2026", "period_average", 0.0008433),
        ("2026", "period_maximum", 0.002159),
        ("2027", "period_minimum", 0.0008118),
        ("2027", "period_average", 0.001666),
        ("2027", "period_maximum", 0.002741),
    }
    assert all("visible_annual_summary" in row["methodologyVersion"] for row in claims)


def test_digitalcoinprice_uses_visible_month_table_not_embedded_daily_candles() -> None:
    fetched = datetime(2026, 4, 6, 9, 5, 57, tzinfo=timezone.utc)
    url = "https://digitalcoinprice.com/forecast/tagger"
    document = parse_document(
        url=url,
        html=(
            "<html><title>Tagger (TAG) Price Prediction 2026-2030</title><p>"
            "Tagger (TAG) Price Prediction 2026 Month Minimum Price Average Price Maximum Price Avg. Change "
            "Apr 2026 $0.000655 $0.000793 $0.000931 24.28 % "
            "May 2026 $0.000727 $0.000752 $0.000776 17.82 % "
            "In the first week of April 2026</p>"
            "<script type='application/json'>"
            '{"date":"2026-04-05T17:38:00.178Z","low":0.0006481821239996036,"high":0.0006896372180708402}'
            "</script></html>"
        ),
        fetched_at=fetched,
    )
    claims = adapter_for_url(url).parse(source_id="digitalcoinprice-tagger", document=document)
    assert len(claims) == 6
    assert {row["normalizedHorizon"] for row in claims} == {"2026-04", "2026-05"}
    assert {row["targetPrice"] for row in claims if row["normalizedHorizon"] == "2026-04"} == {
        0.000655, 0.000793, 0.000931,
    }
    assert all(row["observedLive"] is False for row in claims)
    assert all("visible_monthly_table" in row["methodologyVersion"] for row in claims)
