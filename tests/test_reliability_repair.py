from __future__ import annotations

import asyncio
import gzip
import json
import re
from collections import defaultdict
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app import main
from app import outbound_requests
from app.outbound_requests import OutboundUnavailable, governed_async_request
from app.phase1_reliability import _persisted_job_result
from app.terminal_usage import (
    UsageGovernor,
    project_external_plan,
    project_scheduler_database_usage,
)


def test_full_24_hour_external_plan_has_explicit_headroom() -> None:
    projection = project_external_plan([
        {"job": "canonical-core", "callsPerCycle": 8, "intervalSeconds": 600},
        {"job": "multi-exchange", "callsPerCycle": 14, "intervalSeconds": 1_200},
        {"job": "provider-shadows", "callsPerCycle": 16, "intervalSeconds": 3_600},
        {"job": "bnb-onchain", "callsPerCycle": 30, "intervalSeconds": 3_600},
        {"job": "verified-supply", "callsPerCycle": 3, "intervalSeconds": 3_600},
        {"job": "discovery", "callsPerCycle": 12, "intervalSeconds": 21_600},
    ])
    assert projection["withinBudget"] is True
    assert projection["dailyHeadroomPct"] >= 30
    assert projection["monthlyHeadroomPct"] >= 30


def test_idle_and_schedule_statement_capacity_is_below_target() -> None:
    projection = project_scheduler_database_usage()
    assert projection["idleClaimStatementsPerDay"] <= 720
    assert projection["projectedStatementsPerDay"] < 10_000
    assert projection["withinTenThousand"] is True


def test_daily_and_monthly_rollover_closes_historical_block() -> None:
    governor = UsageGovernor()
    governor._day = "1900-01-01"
    governor._month = "1900-01"
    governor._daily["external_request"] = 999_999
    governor._monthly["external_request"] = 999_999
    governor._blocked["external_request"] = 4
    governor._blocked_lifetime["external_request"] = 9
    summary = governor.summary()
    assert summary["circuitOpen"] is False
    assert summary["blocked"].get("external_request", 0) == 0
    assert summary["blockedLifetime"]["external_request"] == 9


def test_retry_attempts_are_each_accounted(monkeypatch) -> None:
    governor = UsageGovernor()
    monkeypatch.setattr(outbound_requests, "usage_governor", governor)

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, url: str, **_kwargs) -> httpx.Response:
            self.calls += 1
            return httpx.Response(
                503 if self.calls == 1 else 200,
                json={"ok": self.calls > 1},
                request=httpx.Request("GET", url),
            )

    client = Client()
    response = asyncio.run(governed_async_request(
        client, "GET", "https://api.binance.com/test", provider="binance",
        job="retry-test", attempts=2,
    ))
    assert response.status_code == 200
    assert client.calls == 2
    assert governor.summary()["providers"]["today"]["binance"] == 2


def test_cached_decoded_body_drops_transfer_encoding(monkeypatch) -> None:
    governor = UsageGovernor()
    monkeypatch.setattr(outbound_requests, "usage_governor", governor)
    outbound_requests._response_cache.clear()

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, url: str, **_kwargs) -> httpx.Response:
            self.calls += 1
            return httpx.Response(
                200,
                content=gzip.compress(b'{"ok":true}'),
                headers={"Content-Encoding": "gzip"},
                request=httpx.Request("GET", url),
            )

    client = Client()

    async def run() -> tuple[httpx.Response, httpx.Response]:
        first = await governed_async_request(
            client, "GET", "https://cache.test/encoded", provider="other",
            cache_ttl_seconds=60,
        )
        second = await governed_async_request(
            client, "GET", "https://cache.test/encoded", provider="other",
            cache_ttl_seconds=60,
        )
        return first, second

    first, second = asyncio.run(run())
    assert first.json() == {"ok": True}
    assert second.json() == {"ok": True}
    assert "content-encoding" not in second.headers
    assert client.calls == 1


def test_outbound_failures_never_echo_endpoint_or_secret() -> None:
    failure = OutboundUnavailable("bnb_rpc", "unauthorized")
    text = str(failure)
    assert "https://" not in text
    assert "secret" not in text.lower()
    assert text == "bnb_rpc is temporarily unauthorized"


def test_every_non_ai_http_call_site_uses_governed_wrapper() -> None:
    app_dir = Path("app")
    direct = []
    pattern = re.compile(
        r"\b(?:http|client|http_client|self\._client|self\.client)\.(?:get|post|request)\("
    )
    for path in app_dir.glob("*.py"):
        if path.name == "outbound_requests.py":
            continue
        for match in pattern.finditer(path.read_text(encoding="utf-8")):
            direct.append(f"{path.name}:{match.group(0)}")
    assert direct == []


def test_source_health_reflects_degraded_evidence_items() -> None:
    packet = {
        "status": "degraded",
        "snapshotId": "evidence_fixture",
        "evidenceHash": "a" * 64,
        "dataAsOf": "2026-08-26T12:00:00+00:00",
        "sourceSummary": {"total": 2, "available": 1, "degraded": 1},
        "items": [
            {
                "sourceId": "dex-spot",
                "validationStatus": "unavailable",
                "freshness": "unavailable",
                "degradationStatus": "unavailable",
                "observedAt": None,
                "provenance": {"collector": "dexscreener_pair"},
            }
        ],
    }
    state = {**main.phase1_state, "lastPacket": packet}
    with patch.object(main, "phase1_state", state):
        payload = asyncio.run(main.source_health_payload(authenticated=True))
    assert payload["status"] == "degraded"
    assert payload["evidence"]["status"] == "degraded"
    assert payload["evidence"]["warnings"] == [
        {"sourceId": "dex-spot", "state": "unavailable"}
    ]
    assert payload["minimumLiveServicesReady"] is False


def test_all_oversized_server_job_results_are_compacted() -> None:
    value = json.loads(_persisted_job_result(
        "any_job", {"result": "x" * 40_000, "sourceErrors": ["provider unavailable"]}
    ))
    assert value["schemaVersion"] == "server-job-result-summary-v1"
    assert value["completionState"] == "completed_with_warnings"
    assert value["originalBytes"] > 32_768


def test_tagnext_snapshot_keeps_partial_sections_and_hard_size_bound() -> None:
    async def fake_cached(key: str, _ttl: int, _callback, **_kwargs):
        if "heatmap" in key:
            raise RuntimeError("https://provider.invalid/?token=secret TLS exploded")
        return {"key": key, "payload": "ok"}

    client = TestClient(main.app)
    with (
        patch.object(main, "RELAY_TOKEN", "fixture"),
        patch.object(main, "cached_thread_read", new=fake_cached),
    ):
        response = client.get(
            "/v1/tagnext/app-snapshot",
            headers={"X-Relay-Key": "fixture"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["overallStatus"] == "partial"
    assert payload["sections"]["heatmaps"]["status"] == "unavailable"
    assert "provider.invalid" not in response.text
    assert "secret" not in response.text
    assert len(response.content) <= payload["maximumBytes"]


def test_tagnext_control_center_defaults_to_full_detail() -> None:
    observed = defaultdict(list)

    async def fake_cached(key: str, _ttl: int, _callback, **kwargs):
        observed[key].append(kwargs)
        return {"forecasts": {"1h": {}, "24h": {}, "3m": {}}}

    client = TestClient(main.app)
    with (
        patch.object(main, "SYSTEM_ID", "tagnext"),
        patch.object(main, "RELAY_TOKEN", "fixture"),
        patch.object(main, "cached_thread_read", new=fake_cached),
    ):
        response = client.get(
            "/v1/tag/control-center",
            headers={"X-Relay-Key": "fixture"},
        )
    assert response.status_code == 200
    # FastAPI evaluated the branch's Query default at route registration.
    assert observed["canonical-control-center:detail"][0]["detail"] is True


def test_unchanged_read_uses_etag_without_retransmitting_body() -> None:
    client = TestClient(main.app)
    first = client.get("/")
    etag = first.headers["etag"]
    weak_etag = etag if etag.startswith("W/") else f"W/{etag}"
    unchanged = client.get("/", headers={"If-None-Match": weak_etag})
    assert unchanged.status_code == 304
    assert unchanged.content == b""
