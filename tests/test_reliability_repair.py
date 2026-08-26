from __future__ import annotations

import asyncio
import gzip
import json
import re
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app import main, outbound_requests
from app.outbound_requests import OutboundUnavailable, governed_async_request
from app.phase1_reliability import _persisted_job_result
from app.terminal_usage import (
    UsageGovernor,
    project_external_plan,
    project_scheduler_database_usage,
)


def test_full_day_request_and_database_projections_have_headroom() -> None:
    external = project_external_plan([
        {"job": "canonical-core", "callsPerCycle": 8, "intervalSeconds": 600},
        {"job": "multi-exchange", "callsPerCycle": 14, "intervalSeconds": 1_200},
        {"job": "verified-supply", "callsPerCycle": 4, "intervalSeconds": 3_600},
    ])
    database = project_scheduler_database_usage()
    assert external["withinBudget"] is True
    assert external["dailyHeadroomPct"] >= 50
    assert database["projectedStatementsPerDay"] == 75_936
    assert database["withinTenThousand"] is False
    assert database["withinCapacity"] is True


def test_historical_blocks_do_not_keep_current_circuit_open() -> None:
    governor = UsageGovernor()
    governor._day = "1900-01-01"
    governor._month = "1900-01"
    governor._daily["external_request"] = 999_999
    governor._monthly["external_request"] = 999_999
    governor._blocked["external_request"] = 5
    governor._blocked_lifetime["external_request"] = 8
    summary = governor.summary()
    assert summary["circuitOpen"] is False
    assert summary["blocked"].get("external_request", 0) == 0
    assert summary["blockedLifetime"]["external_request"] == 8


def test_each_retry_is_charged_and_public_error_is_sanitized(monkeypatch) -> None:
    governor = UsageGovernor()
    monkeypatch.setattr(outbound_requests, "usage_governor", governor)

    class Client:
        calls = 0

        async def get(self, url: str, **_kwargs) -> httpx.Response:
            self.calls += 1
            return httpx.Response(
                503 if self.calls == 1 else 200,
                json={"ok": True}, request=httpx.Request("GET", url),
            )

    client = Client()
    response = asyncio.run(governed_async_request(
        client, "GET", "https://api.binance.com/test", provider="binance",
        job="retry-test", attempts=2,
    ))
    assert response.status_code == 200
    assert governor.summary()["providers"]["today"]["binance"] == 2
    assert str(OutboundUnavailable("binance", "unauthorized")) == "binance is temporarily unauthorized"


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


def test_all_non_ai_http_calls_use_governed_wrapper() -> None:
    pattern = re.compile(
        r"\b(?:http|client|http_client|self\._client|self\.client)\.(?:get|post|request)\("
    )
    direct = []
    for path in Path("app").glob("*.py"):
        if path.name == "outbound_requests.py":
            continue
        direct.extend(pattern.findall(path.read_text(encoding="utf-8")))
    assert direct == []


def test_source_health_and_job_completion_expose_warnings() -> None:
    packet = {
        "status": "degraded", "snapshotId": "fixture", "evidenceHash": "a" * 64,
        "dataAsOf": "2026-08-26T12:00:00+00:00",
        "sourceSummary": {"total": 1, "available": 0, "degraded": 1},
        "items": [{
            "sourceId": "dex-spot", "validationStatus": "unavailable",
            "freshness": "unavailable", "degradationStatus": "unavailable",
            "provenance": {"collector": "dexscreener_pair"},
        }],
    }
    with patch.object(main, "phase1_state", {**main.phase1_state, "lastPacket": packet}):
        health = asyncio.run(main.source_health_payload(authenticated=True))
    assert health["status"] == "degraded"
    assert health["evidence"]["warnings"] == [{"sourceId": "dex-spot", "state": "unavailable"}]
    stored = json.loads(_persisted_job_result(
        "collect_canonical_evidence", {"packet": packet, "sourceErrors": ["unavailable"]}
    ))
    assert stored["completionState"] == "completed_with_warnings"


def test_any_oversized_job_result_is_compacted() -> None:
    stored = json.loads(_persisted_job_result("other_job", {"blob": "x" * 40_000}))
    assert stored["schemaVersion"] == "server-job-result-summary-v1"
    assert stored["originalBytes"] > 32_768


def test_unchanged_read_uses_etag_without_retransmitting_body() -> None:
    client = TestClient(main.app)
    first = client.get("/")
    etag = first.headers["etag"]
    weak_etag = etag if etag.startswith("W/") else f"W/{etag}"
    unchanged = client.get("/", headers={"If-None-Match": weak_etag})
    assert unchanged.status_code == 304
    assert unchanged.content == b""
