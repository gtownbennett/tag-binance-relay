from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from app import main


client = TestClient(main.app)


def test_source_health_is_public_and_side_effect_free() -> None:
    with (
        patch.object(main, "get_json") as external_request,
        patch.object(main, "session_scope") as database_session,
        patch.object(main, "openai_client") as openai_client,
    ):
        response = client.get("/v1/tag/source-health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sideEffects"] == "none"
    assert payload["authenticated"] is False
    assert "services" in payload
    assert "sources" in payload
    assert payload["storage"]["databaseCheckedByThisRequest"] is False
    external_request.assert_not_called()
    database_session.assert_not_called()
    openai_client.assert_not_called()


def test_connection_requires_and_validates_relay_key_without_side_effects() -> None:
    with patch.object(main, "RELAY_TOKEN", "test-relay-token"):
        unauthorized = client.get("/v1/tag/connection")
        wrong = client.get(
            "/v1/tag/connection",
            headers={"X-Relay-Key": "wrong"},
        )
        with (
            patch.object(main, "get_json") as external_request,
            patch.object(main, "session_scope") as database_session,
            patch.object(main, "openai_client") as openai_client,
        ):
            accepted = client.get(
                "/v1/tag/connection",
                headers={"X-Relay-Key": "test-relay-token"},
            )

    assert unauthorized.status_code == 401
    assert wrong.status_code == 401
    assert accepted.status_code == 200
    payload = accepted.json()
    assert payload["authenticated"] is True
    assert payload["sideEffects"] == "none"
    external_request.assert_not_called()
    database_session.assert_not_called()
    openai_client.assert_not_called()


def test_minimum_live_status_never_implies_optional_ai_is_required() -> None:
    with patch.object(main, "RELAY_TOKEN", "test-relay-token"):
        response = client.get(
            "/v1/tag/connection",
            headers={"X-Relay-Key": "test-relay-token"},
        )

    assert response.status_code == 200
    services = response.json()["services"]
    assert "optionalAiSynthesis" in services
    assert "collection" in services
    assert "grading" in services
    assert response.json()["minimumLiveServicesReady"] is False


def test_control_center_accepts_portfolio_quantity_only_in_private_header() -> None:
    captured: dict[str, float | None] = {}

    def fake_snapshot(*, portfolio_quantity_tokens: float | None = None) -> dict[str, object]:
        captured["quantity"] = portfolio_quantity_tokens
        return {"authoritative": True, "sideEffects": "none"}

    with (
        patch.object(main, "RELAY_TOKEN", "test-relay-token"),
        patch.object(main, "canonical_control_center_snapshot", side_effect=fake_snapshot),
    ):
        response = client.get(
            "/v1/tag/control-center",
            headers={
                "X-Relay-Key": "test-relay-token",
                "X-Portfolio-Quantity-Tokens": "100812406",
            },
        )

    assert response.status_code == 200
    assert captured["quantity"] == 100_812_406.0
    assert "portfolio_quantity_tokens" not in str(response.request.url)


def test_grader_item_errors_mark_connection_degraded() -> None:
    degraded = {
        "running": True,
        "lastRunAt": "2026-08-05T16:26:24Z",
        "lastResult": {
            "graded": 0,
            "pendingDue": 4,
            "errors": ["TAGUSDT 24h: HTTP 418"],
        },
    }
    with (
        patch.object(main, "RELAY_TOKEN", "test-relay-token"),
        patch.object(main, "REPAIR_MODE", False),
        patch.object(main, "grader_state", degraded),
    ):
        response = client.get(
            "/v1/tag/connection",
            headers={"X-Relay-Key": "test-relay-token"},
        )

    assert response.status_code == 200
    payload = response.json()
    grading = payload["services"]["grading"]
    assert grading["running"] is True
    assert grading["healthy"] is False
    assert grading["errorCount"] == 1
    assert grading["pendingDue"] == 4
    assert payload["minimumLiveServicesReady"] is False
    assert any("degraded" in blocker.lower() for blocker in payload["blockers"])
