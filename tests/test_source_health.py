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
