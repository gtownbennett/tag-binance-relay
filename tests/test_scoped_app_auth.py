from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from app import main


def test_scoped_app_token_can_read_authenticated_tagnext_routes() -> None:
    with (
        patch.object(main, "RELAY_TOKEN", "full-relay-token"),
        patch.object(main, "TAGNEXT_APP_READ_TOKEN", "phone-read-token"),
        TestClient(main.app) as client,
    ):
        response = client.get(
            "/v1/tagnext/identity",
            headers={"X-Relay-Key": "phone-read-token"},
        )
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_scoped_app_token_is_denied_before_every_mutation_route() -> None:
    with (
        patch.object(main, "RELAY_TOKEN", "full-relay-token"),
        patch.object(main, "TAGNEXT_APP_READ_TOKEN", "phone-read-token"),
        TestClient(main.app) as client,
    ):
        response = client.post(
            "/v1/tag/paper/orders",
            headers={"X-Relay-Key": "phone-read-token"},
            json={"side": "BUY", "quantityTokens": 1},
        )
    assert response.status_code == 403
    assert response.json()["detail"].startswith("TAGneXt app token is read-only")


def test_scoped_app_token_cannot_authorize_billable_chad_access() -> None:
    with (
        patch.object(main, "RELAY_TOKEN", "full-relay-token"),
        patch.object(main, "TAGNEXT_APP_READ_TOKEN", "phone-read-token"),
        patch.object(main, "OPENAI_API_KEY", "configured-but-never-called"),
    ):
        try:
            main.require_chad_access("phone-read-token")
        except main.HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("read-only app token authorized Chad")
