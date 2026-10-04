"""Prevent endpoint/function signature drift from hiding stored forecasts."""
import inspect
from fastapi.testclient import TestClient
from app import main
from app.phase4_control_center import canonical_control_center_snapshot


def test_control_center_calls_real_snapshot_signature(monkeypatch):
    signature = inspect.signature(canonical_control_center_snapshot)
    calls = []

    def checked_snapshot(*args, **kwargs):
        signature.bind(*args, **kwargs)
        calls.append((args, kwargs))
        return {"forecasts": [], "currentCall": {"message": "No eligible forecast available."}}

    monkeypatch.setattr(main, "canonical_control_center_snapshot", checked_snapshot)
    monkeypatch.setattr(main, "require_relay_key", lambda key: None)
    response = TestClient(main.app).get("/v1/tag/control-center")
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert calls == [((), {})]
