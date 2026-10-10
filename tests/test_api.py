from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert "status" in body
    assert body["status"] in {"ok", "degraded"}


def test_summary_rejects_bad_source_type():
    response = client.get("/api/summary", params={"source_type": "not-real"})
    assert response.status_code == 400


def test_replay_stop_is_safe():
    response = client.post("/api/pipeline/replay/stop")
    assert response.status_code == 200
    assert response.json()["stop_requested"] is True
