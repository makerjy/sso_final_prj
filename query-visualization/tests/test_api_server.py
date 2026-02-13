from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.server import MAX_ROWS, app


client = TestClient(app)


def test_visualize_rejects_large_rows() -> None:
    payload = {
        "user_query": "월별 추세",
        "sql": "SELECT * FROM sample",
        "rows": [{"x": i} for i in range(MAX_ROWS + 1)],
    }
    response = client.post("/visualize", json=payload)
    assert response.status_code == 413
    body = response.json()
    assert body["detail"]["code"] == "ROWS_LIMIT_EXCEEDED"

