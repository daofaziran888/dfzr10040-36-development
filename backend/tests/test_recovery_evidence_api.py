"""续办证据接口测试：清单回查、尝试证据、领取、写入与分区核对。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.recovery as recovery
from app.main import app

TASK = "BACK-API-1"
PARTITION = "20261123"


@pytest.fixture()
def client(tmp_path) -> TestClient:
    coordinator = recovery.recovery_coordinator
    coordinator.manager.evidence_dir = str(tmp_path)
    return TestClient(app)


def _open(client: TestClient) -> None:
    response = client.post(f"/api/recovery/{TASK}/partition", json={
        "task_code": TASK,
        "partition": PARTITION,
        "rows": [{"row_id": row_id, "payload": {"v": row_id}}
                 for row_id in ["A1", "A2", "A3", "B1", "B2"]],
    })
    assert response.status_code == 200


def _enqueue(entry_id_start: int = 1) -> int:
    entry = recovery.recovery_coordinator.queue.enqueue(
        "FUSI-1", "首段写入超时", attempt_no=1,
        task_code=TASK, partition=PARTITION)
    return entry.entry_id


class TestRecoveryEvidenceApi:
    def test_open_partition_creates_checklist(self, client: TestClient) -> None:
        _open(client)
        body = client.get(f"/api/recovery/{TASK}/evidence").json()
        assert body["total"] == 5
        assert body["pending_rows"] == ["A1", "A2", "A3", "B1", "B2"]

    def test_unknown_task_returns_404(self, client: TestClient) -> None:
        assert client.get("/api/recovery/NO-SUCH/evidence").status_code == 404

    def test_attempt_conflict_and_resolve_flow(self, client: TestClient) -> None:
        _open(client)
        attempt = client.post(f"/api/recovery/{TASK}/attempts", json={
            "attempt_no": 1,
            "scope": ["A1", "A2", "A3"],
            "failure_reason": "首段写入超时",
        }).json()
        assert attempt["failure_reason"] == "首段写入超时"

        receipt = client.post(f"/api/recovery/{TASK}/receipts", json={
            "rows": ["A1", "A2", "A3"],
        }).json()
        assert receipt["status"] == "发生冲突"

        evidence = client.get(f"/api/recovery/{TASK}/attempts?attempt_no=1").json()
        assert evidence["written_rows"] == []
        assert evidence["failure_reason"] == "首段写入超时"

        resolved = client.post(f"/api/recovery/{TASK}/attempts/resolve", json={
            "attempt_no": 1,
        }).json()
        assert resolved["newly_written"] == ["A1", "A2", "A3"]

        checklist = client.get(f"/api/recovery/{TASK}/evidence").json()
        assert checklist["written_rows"] == ["A1", "A2", "A3"]
        assert checklist["pending_rows"] == ["B1", "B2"]

    def test_claim_write_and_verify(self, client: TestClient) -> None:
        _open(client)
        client.post(f"/api/recovery/{TASK}/attempts", json={
            "attempt_no": 1, "scope": ["A1", "A2", "A3"],
            "failure_reason": "首段写入超时"})
        entry_id = _enqueue()

        claim = client.post(f"/api/recovery/{TASK}/claims", json={
            "entry_id": entry_id}).json()
        assert claim["rows"] == ["B1", "B2"]

        written = client.post(f"/api/recovery/{TASK}/claims/write", json={
            "claim": claim["claim"], "written_rows": claim["rows"]}).json()
        assert written["written_rows"] == ["B1", "B2"]

        verified = client.get(f"/api/recovery/{TASK}/partition").json()
        assert [row["row_id"] for row in verified["effective_rows"]] == ["B1", "B2"]
        assert verified["pending_rows"] == ["A1", "A2", "A3"]

    def test_missing_attempt_evidence_returns_404(self, client: TestClient) -> None:
        _open(client)
        assert client.get(f"/api/recovery/{TASK}/attempts?attempt_no=9").status_code == 404
