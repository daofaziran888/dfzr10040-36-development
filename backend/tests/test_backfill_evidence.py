"""续办证据清单测试：断点之外的逐行证据、迟到回执冲突与恢复领取。

覆盖“首段超时—迟到回执冲突—暂扣首段续办尾段—核对有效明细—重放旧回执”的完整链路。
"""
from __future__ import annotations

import pytest

from app.recovery import (
    BackfillEvidenceLedger,
    BackfillRecoveryCoordinator,
    CheckpointManager,
    PartitionRow,
    ReplayQueue,
)

PARTITION = "20261123"


def _rows() -> list[PartitionRow]:
    return [PartitionRow(row_id, {"v": row_id})
            for row_id in ["A1", "A2", "A3", "B1", "B2"]]


def _setup() -> tuple[BackfillRecoveryCoordinator, ReplayQueue, CheckpointManager]:
    manager = CheckpointManager()
    queue = ReplayQueue()
    coordinator = BackfillRecoveryCoordinator(manager, queue)
    coordinator.open_partition("BACK-9", PARTITION, _rows())
    coordinator.record_failed_attempt(
        "BACK-9", 1, ["A1", "A2", "A3"], "首段写入超时")
    return coordinator, queue, manager


class TestEvidenceLedgerBasics:
    """清单按行身份记录，不依赖断点位置。"""

    def test_rows_registered_independently(self) -> None:
        ledger = BackfillEvidenceLedger("BACK-9", PARTITION, _rows())
        assert ledger.row_order == ["A1", "A2", "A3", "B1", "B2"]
        assert ledger.pending_rows == ["A1", "A2", "A3", "B1", "B2"]
        assert ledger.written_rows == []

    def test_claim_returns_pending_rows_not_position(self) -> None:
        ledger = BackfillEvidenceLedger("BACK-9", PARTITION, _rows())
        claim = ledger.claim_next()
        assert [row["row_id"] for row in claim] == ["A1", "A2", "A3", "B1", "B2"]

        ledger.record_claim_write(["B1", "B2"])
        assert ledger.checklist()["next_claim"] == ["A1", "A2", "A3"]

    def test_write_is_idempotent(self) -> None:
        ledger = BackfillEvidenceLedger("BACK-9", PARTITION, _rows())
        first = ledger.record_claim_write(["A1"])
        second = ledger.record_claim_write(["A1"])
        assert first["written_rows"] == ["A1"]
        assert second["written_rows"] == []
        assert second["skipped_rows"] == ["A1"]
        assert ledger.written_rows == ["A1"]


class TestFailedAttemptEvidence:
    """失败尝试保留原失败原因、尝试编号、受影响范围。"""

    def test_attempt_evidence_is_kept(self) -> None:
        coordinator, _, _ = _setup()
        evidence = coordinator.attempt_evidence("BACK-9", 1)
        assert evidence["attempt_no"] == 1
        assert evidence["failure_reason"] == "首段写入超时"
        assert evidence["scope"] == ["A1", "A2", "A3"]
        assert evidence["affected_rows"] == ["A1", "A2", "A3"]
        assert evidence["status"] == "已超时"

    def test_checklist_keeps_source_failure(self) -> None:
        coordinator, _, _ = _setup()
        checklist = coordinator.checklist("BACK-9")
        assert checklist["source_failure"]["attempt_no"] == 1
        assert checklist["source_failure"]["failure_reason"] == "首段写入超时"
        assert checklist["source_failure"]["scope"] == ["A1", "A2", "A3"]


class TestReceiptConflict:
    """迟到的已写回执与未核对的失败尝试冲突。"""

    def test_receipt_does_not_advance_ledger(self) -> None:
        coordinator, _, _ = _setup()
        result = coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])
        assert result["ok"] is False
        assert result["status"] == "发生冲突"
        assert result["attempt_no"] == 1

        checklist = coordinator.checklist("BACK-9")
        assert checklist["written_count"] == 0
        assert checklist["pending_rows"] == ["A1", "A2", "A3", "B1", "B2"]

    def test_conflict_preserves_both_evidences(self) -> None:
        coordinator, _, _ = _setup()
        coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])

        evidence = coordinator.attempt_evidence("BACK-9", 1)
        assert evidence["failure_reason"] == "首段写入超时"
        assert evidence["written_rows"] == []
        assert len(evidence["conflicts"]) == 1

        conflict = evidence["conflicts"][0]
        assert conflict["resolution"] == "待核对"
        assert conflict["receipt_rows"] == ["A1", "A2", "A3"]

    def test_receipt_unknown_row_rejected(self) -> None:
        coordinator, _, _ = _setup()
        result = coordinator.accept_receipt("BACK-9", ["Z9"])
        assert result["ok"] is False
        assert result["status"] == "已拒绝"
        assert result["unknown_rows"] == ["Z9"]


class TestTailRows:
    """尾段两行有独立身份，不因迟到回执被推进。"""

    def test_claim_holds_first_segment_only(self) -> None:
        coordinator, queue, _ = _setup()
        coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])
        entry = queue.enqueue("FUSI-1", "首段写入超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)

        claim = coordinator.issue_claim("BACK-9", entry.entry_id)
        assert claim["ok"] is True
        assert claim["rows"] == ["B1", "B2"]
        assert "暂扣尝试1范围" in claim["note"]

        checklist = coordinator.checklist("BACK-9")
        assert checklist["written_rows"] == []
        assert checklist["next_claim"] == ["B1", "B2"]

    def test_write_tail_rows_then_verify_partition(self) -> None:
        coordinator, queue, _ = _setup()
        coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])
        entry = queue.enqueue("FUSI-1", "超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)
        claim = coordinator.issue_claim("BACK-9", entry.entry_id)

        written = coordinator.write_claim("BACK-9", claim["claim"], claim["rows"])
        assert written["written_rows"] == ["B1", "B2"]
        assert written["pending_rows"] == ["A1", "A2", "A3"]

        verified = coordinator.verify_partition("BACK-9")
        assert [row["row_id"] for row in verified["effective_rows"]] == ["B1", "B2"]
        assert verified["pending_rows"] == ["A1", "A2", "A3"]

    def test_claim_not_written_returns_rows_to_pending(self) -> None:
        """领取后核实未写入时，尾段行回到待补，不能被“已领取”吞掉。"""
        coordinator, queue, _ = _setup()
        entry = queue.enqueue("FUSI-1", "超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)
        claim = coordinator.issue_claim("BACK-9", entry.entry_id)
        result = coordinator.write_claim("BACK-9", claim["claim"], [])
        assert result["written_rows"] == []
        assert result["unwritten_rows"] == ["B1", "B2"]
        assert coordinator.checklist("BACK-9")["pending_rows"] == [
            "A1", "A2", "A3", "B1", "B2"]

    def test_write_outside_claim_rejected(self) -> None:
        coordinator, queue, _ = _setup()
        entry = queue.enqueue("FUSI-1", "超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)
        claim = coordinator.issue_claim("BACK-9", entry.entry_id)
        result = coordinator.write_claim("BACK-9", claim["claim"], ["A1"])
        assert result["ok"] is False
        assert result["outside_rows"] == ["A1"]
        assert coordinator.checklist("BACK-9")["written_rows"] == []


class TestReconcileAndReplay:
    """恢复核对后再送旧回执并续办，每行只写一次，原失败仍可回查。"""

    def test_full_recovery_cycle(self) -> None:
        coordinator, queue, _ = _setup()
        entry = queue.enqueue("FUSI-1", "首段写入超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)
        coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])

        claim = coordinator.issue_claim("BACK-9", entry.entry_id)
        coordinator.write_claim("BACK-9", claim["claim"], claim["rows"])
        coordinator.verify_partition("BACK-9")

        resolved = coordinator.resolve_attempt("BACK-9", 1)
        assert resolved["ok"] is True
        assert resolved["newly_written"] == ["A1", "A2", "A3"]
        assert resolved["remaining_rows"] == []

        replay = coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])
        assert replay["status"] == "已忽略"
        assert replay["duplicate_rows"] == ["A1", "A2", "A3"]

        checklist = coordinator.checklist("BACK-9")
        assert checklist["written_rows"] == ["A1", "A2", "A3", "B1", "B2"]
        assert checklist["pending_rows"] == []

        evidence = coordinator.attempt_evidence("BACK-9", 1)
        assert evidence["failure_reason"] == "首段写入超时"
        assert evidence["written_rows"] == ["A1", "A2", "A3"]
        assert evidence["status"] == "已核对"

    def test_tail_claim_after_resolution_writes_nothing(self) -> None:
        coordinator, queue, _ = _setup()
        entry = queue.enqueue("FUSI-1", "超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)
        claim = coordinator.issue_claim("BACK-9", entry.entry_id)
        coordinator.write_claim("BACK-9", claim["claim"], claim["rows"])
        coordinator.resolve_attempt("BACK-9", 1, ["A1", "A2", "A3"])

        again = coordinator.issue_claim("BACK-9", entry.entry_id)
        assert again["ok"] is False
        assert queue.get_entry(entry.entry_id).queue_status == "已恢复"

    def test_late_receipt_after_resolution_cannot_extend_scope(self) -> None:
        coordinator, _, _ = _setup()
        coordinator.resolve_attempt("BACK-9", 1, [])
        # 首段核对为未写入，随后旧回执进来只能确认首段三行，不能推进尾段。
        result = coordinator.accept_receipt("BACK-9", ["B1", "B2"], attempt_no=1)
        assert result["ok"] is False
        assert result["status"] == "待核对"
        assert result["out_of_scope_rows"] == ["B1", "B2"]
        assert coordinator.checklist("BACK-9")["written_rows"] == []


class TestReplayEntrySourceFailure:
    """重试只更新最近失败原因，首次失败的尝试编号和原因不被覆盖。"""

    def test_retry_keeps_source_failure(self) -> None:
        queue = ReplayQueue()
        entry = queue.enqueue("FUSI-1", "首段写入超时", attempt_no=1,
                              task_code="BACK-9", partition=PARTITION)
        entry = queue.increment_retry(entry.entry_id, "重试后仍失败")
        assert entry.failure_reason == "重试后仍失败"
        assert entry.source_failure_reason == "首段写入超时"
        assert entry.attempt_no == 1
        assert entry.task_code == "BACK-9"
        assert entry.partition == PARTITION


class TestPersistence:
    """清单可落盘，进程重启后仍能逐行回查。"""

    def test_ledger_survives_reload(self, tmp_path: pytest.TempPathFactory) -> None:
        manager = CheckpointManager()
        manager.evidence_dir = str(tmp_path)
        coordinator = BackfillRecoveryCoordinator(manager, ReplayQueue())
        coordinator.open_partition("BACK-9", PARTITION, _rows())
        coordinator.record_failed_attempt("BACK-9", 1, ["A1", "A2", "A3"], "首段写入超时")
        coordinator.accept_receipt("BACK-9", ["A1", "A2", "A3"])

        fresh = CheckpointManager()
        fresh.evidence_dir = str(tmp_path)
        checklist = fresh.evidence_checklist("BACK-9")
        assert checklist is not None
        assert checklist["pending_rows"] == ["A1", "A2", "A3", "B1", "B2"]
        assert checklist["source_failure"]["failure_reason"] == "首段写入超时"
        assert checklist["conflicts"][0]["receipt_rows"] == ["A1", "A2", "A3"]

    def test_dump_load_round_trip(self) -> None:
        ledger = BackfillEvidenceLedger("BACK-9", PARTITION, _rows())
        ledger.record_attempt(1, ["A1", "A2", "A3"], "首段写入超时")
        ledger.accept_receipt(["A1", "A2", "A3"])
        ledger.record_claim_write(["B1", "B2"])

        restored = BackfillEvidenceLedger.load(ledger.dump())
        assert restored.written_rows == ["B1", "B2"]
        assert restored.pending_rows == ["A1", "A2", "A3"]
        assert restored.source_failure["failure_reason"] == "首段写入超时"
        assert restored.checklist()["conflicts"][0]["resolution"] == "待核对"

        # 重建后的清单接受新回执时，回执编号不能与已落盘的重复。
        receipt = restored.accept_receipt(["A1", "A2", "A3"], attempt_no=None)
        assert receipt["receipt"] == "RCPT-20261123-0002"
