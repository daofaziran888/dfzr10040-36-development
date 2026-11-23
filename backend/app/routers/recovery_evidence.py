"""续办证据接口：回查回填尝试证据、按清单领取和核对写入结果。

断点只说明处理到哪里，续办范围一律从这里列出的行身份取得。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.recovery import (
    BackfillRecoveryCoordinator,
    PartitionRow,
    recovery_coordinator,
)


class PartitionRowPayload(BaseModel):
    row_id: str
    payload: dict[str, Any] = Field(default_factory=dict)


class OpenPartitionPayload(BaseModel):
    task_code: str
    partition: str
    rows: list[PartitionRowPayload]


class FailedAttemptPayload(BaseModel):
    attempt_no: int
    scope: list[str]
    failure_reason: str
    note: str = ""


class ReceiptPayload(BaseModel):
    rows: list[str]
    attempt_no: int | None = None
    note: str = ""


class ClaimPayload(BaseModel):
    entry_id: int


class ClaimWritePayload(BaseModel):
    claim: str
    written_rows: list[str]


class ResolveAttemptPayload(BaseModel):
    attempt_no: int
    written_rows: list[str] | None = None
    note: str = ""


class ReplayEntryPayload(BaseModel):
    fusion_job: str
    failure_reason: str
    attempt_no: int = 1
    task_code: str
    partition: str
    backoff_strategy: str = "exponential"
    max_retries: int = 5


def build_router(coordinator: BackfillRecoveryCoordinator | None = None) -> APIRouter:
    router = APIRouter(prefix="/api/recovery", tags=["续办证据"])
    coordinator = coordinator or recovery_coordinator

    def require_ledger(task_code: str):
        if coordinator.manager.get_ledger(task_code) is None:
            raise HTTPException(status_code=404, detail=f"续办证据清单 {task_code} 不存在")

    @router.get("/queue")
    def list_queue() -> dict[str, Any]:
        """恢复队列当前条目，以及每条是否还能按清单领取。"""
        entries = []
        for entry in coordinator.queue.all_entries:
            ledger = coordinator.manager.get_ledger(entry.task_code) \
                if entry.task_code else None
            entries.append({
                "entry_id": entry.entry_id,
                "record_code": entry.record_code,
                "fusion_job": entry.fusion_job,
                "task_code": entry.task_code,
                "partition": entry.partition,
                "attempt_no": entry.attempt_no,
                "queue_status": entry.queue_status,
                "failure_reason": entry.failure_reason,
                "source_failure_reason": entry.source_failure_reason,
                "retry_count": entry.retry_count,
                "claimable_rows": [row["row_id"] for row in ledger.claim_next()]
                if ledger is not None else [],
            })
        return {
            "entries": entries,
            "claims": [
                {
                    "claim": claim.claim_code,
                    "entry_id": claim.entry_id,
                    "task_code": claim.task_code,
                    "partition": claim.partition,
                    "rows": claim.rows,
                    "status": claim.status,
                    "written_rows": claim.written_rows,
                    "unwritten_rows": claim.unwritten_rows,
                }
                for claim in coordinator.queue.all_claims
            ],
        }

    @router.post("/queue")
    def enqueue_replay(payload: ReplayEntryPayload) -> dict[str, Any]:
        """登记一条恢复队列条目，绑定任务、分区和原尝试编号。"""
        entry = coordinator.queue.enqueue(
            payload.fusion_job, payload.failure_reason,
            attempt_no=payload.attempt_no,
            task_code=payload.task_code,
            partition=payload.partition,
            backoff_strategy=payload.backoff_strategy,
            max_retries=payload.max_retries,
        )
        return {
            "ok": True,
            "entry_id": entry.entry_id,
            "record_code": entry.record_code,
            "queue_status": entry.queue_status,
        }

    @router.get("/{task_code}/evidence")
    def evidence_checklist(task_code: str) -> dict[str, Any]:
        """持久回查续办证据清单：已核定写入行、待补行、尝试与回执证据。"""
        require_ledger(task_code)
        return coordinator.checklist(task_code) or {}

    @router.get("/{task_code}/attempts")
    def attempt_evidence(task_code: str,
                         attempt_no: int | None = None) -> dict[str, Any]:
        """凭尝试编号回查原失败原因、受影响范围和随后核实的写入结果。"""
        require_ledger(task_code)
        evidence = coordinator.attempt_evidence(task_code, attempt_no)
        if evidence is None:
            raise HTTPException(status_code=404, detail="尝试证据不存在")
        return evidence

    @router.post("/{task_code}/partition")
    def open_partition(task_code: str, payload: OpenPartitionPayload) -> dict[str, Any]:
        """登记分区行身份并建立续办证据清单。"""
        return coordinator.open_partition(
            task_code, payload.partition,
            [PartitionRow(row.row_id, row.payload) for row in payload.rows],
        )

    @router.post("/{task_code}/attempts")
    def record_failed_attempt(task_code: str,
                              payload: FailedAttemptPayload) -> dict[str, Any]:
        """登记首段失败尝试，保留原失败原因与受影响范围。"""
        require_ledger(task_code)
        return coordinator.record_failed_attempt(
            task_code, payload.attempt_no, payload.scope,
            payload.failure_reason, note=payload.note,
        )

    @router.post("/{task_code}/receipts")
    def accept_receipt(task_code: str, payload: ReceiptPayload) -> dict[str, Any]:
        """接收写入回执；与未核对失败冲突时只登记冲突，不推进清单。"""
        require_ledger(task_code)
        return coordinator.accept_receipt(
            task_code, payload.rows, attempt_no=payload.attempt_no,
            note=payload.note,
        )

    @router.post("/{task_code}/claims")
    def issue_claim(task_code: str, payload: ClaimPayload) -> dict[str, Any]:
        """恢复队列从证据清单领取真实待补范围。"""
        require_ledger(task_code)
        return coordinator.issue_claim(task_code, payload.entry_id)

    @router.post("/{task_code}/claims/write")
    def write_claim(task_code: str, payload: ClaimWritePayload) -> dict[str, Any]:
        """记录本次续办实际写入领到的行。"""
        require_ledger(task_code)
        return coordinator.write_claim(task_code, payload.claim, payload.written_rows)

    @router.get("/{task_code}/partition")
    def verify_partition(task_code: str) -> dict[str, Any]:
        """读取分区核对本次有效明细。"""
        require_ledger(task_code)
        return coordinator.verify_partition(task_code)

    @router.post("/{task_code}/attempts/resolve")
    def resolve_attempt(task_code: str,
                        payload: ResolveAttemptPayload) -> dict[str, Any]:
        """核对失败尝试的真实写入结果，随后重放旧回执只会被忽略。"""
        require_ledger(task_code)
        return coordinator.resolve_attempt(
            task_code, payload.attempt_no, payload.written_rows,
            note=payload.note,
        )

    return router


router = build_router()
