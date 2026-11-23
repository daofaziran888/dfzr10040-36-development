"""恢复与重放：断点续传、中断重放与增量确认机制。

与重放队列联动，融合作业失败后在队列中记录，
按退避策略重试，支持断点恢复。

断点只描述“处理到哪里”，无法说明“哪些行已经真实落库”。为此这里额外维护
一份续办证据清单（BackfillEvidenceLedger）：按行身份记录待补、已核定写入的行，
按尝试编号记录提交范围、失败原因和后来核实的写入结果。恢复队列领取续办范围时
只从清单里的待补行取数，不按断点位置延伸，避免续办重复写或提前推进未处理的尾段。
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ReplayEntry:
    """重放队列条目：记录失败融合片段与重试状态。

    ``failure_reason`` 是“最近一次失败原因”，可能被重试覆盖；
    ``source_failure_reason`` 与 ``attempt_no`` 是首次失败的原始证据，
    重试不会改写，保证原失败始终能凭尝试编号回查。
    """
    entry_id: int
    record_code: str
    fusion_job: str
    failure_reason: str
    retry_count: int = 0
    backoff_strategy: str = "exponential"
    max_retries: int = 5
    last_retry: str = ""
    queue_status: str = "待重放"
    checkpoint_data: dict[str, Any] = field(default_factory=dict)
    source_failure_reason: str = ""
    attempt_no: int = 0
    task_code: str = ""
    partition: str = ""

    def __post_init__(self) -> None:
        if not self.source_failure_reason:
            self.source_failure_reason = self.failure_reason

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries

    def next_backoff_seconds(self) -> int:
        if self.backoff_strategy == "exponential":
            return min(2 ** (self.retry_count + 1), 300)
        elif self.backoff_strategy == "linear":
            return min((self.retry_count + 1) * 10, 300)
        return 60


@dataclass
class PartitionRow:
    """分区里的一行回填数据：行身份独立于断点位置。"""
    row_id: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class BackfillAttempt:
    """某一次回填尝试的提交证据。

    ``scope`` 是这次尝试实际申请的提交范围（首段/尾段等分段），
    ``failure_reason`` 是当时失败的原因；``written_rows`` 是事后核实
    真实写入的行，二者分开保存，失败和完成两份证据不会互相覆盖。
    """
    attempt_no: int
    scope: list[str]
    status: str = "已超时"
    failure_reason: str = ""
    failed_at: str = ""
    written_rows: list[str] = field(default_factory=list)
    resolved_at: str = ""
    note: str = ""
    affected_rows: list[str] = field(default_factory=list)

    @property
    def resolved(self) -> bool:
        return self.status in ("已核对", "已完成")


@dataclass
class WriteReceipt:
    """一次写入回执：可能迟到、重复，甚至与已记录的失败相冲突。"""
    receipt_id: int
    receipt_code: str
    rows: list[str]
    attempt_no: int | None
    received_at: str
    status: str = "待核对"
    adopted_rows: list[str] = field(default_factory=list)
    duplicate_rows: list[str] = field(default_factory=list)
    note: str = ""


class BackfillEvidenceLedger:
    """续办证据清单：按行身份持久记录回填的待补范围与已核定写入结果。

    不依赖断点位置推断进度，任何恢复动作都以这里的行状态为准。
    """

    def __init__(self, task_code: str, partition: str,
                 rows: list[PartitionRow] | None = None) -> None:
        self.task_code = task_code
        self.partition = partition
        self._rows: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self._attempts: dict[int, BackfillAttempt] = {}
        self._receipts: list[WriteReceipt] = []
        self._conflicts: list[dict[str, Any]] = []
        self._source_failure: dict[str, Any] = {}
        self._next_receipt_id = 1
        if rows:
            self.register_rows(rows)

    # ---- 行登记与查询 ----

    def register_rows(self, rows: list[PartitionRow]) -> None:
        for row in rows:
            if row.row_id in self._rows:
                continue
            self._rows[row.row_id] = {
                "row_id": row.row_id,
                "payload": copy.deepcopy(row.payload),
                "written": False,
                "written_at": "",
                "write_source": "",
            }
            self._order.append(row.row_id)

    @property
    def row_order(self) -> list[str]:
        return list(self._order)

    def rows_in(self, scope: list[str]) -> list[str]:
        """按分区原始顺序返回给定范围内的行身份。"""
        wanted = set(scope)
        return [row_id for row_id in self._order if row_id in wanted]

    @property
    def unresolved_attempt(self) -> BackfillAttempt | None:
        pending = [a for a in self._attempts.values() if not a.resolved]
        if not pending:
            return None
        return sorted(pending, key=lambda a: a.attempt_no)[-1]

    def attempt(self, attempt_no: int) -> BackfillAttempt | None:
        return self._attempts.get(attempt_no)

    @property
    def next_attempt_no(self) -> int:
        return max(self._attempts, default=0) + 1

    def attempt_evidence(self, attempt_no: int | None = None) -> dict[str, Any] | None:
        """凭尝试编号回查提交证据：原失败原因、范围、核实写入结果都在。"""
        if attempt_no is None:
            target = self.unresolved_attempt
            if target is None:
                targets = sorted(self._attempts.values(), key=lambda a: a.attempt_no)
                target = targets[-1] if targets else None
        else:
            target = self._attempts.get(attempt_no)
        if target is None:
            return None
        return {
            "task_code": self.task_code,
            "partition": self.partition,
            "attempt_no": target.attempt_no,
            "status": target.status,
            "failure_reason": target.failure_reason,
            "failed_at": target.failed_at,
            "scope": list(target.scope),
            "affected_rows": list(target.affected_rows),
            "written_rows": list(target.written_rows),
            "resolved_at": target.resolved_at,
            "note": target.note,
            "conflicts": [
                copy.deepcopy(c) for c in self._conflicts
                if c.get("attempt_no") == target.attempt_no
            ],
        }

    def conflict_receipt_rows(self, attempt_no: int | None = None) -> list[str]:
        """取出与失败尝试冲突、尚待核对的回执行，用于核对后确认写入结果。"""
        target_no = attempt_no
        if target_no is None and self.unresolved_attempt is not None:
            target_no = self.unresolved_attempt.attempt_no
        rows: list[str] = []
        for conflict in self._conflicts:
            if conflict.get("resolution") != "待核对":
                continue
            if target_no is not None and conflict.get("attempt_no") != target_no:
                continue
            rows.extend(conflict.get("receipt_rows", []))
        return self.rows_in(list(dict.fromkeys(rows)))

    @property
    def written_rows(self) -> list[str]:
        return [row_id for row_id in self._order if self._rows[row_id]["written"]]

    @property
    def pending_rows(self) -> list[str]:
        return [row_id for row_id in self._order if not self._rows[row_id]["written"]]

    @property
    def source_failure(self) -> dict[str, Any]:
        return copy.deepcopy(self._source_failure)

    # ---- 尝试证据 ----

    def record_attempt(self, attempt_no: int, scope: list[str],
                       failure_reason: str, note: str = "",
                       failed_at: str | None = None) -> BackfillAttempt:
        """登记一次已失败/超时的提交尝试，保留原失败原因与受影响范围。"""
        attempt = BackfillAttempt(
            attempt_no=attempt_no,
            scope=self.rows_in(scope),
            status="已超时",
            failure_reason=failure_reason,
            failed_at=failed_at or _now_iso(),
            note=note,
            affected_rows=self.rows_in(scope),
        )
        self._attempts[attempt_no] = attempt
        if not self._source_failure:
            self._source_failure = {
                "attempt_no": attempt_no,
                "failure_reason": failure_reason,
                "failed_at": attempt.failed_at,
                "scope": list(attempt.scope),
            }
        return attempt

    def resolve_attempt(self, attempt_no: int, written_rows: list[str],
                        note: str = "",
                        status: str = "已核对") -> dict[str, Any]:
        """核对某次尝试的真实写入结果；只允许在本次受影响范围内确认。"""
        attempt = self._attempts.get(attempt_no)
        if attempt is None:
            return {"ok": False, "message": f"尝试 {attempt_no} 不存在"}

        allowed = set(attempt.scope) | set(attempt.affected_rows)
        unknown = [row_id for row_id in written_rows if row_id not in self._rows]
        out_of_scope = [row_id for row_id in written_rows if row_id not in allowed]
        if unknown or out_of_scope:
            return {
                "ok": False,
                "message": "核对范围越界：只能确认本次尝试受影响的行",
                "unknown_rows": unknown,
                "out_of_scope_rows": out_of_scope,
            }

        new_rows = [row_id for row_id in self.rows_in(written_rows)
                    if not self._rows[row_id]["written"]]
        self._mark_written(new_rows, source=f"尝试{attempt_no}核对")
        attempt.written_rows = self.rows_in([*attempt.written_rows, *written_rows])
        attempt.status = status
        attempt.resolved_at = _now_iso()
        if note:
            attempt.note = note
        for conflict in self._conflicts:
            if conflict.get("attempt_no") == attempt_no \
                    and conflict.get("resolution") == "待核对":
                conflict["resolution"] = "已核对"
                conflict["resolved_at"] = attempt.resolved_at

        return {
            "ok": True,
            "attempt_no": attempt_no,
            "written_rows": list(attempt.written_rows),
            "newly_written": new_rows,
            "remaining_rows": [r for r in attempt.scope if r not in attempt.written_rows],
            "status": attempt.status,
        }

    # ---- 回执核对 ----

    def accept_receipt(self, rows: list[str], attempt_no: int | None = None,
                       note: str = "") -> dict[str, Any]:
        """接收一份写入回执，并判定它能否推进清单。

        规则：
        - 行身份必须属于本分区，否则整份回执拒绝；
        - 与尚未核对的失败尝试冲突时只登记冲突和受影响范围，不写清单；
        - 回执行必须落在该尝试的提交范围内，否则转为待核对，不推进；
        - 已核定写入的行不重复写，只记为忽略/重复。
        """
        receipt_rows = self.rows_in(rows)
        unknown = [row_id for row_id in rows if row_id not in self._rows]
        receipt = WriteReceipt(
            receipt_id=self._next_receipt_id,
            receipt_code=f"RCPT-{self.partition}-{self._next_receipt_id:04d}",
            rows=list(dict.fromkeys(rows)),
            attempt_no=attempt_no,
            received_at=_now_iso(),
            note=note,
        )
        self._next_receipt_id += 1
        self._receipts.append(receipt)

        if unknown:
            receipt.status = "已拒绝"
            return {"ok": False, "status": receipt.status, "receipt": receipt.receipt_code,
                    "message": "回执包含不属于本分区的行", "unknown_rows": unknown}

        target = self._attempts.get(attempt_no) if attempt_no is not None \
            else self.unresolved_attempt

        if target is not None and not target.resolved:
            receipt.status = "发生冲突"
            conflict = {
                "type": "failure_vs_receipt",
                "attempt_no": target.attempt_no,
                "receipt": receipt.receipt_code,
                "received_at": receipt.received_at,
                "receipt_rows": receipt_rows,
                "source_failure": copy.deepcopy(self._source_failure),
                "resolution": "待核对",
            }
            self._conflicts.append(conflict)
            target.affected_rows = self.rows_in([*target.affected_rows, *receipt_rows])
            return {
                "ok": False,
                "status": receipt.status,
                "receipt": receipt.receipt_code,
                "attempt_no": target.attempt_no,
                "message": "回执与未核对的失败尝试冲突，先保留原失败证据，不推进清单",
                "conflict": copy.deepcopy(conflict),
            }

        if target is not None and target.resolved:
            out_of_scope = [row_id for row_id in receipt_rows
                            if row_id not in set(target.scope) | set(target.affected_rows)]
            if out_of_scope:
                receipt.status = "待核对"
                return {
                    "ok": False,
                    "status": receipt.status,
                    "receipt": receipt.receipt_code,
                    "message": "回执行不在该尝试的提交范围内，不推进未处理的尾段",
                    "out_of_scope_rows": out_of_scope,
                }

        already = set(self.written_rows)
        duplicate_rows = [row_id for row_id in receipt_rows if row_id in already]
        new_rows = [row_id for row_id in receipt_rows if row_id not in already]
        receipt.duplicate_rows = duplicate_rows
        if not new_rows:
            receipt.status = "已忽略"
            return {
                "ok": True, "status": receipt.status, "receipt": receipt.receipt_code,
                "message": "回执行均已核定写入，本次不重复写",
                "duplicate_rows": duplicate_rows, "written_rows": self.written_rows,
            }

        self._mark_written(new_rows, source=f"回执{receipt.receipt_code}")
        receipt.status = "已采用"
        receipt.adopted_rows = new_rows
        if target is not None and target.resolved:
            # 迟到的回执与已核对的尝试对账：确认的写入结果以清单为准。
            target.written_rows = self.rows_in([*target.written_rows, *new_rows])
        return {
            "ok": True, "status": receipt.status, "receipt": receipt.receipt_code,
            "adopted_rows": new_rows, "duplicate_rows": duplicate_rows,
            "written_rows": self.written_rows, "pending_rows": self.pending_rows,
        }

    # ---- 续办领取与写入 ----

    def claim_next(self) -> list[dict[str, Any]]:
        """领取清单中真实待补的行。

        有未核对的失败尝试时，只领取该尝试范围之外的待补行：首段的提交核对
        仍暂扣着，未处理的尾段不能因此被一并推进，但尾段本身有独立身份，
        值班可以按领到的范围当班补写。
        """
        blocked: set[str] = set()
        if self.unresolved_attempt is not None:
            blocked = set(self.unresolved_attempt.scope) | set(self.unresolved_attempt.affected_rows)
        return [
            {"row_id": row_id, "partition": self.partition,
             "payload": copy.deepcopy(self._rows[row_id]["payload"])}
            for row_id in self.pending_rows
            if row_id not in blocked
        ]

    def effective_rows(self) -> list[dict[str, Any]]:
        """读取分区中本次已核定写入的有效明细，用于恢复后核对。"""
        return [
            {
                "row_id": row_id,
                "partition": self.partition,
                "payload": copy.deepcopy(self._rows[row_id]["payload"]),
                "written_at": self._rows[row_id]["written_at"],
                "write_source": self._rows[row_id]["write_source"],
            }
            for row_id in self._order
            if self._rows[row_id]["written"]
        ]

    def record_claim_write(self, claim_rows: list[str]) -> dict[str, Any]:
        """记录续办实际写入的行；已核定行即使重复提交也只记一次。"""
        unknown = [row_id for row_id in claim_rows if row_id not in self._rows]
        if unknown:
            return {"ok": False, "message": "写入行不属于本分区", "unknown_rows": unknown}
        already = set(self.written_rows)
        skipped = [row_id for row_id in claim_rows if row_id in already]
        new_rows = [row_id for row_id in self.rows_in(claim_rows) if row_id not in already]
        self._mark_written(new_rows, source="续办写入")
        return {
            "ok": True,
            "written_rows": new_rows,
            "skipped_rows": skipped,
            "pending_rows": self.pending_rows,
        }

    # ---- 清单输出 ----

    def checklist(self) -> dict[str, Any]:
        """输出可持久回查的续办证据清单。"""
        rows = [
            {
                "row_id": row_id,
                "partition": self.partition,
                "status": "已核定写入" if self._rows[row_id]["written"] else "待补写",
                "written_at": self._rows[row_id]["written_at"],
                "write_source": self._rows[row_id]["write_source"],
            }
            for row_id in self._order
        ]
        return {
            "task_code": self.task_code,
            "partition": self.partition,
            "total": len(self._order),
            "written_count": len(self.written_rows),
            "pending_count": len(self.pending_rows),
            "written_rows": self.written_rows,
            "pending_rows": self.pending_rows,
            "rows": rows,
            "source_failure": self.source_failure,
            "open_attempt": self.unresolved_attempt.attempt_no if self.unresolved_attempt else None,
            "attempts": [
                {
                    "attempt_no": a.attempt_no,
                    "status": a.status,
                    "scope": list(a.scope),
                    "failure_reason": a.failure_reason,
                    "failed_at": a.failed_at,
                    "written_rows": list(a.written_rows),
                    "affected_rows": list(a.affected_rows),
                    "resolved_at": a.resolved_at,
                }
                for a in sorted(self._attempts.values(), key=lambda a: a.attempt_no)
            ],
            "receipts": [
                {
                    "receipt": r.receipt_code,
                    "attempt_no": r.attempt_no,
                    "rows": list(r.rows),
                    "status": r.status,
                    "adopted_rows": list(r.adopted_rows),
                    "duplicate_rows": list(r.duplicate_rows),
                    "received_at": r.received_at,
                }
                for r in self._receipts
            ],
            "conflicts": copy.deepcopy(self._conflicts),
            "next_claim": [
                row_id for row_id in self.pending_rows
                if self.unresolved_attempt is None
                or row_id not in set(self.unresolved_attempt.scope)
                | set(self.unresolved_attempt.affected_rows)
            ],
        }

    def _mark_written(self, row_ids: list[str], source: str) -> None:
        stamp = _now_iso()
        for row_id in row_ids:
            row = self._rows[row_id]
            if row["written"]:
                continue
            row["written"] = True
            row["written_at"] = stamp
            row["write_source"] = source

    # ---- 持久化 ----

    def dump(self) -> dict[str, Any]:
        """导出可落盘的清单快照（只包含证据，不包含断点）。"""
        return {
            "version": 1,
            "task_code": self.task_code,
            "partition": self.partition,
            "rows": [
                {
                    "row_id": row_id,
                    "payload": copy.deepcopy(self._rows[row_id]["payload"]),
                    "written": self._rows[row_id]["written"],
                    "written_at": self._rows[row_id]["written_at"],
                    "write_source": self._rows[row_id]["write_source"],
                }
                for row_id in self._order
            ],
            "attempts": [
                {
                    "attempt_no": a.attempt_no,
                    "scope": list(a.scope),
                    "status": a.status,
                    "failure_reason": a.failure_reason,
                    "failed_at": a.failed_at,
                    "written_rows": list(a.written_rows),
                    "resolved_at": a.resolved_at,
                    "note": a.note,
                    "affected_rows": list(a.affected_rows),
                }
                for a in sorted(self._attempts.values(), key=lambda a: a.attempt_no)
            ],
            "receipts": [
                {
                    "receipt_code": r.receipt_code,
                    "rows": list(r.rows),
                    "attempt_no": r.attempt_no,
                    "received_at": r.received_at,
                    "status": r.status,
                    "adopted_rows": list(r.adopted_rows),
                    "duplicate_rows": list(r.duplicate_rows),
                    "note": r.note,
                }
                for r in self._receipts
            ],
            "conflicts": copy.deepcopy(self._conflicts),
            "source_failure": copy.deepcopy(self._source_failure),
            "next_receipt_id": self._next_receipt_id,
        }

    @classmethod
    def load(cls, snapshot: dict[str, Any]) -> BackfillEvidenceLedger:
        """从 dump 的快照重建清单，恢复后仍能逐行回查写入结果。"""
        ledger = cls(task_code=snapshot["task_code"], partition=snapshot["partition"])
        for row in snapshot.get("rows", []):
            row_id = row["row_id"]
            ledger._rows[row_id] = {
                "row_id": row_id,
                "payload": copy.deepcopy(row.get("payload", {})),
                "written": bool(row.get("written")),
                "written_at": row.get("written_at", ""),
                "write_source": row.get("write_source", ""),
            }
            ledger._order.append(row_id)
        for item in snapshot.get("attempts", []):
            ledger._attempts[item["attempt_no"]] = BackfillAttempt(
                attempt_no=item["attempt_no"],
                scope=list(item.get("scope", [])),
                status=item.get("status", "已超时"),
                failure_reason=item.get("failure_reason", ""),
                failed_at=item.get("failed_at", ""),
                written_rows=list(item.get("written_rows", [])),
                resolved_at=item.get("resolved_at", ""),
                note=item.get("note", ""),
                affected_rows=list(item.get("affected_rows", [])),
            )
        for item in snapshot.get("receipts", []):
            ledger._receipts.append(WriteReceipt(
                receipt_id=len(ledger._receipts) + 1,
                receipt_code=item["receipt_code"],
                rows=list(item.get("rows", [])),
                attempt_no=item.get("attempt_no"),
                received_at=item.get("received_at", ""),
                status=item.get("status", "待核对"),
                adopted_rows=list(item.get("adopted_rows", [])),
                duplicate_rows=list(item.get("duplicate_rows", [])),
                note=item.get("note", ""),
            ))
        ledger._conflicts = copy.deepcopy(snapshot.get("conflicts", []))
        ledger._source_failure = copy.deepcopy(snapshot.get("source_failure", {}))
        ledger._next_receipt_id = max(
            int(snapshot.get("next_receipt_id", 0)),
            len(ledger._receipts) + 1,
        )
        return ledger


@dataclass
class RecoveryClaim:
    """恢复队列发出的一次续办领取单，范围来自续办证据清单。"""
    claim_code: str
    entry_id: int
    task_code: str
    partition: str
    rows: list[str]
    status: str = "已领取"
    claimed_at: str = ""
    written_rows: list[str] = field(default_factory=list)
    unwritten_rows: list[str] = field(default_factory=list)
    note: str = ""


class ReplayQueue:
    """重放队列管理器：管理融合失败数据的自动重放。"""

    def __init__(self) -> None:
        self._entries: dict[int, ReplayEntry] = {}
        self._claims: dict[str, RecoveryClaim] = {}
        self._next_id: int = 1

    def enqueue(self, fusion_job_code: str, failure_reason: str,
                checkpoint_data: dict[str, Any] | None = None,
                backoff_strategy: str = "exponential",
                max_retries: int = 5,
                attempt_no: int = 1,
                task_code: str = "",
                partition: str = "") -> ReplayEntry:
        sid = self._next_id
        self._next_id += 1
        entry = ReplayEntry(
            entry_id=sid,
            record_code=f"REPL-{sid:04d}",
            fusion_job=fusion_job_code,
            failure_reason=failure_reason,
            retry_count=0,
            backoff_strategy=backoff_strategy,
            max_retries=max_retries,
            last_retry="",
            queue_status="待重放",
            checkpoint_data=checkpoint_data or {},
            source_failure_reason=failure_reason,
            attempt_no=attempt_no,
            task_code=task_code,
            partition=partition,
        )
        self._entries[sid] = entry
        return entry

    def dequeue(self, entry_id: int) -> ReplayEntry | None:
        return self._entries.pop(entry_id, None)

    def get_entry(self, entry_id: int) -> ReplayEntry | None:
        return self._entries.get(entry_id)

    def mark_retrying(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "重放中"
        entry.last_retry = _now_iso()
        return entry

    def mark_success(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "已恢复"
        return entry

    def mark_permanent_skip(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "永久跳过"
        return entry

    def increment_retry(self, entry_id: int,
                        new_failure_reason: str = "") -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.retry_count += 1
        if new_failure_reason:
            entry.failure_reason = new_failure_reason
        entry.last_retry = _now_iso()
        if not entry.can_retry():
            entry.queue_status = "永久跳过"
        else:
            entry.queue_status = "待重放"
        return entry

    @property
    def pending_entries(self) -> list[ReplayEntry]:
        return [e for e in self._entries.values() if e.queue_status == "待重放"]

    # ---- 按续办证据清单领取真实范围 ----

    def claim_from_ledger(self, entry_id: int,
                          ledger: BackfillEvidenceLedger) -> RecoveryClaim | None:
        """从续办证据清单领取本次续办范围，而不是按断点位置自行延伸。

        清单里有未核对的失败尝试时，只暂扣该尝试范围内的行，队列仍可领取
        其余真实待补行（例如尾段），避免一笔迟到回执或断点把未处理尾段写成已完成。
        """
        entry = self._entries.get(entry_id)
        if entry is None:
            return None

        rows = ledger.claim_next()
        unresolved = ledger.unresolved_attempt
        if not rows:
            if unresolved is not None:
                entry.queue_status = "待核对"
                entry.checkpoint_data = {
                    **entry.checkpoint_data,
                    "claim_blocked": "首段提交尚待核对",
                    "blocked_attempt": unresolved.attempt_no,
                    "blocked_rows": ledger.rows_in(unresolved.scope),
                }
                return None
            entry.queue_status = "已恢复"
            entry.checkpoint_data = {
                **entry.checkpoint_data,
                "claim": "清单无待补行",
                "pending_rows": [],
            }
            return None

        claim = RecoveryClaim(
            claim_code=f"CLAIM-{entry.record_code}-{len(self._claims) + 1:04d}",
            entry_id=entry_id,
            task_code=entry.task_code or ledger.task_code,
            partition=entry.partition or ledger.partition,
            rows=[row["row_id"] for row in rows],
            claimed_at=_now_iso(),
        )
        if unresolved is not None:
            claim.note = f"暂扣尝试{unresolved.attempt_no}范围，仅领取其余待补行"
        entry.queue_status = "重放中"
        entry.last_retry = claim.claimed_at
        entry.checkpoint_data = {
            **entry.checkpoint_data,
            "claim": claim.claim_code,
            "claimed_rows": list(claim.rows),
            "pending_rows": list(claim.rows),
            "held_attempt": unresolved.attempt_no if unresolved is not None else None,
        }
        self._claims[claim.claim_code] = claim
        return claim

    def record_claim_write(self, ledger: BackfillEvidenceLedger,
                           claim_code: str,
                           written_rows: list[str]) -> dict[str, Any]:
        """记录按领取单实际写入的行，只允许写领取单里的行身份。"""
        claim = self._claims.get(claim_code)
        if claim is None:
            return {"ok": False, "message": f"领取单 {claim_code} 不存在"}
        outside = [row_id for row_id in written_rows if row_id not in set(claim.rows)]
        if outside:
            return {"ok": False, "message": "写入行超出领取范围", "outside_rows": outside}

        result = ledger.record_claim_write(written_rows)
        claim.written_rows = list(result.get("written_rows", []))
        claim.status = "已写入"
        written = set(claim.written_rows)
        # 领取范围内的行若未真实写入，随核对结论回到待补，避免尾段被“已领取”吞掉。
        claim.unwritten_rows = [row_id for row_id in claim.rows if row_id not in written]
        entry = self._entries.get(claim.entry_id)
        if entry is not None:
            entry.checkpoint_data = {
                **entry.checkpoint_data,
                "written_rows": list(claim.written_rows),
                "unwritten_rows": list(claim.unwritten_rows),
                "remaining_rows": ledger.pending_rows,
            }
            if not ledger.pending_rows:
                entry.queue_status = "已恢复"
        return {"ok": True, "claim": claim_code,
                "written_rows": claim.written_rows,
                "unwritten_rows": claim.unwritten_rows,
                "pending_rows": ledger.pending_rows}

    def get_claim(self, claim_code: str) -> RecoveryClaim | None:
        return self._claims.get(claim_code)

    @property
    def all_claims(self) -> list[RecoveryClaim]:
        return list(self._claims.values())

    @property
    def all_entries(self) -> list[ReplayEntry]:
        return list(self._entries.values())

    def domain_snapshot(self) -> dict[str, Any]:
        entries = self.all_entries
        by_status = {
            status: sum(1 for e in entries if e.queue_status == status)
            for status in ["待重放", "重放中", "已恢复", "永久跳过", "待核对"]
        }
        return {
            "module": "replay_queue",
            "entity": "重放记录",
            "count": len(entries),
            "by_status": by_status,
            "pending": by_status.get("待重放", 0),
            "permanently_skipped": by_status.get("永久跳过", 0),
            "awaiting_verification": by_status.get("待核对", 0),
            "claims": len(self._claims),
        }


class CheckpointManager:
    """检查点管理器：维护回填任务的断点位置与恢复元数据。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._recovery_log: list[dict[str, Any]] = []
        self._ledgers: dict[str, BackfillEvidenceLedger] = {}
        # 续办证据清单落盘目录；空字符串表示只保存在当前进程内。
        self.evidence_dir = ""

    # ---- 续办证据清单登记与回查 ----

    def _ledger_path(self, task_code: str) -> Path | None:
        if not self.evidence_dir:
            return None
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in task_code)
        return Path(self.evidence_dir) / f"{safe}.evidence.json"

    def register_ledger(self, ledger: BackfillEvidenceLedger) -> BackfillEvidenceLedger:
        """登记一份续办证据清单，供恢复队列和值班按行身份回查。"""
        self._ledgers[ledger.task_code] = ledger
        self.persist_ledger(ledger.task_code)
        self._recovery_log.append({
            "task_code": ledger.task_code,
            "action": "ledger_registered",
            "at": _now_iso(),
            "result": {"partition": ledger.partition, "rows": len(ledger.row_order)},
        })
        return ledger

    def persist_ledger(self, task_code: str) -> dict[str, Any] | None:
        """把清单快照写到 evidence_dir；没有配置目录时只保留在内存。"""
        ledger = self._ledgers.get(task_code)
        if ledger is None:
            return None
        snapshot = ledger.dump()
        path = self._ledger_path(task_code)
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
        return snapshot

    def get_ledger(self, task_code: str) -> BackfillEvidenceLedger | None:
        ledger = self._ledgers.get(task_code)
        if ledger is not None:
            return ledger
        path = self._ledger_path(task_code)
        if path is None or not path.exists():
            return None
        ledger = BackfillEvidenceLedger.load(
            json.loads(path.read_text(encoding="utf-8")))
        self._ledgers[task_code] = ledger
        return ledger

    def reload_ledger(self, task_code: str) -> BackfillEvidenceLedger | None:
        """丢弃内存副本，强制从落盘快照重建，用于进程重启后的持久回查。"""
        self._ledgers.pop(task_code, None)
        return self.get_ledger(task_code)

    def evidence_checklist(self, task_code: str) -> dict[str, Any] | None:
        """回查某任务当前续办证据清单，断点被清掉后仍可读到已核定写入行。"""
        ledger = self.get_ledger(task_code)
        if ledger is None:
            return None
        return ledger.checklist()

    def get_active_ledgers(self) -> list[dict[str, Any]]:
        return [
            {
                "task_code": ledger.task_code,
                "partition": ledger.partition,
                "written_count": len(ledger.written_rows),
                "pending_count": len(ledger.pending_rows),
                "open_attempt": ledger.unresolved_attempt.attempt_no
                if ledger.unresolved_attempt else None,
            }
            for ledger in self._ledgers.values()
        ]

    def save_checkpoint(self, task_code: str, position: str,
                        metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        checkpoint = {
            "task_code": task_code,
            "position": position,
            "saved_at": _now_iso(),
            "metadata": metadata or {},
        }
        self._checkpoints[task_code] = checkpoint
        return dict(checkpoint)

    def load_checkpoint(self, task_code: str) -> dict[str, Any] | None:
        checkpoint = self._checkpoints.get(task_code)
        if checkpoint is None:
            return None
        return dict(checkpoint)

    def clear_checkpoint(self, task_code: str) -> bool:
        if task_code in self._checkpoints:
            self._recovery_log.append({
                "task_code": task_code,
                "action": "cleared",
                "at": _now_iso(),
                "former_position": self._checkpoints[task_code].get("position"),
            })
            del self._checkpoints[task_code]
            return True
        return False

    def record_recovery(self, task_code: str, action: str,
                        result: dict[str, Any] | None = None) -> None:
        self._recovery_log.append({
            "task_code": task_code,
            "action": action,
            "at": _now_iso(),
            "result": result or {},
        })

    def get_active_checkpoints(self) -> list[dict[str, Any]]:
        return [
            {"task_code": tc, "position": cp["position"], "saved_at": cp["saved_at"]}
            for tc, cp in self._checkpoints.items()
        ]

    def get_log(self) -> list[dict[str, Any]]:
        return self._recovery_log[-100:]

    def replay_from_checkpoint(self, task_code: str,
                               replay_fn: Any,
                               total_items: int,
                               batch_size: int = 100) -> dict[str, Any]:
        """从检查点重放处理：调用 replay_fn(batch_items, start_index) 逐批恢复。"""
        checkpoint = self.load_checkpoint(task_code)
        start_idx = 0
        if checkpoint:
            try:
                start_idx = int(checkpoint.get("position", 0))
            except (ValueError, TypeError):
                start_idx = 0

        self.record_recovery(task_code, "replay_started",
                             {"start_idx": start_idx, "total": total_items})

        processed = start_idx
        current = start_idx
        errors: list[dict[str, Any]] = []

        while current < total_items:
            end_idx = min(current + batch_size, total_items)
            batch = list(range(current, end_idx))
            try:
                result = replay_fn(batch, current)
                if isinstance(result, dict) and not result.get("ok", True):
                    errors.append({"segment": f"{current}-{end_idx}",
                                   "error": result.get("message", "unknown")})
                    break
                processed = end_idx
                self.save_checkpoint(task_code, str(processed))
                current = end_idx
            except Exception as e:
                errors.append({"segment": f"{current}-{end_idx}", "error": str(e)})
                break

        complete = processed >= total_items
        if complete:
            self.clear_checkpoint(task_code)

        self.record_recovery(task_code,
                             "replay_completed" if complete else "replay_failed",
                             {"processed": processed, "total": total_items, "errors": len(errors)})

        return {
            "ok": len(errors) == 0,
            "task_code": task_code,
            "processed": processed,
            "total": total_items,
            "complete": complete,
            "errors": errors,
        }


class BackfillRecoveryCoordinator:
    """续办协调器：把失败尝试、迟到回执、领取、写入与分区核对串成一条证据链。

    所有状态变更都落在续办证据清单里，并立即持久化；断点只是提示，
    恢复动作始终以清单中真实已核定写入的行为准。
    """

    def __init__(self, manager: CheckpointManager | None = None,
                 queue: ReplayQueue | None = None) -> None:
        self.manager = manager if manager is not None else checkpoint_manager
        self.queue = queue if queue is not None else replay_queue

    # ---- 1. 分区与失败尝试 ----

    def open_partition(self, task_code: str, partition: str,
                       rows: list[PartitionRow]) -> dict[str, Any]:
        ledger = BackfillEvidenceLedger(task_code, partition, rows)
        self.manager.register_ledger(ledger)
        return ledger.checklist()

    def record_failed_attempt(self, task_code: str, attempt_no: int,
                              scope: list[str], failure_reason: str,
                              note: str = "") -> dict[str, Any]:
        ledger = self._require(task_code)
        attempt = ledger.record_attempt(attempt_no, scope, failure_reason, note=note)
        self.manager.persist_ledger(task_code)
        return {
            "attempt_no": attempt.attempt_no,
            "failure_reason": attempt.failure_reason,
            "scope": list(attempt.scope),
            "failed_at": attempt.failed_at,
        }

    # ---- 2. 回执 ----

    def accept_receipt(self, task_code: str, rows: list[str],
                       attempt_no: int | None = None,
                       note: str = "") -> dict[str, Any]:
        ledger = self._require(task_code)
        result = ledger.accept_receipt(rows, attempt_no=attempt_no, note=note)
        self.manager.persist_ledger(task_code)
        self.manager.record_recovery(task_code, "receipt_received", result)
        return result

    # ---- 3. 恢复队列领取 ----

    def issue_claim(self, task_code: str, entry_id: int) -> dict[str, Any]:
        ledger = self._require(task_code)
        claim = self.queue.claim_from_ledger(entry_id, ledger)
        self.manager.persist_ledger(task_code)
        if claim is None:
            entry = self.queue.get_entry(entry_id)
            return {
                "ok": False,
                "reason": "无可领取范围" if entry is not None else "重放条目不存在",
                "queue_status": entry.queue_status if entry is not None else None,
                "pending_rows": ledger.pending_rows,
                "held_attempt": ledger.unresolved_attempt.attempt_no
                if ledger.unresolved_attempt else None,
            }
        return {
            "ok": True,
            "claim": claim.claim_code,
            "rows": list(claim.rows),
            "partition": claim.partition,
            "note": claim.note,
        }

    def write_claim(self, task_code: str, claim_code: str,
                    written_rows: list[str]) -> dict[str, Any]:
        ledger = self._require(task_code)
        result = self.queue.record_claim_write(ledger, claim_code, written_rows)
        self.manager.persist_ledger(task_code)
        self.manager.record_recovery(task_code, "claim_written", result)
        return result

    # ---- 4. 分区核对 ----

    def verify_partition(self, task_code: str) -> dict[str, Any]:
        """恢复后读取分区，核对本次有效明细。"""
        ledger = self._require(task_code)
        effective = ledger.effective_rows()
        self.manager.record_recovery(
            task_code, "partition_verified",
            {"effective_rows": [row["row_id"] for row in effective]})
        return {
            "ok": True,
            "partition": ledger.partition,
            "total": len(ledger.row_order),
            "effective_count": len(effective),
            "effective_rows": effective,
            "pending_rows": ledger.pending_rows,
        }

    def resolve_attempt(self, task_code: str, attempt_no: int,
                        written_rows: list[str] | None = None,
                        note: str = "") -> dict[str, Any]:
        """核对失败尝试的真实写入结果；不传行时采用冲突回执里的行。

        核对动作只认证据：可以确认回执里的行确实写入，也可以确认它们没写。
        """
        ledger = self._require(task_code)
        asserted = written_rows
        if asserted is None:
            asserted = ledger.conflict_receipt_rows(attempt_no)
        result = ledger.resolve_attempt(attempt_no, asserted, note=note)
        self.manager.persist_ledger(task_code)
        self.manager.record_recovery(task_code, "attempt_resolved", result)
        return result

    def attempt_evidence(self, task_code: str,
                         attempt_no: int | None = None) -> dict[str, Any] | None:
        ledger = self._require(task_code)
        return ledger.attempt_evidence(attempt_no)

    def checklist(self, task_code: str) -> dict[str, Any] | None:
        return self.manager.evidence_checklist(task_code)

    def _require(self, task_code: str) -> BackfillEvidenceLedger:
        ledger = self.manager.get_ledger(task_code)
        if ledger is None:
            raise KeyError(task_code)
        return ledger


# 全局单例
checkpoint_manager = CheckpointManager()
replay_queue = ReplayQueue()
recovery_coordinator = BackfillRecoveryCoordinator()