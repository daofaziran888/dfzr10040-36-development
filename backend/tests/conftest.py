"""测试隔离：续办证据清单默认只留在内存，避免测试往仓库里写运行期状态。"""
from __future__ import annotations

import pytest

from app.recovery import checkpoint_manager, replay_queue


@pytest.fixture(autouse=True)
def _isolate_recovery_state():
    original_dir = checkpoint_manager.evidence_dir
    original_ledgers = dict(checkpoint_manager._ledgers)
    original_entries = dict(replay_queue._entries)
    original_claims = dict(replay_queue._claims)
    checkpoint_manager.evidence_dir = ""
    checkpoint_manager._ledgers.clear()
    replay_queue._entries.clear()
    replay_queue._claims.clear()
    try:
        yield
    finally:
        checkpoint_manager.evidence_dir = original_dir
        checkpoint_manager._ledgers = original_ledgers
        replay_queue._entries = original_entries
        replay_queue._claims = original_claims
