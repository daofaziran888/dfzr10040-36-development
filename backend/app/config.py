"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _default_evidence_dir() -> str:
    """续办证据清单的落盘目录，默认放在后端仓库内的 .state 下（已被 git 忽略）。"""
    configured = os.environ.get("FURNACE_EVIDENCE_DIR")
    if configured:
        return configured
    return str(Path(__file__).resolve().parent.parent / ".state" / "evidence")


@dataclass(frozen=True)
class Settings:
    app_name: str = "熔炉工坊 · 实时数据融合与宽表构建平台"
    env: str = "local"
    port: int = 8000
    evidence_dir: str = field(default_factory=_default_evidence_dir)
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200


settings = Settings()
