"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import recovery_evidence, resource

# 固定前缀的续办证据路由先注册，避免被通用 /api/{module} 动态段抢先匹配。
ROUTERS = [recovery_evidence, resource]
