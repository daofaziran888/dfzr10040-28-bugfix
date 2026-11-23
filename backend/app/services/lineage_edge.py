"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class LineageEdgeService(BaseModuleService):
    MODULE = "lineage_edge"
    ENTITY = "血缘边"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["边编号", "上游节点", "下游节点", "关联字段", "转换函数", "关联类型", "依赖层级", "最后追踪"]
    REQUIRED_FIELDS = ["边编号", "上游节点", "下游节点"]
    STATUS_ORDER = ["已追踪", "已确认", "疑似缺失", "已废弃"]
    ACTION_RULES = {"确认血缘": "已确认", "标记缺失": "疑似缺失", "废弃边": "已废弃"}
    NEGATIVE_ACTIONS = ["废弃边", "标记缺失"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """废弃边同步标记为撤销，避免只改列表状态却仍被指标计算当作可用依据。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "废弃边":
            entry["已撤销"] = True
            entry["superseded_by"] = ""
        elif action in ("确认血缘", "标记缺失"):
            entry["已撤销"] = False
        return entry, message
