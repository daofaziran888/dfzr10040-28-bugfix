"""指标聚合领域服务：通用规则流转之上，额外维护聚合依据回执与物化读取。"""
from __future__ import annotations

from typing import Any

from app.fusion_engine import fusion_engine
from app.services.base import BaseModuleService


class MetricAggrService(BaseModuleService):
    MODULE = "metric_aggr"
    ENTITY = "聚合规则"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["规则编号", "目标宽表", "聚合函数", "分组维度", "时间粒度", "预计算窗口", "存储表名", "最近计算"]
    REQUIRED_FIELDS = ["规则编号", "目标宽表", "聚合函数"]
    STATUS_ORDER = ["待计算", "计算中", "已物化", "需重建"]
    ACTION_RULES = {"触发计算": "计算中", "重建物化": "已物化", "标记失效": "需重建"}
    NEGATIVE_ACTIONS = []

    def materializations(self, target_wide_table: str | None = None) -> list[dict[str, Any]]:
        """按目标表列出物化记录，供详情页核对采用的字段与血缘版本。"""
        return fusion_engine.list_materializations(target_wide_table=target_wide_table)

    def read_materialization(self, ref: str, allow_stale: bool = False) -> dict[str, Any]:
        """读取物化：回执失效时返回拒绝依据，不把历史分组当当前结果。"""
        return fusion_engine.read_materialization(ref, allow_stale=allow_stale)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """聚合规则详情附带该规则的物化记录，方便核对分组与来源字段。"""
        entry = super().get_entry(entry_id)
        if entry is None:
            return None
        detail = dict(entry)
        detail["物化记录"] = self.materializations_by_rule(str(entry.get("规则编号", "")))
        return detail

    def materializations_by_rule(self, metric_rule_code: str) -> list[dict[str, Any]]:
        return fusion_engine.list_materializations(metric_rule_code=metric_rule_code)
