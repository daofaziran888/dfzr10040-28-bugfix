"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import (
    BackfillTask,
    ColdStoreStrategy,
    CrossValidatePlan,
    DataDriftRecord,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    MetricMaterialization,
    AggregationBasisReceipt,
    PartitionRule,
    QualityGateRule,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._aggregation_receipts: dict[str, AggregationBasisReceipt] = {}
        self._materializations: dict[str, MetricMaterialization] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "") -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
        )
        self._snapshots[sid] = snap
        return snap

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "") -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
        )
        self._backfills[sid] = task
        return task

    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    #: 只有这两种状态的血缘边能证明字段关系适用于一次计算。
    USABLE_LINEAGE_STATUSES = ("已追踪", "已确认")

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1,
                            edge_status: str = "已追踪",
                            version: int = 1) -> LineageEdge:
        """建立一条数据血缘边。

        边默认从 v1 的「已追踪」开始。更正边由 publish_correction_edge 生成，
        初始为「待确认」，在确认前不能用于证明字段关系。
        """
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=list(related_fields),
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
            version=version,
            edge_status=edge_status,
        )
        self._lineages[sid] = edge
        return edge

    def _find_lineage_edge(self, edge_ref: int | str) -> LineageEdge | None:
        """按主键或边编号查找血缘边，兼容接口上传边编号的调用方式。"""
        if isinstance(edge_ref, str):
            for edge in self._lineages.values():
                if edge.edge_code == edge_ref:
                    return edge
            try:
                edge_ref = int(edge_ref)
            except (TypeError, ValueError):
                return None
        return self._lineages.get(int(edge_ref))

    @classmethod
    def is_lineage_edge_usable(cls, edge: LineageEdge | None) -> bool:
        """血缘边是否仍可用于证明字段关系（未被撤销、未处于待确认/疑似缺失）。"""
        return bool(
            edge is not None
            and not edge.revoked
            and edge.edge_status in cls.USABLE_LINEAGE_STATUSES
        )

    def revoke_lineage_edge(self, edge_ref: int | str, reason: str = "",
                            superseded_by: str = "") -> dict[str, Any]:
        """撤销血缘边：撤销后立即失去取数证明力，后续计算与物化读取都不得再采用。"""
        edge = self._find_lineage_edge(edge_ref)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {edge_ref} 不存在"}
        if edge.revoked:
            return {
                "ok": True, "already_revoked": True,
                "edge_code": edge.edge_code, "version": edge.version,
                "message": "血缘边已处于撤销状态",
            }
        edge.revoked = True
        edge.edge_status = "已废弃"
        edge.change_reason = reason or edge.change_reason
        if superseded_by:
            edge.superseded_by = superseded_by
        edge.revoked_at = _now_iso()
        return {
            "ok": True,
            "edge_code": edge.edge_code,
            "version": edge.version,
            "status": edge.edge_status,
            "revoked_at": edge.revoked_at,
            "message": f"血缘边 {edge.edge_code} v{edge.version} 已撤销",
        }

    def publish_correction_edge(self, original_edge_ref: int | str,
                                related_fields: list[str] | None = None,
                                transform_function: str = "",
                                reason: str = "") -> dict[str, Any]:
        """发布更正边：继承原边的上下游与版本号 +1，初始为「待确认」。

        更正边在确认前不能替代原边，也不能用于裁定新的计算依据；确认后原边自动撤销。
        """
        original = self._find_lineage_edge(original_edge_ref)
        if original is None:
            return {"ok": False, "message": f"数据血缘边 {original_edge_ref} 不存在"}
        correction = self.create_lineage_edge(
            upstream_node=original.upstream_node,
            downstream_node=original.downstream_node,
            related_fields=related_fields if related_fields is not None else original.related_fields,
            transform_function=transform_function or original.transform_function,
            edge_type="correction",
            dependency_level=original.dependency_level,
            edge_status="待确认",
            version=original.version + 1,
        )
        correction.change_reason = reason
        correction.supersedes = original.edge_code
        original.superseded_by = correction.edge_code
        if reason:
            original.change_reason = reason
        return {
            "ok": True,
            "edge_code": correction.edge_code,
            "version": correction.version,
            "status": correction.edge_status,
            "fields": list(correction.related_fields),
            "original_edge_code": original.edge_code,
            "original_version": original.version,
            "original_revoked": original.revoked,
            "message": f"更正边 {correction.edge_code} v{correction.version} 已发布，待确认后生效",
        }

    def confirm_lineage_edge(self, edge_ref: int | str) -> dict[str, Any]:
        """确认血缘边；确认更正边时，被它替代的旧边同步撤销。"""
        edge = self._find_lineage_edge(edge_ref)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {edge_ref} 不存在"}
        if edge.revoked:
            return {"ok": False, "message": f"血缘边 {edge.edge_code} 已撤销，不能确认"}
        edge.edge_status = "已确认"
        edge.last_traced = _now_iso()
        superseded: list[str] = []
        for other in self._lineages.values():
            if (other.edge_id != edge.edge_id
                    and other.superseded_by == edge.edge_code):
                if not other.revoked:
                    self.revoke_lineage_edge(
                        other.edge_id,
                        reason=f"被更正边 {edge.edge_code} v{edge.version} 替代",
                        superseded_by=edge.edge_code,
                    )
                superseded.append(other.edge_code)
        return {
            "ok": True,
            "edge_code": edge.edge_code,
            "version": edge.version,
            "status": edge.edge_status,
            "superseded_edges": superseded,
            "message": f"血缘边 {edge.edge_code} v{edge.version} 已确认",
        }

    def _resolve_lineage_coverage(
        self,
        target_wide_table: str,
        fields: list[str],
        preferred_edge: LineageEdge,
        implicitly_allowed: set[str] | None = None,
    ) -> dict[str, Any]:
        """核对目标表归属与逐字段取数权限。

        只有本次传入的血缘边能证明字段关系：边下游节点必须等于目标表，字段必须
        出现在该边的关联字段里。不会借用同表其他边的权限，避免跨边提权；字段
        未覆盖时区分「边归属错误」「根本没有边覆盖」「边已撤销」「更正边待确认」。
        """
        implicitly_allowed = implicitly_allowed or set()
        ownership_ok = preferred_edge.downstream_node == target_wide_table
        field_permissions: dict[str, dict[str, Any]] = {}
        denied: dict[str, str] = {}

        for field in fields:
            if field in implicitly_allowed:
                field_permissions[field] = {
                    "read": True, "edge_code": "", "edge_version": 0,
                    "source": "target_table_partition_key",
                }
                continue

            if field in preferred_edge.related_fields:
                field_permissions[field] = {
                    "read": True,
                    "edge_code": preferred_edge.edge_code,
                    "edge_version": preferred_edge.version,
                    "edge_status": preferred_edge.edge_status,
                    "source": f"{preferred_edge.edge_code}@v{preferred_edge.version}",
                }
                continue

            related = [
                edge for edge in self._lineages.values()
                if edge.downstream_node == target_wide_table
                and field in edge.related_fields
            ]
            if any(edge.revoked for edge in related):
                denied[field] = f"字段 {field} 的血缘边已被撤销，当前没有可证明适用的字段关系"
            elif related:
                denied[field] = (
                    f"血缘边 {preferred_edge.edge_code} v{preferred_edge.version} 未覆盖字段 "
                    f"{field}，不能借用其他血缘边，本次无读取权限"
                )
            elif not ownership_ok:
                denied[field] = (
                    f"血缘边 {preferred_edge.edge_code} 的归属表是 "
                    f"{preferred_edge.downstream_node}，与目标表 {target_wide_table} 无关"
                )
            else:
                denied[field] = (
                    f"血缘边 {preferred_edge.edge_code} v{preferred_edge.version} 未覆盖字段 "
                    f"{field}，目标表 {target_wide_table} 无读取权限"
                )

        return {
            "ownership_ok": ownership_ok,
            "field_permissions": field_permissions,
            "denied_fields": denied,
        }

    def _aggregation_permissions(self, target_wide_table: str,
                                 field_permissions: dict[str, dict[str, Any]],
                                 primary_edge: LineageEdge,
                                 measure_field: str | None,
                                 group_dimensions: list[str]) -> dict[str, Any]:
        """聚合依据回执中的表/字段取数权限说明。"""
        fields: dict[str, dict[str, Any]] = {}
        adopted: dict[str, dict[str, Any]] = {}
        for field, permission in field_permissions.items():
            role = "measure" if field == measure_field else "group"
            fields[field] = {**permission, "role": role}
            if permission.get("edge_code"):
                key = f"{permission['edge_code']}@v{permission['edge_version']}"
                adopted.setdefault(key, {
                    "edge_code": permission["edge_code"],
                    "version": permission["edge_version"],
                    "fields": [],
                })
                adopted[key]["fields"].append(field)

        for item in adopted.values():
            edge = self._find_lineage_edge(item["edge_code"])
            if edge is not None:
                item["upstream_node"] = edge.upstream_node
                item["downstream_node"] = edge.downstream_node
                item["status"] = edge.edge_status
                item["related_fields"] = list(edge.related_fields)

        return {
            "target_table": target_wide_table,
            "table": {
                "name": target_wide_table,
                "read": True,
                "owned_by_edge": primary_edge.edge_code,
                "downstream_node": primary_edge.downstream_node,
            },
            "fields": fields,
            "field_permissions": fields,
            "allowed_fields": sorted(fields),
            "measure_field": measure_field,
            "group_dimensions": list(group_dimensions),
            "adopted_edges": sorted(
                adopted.values(),
                key=lambda item: (item["edge_code"], item["version"]),
            ),
        }

    def _rows_for_target_table(self, target_wide_table: str) -> list[dict[str, Any]]:
        job = self._find_fusion_job_by_table(target_wide_table)
        if job is None:
            return []
        return list(job.merged_records)

    @staticmethod
    def _is_missing_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == "" or value.strip().upper() in {"NULL", "NONE"}
        return False

    @staticmethod
    def _numeric_value(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _materialize_from_receipt(
        self,
        receipt: AggregationBasisReceipt,
    ) -> MetricMaterialization:
        """按聚合依据回执读取真实输入范围并物化分组明细。

        只读取回执授权的字段；分组键缺失的行不会进入任何分组，也不会有空键分组
        被当作合法分组展示。
        """
        rows = self._rows_for_target_table(receipt.target_wide_table)
        allowed_fields = set(receipt.permissions.get("allowed_fields", []))
        group_dimensions = list(receipt.group_dimensions)
        measure_field = receipt.measure_field

        groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        ordered_values: dict[tuple[Any, ...], list[Any]] = {}
        skipped_missing_key = 0
        skipped_missing_measure = 0
        skipped_unpermitted = 0
        rows_used = 0

        required_read_fields = set(group_dimensions)
        if measure_field is not None:
            required_read_fields.add(measure_field)
        if not required_read_fields <= allowed_fields:
            # 回执没有授权全部分组/度量字段时不做任何取数。
            skipped_unpermitted = len(rows)
            rows = []

        for row in rows:
            # 输入范围只取回执授权的字段；未授权字段不会被读取。
            projected = {
                field: row.get(field)
                for field in allowed_fields
                if field in row
            }
            if any(self._is_missing_value(projected.get(field))
                   for field in group_dimensions):
                skipped_missing_key += 1
                continue

            key = tuple(str(projected.get(field)).strip() for field in group_dimensions)
            rows_used += 1
            group = groups.get(key)
            if group is None:
                group = {
                    "group": {
                        field: projected.get(field) for field in group_dimensions
                    },
                    "group_key": ",".join(
                        f"{field}={projected.get(field)}"
                        for field in group_dimensions
                    ),
                    "rows": 0,
                    "group_fields": list(group_dimensions),
                    "source_fields": {
                        field: receipt.permissions["fields"][field]["source"]
                        for field in group_dimensions
                    },
                }
                groups[key] = group
                ordered_values[key] = []
            group["rows"] += 1

            if measure_field is None:
                continue
            measure_raw = projected.get(measure_field)
            if self._is_missing_value(measure_raw):
                group["rows_missing_measure"] = group.get("rows_missing_measure", 0) + 1
                skipped_missing_measure += 1
                continue
            numeric = self._numeric_value(measure_raw)
            if numeric is None:
                if receipt.agg_function.upper() == "COUNT_DISTINCT":
                    ordered_values[key].append(measure_raw)
                else:
                    group["rows_missing_measure"] = group.get("rows_missing_measure", 0) + 1
                    skipped_missing_measure += 1
                continue
            ordered_values[key].append(numeric)

        for key, group in groups.items():
            values = ordered_values.get(key, [])
            group["measure"] = self._aggregate_values(receipt.agg_function, values)
            if receipt.agg_function.upper() == "COUNT" and receipt.measure_field is None:
                group["measure"] = group["rows"]
            group["adopted_edges"] = list(receipt.adopted_edge_versions)

        input_range = {
            "target_table": receipt.target_wide_table,
            "source_nodes": sorted({
                item.get("upstream_node", "")
                for item in receipt.adopted_edge_versions
                if item.get("upstream_node")
            }),
            "read_fields": sorted(allowed_fields),
            "field_sources": {
                field: receipt.permissions["fields"][field]["source"]
                for field in receipt.permissions["fields"]
            },
            "rows_scanned": len(rows),
            "rows_used": rows_used,
            "rows_skipped_missing_key": skipped_missing_key,
            "rows_skipped_missing_measure": skipped_missing_measure,
            "rows_skipped_unpermitted": skipped_unpermitted,
        }

        materialization = MetricMaterialization(
            materialization_code=f"MATL-{len(self._materializations) + 1:04d}",
            receipt_code=receipt.receipt_code,
            metric_rule_code=receipt.metric_rule_code,
            target_wide_table=receipt.target_wide_table,
            agg_function=receipt.agg_function,
            measure_field=receipt.measure_field,
            group_dimensions=list(receipt.group_dimensions),
            groups=list(groups.values()),
            skipped_missing_key=skipped_missing_key,
            skipped_unpermitted=skipped_unpermitted,
            adopted_edge_versions=copy.deepcopy(receipt.adopted_edge_versions),
            input_range=input_range,
            created_at=_now_iso(),
        )
        self._materializations[materialization.materialization_code] = materialization
        return materialization

    @staticmethod
    def _aggregate_values(agg_function: str, values: list[Any]) -> Any:
        function = (agg_function or "").upper()
        if function == "COUNT":
            return len(values)
        numeric = [value for value in values if isinstance(value, (int, float))]
        if function == "COUNT_DISTINCT":
            return len(set(values))
        if not numeric:
            return None
        if function == "AVG":
            return sum(numeric) / len(numeric)
        if function == "MAX":
            return max(numeric)
        if function == "MIN":
            return min(numeric)
        return sum(numeric)

    @staticmethod
    def _normalize_dimensions(dimensions: list[str] | str | None) -> list[str]:
        """分组维度兼容列表、逗号分隔字符串和空值，去重后保持原顺序。"""
        if dimensions is None:
            return []
        if isinstance(dimensions, str):
            items = dimensions.replace("，", ",").split(",")
        else:
            items = list(dimensions)
        normalized = [str(item).strip() for item in items if str(item).strip()]
        return list(dict.fromkeys(normalized))

    def compute_metric_from_lineage(
        self,
        lineage_edge_id: int | str,
        target_wide_table: str,
        agg_function: str,
        group_dimensions: list[str],
        measure_field: str | None = None,
        *,
        materialize: bool = True,
    ) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则，并生成聚合依据回执与物化记录。

        取数前先核对边对目标表的归属与逐字段权限：
        - 边下游节点不是目标表时立即拒绝，且不创建规则/回执/物化；
        - 分组或度量字段没有被任何可用血缘边覆盖时拒绝，不读取表数据。
        只有核对通过才会冻结回执，回执记录实际采用的血缘边编号与版本。
        """
        edge = self._find_lineage_edge(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}

        if edge.downstream_node != target_wide_table:
            return {
                "ok": False,
                "unauthorized": True,
                "message": (
                    f"越权取数：血缘边 {edge.edge_code} 归属表为 "
                    f"{edge.downstream_node}，不能为目标表 {target_wide_table} 计算指标"
                ),
                "lineage_edge": {
                    "edge_code": edge.edge_code,
                    "version": edge.version,
                    "upstream_node": edge.upstream_node,
                    "downstream_node": edge.downstream_node,
                },
                "target_table": target_wide_table,
            }

        if not self.is_lineage_edge_usable(edge):
            if edge.revoked:
                reason = f"血缘边 {edge.edge_code} v{edge.version} 已撤销，不能作为本次计算依据"
            else:
                reason = (
                    f"血缘边 {edge.edge_code} v{edge.version} 状态为「{edge.edge_status}」，"
                    "不能证明字段关系适用于本次计算"
                )
            return {
                "ok": False,
                "basis_rejected": True,
                "message": reason,
                "lineage_edge": {
                    "edge_code": edge.edge_code,
                    "version": edge.version,
                    "status": edge.edge_status,
                },
                "target_table": target_wide_table,
            }

        group_dimensions = self._normalize_dimensions(group_dimensions)
        agg_function = str(agg_function or "").strip().upper()

        wide_table = next(
            (table for table in self._wide_tables.values()
             if table.table_name == target_wide_table),
            None,
        )
        # 宽表自身登记的分区键属于目标表元数据，不依赖血缘边授权；
        # 其余字段一律需要可用血缘边覆盖。
        implicitly_allowed = {wide_table.partition_key} if wide_table else set()

        if measure_field is None and (agg_function or "").upper() != "COUNT":
            for field in edge.related_fields:
                if field not in group_dimensions:
                    measure_field = field
                    break

        required_fields = list(group_dimensions)
        if measure_field is not None:
            required_fields.append(measure_field)
        required_fields = list(dict.fromkeys(required_fields))

        coverage = self._resolve_lineage_coverage(
            target_wide_table, required_fields, edge, implicitly_allowed)
        if coverage["denied_fields"]:
            return {
                "ok": False,
                "unauthorized": True,
                "message": "拒绝越权聚合：" + "；".join(coverage["denied_fields"].values()),
                "denied_fields": coverage["denied_fields"],
                "target_table": target_wide_table,
                "lineage_edge": {
                    "edge_code": edge.edge_code,
                    "version": edge.version,
                    "downstream_node": edge.downstream_node,
                    "related_fields": list(edge.related_fields),
                },
                "permissions": {
                    "target_table": target_wide_table,
                    "fields": coverage["field_permissions"],
                    "denied_fields": coverage["denied_fields"],
                },
            }

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        permissions = self._aggregation_permissions(
            target_wide_table, coverage["field_permissions"], edge,
            measure_field, group_dimensions,
        )
        receipt_code = f"RCPT-{len(self._aggregation_receipts) + 1:04d}"
        receipt = AggregationBasisReceipt(
            receipt_code=receipt_code,
            metric_rule_code=rule.rule_code,
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            measure_field=measure_field,
            group_dimensions=list(group_dimensions),
            permissions=copy.deepcopy(permissions),
            input_range={},
            adopted_edge_versions=copy.deepcopy(permissions["adopted_edges"]),
            created_at=_now_iso(),
        )
        self._aggregation_receipts[receipt_code] = receipt

        result: dict[str, Any] = {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "lineage_edge_version": edge.version,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
            "measure_field": measure_field,
            "receipt_code": receipt_code,
            "basis_receipt": receipt_code,
            "receipt": {
                "receipt_code": receipt_code,
                "metric_rule_code": rule.rule_code,
                "target_wide_table": target_wide_table,
                "permissions": copy.deepcopy(permissions),
                "adopted_edge_versions": copy.deepcopy(receipt.adopted_edge_versions),
                "created_at": receipt.created_at,
            },
            "adopted_edge_versions": copy.deepcopy(receipt.adopted_edge_versions),
            "permissions": copy.deepcopy(permissions),
        }

        if materialize:
            materialization = self._materialize_from_receipt(receipt)
            receipt.input_range = copy.deepcopy(materialization.input_range)
            receipt.input_range["read_permissions"] = copy.deepcopy(
                receipt.permissions.get("fields", {}))
            result["materialization_code"] = materialization.materialization_code
            result["input_range"] = copy.deepcopy(receipt.input_range)
            result["groups"] = copy.deepcopy(materialization.groups)
            result["skipped_missing_key"] = materialization.skipped_missing_key
            result["materialization"] = {
                "materialization_code": materialization.materialization_code,
                "receipt_code": materialization.receipt_code,
                "basis_receipt": receipt.receipt_code,
                "metric_rule_code": materialization.metric_rule_code,
                "target_wide_table": materialization.target_wide_table,
                "agg_function": materialization.agg_function,
                "measure_field": materialization.measure_field,
                "group_dimensions": list(materialization.group_dimensions),
                "groups": copy.deepcopy(materialization.groups),
                "skipped_missing_key": materialization.skipped_missing_key,
                "skipped_unpermitted": materialization.skipped_unpermitted,
                "adopted_edge_versions": copy.deepcopy(materialization.adopted_edge_versions),
                "input_range": copy.deepcopy(receipt.input_range),
                "created_at": materialization.created_at,
            }

        return result

    def get_aggregation_receipt(self, receipt_ref: str) -> dict[str, Any] | None:
        receipt = self._aggregation_receipts.get(receipt_ref)
        if receipt is None:
            return None
        return {
            "receipt_code": receipt.receipt_code,
            "metric_rule_code": receipt.metric_rule_code,
            "target_wide_table": receipt.target_wide_table,
            "agg_function": receipt.agg_function,
            "measure_field": receipt.measure_field,
            "group_dimensions": list(receipt.group_dimensions),
            "permissions": copy.deepcopy(receipt.permissions),
            "input_range": copy.deepcopy(receipt.input_range),
            "adopted_edge_versions": copy.deepcopy(receipt.adopted_edge_versions),
            "created_at": receipt.created_at,
        }

    def _find_materialization(self, ref: str) -> MetricMaterialization | None:
        if ref in self._materializations:
            return self._materializations[ref]
        for materialization in reversed(list(self._materializations.values())):
            if materialization.metric_rule_code == ref:
                return materialization
        for materialization in reversed(list(self._materializations.values())):
            if materialization.receipt_code == ref:
                return materialization
        return None

    def _check_materialization_basis(
        self, materialization: MetricMaterialization,
    ) -> list[dict[str, Any]]:
        """核对物化采用的每条血缘边版本是否仍能证明适用于该次计算。"""
        stale: list[dict[str, Any]] = []
        for adopted in materialization.adopted_edge_versions:
            edge = self._find_lineage_edge(adopted.get("edge_code", ""))
            fields = list(adopted.get("fields", []))
            detail = {
                "edge_code": adopted.get("edge_code"),
                "adopted_version": adopted.get("version"),
                "fields": fields,
            }
            if edge is None:
                detail.update({"current_version": None, "reason": "血缘边已不存在"})
                stale.append(detail)
                continue
            detail["current_version"] = edge.version
            detail["current_status"] = edge.edge_status
            if edge.revoked:
                detail["reason"] = "采用的血缘边已被撤销"
            elif edge.edge_status not in self.USABLE_LINEAGE_STATUSES:
                detail["reason"] = f"采用的血缘边当前状态为「{edge.edge_status}」"
            elif edge.version != adopted.get("version"):
                detail["reason"] = (
                    f"采用的版本 v{adopted.get('version')} 与当前 v{edge.version} 不一致"
                )
            elif any(field not in edge.related_fields for field in fields):
                detail["reason"] = "当前边的字段覆盖范围已变化"
            else:
                continue
            stale.append(detail)
        return stale

    def _pending_corrections(self, materialization: MetricMaterialization) -> list[dict[str, Any]]:
        """列出与该次物化采用的老边相关、但尚未确认的更正边。"""
        pending: list[dict[str, Any]] = []
        for adopted in materialization.adopted_edge_versions:
            edge = self._find_lineage_edge(adopted.get("edge_code", ""))
            if edge is None:
                continue
            corrections = [
                other for other in self._lineages.values()
                if other.supersedes == edge.edge_code
                or other.edge_code == edge.superseded_by
            ]
            for correction in corrections:
                pending.append({
                    "edge_code": correction.edge_code,
                    "version": correction.version,
                    "status": correction.edge_status,
                    "usable": self.is_lineage_edge_usable(correction),
                    "fields": list(correction.related_fields),
                })
        return pending

    def read_materialization(self, materialization_ref: str,
                             allow_stale: bool = False) -> dict[str, Any]:
        """读取物化详情，并核对本次物化实际采用的血缘版本。

        回执在受理时冻结，之后的撤销/更正不会改写历史记录；一旦采用的血缘边
        已撤销、被替代或字段范围变化，默认拒绝把这份物化当作当前有效依据，
        同时在输出中保留采用版本与失效原因供核对。
        """
        materialization = self._find_materialization(materialization_ref)
        if materialization is None:
            return {"ok": False, "message": f"物化记录 {materialization_ref} 不存在"}

        stale = self._check_materialization_basis(materialization)
        base: dict[str, Any] = {
            "materialization_code": materialization.materialization_code,
            "receipt_code": materialization.receipt_code,
            "basis_receipt": materialization.receipt_code,
            "metric_rule_code": materialization.metric_rule_code,
            "target_wide_table": materialization.target_wide_table,
            "agg_function": materialization.agg_function,
            "measure_field": materialization.measure_field,
            "group_dimensions": list(materialization.group_dimensions),
            "adopted_edge_versions": copy.deepcopy(materialization.adopted_edge_versions),
            "input_range": copy.deepcopy(materialization.input_range),
            "group_count": len(materialization.groups),
            "skipped_missing_key": materialization.skipped_missing_key,
            "skipped_unpermitted": materialization.skipped_unpermitted,
            "created_at": materialization.created_at,
            "basis_valid": not stale,
        }

        if stale:
            base.update({
                "ok": False,
                "status": "依据已失效",
                "message": (
                    "本次物化采用的血缘关系已不能证明适用于该次计算："
                    + "；".join(
                        f"{item['edge_code']} v{item['adopted_version']} {item['reason']}"
                        for item in stale
                    )
                ),
                "rejection_basis": "本次物化采用的血缘边已撤销或被更正，"
                                   "且更正边未参与该次计算，不能据此继续取数；请确认新边后重算",
                "stale_edges": stale,
                "basis_frozen": True,
                "requires_recompute": True,
                "historical_groups": copy.deepcopy(materialization.groups),
                "pending_corrections": self._pending_corrections(materialization),
            })
            if allow_stale:
                base["groups"] = copy.deepcopy(materialization.groups)
            return base

        base["pending_corrections"] = self._pending_corrections(materialization)

        base.update({
            "ok": True,
            "status": "依据有效",
            "groups": copy.deepcopy(materialization.groups),
            "materialization": {
                "materialization_code": materialization.materialization_code,
                "receipt_code": materialization.receipt_code,
                "metric_rule_code": materialization.metric_rule_code,
                "target_wide_table": materialization.target_wide_table,
                "agg_function": materialization.agg_function,
                "measure_field": materialization.measure_field,
                "group_dimensions": list(materialization.group_dimensions),
                "groups": copy.deepcopy(materialization.groups),
                "skipped_missing_key": materialization.skipped_missing_key,
                "skipped_unpermitted": materialization.skipped_unpermitted,
                "adopted_edge_versions": copy.deepcopy(materialization.adopted_edge_versions),
                "input_range": copy.deepcopy(materialization.input_range),
                "created_at": materialization.created_at,
            },
        })
        return base

    # 兼容按「物化/聚合详情」语义调用的入口。
    get_materialization = read_materialization

    def list_materializations(self, metric_rule_code: str | None = None,
                              target_wide_table: str | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for materialization in self._materializations.values():
            if metric_rule_code and materialization.metric_rule_code != metric_rule_code:
                continue
            if target_wide_table and materialization.target_wide_table != target_wide_table:
                continue
            items.append({
                "materialization_code": materialization.materialization_code,
                "receipt_code": materialization.receipt_code,
                "metric_rule_code": materialization.metric_rule_code,
                "target_wide_table": materialization.target_wide_table,
                "adopted_edge_versions": copy.deepcopy(materialization.adopted_edge_versions),
                "group_count": len(materialization.groups),
                "created_at": materialization.created_at,
            })
        return items

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=list(group_dimensions),
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int) -> dict[str, Any]:
        """从分区快照执行数据导出。"""
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        records = fusion_job.merged_records if fusion_job else []

        if plan.field_filter:
            records = [
                {k: v for k, v in rec.items() if k in plan.field_filter}
                for rec in records
            ]

        if plan.row_limit > 0 and len(records) > plan.row_limit:
            records = records[:plan.row_limit]

        plan.export_status = "已完成"

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(records),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": records,
        }

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生数据血缘边。"""
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        wt.source_count = len(job.participant_sources)
        wt.last_updated = _now_iso()

        lineages: list[str] = []
        for source_code in job.participant_sources:
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            lineages.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": wt.source_count,
            "lineage_edges": lineages,
            "last_updated": wt.last_updated,
        }

    # ---- 辅助方法 ----

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "usable_lineages": sum(
                1 for edge in self._lineages.values()
                if self.is_lineage_edge_usable(edge)
            ),
            "revoked_lineages": sum(1 for edge in self._lineages.values() if edge.revoked),
            "metrics": len(self._metrics),
            "aggregation_receipts": len(self._aggregation_receipts),
            "materializations": len(self._materializations),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()